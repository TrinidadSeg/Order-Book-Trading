"""
main.py — polls orderbook + trades every 1s, computes trading signals, saves everything.
"""

import asyncio
import csv
import json
import logging
import os
import signal
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kalshi.scraper   import KalshiScraper
from kalshi.orderbook import OrderBookManager

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

TICKER   = os.environ["TICKER"]
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
FMT      = os.getenv("STORAGE_FMT", "jsonl")

# Rolling mid price history for velocity calc (last 60 samples)
mid_history: deque = deque(maxlen=60)
# Seen trade IDs to avoid double-writing
seen_trade_ids: set[str] = set()
# In-memory recent trades for signals (last 200)
recent_trades: deque = deque(maxlen=200)
# Market metadata cache
market_meta: dict = {}


# ── Fetch market metadata once on startup ───────────────

async def fetch_meta(scraper: KalshiScraper) -> dict:
    try:
        data   = await scraper.get_market(TICKER)
        market = data.get("market", data)
        meta   = {
            "ticker":        TICKER,
            "title":         market.get("title") or market.get("yes_sub_title") or TICKER,
            "subtitle":      market.get("yes_sub_title", ""),
            "rules":         market.get("rules_primary", ""),
            "close_time":    market.get("close_time", ""),
            "volume_24h":    float(market.get("volume_24h_fp") or market.get("volume_24h") or 0),
            "volume_total":  float(market.get("volume_fp") or market.get("volume") or 0),
            "open_interest": float(market.get("open_interest_fp") or 0),
            "last_price":    float(market.get("last_price_dollars") or 0),
            "fetched_at":    datetime.now(timezone.utc).isoformat(),
        }
        # Save to disk so server can read it
        path = DATA_DIR / f"{TICKER}_meta.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(meta, indent=2))
        logger.info("Market: %s  closes: %s  vol_24h: %.0f", meta["title"], meta["close_time"], meta["volume_24h"])
        return meta
    except Exception as e:
        logger.warning("Could not fetch market meta: %s", e)
        return {"ticker": TICKER, "title": TICKER}


# ── Compute signals from rolling state ──────────────────

def compute_signals(mid: float | None) -> dict:
    now = time.time()

    # Price velocity — mid change over last 10s and 30s
    vel_10s = vel_30s = None
    if mid is not None:
        mid_history.append((now, mid))
        pts_10 = [(t, m) for t, m in mid_history if now - t <= 10]
        pts_30 = [(t, m) for t, m in mid_history if now - t <= 30]
        if len(pts_10) >= 2:
            vel_10s = round((pts_10[-1][1] - pts_10[0][1]) * 100, 4)   # cents/10s
        if len(pts_30) >= 2:
            vel_30s = round((pts_30[-1][1] - pts_30[0][1]) * 100, 4)

    # Volume from trades in last 5 / 10 / 30 min
    def vol_in(seconds):
        cutoff = now - seconds
        return sum(float(t.get("count") or 0) for t in recent_trades if t.get("_ts", 0) >= cutoff)

    # Time to expiry
    tte_seconds = None
    close_time  = market_meta.get("close_time", "")
    if close_time:
        try:
            ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            tte_seconds = max(0, int((ct - datetime.now(timezone.utc)).total_seconds()))
        except Exception:
            pass

    return {
        "price_velocity_10s":  vel_10s,
        "price_velocity_30s":  vel_30s,
        "volume_5m":           round(vol_in(300),  2),
        "volume_10m":          round(vol_in(600),  2),
        "volume_30m":          round(vol_in(1800), 2),
        "time_to_expiry_s":    tte_seconds,
        "computed_at":         round(now, 3),
    }


# ── Trade writer ─────────────────────────────────────────

def process_trades(raw_trades: list[dict]) -> list[dict]:
    new = []
    now = time.time()
    for t in raw_trades:
        tid = t.get("trade_id") or t.get("id", "")
        if not tid or tid in seen_trade_ids:
            continue
        seen_trade_ids.add(tid)
        # Parse actual trade timestamp for accurate volume windows
        created = t.get("created_time", "")
        try:
            from datetime import timezone
            trade_ts = datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()
        except Exception:
            trade_ts = now
        row = {
            "trade_id":   tid,
            "timestamp":  created,
            "ticker":     t.get("ticker", TICKER),
            "taker_side": t.get("taker_side", ""),
            "yes_price":  float(t.get("yes_price_dollars") or t.get("yes_price") or 0),
            "no_price":   float(t.get("no_price_dollars")  or t.get("no_price")  or 0),
            "count":      float(t.get("count_fp") or t.get("count") or 0),
            "_ts":        trade_ts,
        }
        new.append(row)
        recent_trades.appendleft(row)
    return new


def save_trades(trades: list[dict]) -> None:
    if not trades:
        return
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = DATA_DIR / date / f"{TICKER}_trades.{FMT}"
    path.parent.mkdir(parents=True, exist_ok=True)
    clean = [{k: v for k, v in t.items() if k != "_ts"} for t in trades]
    if FMT == "jsonl":
        with path.open("a") as f:
            for row in clean:
                f.write(json.dumps(row) + "\n")
    else:
        write_header = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(clean[0].keys()))
            if write_header: w.writeheader()
            w.writerows(clean)


# ── Save signals ─────────────────────────────────────────

def save_signals(signals: dict) -> None:
    path = DATA_DIR / f"{TICKER}_signals.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(signals))


# ── Save recent trades cache for server ──────────────────

def save_trades_cache() -> None:
    path = DATA_DIR / f"{TICKER}_trades_cache.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{k: v for k, v in t.items() if k != "_ts"} for t in list(recent_trades)[:50]]
    path.write_text(json.dumps(rows))


# ── Main loop ────────────────────────────────────────────

async def main() -> None:
    global market_meta
    manager    = OrderBookManager(data_dir=str(DATA_DIR), fmt=FMT)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT,  lambda: stop_event.set())
    loop.add_signal_handler(signal.SIGTERM, lambda: stop_event.set())

    async with KalshiScraper() as scraper:
        # Fetch market metadata once on startup
        market_meta = await fetch_meta(scraper)

        logger.info("Polling %s every 1s — Ctrl-C to stop", TICKER)
        poll_count   = 0
        last_flush_t = time.time()

        while not stop_event.is_set():
            t_start = time.time()

            try:
                # Fetch orderbook and trades concurrently
                ob_data, tr_data = await asyncio.gather(
                    scraper.get_orderbook(TICKER, depth=10),
                    scraper.get_trades(TICKER, limit=50),
                    return_exceptions=True,
                )

                # ── Order book ───────────────────────────
                if not isinstance(ob_data, Exception):
                    ob    = ob_data.get("orderbook_fp", ob_data)
                    state = manager.on_snapshot(TICKER, ob)
                    signals = compute_signals(state.mid)
                    save_signals({**signals, "mid": state.mid, "spread": state.spread, "imbalance": state.imbalance})
                else:
                    state = None
                    signals = compute_signals(None)

                # ── Trades ───────────────────────────────
                if not isinstance(tr_data, Exception):
                    raw    = tr_data.get("trades", [])
                    new_tr = process_trades(raw)
                    save_trades(new_tr)
                    if new_tr:
                        save_trades_cache()

                poll_count += 1
                flush_age = time.time() - last_flush_t

                # Log line with everything
                mid_str   = f"{state.mid:.4f}"        if state and state.mid       is not None else "N/A"
                vel_str   = f"{signals['price_velocity_10s']:+.2f}¢" if signals['price_velocity_10s'] is not None else "N/A"
                vol5_str  = f"{signals['volume_5m']:.1f}"
                tte       = signals["time_to_expiry_s"]
                tte_str   = f"{tte//60}m{tte%60:02d}s" if tte is not None else "N/A"
                flush_str = f"{flush_age:.1f}s ago"

                logger.info(
                    "poll=%-4d  mid=%-8s  vel(10s)=%-8s  vol(5m)=%-6s  tte=%-10s  flushed=%s",
                    poll_count, mid_str, vel_str, vol5_str, tte_str, flush_str,
                )

                # Update flush timestamp when manager flushes
                if poll_count % 2 == 0:
                    last_flush_t = time.time()

            except Exception as e:
                logger.warning("poll error: %s", e)

            # Sleep for remainder of 1s
            elapsed = time.time() - t_start
            await asyncio.sleep(max(0, 1.0 - elapsed))

    manager.flush_all()
    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())