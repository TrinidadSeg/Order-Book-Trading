"""
OrderBook — maintains full L2 order book state and saves every snapshot to disk.
"""

import csv
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Level:
    price: float   # dollars  e.g. 0.42
    size:  float   # contracts


@dataclass
class BookState:
    ticker:    str
    timestamp: float
    yes_bids:  list[Level]   # sorted best → worst
    no_bids:   list[Level]

    # ── Derived metrics ─────────────────────────────────────

    @property
    def yes_best_bid(self) -> float | None:
        return self.yes_bids[0].price if self.yes_bids else None

    @property
    def no_best_bid(self) -> float | None:
        return self.no_bids[0].price if self.no_bids else None

    @property
    def yes_ask(self) -> float | None:
        """Implied yes ask = 1 - best no bid"""
        return round(1.0 - self.no_bids[0].price, 6) if self.no_bids else None

    @property
    def no_ask(self) -> float | None:
        """Implied no ask = 1 - best yes bid"""
        return round(1.0 - self.yes_bids[0].price, 6) if self.yes_bids else None

    @property
    def mid(self) -> float | None:
        b, a = self.yes_best_bid, self.yes_ask
        if b is None or a is None:
            return None
        return round((b + a) / 2.0, 6)

    @property
    def spread(self) -> float | None:
        b, a = self.yes_best_bid, self.yes_ask
        if b is None or a is None:
            return None
        return round(a - b, 6)

    @property
    def yes_depth(self) -> float:
        return sum(lv.size for lv in self.yes_bids)

    @property
    def no_depth(self) -> float:
        return sum(lv.size for lv in self.no_bids)

    @property
    def total_depth(self) -> float:
        return self.yes_depth + self.no_depth

    @property
    def imbalance(self) -> float | None:
        """(yes_depth - no_depth) / total  →  -1.0 to +1.0"""
        t = self.total_depth
        return round((self.yes_depth - self.no_depth) / t, 6) if t else None

    @property
    def yes_depth_at(self) -> dict[float, float]:
        return {lv.price: lv.size for lv in self.yes_bids}

    @property
    def no_depth_at(self) -> dict[float, float]:
        return {lv.price: lv.size for lv in self.no_bids}


class OrderBook:
    def __init__(self, ticker: str):
        self.ticker  = ticker
        self._yes:   dict[float, float] = {}
        self._no:    dict[float, float] = {}
        self.updates = 0

    def apply_snapshot(self, msg: dict) -> BookState:
        self._yes = {float(p): float(s) for p, s in msg.get("yes_dollars", []) if float(s) > 0}
        self._no  = {float(p): float(s) for p, s in msg.get("no_dollars",  []) if float(s) > 0}
        self.updates += 1
        return self._build()

    def _build(self) -> BookState:
        return BookState(
            ticker    = self.ticker,
            timestamp = time.time(),
            yes_bids  = sorted([Level(p, s) for p, s in self._yes.items()], key=lambda l: -l.price),
            no_bids   = sorted([Level(p, s) for p, s in self._no.items()],  key=lambda l: -l.price),
        )


class OrderBookManager:
    """Routes snapshots to the right OrderBook and saves every row to disk."""

    def __init__(self, data_dir: str = "data", fmt: str = "jsonl"):
        self.data_dir = Path(data_dir)
        self.fmt      = fmt
        self._books:   dict[str, OrderBook] = {}
        self._buffers: dict[str, list[dict]] = {}
        self._flush_every = 2   # flush every 2 rows (≈2s at 1s poll)

    def on_snapshot(self, ticker: str, msg: dict) -> BookState:
        if ticker not in self._books:
            self._books[ticker] = OrderBook(ticker)
        state = self._books[ticker].apply_snapshot(msg)
        self._buffer(state)
        return state

    def flush_all(self) -> None:
        for ticker in list(self._buffers):
            self._flush(ticker)

    # ── Storage ─────────────────────────────────────────────

    def _buffer(self, state: BookState) -> None:
        row = self._flatten(state)
        self._buffers.setdefault(state.ticker, []).append(row)
        if len(self._buffers[state.ticker]) >= self._flush_every:
            self._flush(state.ticker)

    def _flush(self, ticker: str) -> None:
        rows = self._buffers.pop(ticker, [])
        if not rows:
            return
        date = rows[0]["date"]
        path = self.data_dir / date / f"{ticker}.{self.fmt}"
        path.parent.mkdir(parents=True, exist_ok=True)

        if self.fmt == "jsonl":
            with path.open("a") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")
        else:
            write_header = not path.exists()
            with path.open("a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                if write_header:
                    w.writeheader()
                w.writerows(rows)

        logger.debug("[storage] flushed %d rows → %s", len(rows), path)

    @staticmethod
    def _flatten(s: BookState) -> dict:
        dt  = datetime.fromtimestamp(s.timestamp, tz=timezone.utc)
        row = {
            "date":           dt.strftime("%Y-%m-%d"),
            "timestamp":      round(s.timestamp, 3),
            "datetime":       dt.isoformat(),
            "ticker":         s.ticker,
            # ── Derived ──────────────────────────────────
            "mid":            s.mid,
            "spread":         s.spread,
            "imbalance":      s.imbalance,
            "yes_best_bid":   s.yes_best_bid,
            "yes_ask":        s.yes_ask,
            "no_best_bid":    s.no_best_bid,
            "no_ask":         s.no_ask,
            "yes_depth":      s.yes_depth,
            "no_depth":       s.no_depth,
            "total_depth":    s.total_depth,
        }
        # Full yes ladder (up to 10 levels)
        for i, lv in enumerate(s.yes_bids[:10]):
            row[f"yes_bid_{i+1}_price"] = lv.price
            row[f"yes_bid_{i+1}_size"]  = lv.size
        # Full no ladder (up to 10 levels)
        for i, lv in enumerate(s.no_bids[:10]):
            row[f"no_bid_{i+1}_price"]  = lv.price
            row[f"no_bid_{i+1}_size"]   = lv.size
        return row