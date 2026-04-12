"""
server.py — serves all live data to the dashboard.

GET /api/meta       — market context (title, expiry, rules, volume)
GET /api/book       — latest order book snapshot
GET /api/history    — last N book snapshots
GET /api/trades     — recent trades
GET /api/signals    — velocity, volume buckets, time to expiry
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

TICKER   = os.environ["TICKER"]
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))


def today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def read_json(path: Path) -> dict | list:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}

def latest_rows(filename: str, n: int = 120) -> list[dict]:
    path = DATA_DIR / today() / filename
    if not path.exists():
        return []
    try:
        lines = path.read_text().strip().splitlines()
        return [json.loads(l) for l in lines[-n:] if l.strip()]
    except Exception:
        return []


@app.get("/api/meta")
def get_meta():
    meta = read_json(DATA_DIR / f"{TICKER}_meta.json")
    if not meta:
        return {"ticker": TICKER, "title": TICKER}

    # Compute live time-to-expiry
    tte = None
    close_time = meta.get("close_time", "")
    if close_time:
        try:
            ct  = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            tte = max(0, int((ct - datetime.now(timezone.utc)).total_seconds()))
        except Exception:
            pass
    meta["time_to_expiry_s"] = tte
    return meta


@app.get("/api/book")
def get_book():
    rows = latest_rows(f"{TICKER}.jsonl", 1)
    return rows[-1] if rows else {"error": "no data yet"}


@app.get("/api/history")
def get_history(n: int = 120):
    return latest_rows(f"{TICKER}.jsonl", n)


@app.get("/api/trades")
def get_trades():
    cache = read_json(DATA_DIR / f"{TICKER}_trades_cache.json")
    if isinstance(cache, list):
        return cache
    return latest_rows(f"{TICKER}_trades.jsonl", 50)


@app.get("/api/signals")
def get_signals():
    sig = read_json(DATA_DIR / f"{TICKER}_signals.json")
    if not sig:
        return {"error": "no signals yet"}
    return sig