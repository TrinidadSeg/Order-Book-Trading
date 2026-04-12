# Kalshi Order Book Dashboard

Real-time order book data pipeline and trading dashboard for [Kalshi](https://kalshi.com) prediction markets.

![Python](https://img.shields.io/badge/python-3.11+-blue) ![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green) ![License](https://img.shields.io/badge/license-MIT-lightgrey)

## What it does

- Polls the live Kalshi order book every second via public REST API
- Maintains a full L2 bid ladder (yes + no sides, up to 10 levels)
- Streams real trade flow with taker side classification
- Computes mid price, spread, imbalance, price velocity, and volume windows
- Tracks time to expiry with urgency signaling
- Serves everything through a FastAPI backend to a live browser dashboard

## Dashboard

![dashboard preview](docs/preview.png)

## Project structure

```
kalshi-flow/
├── kalshi/
│   ├── scraper.py      # REST scraper with rate limiter + circuit breaker
│   └── orderbook.py    # L2 order book state + disk writer
├── Main.py             # Data pipeline (order book + trades + signals)
├── server.py           # FastAPI — serves live data to the dashboard
├── dashboard.html      # Browser dashboard, no build step needed
├── .env.example        # Environment variable template
└── requirements.txt
```

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/kalshi-order-book.git
cd kalshi-order-book

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# Edit .env and set TICKER to any open Kalshi market
```

## Usage

Run both in separate terminals:

```bash
# Terminal 1 — data pipeline
python Main.py

# Terminal 2 — API server
uvicorn server:app --port 8000
```

Then open `dashboard.html` in your browser.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `TICKER` | *(required)* | Kalshi market ticker e.g. `KXHIGHNY-25JAN01-T60` |
| `DATA_DIR` | `data` | Directory where JSONL files are saved |
| `STORAGE_FMT` | `jsonl` | `jsonl` or `csv` |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING` |

## Signals computed

| Signal | Description |
|---|---|
| `mid` | (best_yes_bid + implied_yes_ask) / 2 |
| `spread` | yes_ask - yes_bid |
| `imbalance` | (yes_depth - no_depth) / total_depth |
| `price_velocity_10s` | mid price change over last 10s in cents |
| `volume_5m / 10m / 30m` | contracts traded in each window |
| `time_to_expiry_s` | seconds until market closes |

## Data saved

Every poll writes a row to `data/YYYY-MM-DD/{TICKER}.jsonl` with the full ladder, all derived metrics, and timestamps. Trades saved separately to `{TICKER}_trades.jsonl`.

## Roadmap

- [ ] VPIN (Volume-Synchronized Probability of Informed Trading)
- [ ] HMM regime detection
- [ ] Hawkes process trade arrival intensity
- [ ] Paper trading engine

## License

MIT