"""
kalshi/scraper.py — public REST scraper with rate limiter + circuit breaker.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum

import httpx

logger = logging.getLogger(__name__)
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


class RateLimiter:
    def __init__(self, rate: float = 10.0, burst: int = 20):
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self.rate)


class State(Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    failure_threshold: int   = 5
    recovery_timeout:  float = 30.0
    success_threshold: int   = 2
    _state:           State = field(default=State.CLOSED, init=False, repr=False)
    _failures:        int   = field(default=0,   init=False, repr=False)
    _successes:       int   = field(default=0,   init=False, repr=False)
    _last_failure_ts: float = field(default=0.0, init=False, repr=False)

    @property
    def state(self) -> State:
        if self._state == State.OPEN and time.monotonic() - self._last_failure_ts >= self.recovery_timeout:
            self._state = State.HALF_OPEN
            self._successes = 0
        return self._state

    def allow(self): return self.state != State.OPEN

    def success(self):
        if self._state == State.HALF_OPEN:
            self._successes += 1
            if self._successes >= self.success_threshold:
                self._state = State.CLOSED
                self._failures = 0
        elif self._state == State.CLOSED:
            self._failures = max(0, self._failures - 1)

    def failure(self):
        self._failures += 1
        self._last_failure_ts = time.monotonic()
        if self._failures >= self.failure_threshold:
            self._state = State.OPEN


class KalshiScraper:
    def __init__(self, rate: float = 10.0, burst: int = 20):
        self._rl      = RateLimiter(rate, burst)
        self._breaker = CircuitBreaker()
        self._http: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self._http = httpx.AsyncClient(base_url=BASE_URL, timeout=10.0)
        return self

    async def __aexit__(self, *_):
        if self._http:
            await self._http.aclose()

    async def _get(self, path: str, params: dict | None = None, _retries: int = 3) -> dict:
        if not self._breaker.allow():
            raise RuntimeError("[circuit OPEN]")
        await self._rl.acquire()
        try:
            resp = await self._http.get(path, params=params)
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 2.0))
                if _retries > 0:
                    logger.warning("[scraper] 429 — retrying in %.1fs", wait)
                    await asyncio.sleep(wait)
                    return await self._get(path, params, _retries - 1)
                raise RuntimeError(f"429 rate limit on {path}")
            resp.raise_for_status()
            self._breaker.success()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500:
                self._breaker.failure()
            logger.error("[scraper] HTTP %d — %s", e.response.status_code, path)
            raise
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            self._breaker.failure()
            logger.error("[scraper] network error: %s", e)
            raise

    async def get_market(self, ticker: str) -> dict:
        return await self._get(f"/markets/{ticker}")

    async def get_markets(self, status=None, limit=100, cursor=None) -> dict:
        params: dict = {"limit": limit}
        if status: params["status"] = status
        if cursor: params["cursor"] = cursor
        return await self._get("/markets", params)

    async def get_all_markets(self, **kwargs) -> list[dict]:
        resp = await self.get_markets(**kwargs)
        markets = resp.get("markets", [])
        logger.info("[scraper] fetched %d markets", len(markets))
        return markets

    async def get_orderbook(self, ticker: str, depth: int = 10) -> dict:
        return await self._get(f"/markets/{ticker}/orderbook", {"depth": depth})

    async def get_trades(self, ticker: str, limit: int = 50, cursor=None) -> dict:
        params: dict = {"ticker": ticker, "limit": limit}
        if cursor: params["cursor"] = cursor
        return await self._get("/markets/trades", params)

    @property
    def circuit_state(self): return self._breaker.state.value