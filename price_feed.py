"""Live BTC prices.

* ChainlinkTwapFeed - Polymarket's real-time data socket, topic crypto_prices_chainlink.
  Its values ARE the Chainlink BTC/USD TWAP-60s stream the markets resolve on: the official
  "price to beat" equals the value at the window's first second, and the settle value is the
  value at the window's last second.
* BybitSpotFeed - BTCUSDT perpetual mid price, used as the (unsmoothed) underlying price and for volatility.
  A basis term maps Bybit onto Chainlink's level: basis = TWAP_now - avg(Bybit over last 60s).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import httpx
import websockets

from model import realized_sigma_per_sec

logger = logging.getLogger(__name__)


class SecondSeries:
    """Last price per unix second, keeping `keep_seconds` of history."""

    def __init__(self, keep_seconds: int = 1800) -> None:
        self.keep_seconds = keep_seconds
        self.prices: dict[int, float] = {}

    def add(self, second: int, price: float) -> None:
        self.prices[second] = price
        if len(self.prices) > self.keep_seconds + 120:
            cutoff = max(self.prices) - self.keep_seconds
            self.prices = {s: p for s, p in self.prices.items() if s >= cutoff}

    def latest(self) -> tuple[int, float] | None:
        if not self.prices:
            return None
        second = max(self.prices)
        return second, self.prices[second]

    def at(self, second: int) -> float | None:
        return self.prices.get(second)

    def age_seconds(self, now: float | None = None) -> float:
        latest = self.latest()
        return (now or time.time()) - latest[0] if latest else float("inf")

    def average(self, start: int, end: int) -> tuple[float, int] | None:
        """Mean over seconds in (start, end], forward-filling gaps; returns (mean, real samples)."""
        if not self.prices or end <= start:
            return None
        before = [s for s in self.prices if s <= start + 1]
        last = self.prices[max(before)] if before else None
        total, count, real = 0.0, 0, 0
        for s in range(start + 1, end + 1):
            if s in self.prices:
                last = self.prices[s]
                real += 1
            if last is not None:
                total += last
                count += 1
        return (total / count, real) if count else None

    def sigma_per_sec(self, lookback_seconds: int = 900, step: int = 5) -> float:
        if not self.prices:
            return 0.0
        end = max(self.prices)
        samples = [self.prices[s] for s in range(end - lookback_seconds, end + 1, step) if s in self.prices]
        return realized_sigma_per_sec(samples, step)


class _WsFeed:
    name = "feed"

    def __init__(self, ws_url: str, keep_seconds: int = 1800) -> None:
        self.ws_url = ws_url
        self.series = SecondSeries(keep_seconds)
        self.last_message_at = 0.0     # any frame, incl. pongs: tells us the connection is alive
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name=self.name)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=None, open_timeout=10) as ws:
                    for message in self.subscribe_messages():
                        await ws.send(json.dumps(message))
                        await asyncio.sleep(0.3)
                    backoff = 1
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        async for raw in ws:
                            self.last_message_at = time.time()
                            try:
                                msg = json.loads(raw)
                            except (ValueError, TypeError):
                                continue  # PONG frames
                            if isinstance(msg, dict):
                                self.handle(msg)
                    finally:
                        pinger.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("%s disconnected: %s", self.name, exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _ping(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(5 if self.name == "chainlink" else 20)
            await ws.send(self.ping_message())

    def subscribe_messages(self) -> list[dict]:
        raise NotImplementedError

    def ping_message(self) -> str:
        raise NotImplementedError

    def handle(self, msg: dict) -> None:
        raise NotImplementedError


class ChainlinkTwapFeed(_WsFeed):
    name = "chainlink"

    def subscribe_messages(self) -> list[dict]:
        # The filtered subscription only returns a ~60s backfill; live updates need the
        # unfiltered "update" stream (all symbols), so both are sent as separate messages.
        return [
            {"action": "subscribe", "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": "btc/usd"})}]},
            {"action": "subscribe", "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "update", "filters": ""}]},
        ]

    def ping_message(self) -> str:
        return "PING"

    def handle(self, msg: dict) -> None:
        payload = msg.get("payload")
        if not isinstance(payload, dict):
            return
        if isinstance(payload.get("data"), list):
            rows = payload["data"]                      # btc-filtered backfill
        elif payload.get("symbol") == "btc/usd":
            rows = [payload]
        else:
            return
        for row in rows:
            if isinstance(row, dict) and "timestamp" in row and "value" in row:
                self.series.add(int(row["timestamp"]) // 1000, float(row["value"]))


class BybitSpotFeed(_WsFeed):
    """BTCUSDT perpetual mid price from the level-1 order book (updates every second; trades are sparser)."""

    name = "bybit"

    def __init__(self, ws_url: str, keep_seconds: int = 1800) -> None:
        super().__init__(ws_url, keep_seconds)
        self._bid = 0.0
        self._ask = 0.0

    def subscribe_messages(self) -> list[dict]:
        return [{"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}]

    def ping_message(self) -> str:
        return json.dumps({"op": "ping"})

    def current(self, now: float | None = None) -> tuple[int, float] | None:
        """Latest mid, carried forward to `now`: Bybit only pushes level-1 when a quote changes."""
        latest = self.series.latest()
        if latest is None:
            return None
        now_second = int(now or time.time())
        if now_second > latest[0]:
            self.series.add(now_second, latest[1])
        return now_second, latest[1]

    def handle(self, msg: dict) -> None:
        if msg.get("topic") != "orderbook.1.BTCUSDT":
            return
        data = msg.get("data") or {}
        if data.get("b"):
            self._bid = float(data["b"][0][0])
        if data.get("a"):
            self._ask = float(data["a"][0][0])
        if self._bid > 0 and self._ask > 0:
            self.series.add(int(msg["ts"]) // 1000, (self._bid + self._ask) / 2)


async def bybit_kline_sigma_per_sec(http: httpx.AsyncClient, base_url: str, minutes: int = 60) -> float:
    """Per-second vol implied by the last `minutes` 1-minute BTCUSDT closes."""
    resp = await http.get(f"{base_url}/v5/market/kline",
                          params={"category": "linear", "symbol": "BTCUSDT", "interval": "1", "limit": minutes + 1})
    resp.raise_for_status()
    rows = resp.json().get("result", {}).get("list", [])
    closes = [float(r[4]) for r in reversed(rows)]  # newest first from Bybit
    return realized_sigma_per_sec(closes, 60.0)

