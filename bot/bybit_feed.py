from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import UTC, datetime
from typing import Any

import websockets


logger = logging.getLogger(__name__)


class BybitFeed:
    def __init__(self, ws_url: str = "wss://stream.bybit.com/v5/public/linear") -> None:
        self.ws_url = ws_url
        self._connected = False
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self._latest_price = 0.0
        self._last_price_timestamp: datetime | None = None
        self._price_history: deque[tuple[datetime, float]] = deque(maxlen=300)
        self._candles: deque[dict[str, Any]] = deque(maxlen=200)
        self._orderbook_bids: dict[float, float] = {}
        self._orderbook_asks: dict[float, float] = {}
        self._ws: Any = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self.connect(), name="bybit-feed")

    async def stop(self) -> None:
        self._stop_event.set()
        self._connected = False
        if self._ws is not None:
            await self._ws.close()
        if self._task:
            await asyncio.wait([self._task], timeout=5)

    async def connect(self) -> None:
        backoff = 1
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                    self._ws = ws
                    await self._subscribe(ws)
                    self._connected = True
                    backoff = 1
                    async for message in ws:
                        if self._stop_event.is_set():
                            break
                        self._handle_message(message)
            except Exception as exc:  # noqa: BLE001
                self._connected = False
                logger.warning("Bybit feed disconnected: %s", exc)
                if self._stop_event.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            finally:
                self._connected = False
                self._ws = None

    async def _subscribe(self, ws: Any) -> None:
        payload = {
            "op": "subscribe",
            "args": [
                "tickers.BTCUSDT",
                "orderbook.50.BTCUSDT",
                "kline.1.BTCUSDT",
            ],
        }
        async with self._send_lock:
            await ws.send(json.dumps(payload))

    def _handle_message(self, raw: str) -> None:
        message = json.loads(raw)
        topic = message.get("topic", "")
        data = message.get("data")
        if topic == "tickers.BTCUSDT" and data:
            self._handle_ticker(data)
        elif topic == "orderbook.50.BTCUSDT" and data:
            self._handle_orderbook(message.get("type", "snapshot"), data)
        elif topic == "kline.1.BTCUSDT" and data:
            self._handle_kline(data)

    def _handle_ticker(self, data: dict[str, Any]) -> None:
        price = float(data.get("lastPrice") or data.get("markPrice") or 0.0)
        if price <= 0:
            return
        self._latest_price = price
        now = datetime.now(UTC)
        if self._last_price_timestamp is None or (now - self._last_price_timestamp).total_seconds() >= 1:
            self._price_history.append((now, price))
            self._last_price_timestamp = now

    def _handle_orderbook(self, message_type: str, data: dict[str, Any]) -> None:
        if message_type == "snapshot":
            self._orderbook_bids = {float(price): float(size) for price, size in data.get("b", [])}
            self._orderbook_asks = {float(price): float(size) for price, size in data.get("a", [])}
            return

        for price, size in data.get("b", []):
            p = float(price)
            s = float(size)
            if s == 0:
                self._orderbook_bids.pop(p, None)
            else:
                self._orderbook_bids[p] = s
        for price, size in data.get("a", []):
            p = float(price)
            s = float(size)
            if s == 0:
                self._orderbook_asks.pop(p, None)
            else:
                self._orderbook_asks[p] = s

    def _handle_kline(self, data: list[dict[str, Any]]) -> None:
        for candle in data:
            row = {
                "start": int(candle.get("start") or 0),
                "end": int(candle.get("end") or 0),
                "open": float(candle.get("open") or 0.0),
                "high": float(candle.get("high") or 0.0),
                "low": float(candle.get("low") or 0.0),
                "close": float(candle.get("close") or 0.0),
                "volume": float(candle.get("volume") or 0.0),
                "confirm": bool(candle.get("confirm", False)),
            }
            if self._candles and self._candles[-1]["start"] == row["start"]:
                self._candles[-1] = row
            else:
                self._candles.append(row)

    def get_latest_price(self) -> float:
        return self._latest_price

    def get_price_history(self, seconds: int = 90) -> list[float]:
        now = datetime.now(UTC)
        return [price for ts, price in self._price_history if (now - ts).total_seconds() <= seconds]

    def get_timed_price_history(self, seconds: int = 90) -> list[tuple[int, float]]:
        now = datetime.now(UTC)
        return [
            (int(ts.timestamp() * 1000), price)
            for ts, price in self._price_history
            if (now - ts).total_seconds() <= seconds
        ]

    def get_candle_closes(self, limit: int = 50) -> list[float]:
        return [float(candle["close"]) for candle in list(self._candles)[-limit:]]

    def get_orderbook_imbalance(self) -> float:
        top_bids = sorted(self._orderbook_bids.items(), key=lambda item: item[0], reverse=True)[:10]
        top_asks = sorted(self._orderbook_asks.items(), key=lambda item: item[0])[:10]
        bid_volume = sum(size for _, size in top_bids)
        ask_volume = sum(size for _, size in top_asks)
        total = bid_volume + ask_volume
        if total <= 0:
            return 0.0
        return (bid_volume - ask_volume) / total

    def is_connected(self) -> bool:
        return self._connected
