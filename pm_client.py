"""Polymarket access for BTC up/down markets: discovery, strike, order book, resolution, orders."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from config import Settings
from fees import FeeSchedule

logger = logging.getLogger(__name__)

TIMEFRAME_SECONDS = {"5m": 300, "15m": 900}
CRYPTO_PRICE_VARIANT = {"5m": "fiveminute", "15m": "fifteen"}


@dataclass(slots=True)
class UpDownMarket:
    slug: str
    timeframe: str
    start: int                    # unix seconds, window open
    end: int                      # unix seconds, window close
    tokens: dict[str, str]        # "Up"/"Down" -> CLOB token id
    fee_schedule: FeeSchedule
    twap_seconds: int
    tick_size: float
    min_order_size: float
    accepting_orders: bool
    closed: bool
    outcome_prices: dict[str, float]

    @property
    def winning_side(self) -> str | None:
        """Set only once Polymarket has resolved the market (one outcome at 1, the other at 0)."""
        if not self.closed:
            return None
        for side, price in self.outcome_prices.items():
            if price >= 0.999:
                return side
        return None


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_list(value: Any) -> list:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return []
    return list(value or [])


def parse_market(row: dict[str, Any], timeframe: str, start: int, fallback_fee_rate: float) -> UpDownMarket:
    outcomes = [str(o) for o in _json_list(row.get("outcomes"))]
    token_ids = [str(t) for t in _json_list(row.get("clobTokenIds"))]
    prices = [float(p) for p in _json_list(row.get("outcomePrices"))]
    config = row.get("cryptoMarketConfig") or {}
    twap = int(config.get("twapLookbackSeconds") or 0) if config.get("twapEnabled", True) else 0
    end = int(datetime.fromisoformat(str(row["endDate"]).replace("Z", "+00:00")).timestamp())
    return UpDownMarket(
        slug=str(row.get("slug")),
        timeframe=timeframe,
        start=start,
        end=end,
        tokens=dict(zip(outcomes, token_ids)),
        fee_schedule=FeeSchedule.from_market(row, fallback_fee_rate),
        twap_seconds=twap if config else 60,
        tick_size=float(row.get("orderPriceMinTickSize") or 0.01),
        min_order_size=float(row.get("orderMinSize") or 5),
        accepting_orders=bool(row.get("acceptingOrders", True)),
        closed=bool(row.get("closed")),
        outcome_prices=dict(zip(outcomes, prices)),
    )


class PolymarketClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.http = httpx.AsyncClient(timeout=settings.request_timeout_seconds,
                                      headers={"User-Agent": "Mozilla/5.0 polymarket-late-favourite"})
        self._clob = None
        self._clob_lock = asyncio.Lock()
        self._strike_cache: dict[tuple[str, int], float] = {}
        self._strike_retry_at: dict[tuple[str, int], float] = {}

    async def aclose(self) -> None:
        await self.http.aclose()

    # --- market data (public) ---
    async def get_market(self, timeframe: str, start: int) -> UpDownMarket | None:
        slug = f"btc-updown-{timeframe}-{start}"
        resp = await self.http.get(f"{self.settings.gamma_base_url}/markets/slug/{slug}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return parse_market(resp.json(), timeframe, start, self.settings.fallback_fee_rate)

    async def get_crypto_price(self, timeframe: str, start: int) -> dict[str, Any]:
        """Polymarket's own open ("price to beat") and close for a window, from Chainlink."""
        resp = await self.http.get(self.settings.crypto_price_url, params={
            "symbol": "BTC",
            "eventStartTime": _iso(start),
            "variant": CRYPTO_PRICE_VARIANT[timeframe],
            "endDate": _iso(start + TIMEFRAME_SECONDS[timeframe]),
        })
        resp.raise_for_status()
        return resp.json()

    async def get_strike(self, timeframe: str, start: int) -> float | None:
        """Official price to beat. Returns None (try again later) on rate limits or before it is published."""
        key = (timeframe, start)
        if key in self._strike_cache:
            return self._strike_cache[key]
        if time.time() < self._strike_retry_at.get(key, 0):
            return None
        try:
            data = await self.get_crypto_price(timeframe, start)
        except httpx.HTTPError as exc:
            logger.warning("price-to-beat lookup failed for %s %s: %s", timeframe, start, exc)
            self._strike_retry_at[key] = time.time() + 10
            return None
        if data.get("openPrice") is None:
            self._strike_retry_at[key] = time.time() + 5
            return None
        self._strike_cache[key] = float(data["openPrice"])
        return self._strike_cache[key]

    async def get_asks(self, token_id: str) -> list[tuple[float, float]]:
        resp = await self.http.get(f"{self.settings.clob_base_url}/book", params={"token_id": token_id})
        resp.raise_for_status()
        return sorted((float(a["price"]), float(a["size"])) for a in resp.json().get("asks") or [])

    # --- trading (authenticated) ---
    def _build_clob(self):
        if self._clob is not None:
            return self._clob
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        s = self.settings
        if not s.polymarket_private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required when DRY_RUN=false")
        client = ClobClient(s.clob_base_url, key=s.polymarket_private_key, chain_id=s.chain_id,
                            signature_type=s.polymarket_signature_type, funder=s.polymarket_funder or None)
        if s.polymarket_api_key and s.polymarket_api_secret and s.polymarket_api_passphrase:
            client.set_api_creds(ApiCreds(api_key=s.polymarket_api_key, api_secret=s.polymarket_api_secret,
                                          api_passphrase=s.polymarket_api_passphrase))
        else:
            client.set_api_creds(client.create_or_derive_api_creds())
        self._clob = client
        return client

    async def _run_clob(self, func_name: str, *args: Any) -> Any:
        async with self._clob_lock:
            client = self._build_clob()
            return await asyncio.to_thread(getattr(client, func_name), *args)

    async def buy_fok(self, token_id: str, usdc: float, worst_price: float) -> dict[str, Any]:
        """Fill-or-kill market BUY spending `usdc` (excl. fees) at prices no worse than `worst_price`."""
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        args = MarketOrderArgs(token_id=token_id, amount=round(usdc, 2), side=BUY,
                               price=worst_price, order_type=OrderType.FOK)
        signed = await self._run_clob("create_market_order", args)
        resp = await self._run_clob("post_order", signed, OrderType.FOK)
        return resp if isinstance(resp, dict) else {"raw": str(resp)}

    async def get_usdc_balance(self) -> float:
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

        result = await self._run_clob("get_balance_allowance", BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL, signature_type=self.settings.polymarket_signature_type))
        return float(result.get("balance", 0)) / 1_000_000
