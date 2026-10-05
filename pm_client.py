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
        await self.aclose_trading()
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
    async def _trading_client(self):
        """Official Polymarket SDK client acting for the bot's Deposit Wallet.

        Polymarket only accepts API orders from Deposit Wallets now ("maker address not allowed,
        please use the deposit wallet flow" for older proxy wallets). The builder API key pays
        for gasless wallet transactions.
        """
        if self._clob is not None:
            return self._clob
        from polymarket import AsyncSecureClient, BuilderApiKey

        s = self.settings
        if not s.polymarket_private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required for trading and balance lookups")
        api_key = None
        if s.polymarket_builder_api_key:
            api_key = BuilderApiKey(key=s.polymarket_builder_api_key, secret=s.polymarket_builder_secret,
                                    passphrase=s.polymarket_builder_passphrase)
        async with self._clob_lock:
            if self._clob is None:
                self._clob = await AsyncSecureClient.create(private_key=s.polymarket_private_key,
                                                            wallet=s.polymarket_funder or None, api_key=api_key)
        return self._clob

    async def buy_fok(self, token_id: str, usdc: float, worst_price: float, tick_size: float = 0.01) -> dict[str, Any]:
        """Fill-or-kill market BUY spending `usdc` (excl. fees) at prices no worse than `worst_price`.

        Returns {"success", "status", "orderID", "makingAmount" ($ spent), "takingAmount" (shares)}.
        """
        client = await self._trading_client()
        resp = await client.place_market_order(asset_id=token_id, side="BUY", amount=round(usdc, 2),
                                               max_price=worst_price, order_type="FOK")
        if not resp.ok:
            return {"success": False, "status": resp.code, "error": resp.message}
        return {"success": True, "status": str(resp.status), "orderID": str(resp.order_id),
                "makingAmount": float(resp.making_amount), "takingAmount": float(resp.taking_amount)}

    async def get_usdc_balance(self) -> float:
        """Collateral (Polymarket USD) available to trade, in dollars."""
        client = await self._trading_client()
        result = await client.get_balance_allowance(asset_type="COLLATERAL")
        return result.balance / 1_000_000

    async def aclose_trading(self) -> None:
        if self._clob is not None:
            await self._clob.close()
            self._clob = None

    # --- portfolio (read-only) ---
    async def get_positions(self, address: str) -> list[dict[str, Any]]:
        resp = await self.http.get(f"{self.settings.data_api_base_url}/positions",
                                   params={"user": address, "sizeThreshold": 0.01, "limit": 500})
        resp.raise_for_status()
        rows = resp.json()
        return rows if isinstance(rows, list) else []

    async def get_portfolio(self) -> dict[str, Any] | None:
        """Real Polymarket account: USDC cash + open positions at current prices. None if no wallet configured."""
        address = self.settings.trading_address
        if not address:
            return None
        cash = None
        if self.settings.polymarket_private_key:
            try:
                cash = await self.get_usdc_balance()
            except Exception as exc:  # noqa: BLE001
                logger.warning("cash balance lookup failed: %s", exc)
        positions = []
        for row in await self.get_positions(address):
            positions.append({
                "title": row.get("title"),
                "slug": row.get("slug"),
                "outcome": row.get("outcome"),
                "shares": float(row.get("size") or 0),
                "avg_price": float(row.get("avgPrice") or 0),
                "cur_price": float(row.get("curPrice") or 0),
                "value": float(row.get("currentValue") or 0),
                "cost": float(row.get("initialValue") or 0),
                "pnl": float(row.get("cashPnl") or 0),
                "redeemable": bool(row.get("redeemable")),
                "end_date": row.get("endDate"),
            })
        positions.sort(key=lambda p: p["value"], reverse=True)
        positions_value = sum(p["value"] for p in positions)
        return {
            "address": address,
            "cash_usdc": cash,
            "positions_value_usdc": positions_value,
            "redeemable_usdc": sum(p["value"] for p in positions if p["redeemable"]),
            "total_usdc": (cash or 0.0) + positions_value,
            "positions": positions,
        }
