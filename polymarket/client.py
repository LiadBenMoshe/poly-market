from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    BookParams,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderType,
)
from py_clob_client.order_builder.constants import BUY, SELL

from config import Settings, get_settings
from polymarket.auth import WalletAuth, build_wallet_auth


logger = logging.getLogger(__name__)
USDC_DECIMALS = 1_000_000


class PolymarketClient:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        timeout = httpx.Timeout(self.settings.request_timeout_seconds)
        self._http = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "polymarket-bot/1.0"})
        self._sync_lock = asyncio.Lock()
        self._clob: ClobClient | None = None
        self._wallet: WalletAuth | None = None
        self._market_cache: tuple[datetime, list[dict[str, Any]]] | None = None

    async def aclose(self) -> None:
        await self._http.aclose()

    def _build_clob(self) -> ClobClient:
        if self._clob is not None:
            return self._clob

        if not self.settings.polymarket_private_key:
            self._clob = ClobClient(self.settings.clob_base_url)
            return self._clob

        self._wallet = build_wallet_auth(self.settings)
        client = ClobClient(
            self.settings.clob_base_url,
            key=self._wallet.private_key,
            chain_id=self._wallet.chain_id,
            signature_type=self._wallet.signature_type,
            funder=self._wallet.funder,
        )

        if self.settings.polymarket_api_key and self.settings.polymarket_api_secret and self.settings.polymarket_api_passphrase:
            client.set_api_creds(
                ApiCreds(
                    api_key=self.settings.polymarket_api_key,
                    api_secret=self.settings.polymarket_api_secret,
                    api_passphrase=self.settings.polymarket_api_passphrase,
                )
            )
        else:
            client.set_api_creds(client.create_or_derive_api_creds())

        self._clob = client
        return self._clob

    @property
    def signer_address(self) -> str:
        return self.settings.wallet_address or ""

    @property
    def trading_address(self) -> str:
        if self.settings.wallet_address:
            return self.settings.polymarket_funder or self.settings.wallet_address
        return ""

    @property
    def wallet_address(self) -> str:
        return self.trading_address

    async def _run_clob(self, func_name: str, *args: Any, **kwargs: Any) -> Any:
        async with self._sync_lock:
            client = self._build_clob()
            func = getattr(client, func_name)
            return await asyncio.to_thread(func, *args, **kwargs)

    async def get_balance(self) -> dict[str, Any]:
        balance = 0.0
        buying_power = 0.0
        warning: str | None = None
        if self.settings.polymarket_private_key:
            try:
                result = await self._run_clob(
                    "get_balance_allowance",
                    BalanceAllowanceParams(
                        asset_type=AssetType.COLLATERAL,
                        signature_type=self.settings.polymarket_signature_type,
                    ),
                )
                balance = self._normalize_usdc(result.get("balance", 0.0))
                buying_power = self._normalize_usdc(result.get("allowances", {}).get("default", balance))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Falling back to zero balance: %s", exc)
                warning = f"Authenticated collateral lookup failed: {exc}"
        return {
            "wallet_address": self.wallet_address,
            "signer_address": self.signer_address,
            "trading_address": self.trading_address,
            "funder_address": self.settings.polymarket_funder or None,
            "usdc_balance": balance,
            "free_collateral": balance,
            "buying_power": buying_power or balance,
            "warning": warning,
            "updated_at": datetime.now(UTC),
        }

    @staticmethod
    def _normalize_usdc(value: Any) -> float:
        amount = float(value or 0.0)
        return amount / USDC_DECIMALS if amount > 10_000 else amount

    async def get_active_markets(self, limit: int = 25) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        if self._market_cache and now - self._market_cache[0] < timedelta(seconds=20):
            return self._market_cache[1][:limit]
        response = await self._http.get(
            f"{self.settings.gamma_base_url}/markets",
            params={"active": "true", "closed": "false", "limit": limit},
        )
        response.raise_for_status()
        payload = response.json()
        markets = payload if isinstance(payload, list) else payload.get("data", [])
        self._market_cache = (now, markets)
        return markets[:limit]

    async def get_orderbook(self, token_id: str) -> dict[str, Any]:
        try:
            book = await self._run_clob("get_order_book", token_id)
            return book if isinstance(book, dict) else book.model_dump()
        except Exception:
            books = await self._run_clob("get_order_books", [BookParams(token_id=token_id)])
            first = books[0]
            return first if isinstance(first, dict) else first.model_dump()

    async def get_open_orders(self) -> list[dict[str, Any]]:
        if not self.settings.polymarket_private_key:
            return []
        try:
            orders = await self._run_clob("get_orders", OpenOrderParams())
            return list(orders)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch open orders: %s", exc)
            return []

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        if not self.settings.polymarket_private_key:
            raise RuntimeError("Trading credentials are not configured.")
        result = await self._run_clob("cancel", order_id)
        return result if isinstance(result, dict) else {"id": order_id, "status": "cancelled"}

    async def place_order(
        self,
        *,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type: str,
    ) -> dict[str, Any]:
        if not self.settings.polymarket_private_key:
            raise RuntimeError("Trading credentials are not configured.")
        sdk_side = BUY if side.upper() == "BUY" else SELL
        if order_type == "market":
            signed_order = await self._run_clob(
                "create_market_order",
                MarketOrderArgs(token_id=token_id, amount=size, price=price, side=sdk_side, order_type=OrderType.FOK),
            )
            clob_order_type = OrderType.FOK
        else:
            signed_order = await self._run_clob(
                "create_order",
                OrderArgs(token_id=token_id, price=price, size=size, side=sdk_side),
            )
            clob_order_type = OrderType.GTC
        response = await self._run_clob("post_order", signed_order, clob_order_type)
        return response if isinstance(response, dict) else {"status": "submitted"}

    async def get_positions(self) -> list[dict[str, Any]]:
        if not self.wallet_address:
            return []
        response = await self._http.get(
            f"{self.settings.data_api_base_url}/positions",
            params={"user": self.wallet_address, "sizeThreshold": 0.01},
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else payload.get("data", [])

    async def get_closed_positions(self) -> list[dict[str, Any]]:
        if not self.wallet_address:
            return []
        response = await self._http.get(
            f"{self.settings.data_api_base_url}/closed-positions",
            params={"user": self.wallet_address},
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else payload.get("data", [])

    async def get_trades(self) -> list[dict[str, Any]]:
        if not self.settings.polymarket_private_key:
            return []
        try:
            trades = await self._run_clob("get_trades")
            return list(trades)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch trades: %s", exc)
            return []

    @asynccontextmanager
    async def lifespan(self) -> Any:
        try:
            yield self
        finally:
            await self.aclose()
