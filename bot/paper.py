from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from schemas import BalanceResponse, MarketResponse, OrderResponse, PnlBucket, PnlResponse, PositionResponse, StrategyOrder

MAX_PAPER_ORDERS = 100
MAX_PAPER_REALIZED_HISTORY = 1000


@dataclass(slots=True)
class PaperTradingEngine:
    state_path: Path

    def __post_init__(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

    def _empty_state(self) -> dict[str, Any]:
        return {
            "initialized": False,
            "cash": 0.0,
            "positions": {},
            "orders": [],
            "realized_history": [],
            "updated_at": datetime.now(UTC).isoformat(),
        }

    def _load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._empty_state()
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return self._empty_state()

    def _save(self, state: dict[str, Any]) -> None:
        self._prune_state(state)
        state["updated_at"] = datetime.now(UTC).isoformat()
        self.state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def initialize(self, initial_cash: float) -> None:
        state = self._load()
        if state.get("initialized"):
            return
        state["initialized"] = True
        state["cash"] = round(max(initial_cash, 0.0), 2)
        self._save(state)

    def execute_order(self, order: StrategyOrder, market: MarketResponse) -> OrderResponse:
        state = self._load()
        cost = round(order.size, 2)
        if state["cash"] < cost:
            raise ValueError("Paper portfolio has insufficient cash for this trade.")

        key = self._position_key(order.market_id, order.outcome)
        position = state["positions"].get(
            key,
            {
                "market_id": order.market_id,
                "market_slug": market.slug,
                "market_title": market.title,
                "outcome": order.outcome,
                "cost_basis": 0.0,
                "shares": 0.0,
                "entry_price": 0.0,
                "realized_pnl": 0.0,
            },
        )

        shares = cost / max(order.price, 1e-9)
        total_cost_basis = float(position["cost_basis"]) + cost
        total_shares = float(position["shares"]) + shares
        avg_entry = total_cost_basis / max(total_shares, 1e-9)

        position["cost_basis"] = round(total_cost_basis, 4)
        position["shares"] = round(total_shares, 8)
        position["entry_price"] = round(avg_entry, 6)
        state["positions"][key] = position
        state["cash"] = round(float(state["cash"]) - cost, 2)

        created_at = datetime.now(UTC)
        order_row = {
            "id": f"paper-{uuid4().hex[:12]}",
            "market_id": order.market_id,
            "token_id": order.token_id,
            "title": market.title,
            "side": order.side,
            "outcome": order.outcome,
            "order_type": "paper-fill",
            "size": cost,
            "price": order.price,
            "status": "filled",
            "created_at": created_at.isoformat(),
        }
        state["orders"].insert(0, order_row)
        state["orders"] = state["orders"][:MAX_PAPER_ORDERS]
        self._save(state)

        return OrderResponse(
            id=order_row["id"],
            market_id=order.market_id,
            token_id=order.token_id,
            title=market.title,
            side=order.side,
            outcome=order.outcome,
            order_type="paper-fill",
            size=cost,
            price=order.price,
            status="filled",
            created_at=created_at,
        )

    def get_balance(
        self,
        *,
        signer_address: str,
        trading_address: str,
        funder_address: str | None,
        markets: dict[str, MarketResponse],
    ) -> BalanceResponse:
        state = self._load()
        positions = self._position_rows(state, markets)
        total_position_value = round(sum(position.notional_value for position in positions), 2)
        cash = round(float(state["cash"]), 2)
        return BalanceResponse(
            mode="paper",
            wallet_address=trading_address,
            signer_address=signer_address,
            trading_address=trading_address,
            funder_address=funder_address,
            usdc_balance=cash,
            free_collateral=cash,
            buying_power=cash,
            total_position_value=total_position_value,
            account_equity=round(cash + total_position_value, 2),
            warning="Paper trading mode is active. Balances and positions are simulated.",
            updated_at=self._state_updated_at(state),
        )

    def get_positions(self, markets: dict[str, MarketResponse]) -> list[PositionResponse]:
        state = self._load()
        return self._position_rows(state, markets)

    def has_position(self, *, market_id: str, outcome: str) -> bool:
        state = self._load()
        return self._position_key(market_id, outcome) in state["positions"]

    def get_orders(self) -> list[OrderResponse]:
        state = self._load()
        orders: list[OrderResponse] = []
        for row in state["orders"]:
            created_at = self._parse_dt(row.get("created_at"))
            orders.append(
                OrderResponse(
                    id=str(row["id"]),
                    market_id=str(row["market_id"]),
                    token_id=row.get("token_id"),
                    title=row.get("title"),
                    side=str(row["side"]).upper(),
                    outcome=str(row["outcome"]).upper(),
                    order_type=str(row.get("order_type") or "paper-fill"),
                    size=float(row.get("size") or 0.0),
                    price=float(row.get("price") or 0.0),
                    status=str(row.get("status") or "filled"),
                    created_at=created_at,
                )
            )
        return orders

    def get_pnl(self, markets: dict[str, MarketResponse]) -> PnlResponse:
        state = self._load()
        daily_map: dict[str, float] = defaultdict(float)
        weekly_map: dict[str, float] = defaultdict(float)
        for row in state["realized_history"]:
            when = self._parse_dt(row.get("timestamp")) or datetime.now(UTC)
            realized = float(row.get("realized_pnl") or 0.0)
            daily_map[when.strftime("%Y-%m-%d")] += realized
            weekly_map[f"{when.strftime('%Y')}-W{when.isocalendar().week:02d}"] += realized

        daily = [PnlBucket(label=key, realized_pnl=value) for key, value in sorted(daily_map.items())[-7:]]
        weekly = [PnlBucket(label=key, realized_pnl=value) for key, value in sorted(weekly_map.items())[-8:]]
        total = round(sum(bucket.realized_pnl for bucket in daily), 2) if daily else round(sum(daily_map.values()), 2)
        return PnlResponse(
            mode="paper",
            daily=daily,
            weekly=weekly,
            total_realized_pnl=total,
            updated_at=self._state_updated_at(state),
        )

    def settle_position(self, *, market_id: str, outcome: str, winning_outcome: str) -> float:
        state = self._load()
        key = self._position_key(market_id, outcome)
        position = state["positions"].get(key)
        if not position:
            return 0.0

        cost_basis = float(position.get("cost_basis") or 0.0)
        shares = float(position.get("shares") or 0.0)
        payout = shares if outcome.upper() == winning_outcome.upper() else 0.0
        pnl = round(payout - cost_basis, 2)
        state["cash"] = round(float(state["cash"]) + payout, 2)
        state["realized_history"].append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "realized_pnl": pnl,
                "market_id": market_id,
                "outcome": outcome,
            }
        )
        del state["positions"][key]
        self._save(state)
        return pnl

    def close_position(self, *, market_id: str, outcome: str, exit_price: float) -> float:
        state = self._load()
        key = self._position_key(market_id, outcome)
        position = state["positions"].get(key)
        if not position:
            return 0.0

        cost_basis = float(position.get("cost_basis") or 0.0)
        shares = float(position.get("shares") or 0.0)
        proceeds = round(shares * max(exit_price, 0.0), 2)
        pnl = round(proceeds - cost_basis, 2)
        state["cash"] = round(float(state["cash"]) + proceeds, 2)
        state["realized_history"].append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "realized_pnl": pnl,
                "market_id": market_id,
                "outcome": outcome,
                "exit_price": round(exit_price, 4),
                "type": "take_profit",
            }
        )
        del state["positions"][key]
        self._save(state)
        return pnl

    def _position_rows(self, state: dict[str, Any], markets: dict[str, MarketResponse]) -> list[PositionResponse]:
        positions: list[PositionResponse] = []
        now = self._state_updated_at(state)
        for row in state["positions"].values():
            market = markets.get(str(row["market_id"]))
            current_price = self._current_price(market, str(row["outcome"]))
            shares = float(row.get("shares") or 0.0)
            cost_basis = float(row.get("cost_basis") or 0.0)
            current_value = shares * current_price
            unrealized = current_value - cost_basis
            positions.append(
                PositionResponse(
                    market_id=str(row["market_id"]),
                    market_slug=row.get("market_slug"),
                    market_title=str(row.get("market_title") or "Untitled market"),
                    outcome="NO" if str(row["outcome"]).upper() == "NO" else "YES",
                    size=round(cost_basis, 2),
                    entry_price=float(row.get("entry_price") or 0.0),
                    current_price=current_price,
                    notional_value=round(current_value, 2),
                    unrealized_pnl=round(unrealized, 2),
                    unrealized_pnl_pct=round((unrealized / max(cost_basis, 1e-9)) * 100, 2),
                    realized_pnl=float(row.get("realized_pnl") or 0.0),
                    updated_at=now,
                )
            )
        return positions

    @staticmethod
    def _position_key(market_id: str, outcome: str) -> str:
        return f"{market_id}:{outcome.upper()}"

    @staticmethod
    def _current_price(market: MarketResponse | None, outcome: str) -> float:
        if market is None:
            return 0.0
        return float(market.no_price if outcome.upper() == "NO" else market.yes_price)

    @staticmethod
    def _parse_dt(raw: object) -> datetime | None:
        if not raw:
            return None
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None

    def _state_updated_at(self, state: dict[str, Any]) -> datetime:
        return self._parse_dt(state.get("updated_at")) or datetime.now(UTC)

    @staticmethod
    def _prune_state(state: dict[str, Any]) -> None:
        state["orders"] = list(state.get("orders", []))[:MAX_PAPER_ORDERS]
        state["realized_history"] = list(state.get("realized_history", []))[-MAX_PAPER_REALIZED_HISTORY:]
