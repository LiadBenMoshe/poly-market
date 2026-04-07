from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: Literal["ok"]
    service: str
    timestamp: datetime


class BalanceResponse(BaseModel):
    wallet_address: str
    signer_address: str
    trading_address: str
    funder_address: str | None = None
    usdc_balance: float
    free_collateral: float
    buying_power: float
    total_position_value: float = 0.0
    account_equity: float = 0.0
    currency: str = "USDC"
    warning: str | None = None
    updated_at: datetime


class PositionResponse(BaseModel):
    market_id: str
    market_slug: str | None = None
    market_title: str
    outcome: Literal["YES", "NO"]
    size: float
    entry_price: float
    current_price: float
    notional_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    realized_pnl: float = 0.0
    updated_at: datetime


class PositionsResponse(BaseModel):
    positions: list[PositionResponse]
    total_unrealized_pnl: float
    total_notional: float
    updated_at: datetime


class PnlBucket(BaseModel):
    label: str
    realized_pnl: float


class PnlResponse(BaseModel):
    daily: list[PnlBucket]
    weekly: list[PnlBucket]
    total_realized_pnl: float
    updated_at: datetime


class MarketResponse(BaseModel):
    market_id: str
    question_id: str | None = None
    slug: str | None = None
    title: str
    yes_token_id: str | None = None
    no_token_id: str | None = None
    yes_price: float
    no_price: float
    volume: float
    liquidity: float | None = None
    end_date: datetime | None = None
    active: bool = True


class MarketsResponse(BaseModel):
    markets: list[MarketResponse]
    updated_at: datetime


class OrderResponse(BaseModel):
    id: str
    market_id: str
    token_id: str | None = None
    title: str | None = None
    side: Literal["BUY", "SELL"]
    outcome: Literal["YES", "NO"]
    order_type: str
    size: float
    price: float
    status: str
    created_at: datetime | None = None


class OrdersResponse(BaseModel):
    orders: list[OrderResponse]
    updated_at: datetime


class OrderActionResponse(BaseModel):
    message: str
    order: OrderResponse


class CancelOrderResponse(BaseModel):
    id: str
    result: dict[str, object]
    message: str


class PlaceOrderRequest(BaseModel):
    market_id: str = Field(..., min_length=1)
    side: Literal["YES", "NO"]
    size: float = Field(..., gt=0)
    price: float = Field(..., gt=0, lt=1)
    order_type: Literal["market", "limit"] = "limit"


class BotStatusResponse(BaseModel):
    running: bool
    strategy: str
    available_strategies: list[str]
    last_action: str
    next_run: datetime | None = None
    recent_actions: list[str]
    risk: dict[str, float]
    updated_at: datetime


class BotControlResponse(BaseModel):
    running: bool
    message: str
    updated_at: datetime


class StrategySelectionRequest(BaseModel):
    strategy: str = Field(..., min_length=1)


class StrategySelectionResponse(BaseModel):
    strategy: str
    available_strategies: list[str]
    message: str
    updated_at: datetime


class StrategyOrder(BaseModel):
    market_id: str
    token_id: str
    side: Literal["BUY", "SELL"]
    outcome: Literal["YES", "NO"]
    price: float
    size: float
    reason: str
