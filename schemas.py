from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: Literal["ok"]
    service: str
    timestamp: datetime


class BalanceResponse(BaseModel):
    mode: Literal["live", "paper"] = "live"
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
    mode: Literal["live", "paper"] = "live"
    positions: list[PositionResponse]
    total_unrealized_pnl: float
    total_notional: float
    updated_at: datetime


class PnlBucket(BaseModel):
    label: str
    realized_pnl: float


class PnlResponse(BaseModel):
    mode: Literal["live", "paper"] = "live"
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


class ArbSignalResponse(BaseModel):
    connected: bool
    score: float
    direction: str
    confidence: float
    signals: dict[str, float]
    timestamp: datetime


class BtcPricePoint(BaseModel):
    ts: int
    price: float


class ArbBtcPriceResponse(BaseModel):
    connected: bool
    latest_price: float
    change_90s_pct: float
    history: list[BtcPricePoint]


class ArbMarketResponse(BaseModel):
    id: str
    title: str
    yes_price: float
    no_price: float
    closes_at: datetime
    seconds_until_close: int
    liquidity: float
    recommended_side: Literal["YES", "NO"]
    market_price: float
    fair_value: float
    edge: float
    kelly_fraction: float
    recommended_size_usdc: float


class ArbMarketsResponse(BaseModel):
    markets: list[ArbMarketResponse]
    scan_diagnostics: dict[str, object] = Field(default_factory=dict)
    updated_at: datetime


class ArbTradeResponse(BaseModel):
    id: str
    timestamp: datetime
    market_id: str
    title: str
    side: Literal["YES", "NO"]
    entry_price: float
    expected_price: float
    size: float
    edge: float
    signal_score: float
    status: str
    pnl: float
    result: str
    warning: str | None = None
    closes_at: datetime
    resolved_at: datetime | None = None


class ArbTradesResponse(BaseModel):
    trades: list[ArbTradeResponse]
    updated_at: datetime


class ArbPerformanceResponse(BaseModel):
    running: bool
    execution_mode: Literal["live", "paper"]
    total_trades: int
    closed_trades: int
    win_rate: float
    avg_edge_captured: float
    total_pnl: float
    daily_pnl: float
    avg_time_to_profit_sec: float = 0.0
    alerts: list[str]
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
    mode: Literal["live", "paper"] = "live"
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
    execution_mode: Literal["live", "paper"]
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


class ArbControlResponse(BaseModel):
    running: bool
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
