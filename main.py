from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from bot.scheduler import TradingBotScheduler
from config import Settings, get_settings
from polymarket.client import PolymarketClient
from polymarket.markets import fetch_markets
from polymarket.orders import list_open_orders, submit_order
from polymarket.positions import fetch_pnl, fetch_positions
from schemas import (
    BalanceResponse,
    BotControlResponse,
    BotStatusResponse,
    CancelOrderResponse,
    HealthResponse,
    MarketsResponse,
    OrderActionResponse,
    OrdersResponse,
    PlaceOrderRequest,
    PnlResponse,
    PositionsResponse,
    StrategySelectionRequest,
    StrategySelectionResponse,
)


BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = BASE_DIR / "dashboard" / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    client = PolymarketClient(settings)
    bot = TradingBotScheduler(client, settings)
    app.state.settings = settings
    app.state.client = client
    app.state.bot = bot
    try:
        yield
    finally:
        bot.stop()
        await client.aclose()


app = FastAPI(title="Polymarket Bot", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_client() -> PolymarketClient:
    return app.state.client  # type: ignore[no-any-return]


def get_bot() -> TradingBotScheduler:
    return app.state.bot  # type: ignore[no-any-return]


def get_app_settings() -> Settings:
    return app.state.settings  # type: ignore[no-any-return]


@app.get("/", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_PATH)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="polymarket-bot", timestamp=datetime.now(UTC))


@app.get("/api/balance", response_model=BalanceResponse)
async def get_balance(client: PolymarketClient = Depends(get_client)) -> BalanceResponse:
    try:
        balance_data = await client.get_balance()
        positions = await fetch_positions(client)
        total_position_value = round(sum(position.notional_value for position in positions), 2)
        return BalanceResponse(
            **balance_data,
            total_position_value=total_position_value,
            account_equity=round(float(balance_data["free_collateral"]) + total_position_value, 2),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch balance: {exc}") from exc


@app.get("/api/positions", response_model=PositionsResponse)
async def get_positions(client: PolymarketClient = Depends(get_client)) -> PositionsResponse:
    try:
        positions = await fetch_positions(client)
        return PositionsResponse(
            positions=positions,
            total_unrealized_pnl=round(sum(position.unrealized_pnl for position in positions), 2),
            total_notional=round(sum(position.notional_value for position in positions), 2),
            updated_at=datetime.now(UTC),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch positions: {exc}") from exc


@app.get("/api/pnl", response_model=PnlResponse)
async def get_pnl(client: PolymarketClient = Depends(get_client)) -> PnlResponse:
    try:
        return await fetch_pnl(client)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch PnL: {exc}") from exc


@app.get("/api/markets", response_model=MarketsResponse)
async def get_markets(client: PolymarketClient = Depends(get_client)) -> MarketsResponse:
    try:
        markets = await fetch_markets(client)
        return MarketsResponse(markets=markets, updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch markets: {exc}") from exc


@app.get("/api/orders", response_model=OrdersResponse)
async def get_orders(client: PolymarketClient = Depends(get_client)) -> OrdersResponse:
    try:
        orders = await list_open_orders(client)
        return OrdersResponse(orders=orders, updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch orders: {exc}") from exc


@app.post("/api/order", response_model=OrderActionResponse)
async def create_order(
    payload: PlaceOrderRequest,
    client: PolymarketClient = Depends(get_client),
) -> OrderActionResponse:
    try:
        order = await submit_order(client, payload)
        return OrderActionResponse(order=order, message="Order submitted successfully.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to submit order: {exc}") from exc


@app.delete("/api/order/{order_id}", response_model=CancelOrderResponse)
async def delete_order(order_id: str, client: PolymarketClient = Depends(get_client)) -> CancelOrderResponse:
    try:
        result = await client.cancel_order(order_id)
        return CancelOrderResponse(id=order_id, result=result, message="Order cancelled successfully.")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to cancel order: {exc}") from exc


@app.get("/api/bot/status", response_model=BotStatusResponse)
async def bot_status(bot: TradingBotScheduler = Depends(get_bot)) -> BotStatusResponse:
    try:
        return BotStatusResponse(**bot.status())
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to fetch bot status: {exc}") from exc


@app.post("/api/bot/start", response_model=BotControlResponse)
async def start_bot(bot: TradingBotScheduler = Depends(get_bot)) -> BotControlResponse:
    try:
        bot.start()
        return BotControlResponse(running=True, message="Bot started.", updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to start bot: {exc}") from exc


@app.post("/api/bot/stop", response_model=BotControlResponse)
async def stop_bot(bot: TradingBotScheduler = Depends(get_bot)) -> BotControlResponse:
    try:
        bot.stop()
        return BotControlResponse(running=False, message="Bot stopped.", updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to stop bot: {exc}") from exc


@app.post("/api/bot/strategy", response_model=StrategySelectionResponse)
async def set_bot_strategy(
    payload: StrategySelectionRequest,
    bot: TradingBotScheduler = Depends(get_bot),
) -> StrategySelectionResponse:
    try:
        selected = bot.set_strategy(payload.strategy)
        status = bot.status()
        return StrategySelectionResponse(
            strategy=selected,
            available_strategies=status["available_strategies"],
            message=f"Strategy set to {selected}.",
            updated_at=datetime.now(UTC),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to update strategy: {exc}") from exc


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=settings.debug)
