from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from bot.arbitrage import BtcArbitrageStrategy
from bot.scheduler import TradingBotScheduler
from bot.whale_scanner import WhaleScanner
from config import Settings, get_settings
from polymarket.client import PolymarketClient
from polymarket.markets import fetch_markets
from polymarket.orders import list_open_orders, submit_order
from polymarket.positions import fetch_pnl, fetch_positions
from schemas import (
    ArbBtcPriceResponse,
    ArbControlResponse,
    ArbMarketResponse,
    ArbMarketsResponse,
    ArbPerformanceResponse,
    ArbSignalResponse,
    ArbTradeResponse,
    ArbTradesResponse,
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
    StrategyOrder,
    StrategySelectionRequest,
    StrategySelectionResponse,
)


BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = BASE_DIR / "dashboard" / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    client = PolymarketClient(settings)
    whale_scanner = WhaleScanner(client, settings)
    bot = TradingBotScheduler(client, settings, whale_scanner=whale_scanner)
    arb = BtcArbitrageStrategy(
        client,
        settings,
        whale_scanner=whale_scanner,
        paper_engine=bot.paper,
        stop_all_bots=lambda: bot.stop(),
    )
    app.state.settings = settings
    app.state.client = client
    app.state.whale_scanner = whale_scanner
    app.state.bot = bot
    app.state.arb = arb
    whale_scanner.start()
    await arb.start_feed()
    if settings.arbot_enabled:
        arb.start()
    try:
        yield
    finally:
        bot.stop()
        arb.stop()
        whale_scanner.stop()
        await arb.stop_feed()
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


def get_arb() -> BtcArbitrageStrategy:
    return app.state.arb  # type: ignore[no-any-return]


def get_whale_scanner() -> WhaleScanner:
    return app.state.whale_scanner  # type: ignore[no-any-return]


def get_app_settings() -> Settings:
    return app.state.settings  # type: ignore[no-any-return]


@app.get("/", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_PATH)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="polymarket-bot", timestamp=datetime.now(UTC))


@app.get("/api/balance", response_model=BalanceResponse)
async def get_balance(
    client: PolymarketClient = Depends(get_client),
    bot: TradingBotScheduler = Depends(get_bot),
    settings: Settings = Depends(get_app_settings),
) -> BalanceResponse:
    try:
        if settings.dry_run:
            live_balance = await client.get_balance()
            markets = await fetch_markets(client, limit=100)
            market_map = {market.market_id: market for market in markets}
            bot.paper.initialize(max(float(live_balance["buying_power"]), 100.0))
            return bot.paper.get_balance(
                signer_address=client.signer_address,
                trading_address=client.trading_address,
                funder_address=settings.polymarket_funder or None,
                markets=market_map,
            )
        balance_data = await client.get_balance()
        positions = await fetch_positions(client)
        total_position_value = round(sum(position.notional_value for position in positions), 2)
        return BalanceResponse(
            **balance_data,
            mode="live",
            total_position_value=total_position_value,
            account_equity=round(float(balance_data["free_collateral"]) + total_position_value, 2),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch balance: {exc}") from exc


@app.get("/api/positions", response_model=PositionsResponse)
async def get_positions(
    client: PolymarketClient = Depends(get_client),
    bot: TradingBotScheduler = Depends(get_bot),
    settings: Settings = Depends(get_app_settings),
) -> PositionsResponse:
    try:
        if settings.dry_run:
            markets = await fetch_markets(client, limit=100)
            market_map = {market.market_id: market for market in markets}
            positions = bot.paper.get_positions(market_map)
            return PositionsResponse(
                mode="paper",
                positions=positions,
                total_unrealized_pnl=round(sum(position.unrealized_pnl for position in positions), 2),
                total_notional=round(sum(position.notional_value for position in positions), 2),
                updated_at=datetime.now(UTC),
            )
        positions = await fetch_positions(client)
        return PositionsResponse(
            mode="live",
            positions=positions,
            total_unrealized_pnl=round(sum(position.unrealized_pnl for position in positions), 2),
            total_notional=round(sum(position.notional_value for position in positions), 2),
            updated_at=datetime.now(UTC),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch positions: {exc}") from exc


@app.get("/api/pnl", response_model=PnlResponse)
async def get_pnl(
    client: PolymarketClient = Depends(get_client),
    bot: TradingBotScheduler = Depends(get_bot),
    settings: Settings = Depends(get_app_settings),
) -> PnlResponse:
    try:
        if settings.dry_run:
            markets = await fetch_markets(client, limit=100)
            market_map = {market.market_id: market for market in markets}
            return bot.paper.get_pnl(market_map)
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
async def get_orders(
    client: PolymarketClient = Depends(get_client),
    bot: TradingBotScheduler = Depends(get_bot),
    settings: Settings = Depends(get_app_settings),
) -> OrdersResponse:
    try:
        if settings.dry_run:
            return OrdersResponse(mode="paper", orders=bot.paper.get_orders(), updated_at=datetime.now(UTC))
        orders = await list_open_orders(client)
        return OrdersResponse(mode="live", orders=orders, updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to fetch orders: {exc}") from exc


@app.post("/api/order", response_model=OrderActionResponse)
async def create_order(
    payload: PlaceOrderRequest,
    client: PolymarketClient = Depends(get_client),
    bot: TradingBotScheduler = Depends(get_bot),
    settings: Settings = Depends(get_app_settings),
) -> OrderActionResponse:
    try:
        if settings.dry_run:
            markets = await fetch_markets(client, limit=100)
            market_map = {market.market_id: market for market in markets}
            market = market_map.get(payload.market_id)
            if not market:
                raise ValueError(f"Market {payload.market_id} was not found.")
            token_id = market.yes_token_id if payload.side == "YES" else market.no_token_id
            if not token_id:
                raise ValueError(f"Market {payload.market_id} does not expose a tradable token for {payload.side}.")
            live_balance = await client.get_balance()
            bot.paper.initialize(max(float(live_balance["buying_power"]), 100.0))
            order = bot.paper.execute_order(
                StrategyOrder(
                    market_id=payload.market_id,
                    token_id=token_id,
                    side="BUY",
                    outcome=payload.side,
                    price=payload.price,
                    size=payload.size,
                    reason="Manual paper trade from dashboard.",
                ),
                market,
            )
            return OrderActionResponse(order=order, message="Paper order filled successfully.")
        order = await submit_order(client, payload)
        return OrderActionResponse(order=order, message="Order submitted successfully.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to submit order: {exc}") from exc


@app.delete("/api/order/{order_id}", response_model=CancelOrderResponse)
async def delete_order(
    order_id: str,
    client: PolymarketClient = Depends(get_client),
    settings: Settings = Depends(get_app_settings),
) -> CancelOrderResponse:
    try:
        if settings.dry_run:
            raise HTTPException(status_code=400, detail="Paper fills are immediate and cannot be cancelled.")
        result = await client.cancel_order(order_id)
        return CancelOrderResponse(id=order_id, result=result, message="Order cancelled successfully.")
    except HTTPException:
        raise
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


@app.get("/api/arb/signal", response_model=ArbSignalResponse)
async def arb_signal(arb: BtcArbitrageStrategy = Depends(get_arb)) -> ArbSignalResponse:
    try:
        return ArbSignalResponse(**arb.get_signal_snapshot())
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage signal: {exc}") from exc


@app.get("/api/arb/btc-price", response_model=ArbBtcPriceResponse)
async def arb_btc_price(arb: BtcArbitrageStrategy = Depends(get_arb)) -> ArbBtcPriceResponse:
    try:
        return ArbBtcPriceResponse(**arb.get_btc_price_snapshot())
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to fetch BTC price snapshot: {exc}") from exc


@app.get("/api/arb/markets", response_model=ArbMarketsResponse)
async def arb_markets(arb: BtcArbitrageStrategy = Depends(get_arb)) -> ArbMarketsResponse:
    try:
        snapshot = await arb.get_market_snapshot()
        return ArbMarketsResponse(
            markets=[ArbMarketResponse(**row) for row in snapshot["markets"]],
            scan_diagnostics=snapshot.get("scan_diagnostics", {}),
            updated_at=datetime.now(UTC),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage markets: {exc}") from exc


@app.get("/api/arb/trades", response_model=ArbTradesResponse)
async def arb_trades(arb: BtcArbitrageStrategy = Depends(get_arb)) -> ArbTradesResponse:
    try:
        rows = [ArbTradeResponse(**row) for row in arb.get_trades()]
        return ArbTradesResponse(trades=rows, updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage trades: {exc}") from exc


@app.get("/api/arb/performance", response_model=ArbPerformanceResponse)
async def arb_performance(arb: BtcArbitrageStrategy = Depends(get_arb)) -> ArbPerformanceResponse:
    try:
        return ArbPerformanceResponse(**arb.get_performance(), updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to fetch arbitrage performance: {exc}") from exc


@app.get("/api/whales")
async def whale_snapshot(whale_scanner: WhaleScanner = Depends(get_whale_scanner)) -> dict[str, object]:
    try:
        return whale_scanner.snapshot()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to fetch whale scanner snapshot: {exc}") from exc


@app.post("/api/arb/start", response_model=ArbControlResponse)
async def arb_start(arb: BtcArbitrageStrategy = Depends(get_arb)) -> ArbControlResponse:
    try:
        arb.start()
        return ArbControlResponse(running=True, message="BTC arbitrage started.", updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to start arbitrage bot: {exc}") from exc


@app.post("/api/arb/stop", response_model=ArbControlResponse)
async def arb_stop(arb: BtcArbitrageStrategy = Depends(get_arb)) -> ArbControlResponse:
    try:
        arb.stop()
        return ArbControlResponse(running=False, message="BTC arbitrage stopped.", updated_at=datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to stop arbitrage bot: {exc}") from exc


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=settings.debug)
