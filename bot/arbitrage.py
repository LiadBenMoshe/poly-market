from __future__ import annotations

import json
import logging
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from bot.bybit_feed import BybitFeed
from bot.market_scanner import BtcMarket, EdgeResult, MarketScanner
from bot.paper import PaperTradingEngine
from bot.signal_engine import SignalEngine, SignalResult
from config import Settings
from polymarket.client import PolymarketClient
from schemas import OrderResponse, StrategyOrder


logger = logging.getLogger(__name__)
MAX_ARB_DECISIONS = 1000
RESOLVED_TRADE_RETENTION_DAYS = 7


class BtcArbitrageStrategy:
    def __init__(
        self,
        client: PolymarketClient,
        settings: Settings,
        *,
        paper_engine: PaperTradingEngine | None = None,
        stop_all_bots: Callable[[], None] | None = None,
    ) -> None:
        self.client = client
        self.settings = settings
        self.feed = BybitFeed(settings.bybit_ws_url)
        self.signal_engine = SignalEngine(self.feed)
        self.market_scanner = MarketScanner(client, settings)
        self.paper_engine = paper_engine
        self.stop_all_bots = stop_all_bots or (lambda: None)
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.state_path = Path("data") / "arb_state.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.last_signal: SignalResult | None = None
        self.last_market_edges: list[dict[str, Any]] = []
        self.last_scan_diagnostics: dict[str, Any] = {}
        self.alerts: deque[str] = deque(maxlen=20)

    async def start_feed(self) -> None:
        await self.feed.start()

    async def stop_feed(self) -> None:
        await self.feed.stop()

    def start(self) -> None:
        if not self.scheduler.running:
            self.scheduler.start()
        if not self.scheduler.get_job("btc-arb-loop"):
            self.scheduler.add_job(
                self.run_cycle,
                IntervalTrigger(seconds=self.settings.arb_scheduler_interval_seconds),
                id="btc-arb-loop",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        self._alert("BTC arbitrage strategy started.")

    def stop(self) -> None:
        if self.scheduler.get_job("btc-arb-loop"):
            self.scheduler.remove_job("btc-arb-loop")
        self._alert("BTC arbitrage strategy stopped.")

    def is_running(self) -> bool:
        return self.scheduler.running and self.scheduler.get_job("btc-arb-loop") is not None

    async def run_cycle(self) -> None:
        state = self._load_state()
        await self._reconcile_positions(state)

        if self._daily_pnl(state) <= -abs(self.settings.daily_loss_limit):
            self.stop()
            self.stop_all_bots()
            self._log_decision(state, "halt", {"reason": "Daily loss limit breached."})
            self._save_state(state)
            return

        if not self.feed.is_connected():
            self._log_decision(state, "skip", {"reason": "Bybit feed disconnected."})
            self._save_state(state)
            return

        signal = self.signal_engine.compute()
        self.last_signal = signal
        self._log_decision(state, "signal", {"signal": self._signal_payload(signal)})
        if abs(signal.score) < self.settings.min_signal_score:
            self._log_decision(state, "no_trade", {"reason": "Signal below threshold.", "score": signal.score})
            self._save_state(state)
            return

        balance = await self.client.get_balance()
        bankroll = (
            self.paper_engine.get_balance(
                signer_address=self.client.signer_address,
                trading_address=self.client.trading_address,
                funder_address=self.settings.polymarket_funder or None,
                markets={},
            ).buying_power
            if self.settings.dry_run and self.paper_engine is not None
            else float(balance["buying_power"])
        )

        markets = await self.market_scanner.find_tradeable_markets()
        self.last_scan_diagnostics = self.market_scanner.diagnostics()
        self.last_market_edges = [
            self._edge_payload(market, self.market_scanner.calculate_edge(market, signal, bankroll))
            for market in markets
        ]
        if not markets:
            self._log_decision(
                state,
                "no_trade",
                {
                    "reason": "No BTC 5-minute markets passed filters.",
                    "scan": self.last_scan_diagnostics,
                    "score": signal.score,
                },
            )
            self._save_state(state)
            return
        open_positions = [trade for trade in state["trades"] if trade["status"] == "open"]

        for market in markets:
            if market.seconds_until_close <= 30:
                self._log_decision(state, "skip_market", {"market_id": market.id, "reason": "Too close to resolution."})
                continue
            if self._window_trade_count(state, market.title) >= self.settings.max_trades_per_window:
                self._log_decision(state, "skip_market", {"market_id": market.id, "reason": "Window trade cap reached."})
                continue
            if self._in_cooldown(state, market.id):
                continue
            if len(open_positions) >= self.settings.max_open_positions:
                self._log_decision(state, "skip_market", {"market_id": market.id, "reason": "Max open positions reached."})
                break

            edge = self.market_scanner.calculate_edge(market, signal, bankroll)
            if not self._signal_allows_trade(edge.side, signal.score):
                self._log_decision(
                    state,
                    "skip_market",
                    {
                        "market_id": market.id,
                        "reason": "Signal outside historical win zone.",
                        "side": edge.side,
                        "score": signal.score,
                    },
                )
                continue
            if edge.edge < self.settings.min_edge or edge.recommended_size_usdc < self.settings.min_trade_usdc:
                self._log_decision(state, "skip_market", {"market_id": market.id, "reason": "Edge below threshold.", "edge": edge.edge})
                continue

            trade = await self._execute_trade(state, market, edge, signal)
            open_positions.append(trade)
            self._save_state(state)
            return

        self._save_state(state)

    def get_signal_snapshot(self) -> dict[str, Any]:
        signal = self.last_signal or self.signal_engine.compute()
        payload = self._signal_payload(signal)
        payload["connected"] = self.feed.is_connected()
        return payload

    def get_btc_price_snapshot(self) -> dict[str, Any]:
        history = self.feed.get_timed_price_history(90)
        latest = self.feed.get_latest_price()
        previous = history[0][1] if history else latest
        change_pct = ((latest - previous) / previous * 100) if previous else 0.0
        return {
            "connected": self.feed.is_connected(),
            "latest_price": round(latest, 2),
            "change_90s_pct": round(change_pct, 4),
            "history": [{"ts": ts, "price": price} for ts, price in history],
        }

    async def get_market_snapshot(self) -> dict[str, Any]:
        signal = self.last_signal or self.signal_engine.compute()
        balance = await self.client.get_balance()
        bankroll = (
            self.paper_engine.get_balance(
                signer_address=self.client.signer_address,
                trading_address=self.client.trading_address,
                funder_address=self.settings.polymarket_funder or None,
                markets={},
            ).buying_power
            if self.settings.dry_run and self.paper_engine is not None
            else float(balance["buying_power"])
        )
        markets = await self.market_scanner.find_tradeable_markets()
        self.last_scan_diagnostics = self.market_scanner.diagnostics()
        return {
            "markets": [self._edge_payload(market, self.market_scanner.calculate_edge(market, signal, bankroll)) for market in markets],
            "scan_diagnostics": self.last_scan_diagnostics,
        }

    def get_trades(self) -> list[dict[str, Any]]:
        state = self._load_state()
        return list(state["trades"])[-50:][::-1]

    def get_performance(self) -> dict[str, Any]:
        state = self._load_state()
        trades = state["trades"]
        closed = [trade for trade in trades if trade["status"] == "resolved"]
        wins = [trade for trade in closed if float(trade.get("pnl", 0.0)) > 0]
        take_profit_times = [float(trade.get("time_to_profit_sec") or 0.0) for trade in closed if trade.get("time_to_profit_sec")]
        avg_edge = sum(float(trade.get("edge", 0.0)) for trade in trades) / len(trades) if trades else 0.0
        total_pnl = sum(float(trade.get("pnl", 0.0)) for trade in trades)
        return {
            "running": self.is_running(),
            "execution_mode": "paper" if self.settings.dry_run else "live",
            "total_trades": len(trades),
            "closed_trades": len(closed),
            "win_rate": round((len(wins) / len(closed) * 100) if closed else 0.0, 2),
            "avg_edge_captured": round(avg_edge, 4),
            "total_pnl": round(total_pnl, 2),
            "daily_pnl": round(self._daily_pnl(state), 2),
            "avg_time_to_profit_sec": round(sum(take_profit_times) / len(take_profit_times), 2) if take_profit_times else 0.0,
            "alerts": list(self.alerts),
        }

    async def _execute_trade(self, state: dict[str, Any], market: BtcMarket, edge: EdgeResult, signal: SignalResult) -> dict[str, Any]:
        token_id = market.yes_token_id if edge.side == "YES" else market.no_token_id
        expected_price = min((market.yes_price if edge.side == "YES" else market.no_price) + 0.005, 0.99)
        strategy_order = StrategyOrder(
            market_id=market.id,
            token_id=token_id or "",
            side="BUY",
            outcome=edge.side,
            price=expected_price,
            size=edge.recommended_size_usdc,
            reason=f"BTC arbitrage signal {signal.direction} score {signal.score:.2f}",
        )

        fill_price = expected_price
        if self.settings.dry_run and self.paper_engine is not None:
            fill = self.paper_engine.execute_order(strategy_order, self._market_to_market_response(market))
        else:
            response = await self.client.place_order(
                token_id=token_id or "",
                side="BUY",
                price=expected_price,
                size=edge.recommended_size_usdc,
                order_type="limit",
            )
            fill_price = float(response.get("price") or expected_price)
            fill = OrderResponse(
                id=str(response.get("orderID") or response.get("id") or ""),
                market_id=market.id,
                token_id=token_id,
                title=market.title,
                side="BUY",
                outcome=edge.side,
                order_type="limit",
                size=edge.recommended_size_usdc,
                price=fill_price,
                status=str(response.get("status") or "submitted"),
                created_at=datetime.now(UTC),
            )

        warning = "slippage_warning" if abs(fill.price - expected_price) > 0.03 else ""
        trade = {
            "id": fill.id or f"arb-{datetime.now(UTC).timestamp()}",
            "timestamp": datetime.now(UTC).isoformat(),
            "market_id": market.id,
            "token_id": token_id,
            "title": market.title,
            "side": edge.side,
            "entry_price": fill.price,
            "expected_price": expected_price,
            "size": edge.recommended_size_usdc,
            "edge": edge.edge,
            "signal_score": signal.score,
            "status": "open",
            "pnl": 0.0,
            "result": "pending",
            "warning": warning,
            "closes_at": market.closes_at.isoformat(),
            "stop_loss_fraction": self.settings.arb_stop_loss_fraction,
            "stop_loss_trigger_price": round(fill.price * (1 - self.settings.arb_stop_loss_fraction), 4),
            "stop_loss_cutoff_seconds": self.settings.arb_stop_loss_cutoff_seconds,
        }
        state["trades"].append(trade)
        state["cooldowns"][market.id] = datetime.now(UTC).isoformat()
        self._alert(f"{'Paper' if self.settings.dry_run else 'Live'} BTC arb trade: {edge.side} {market.id} @ {fill.price:.3f} edge {edge.edge:.3f}")
        self._log_decision(state, "trade", {"trade": trade, "signal": self._signal_payload(signal)})
        return trade

    async def _reconcile_positions(self, state: dict[str, Any]) -> None:
        open_order_ids: set[str] = set()
        if not self.settings.dry_run:
            try:
                open_order_ids = {
                    str(order.get("id") or order.get("orderID") or "")
                    for order in await self.client.get_open_orders()
                }
            except Exception:  # noqa: BLE001
                open_order_ids = set()

        for trade in state["trades"]:
            if trade["status"] != "open":
                continue
            trade_time = datetime.fromisoformat(trade["timestamp"])
            order_id = str(trade.get("id") or "")
            if not self.settings.dry_run and order_id in open_order_ids and (datetime.now(UTC) - trade_time).total_seconds() > 90:
                await self.client.cancel_order(order_id)
                trade["status"] = "cancelled"
                trade["result"] = "cancelled"
                trade["resolved_at"] = datetime.now(UTC).isoformat()
                trade["warning"] = "auto_cancelled_after_90s"
                continue
            market = await self.client.get_market(trade["market_id"])
            current_price = self._current_outcome_price(market, str(trade["side"]))
            unrealized_pnl = self._unrealized_pnl(trade, current_price)
            if unrealized_pnl >= self.settings.take_profit_usdc:
                if self.settings.dry_run and self.paper_engine is not None:
                    total_pnl = self._close_paper_trade_batch(
                        state,
                        trade,
                        exit_price=current_price,
                        result="take_profit",
                        warning="take_profit_hit",
                        mark_time_to_profit=True,
                    )
                    self._alert(f"Paper take-profit exit: {trade['side']} {trade['market_id']} pnl ${total_pnl:.2f}")
                    continue
                token_id = str(trade.get("token_id") or "")
                if token_id:
                    exit_price = max(min(current_price - 0.005, 0.99), 0.01)
                    await self.client.place_order(
                        token_id=token_id,
                        side="SELL",
                        price=exit_price,
                        size=float(trade["size"]),
                        order_type="limit",
                    )
                    trade["status"] = "resolved"
                    trade["result"] = "take_profit_exit"
                    trade["pnl"] = round(unrealized_pnl, 2)
                    trade["resolved_at"] = datetime.now(UTC).isoformat()
                    trade["exit_price"] = round(exit_price, 4)
                    trade["warning"] = "take_profit_order_submitted"
                    trade["time_to_profit_sec"] = self._seconds_since_open(trade)
                    self._alert(f"Live take-profit exit submitted: {trade['side']} {trade['market_id']} pnl ${trade['pnl']:.2f}")
                    continue
            if self._should_trigger_stop_loss(trade, current_price):
                if self.settings.dry_run and self.paper_engine is not None:
                    total_pnl = self._close_paper_trade_batch(
                        state,
                        trade,
                        exit_price=current_price,
                        result="stop_loss",
                        warning="stop_loss_hit",
                    )
                    self._alert(f"Paper stop-loss exit: {trade['side']} {trade['market_id']} pnl ${total_pnl:.2f}")
                    continue
                token_id = str(trade.get("token_id") or "")
                if token_id:
                    exit_price = max(min(current_price - 0.005, 0.99), 0.01)
                    await self.client.place_order(
                        token_id=token_id,
                        side="SELL",
                        price=exit_price,
                        size=float(trade["size"]),
                        order_type="limit",
                    )
                    trade["status"] = "resolved"
                    trade["result"] = "stop_loss_exit"
                    trade["pnl"] = round(unrealized_pnl, 2)
                    trade["resolved_at"] = datetime.now(UTC).isoformat()
                    trade["exit_price"] = round(exit_price, 4)
                    trade["warning"] = "stop_loss_order_submitted"
                    self._alert(f"Live stop-loss exit submitted: {trade['side']} {trade['market_id']} pnl ${trade['pnl']:.2f}")
                    continue
            closes_at = datetime.fromisoformat(trade["closes_at"])
            if datetime.now(UTC) < closes_at + timedelta(seconds=5):
                continue
            resolved = self._resolve_market_result(market)
            if resolved is None:
                trade["warning"] = "resolution_pending"
                continue
            if self.settings.dry_run and self.paper_engine is not None:
                self._settle_paper_trade_batch(state, trade, winning_outcome=resolved)
                continue
            trade["status"] = "resolved"
            trade["result"] = "win" if resolved == trade["side"] else "loss"
            payout = trade["size"] / max(float(trade["entry_price"]), 1e-9) if trade["result"] == "win" else 0.0
            trade["pnl"] = round(payout - trade["size"], 2)
            trade["resolved_at"] = datetime.now(UTC).isoformat()

    def _resolve_market_result(self, market: dict[str, Any]) -> str | None:
        raw_prices = market.get("outcomePrices")
        prices = []
        if isinstance(raw_prices, str):
            try:
                prices = json.loads(raw_prices)
            except json.JSONDecodeError:
                prices = []
        elif isinstance(raw_prices, list):
            prices = raw_prices
        if len(prices) >= 2:
            yes_price = float(prices[0])
            no_price = float(prices[1])
            if yes_price >= 0.99:
                return "YES"
            if no_price >= 0.99:
                return "NO"
        return None

    @staticmethod
    def _current_outcome_price(market: dict[str, Any], outcome: str) -> float:
        raw_prices = market.get("outcomePrices")
        prices = []
        if isinstance(raw_prices, str):
            try:
                prices = json.loads(raw_prices)
            except json.JSONDecodeError:
                prices = []
        elif isinstance(raw_prices, list):
            prices = raw_prices
        yes_price = float(prices[0]) if len(prices) > 0 else float(market.get("bestAsk", 0.5) or 0.5)
        no_price = float(prices[1]) if len(prices) > 1 else max(0.0, 1 - yes_price)
        return no_price if outcome.upper() == "NO" else yes_price

    @staticmethod
    def _unrealized_pnl(trade: dict[str, Any], current_price: float) -> float:
        return round(BtcArbitrageStrategy._raw_unrealized_pnl(trade, current_price), 2)

    @staticmethod
    def _raw_unrealized_pnl(trade: dict[str, Any], current_price: float) -> float:
        entry_price = float(trade.get("entry_price") or 0.0)
        size = float(trade.get("size") or 0.0)
        shares = size / max(entry_price, 1e-9)
        return (shares * current_price) - size

    def _should_trigger_stop_loss(self, trade: dict[str, Any], current_price: float) -> bool:
        seconds_to_close = self._seconds_to_close(trade)
        cutoff_seconds = int(trade.get("stop_loss_cutoff_seconds") or self.settings.arb_stop_loss_cutoff_seconds)
        if seconds_to_close <= cutoff_seconds:
            return False
        trigger_price = float(
            trade.get("stop_loss_trigger_price")
            or max(0.0, float(trade.get("entry_price") or 0.0) * (1 - self.settings.arb_stop_loss_fraction))
        )
        return current_price <= trigger_price

    def _close_paper_trade_batch(
        self,
        state: dict[str, Any],
        trade: dict[str, Any],
        *,
        exit_price: float,
        result: str,
        warning: str,
        mark_time_to_profit: bool = False,
    ) -> float:
        if self.paper_engine is None:
            return 0.0
        matching = self._matching_open_paper_trades(state, trade)
        if not matching:
            return 0.0
        if not self.paper_engine.has_position(market_id=str(trade["market_id"]), outcome=str(trade["side"])):
            resolved_at = datetime.now(UTC).isoformat()
            for row in matching:
                row["status"] = "resolved"
                row["result"] = "paper_position_missing"
                row["pnl"] = 0.0
                row["resolved_at"] = resolved_at
                row["exit_price"] = round(exit_price, 4)
                row["warning"] = "paper_position_not_found"
            return 0.0
        total_pnl = self.paper_engine.close_position(
            market_id=trade["market_id"],
            outcome=trade["side"],
            exit_price=exit_price,
        )
        raw_pnls = [self._raw_unrealized_pnl(row, exit_price) for row in matching]
        rounded_pnls = self._round_batch_pnls(raw_pnls, total_pnl)
        resolved_at = datetime.now(UTC).isoformat()
        for index, row in enumerate(matching):
            row["status"] = "resolved"
            row["result"] = result
            row["pnl"] = rounded_pnls[index]
            row["resolved_at"] = resolved_at
            row["exit_price"] = round(exit_price, 4)
            row["warning"] = warning
            if result == "stop_loss":
                row["time_to_stop_sec"] = self._seconds_since_open(row)
            if mark_time_to_profit:
                row["time_to_profit_sec"] = self._seconds_since_open(row)
        return total_pnl

    def _settle_paper_trade_batch(self, state: dict[str, Any], trade: dict[str, Any], *, winning_outcome: str) -> None:
        if self.paper_engine is None:
            return
        matching = self._matching_open_paper_trades(state, trade)
        if not matching:
            return
        if not self.paper_engine.has_position(market_id=str(trade["market_id"]), outcome=str(trade["side"])):
            resolved_at = datetime.now(UTC).isoformat()
            for row in matching:
                row["status"] = "resolved"
                row["result"] = "paper_position_missing"
                row["pnl"] = 0.0
                row["resolved_at"] = resolved_at
                row["warning"] = "paper_position_not_found"
            return
        total_pnl = self.paper_engine.settle_position(
            market_id=trade["market_id"],
            outcome=trade["side"],
            winning_outcome=winning_outcome,
        )
        raw_pnls = [self._raw_settlement_pnl(row, winning_outcome) for row in matching]
        rounded_pnls = self._round_batch_pnls(raw_pnls, total_pnl)
        resolved_at = datetime.now(UTC).isoformat()
        for index, row in enumerate(matching):
            row["status"] = "resolved"
            row["result"] = "win" if winning_outcome == row["side"] else "loss"
            row["pnl"] = rounded_pnls[index]
            row["resolved_at"] = resolved_at

    @staticmethod
    def _matching_open_paper_trades(state: dict[str, Any], trade: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            row
            for row in state.get("trades", [])
            if row.get("status") == "open"
            and str(row.get("market_id")) == str(trade.get("market_id"))
            and str(row.get("side")) == str(trade.get("side"))
        ]

    @staticmethod
    def _raw_settlement_pnl(trade: dict[str, Any], winning_outcome: str) -> float:
        size = float(trade.get("size") or 0.0)
        if str(trade.get("side")).upper() != winning_outcome.upper():
            return -size
        entry_price = float(trade.get("entry_price") or 0.0)
        shares = size / max(entry_price, 1e-9)
        return shares - size

    @staticmethod
    def _round_batch_pnls(raw_pnls: list[float], total_pnl: float) -> list[float]:
        if not raw_pnls:
            return []
        rounded: list[float] = []
        running_total = 0.0
        for raw_value in raw_pnls[:-1]:
            pnl = round(raw_value, 2)
            rounded.append(pnl)
            running_total += pnl
        rounded.append(round(total_pnl - running_total, 2))
        return rounded

    def _log_decision(self, state: dict[str, Any], action: str, payload: dict[str, Any]) -> None:
        state["decisions"].append({"timestamp": datetime.now(UTC).isoformat(), "action": action, **payload})
        state["decisions"] = state["decisions"][-MAX_ARB_DECISIONS:]

    def _window_trade_count(self, state: dict[str, Any], market_title: str) -> int:
        window_key = self._window_key(market_title)
        count = 0
        for trade in state.get("trades", []):
            if self._window_key(str(trade.get("title") or "")) == window_key:
                count += 1
        return count

    def _signal_allows_trade(self, side: str, score: float) -> bool:
        if side == "YES":
            return self.settings.yes_signal_floor <= score <= self.settings.yes_win_zone_max_signal
        return self.settings.no_win_zone_floor_signal <= score <= -abs(self.settings.no_signal_floor)

    @staticmethod
    def _window_key(market_title: str) -> str:
        return market_title.strip().lower()

    def _in_cooldown(self, state: dict[str, Any], market_id: str) -> bool:
        raw = state["cooldowns"].get(market_id)
        if not raw:
            return False
        try:
            last = datetime.fromisoformat(raw)
        except ValueError:
            return False
        return (datetime.now(UTC) - last).total_seconds() < self.settings.trade_cooldown_sec

    def _signal_payload(self, signal: SignalResult) -> dict[str, Any]:
        return {
            "score": signal.score,
            "direction": signal.direction,
            "confidence": signal.confidence,
            "signals": signal.signals_dict,
            "timestamp": signal.timestamp.isoformat(),
        }

    def _edge_payload(self, market: BtcMarket, edge: EdgeResult) -> dict[str, Any]:
        return {
            "id": market.id,
            "title": market.title,
            "yes_price": market.yes_price,
            "no_price": market.no_price,
            "closes_at": market.closes_at.isoformat(),
            "seconds_until_close": market.seconds_until_close,
            "liquidity": market.liquidity,
            "recommended_side": edge.side,
            "market_price": edge.market_price,
            "fair_value": edge.fair_value,
            "edge": edge.edge,
            "kelly_fraction": edge.kelly_fraction,
            "recommended_size_usdc": edge.recommended_size_usdc,
        }

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"trades": [], "decisions": [], "cooldowns": {}}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"trades": [], "decisions": [], "cooldowns": {}}

    def _save_state(self, state: dict[str, Any]) -> None:
        self._prune_state(state)
        self.state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _prune_state(self, state: dict[str, Any]) -> None:
        cutoff = datetime.now(UTC) - timedelta(days=RESOLVED_TRADE_RETENTION_DAYS)
        pruned_trades: list[dict[str, Any]] = []
        for trade in state.get("trades", []):
            if trade.get("status") == "open":
                pruned_trades.append(trade)
                continue
            resolved_at = trade.get("resolved_at")
            if not resolved_at:
                pruned_trades.append(trade)
                continue
            try:
                resolved_dt = datetime.fromisoformat(str(resolved_at))
            except ValueError:
                pruned_trades.append(trade)
                continue
            if resolved_dt >= cutoff:
                pruned_trades.append(trade)
        state["trades"] = pruned_trades
        state["decisions"] = list(state.get("decisions", []))[-MAX_ARB_DECISIONS:]

    def _daily_pnl(self, state: dict[str, Any]) -> float:
        today = datetime.now(UTC).date()
        total = 0.0
        for trade in state["trades"]:
            resolved_at = trade.get("resolved_at")
            if not resolved_at:
                continue
            if datetime.fromisoformat(resolved_at).date() == today:
                total += float(trade.get("pnl", 0.0))
        return total

    @staticmethod
    def _seconds_since_open(trade: dict[str, Any]) -> int:
        try:
            opened_at = datetime.fromisoformat(str(trade.get("timestamp")))
        except ValueError:
            return 0
        return max(0, int((datetime.now(UTC) - opened_at).total_seconds()))

    @staticmethod
    def _seconds_to_close(trade: dict[str, Any]) -> int:
        try:
            closes_at = datetime.fromisoformat(str(trade.get("closes_at")))
        except ValueError:
            return 0
        return max(0, int((closes_at - datetime.now(UTC)).total_seconds()))

    def _alert(self, message: str) -> None:
        timestamped = f"{datetime.now(UTC).strftime('%H:%M:%S')} UTC | {message}"
        self.alerts.appendleft(timestamped)
        logger.info(timestamped)

    @staticmethod
    def _market_to_market_response(market: BtcMarket) -> Any:
        from schemas import MarketResponse

        return MarketResponse(
            market_id=market.id,
            title=market.title,
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
            yes_price=market.yes_price,
            no_price=market.no_price,
            volume=market.liquidity,
            liquidity=market.liquidity,
            end_date=market.closes_at,
            active=True,
        )
