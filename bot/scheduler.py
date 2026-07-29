from __future__ import annotations

import logging
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from bot.paper import PaperTradingEngine
from bot.risk import RiskLimits, RiskManager
from bot.strategy import BaseStrategy, MeanReversionStrategy, RelatedMarketArbitrageStrategy, WhaleFollowStrategy
from bot.whale_scanner import WhaleScanner
from config import Settings
from polymarket.client import PolymarketClient
from polymarket.markets import fetch_markets
from polymarket.positions import fetch_positions


logger = logging.getLogger(__name__)


class TradingBotScheduler:
    def __init__(self, client: PolymarketClient, settings: Settings, whale_scanner: WhaleScanner | None = None) -> None:
        self.client = client
        self.settings = settings
        self.whale_scanner = whale_scanner
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.strategy_factories = {
            "related_market_arbitrage": lambda: RelatedMarketArbitrageStrategy(
                volume_threshold=settings.min_market_volume,
                max_trade_fraction=settings.max_trade_fraction,
            ),
            "mean_reversion": lambda: MeanReversionStrategy(
                volume_threshold=settings.min_market_volume,
                max_trade_fraction=settings.max_trade_fraction,
            ),
            "whale_following": lambda: WhaleFollowStrategy(
                whale_scanner=self.whale_scanner,
                min_score=settings.whale_min_conviction_score,
                fallback_max_trade_fraction=settings.max_trade_fraction,
            ),
        }
        self.strategy: BaseStrategy = self.strategy_factories["related_market_arbitrage"]()
        self.risk_manager = RiskManager(
            RiskLimits(
                max_total_exposure_fraction=settings.max_total_exposure_fraction,
                max_market_exposure_fraction=settings.max_market_exposure_fraction,
                stop_loss_fraction=settings.stop_loss_fraction,
            )
        )
        self.paper = PaperTradingEngine(Path("data") / "paper_portfolio.json")
        self.last_action = "Idle"
        self.action_log: deque[str] = deque(maxlen=10)

    def status(self) -> dict[str, object]:
        job = self.scheduler.get_job("trading-loop")
        next_run = job.next_run_time.astimezone(UTC) if job and job.next_run_time else None
        return {
            "running": self.scheduler.running and job is not None,
            "strategy": self.strategy.name,
            "available_strategies": sorted(self.strategy_factories.keys()),
            "execution_mode": "paper" if self.settings.dry_run else "live",
            "last_action": self.last_action,
            "next_run": next_run,
            "recent_actions": list(self.action_log),
            "risk": {
                "max_total_exposure_fraction": self.settings.max_total_exposure_fraction,
                "max_market_exposure_fraction": self.settings.max_market_exposure_fraction,
                "stop_loss_fraction": self.settings.stop_loss_fraction,
            },
            "whale_scanner": self.whale_scanner.snapshot() if self.whale_scanner is not None else {},
            "updated_at": datetime.now(UTC),
        }

    def set_strategy(self, name: str) -> str:
        if name not in self.strategy_factories:
            raise ValueError(f"Unknown strategy '{name}'.")
        self.strategy = self.strategy_factories[name]()
        self._record(f"Strategy switched to {name}.")
        return self.strategy.name

    def start(self) -> None:
        if not self.scheduler.running:
            self.scheduler.start()
        if not self.scheduler.get_job("trading-loop"):
            self.scheduler.add_job(
                self.run_cycle,
                IntervalTrigger(seconds=self.settings.scheduler_interval_seconds),
                id="trading-loop",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        self._record("Scheduler started.")

    def stop(self) -> None:
        if self.scheduler.get_job("trading-loop"):
            self.scheduler.remove_job("trading-loop")
        self._record("Scheduler stopped.")

    async def run_cycle(self) -> None:
        try:
            balance = await self.client.get_balance()
            if self.settings.dry_run:
                self.paper.initialize(max(float(balance["buying_power"]), 100.0))
            markets = await fetch_markets(self.client, limit=50)
            market_map = {market.market_id: market for market in markets}
            positions = self.paper.get_positions(market_map) if self.settings.dry_run else await fetch_positions(self.client)
            bankroll = (
                self.paper.get_balance(
                    signer_address=self.client.signer_address,
                    trading_address=self.client.trading_address,
                    funder_address=self.settings.polymarket_funder or None,
                    markets=market_map,
                ).buying_power
                if self.settings.dry_run
                else float(balance["buying_power"])
            )

            for loser in self.risk_manager.stop_loss_actions(positions):
                self._record(f"Stop-loss triggered on {loser.market_title} ({loser.outcome}). Manual close required.")

            proposals = self.strategy.generate_orders(markets, bankroll)
            if not proposals:
                self._record("Cycle completed with no trade.")
                return

            for proposal in proposals:
                allowed, reason = self.risk_manager.can_open_order(
                    bankroll=bankroll,
                    existing_positions=positions,
                    proposed_order=proposal,
                )
                if not allowed:
                    self._record(f"{proposal.market_id}: {reason}")
                    continue
                if self.settings.dry_run:
                    market = market_map.get(proposal.market_id)
                    if market is None:
                        self._record(f"{proposal.market_id}: market metadata unavailable for paper fill.")
                        continue
                    fill = self.paper.execute_order(proposal, market)
                    self._record(
                        f"Paper fill: bought {fill.outcome} on {fill.market_id} for ${fill.size:.2f} "
                        f"at {fill.price:.3f}. {proposal.reason}"
                    )
                    continue
                response = await self.client.place_order(
                    token_id=proposal.token_id,
                    side=proposal.side,
                    price=proposal.price,
                    size=proposal.size,
                    order_type="limit",
                )
                self._record(
                    f"Placed {proposal.outcome} on {proposal.market_id} for ${proposal.size:.2f} at {proposal.price:.3f}. "
                    f"Status: {response.get('status', 'submitted')}"
                )
            if self.settings.dry_run:
                return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Trading cycle failed")
            self._record(f"Cycle error: {exc}")

    def _record(self, message: str) -> None:
        timestamped = f"{datetime.now(UTC).strftime('%H:%M:%S')} UTC | {message}"
        self.last_action = timestamped
        self.action_log.appendleft(timestamped)
