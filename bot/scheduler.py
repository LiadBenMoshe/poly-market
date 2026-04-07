from __future__ import annotations

import logging
from collections import deque
from datetime import UTC, datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from bot.risk import RiskLimits, RiskManager
from bot.strategy import BaseStrategy, MeanReversionStrategy, RelatedMarketArbitrageStrategy
from config import Settings
from polymarket.client import PolymarketClient
from polymarket.markets import fetch_markets
from polymarket.positions import fetch_positions


logger = logging.getLogger(__name__)


class TradingBotScheduler:
    def __init__(self, client: PolymarketClient, settings: Settings) -> None:
        self.client = client
        self.settings = settings
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
        }
        self.strategy: BaseStrategy = self.strategy_factories["related_market_arbitrage"]()
        self.risk_manager = RiskManager(
            RiskLimits(
                max_total_exposure_fraction=settings.max_total_exposure_fraction,
                max_market_exposure_fraction=settings.max_market_exposure_fraction,
                stop_loss_fraction=settings.stop_loss_fraction,
            )
        )
        self.last_action = "Idle"
        self.action_log: deque[str] = deque(maxlen=10)

    def status(self) -> dict[str, object]:
        job = self.scheduler.get_job("trading-loop")
        next_run = job.next_run_time.astimezone(UTC) if job and job.next_run_time else None
        return {
            "running": self.scheduler.running and job is not None,
            "strategy": self.strategy.name,
            "available_strategies": sorted(self.strategy_factories.keys()),
            "last_action": self.last_action,
            "next_run": next_run,
            "recent_actions": list(self.action_log),
            "risk": {
                "max_total_exposure_fraction": self.settings.max_total_exposure_fraction,
                "max_market_exposure_fraction": self.settings.max_market_exposure_fraction,
                "stop_loss_fraction": self.settings.stop_loss_fraction,
            },
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
            bankroll = float(balance["buying_power"])
            positions = await fetch_positions(self.client)

            for loser in self.risk_manager.stop_loss_actions(positions):
                self._record(f"Stop-loss triggered on {loser.market_title} ({loser.outcome}). Manual close required.")

            markets = await fetch_markets(self.client, limit=50)
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
                    self._record(
                        f"Dry run: would place {proposal.outcome} on {proposal.market_id} "
                        f"for ${proposal.size:.2f}. {proposal.reason}"
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
