from __future__ import annotations

from dataclasses import dataclass

from schemas import PositionResponse, StrategyOrder


@dataclass(slots=True)
class RiskLimits:
    max_total_exposure_fraction: float
    max_market_exposure_fraction: float
    stop_loss_fraction: float


class RiskManager:
    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits

    def can_open_order(
        self,
        *,
        bankroll: float,
        existing_positions: list[PositionResponse],
        proposed_order: StrategyOrder,
    ) -> tuple[bool, str]:
        total_exposure = sum(position.notional_value for position in existing_positions)
        if total_exposure + proposed_order.size > bankroll * self.limits.max_total_exposure_fraction:
            return False, "Rejected by max total exposure limit."

        market_exposure = sum(
            position.notional_value for position in existing_positions if position.market_id == proposed_order.market_id
        )
        if market_exposure + proposed_order.size > bankroll * self.limits.max_market_exposure_fraction:
            return False, "Rejected by per-market exposure limit."

        return True, "Risk checks passed."

    def stop_loss_actions(self, positions: list[PositionResponse]) -> list[PositionResponse]:
        return [
            position
            for position in positions
            if position.unrealized_pnl_pct <= -(self.limits.stop_loss_fraction * 100)
        ]
