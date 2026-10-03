"""The single strategy: "late favourite".

Near the end of a BTC up/down window, buy the side the model says wins with >= 90%
probability, but only when the market sells it cheaper than that probability after
fees and book depth. Hold to resolution.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from config import Settings
from fees import FeeSchedule, Fill, fill_from_asks, net_edge
from model import Estimate


@dataclass(slots=True)
class MarketView:
    slug: str
    timeframe: str
    seconds_left: float
    strike: float
    spot: float
    fee_schedule: FeeSchedule
    asks: dict[str, list[tuple[float, float]]]   # side ("Up"/"Down") -> [(price, size)]


@dataclass(slots=True)
class Decision:
    enter: bool
    reason: str
    side: str | None = None
    p_model: float = 0.0
    budget_usdc: float = 0.0
    fill: Fill | None = None
    edge_per_share: float = 0.0
    details: dict = field(default_factory=dict)


def kelly_budget(p: float, cost_per_share: float, bankroll: float, settings: Settings, open_exposure: float) -> float:
    if cost_per_share >= 1.0 or p <= cost_per_share:
        return 0.0
    f_star = (p - cost_per_share) / (1.0 - cost_per_share)
    budget = settings.kelly_fraction * f_star * bankroll
    budget = min(budget, settings.max_trade_usdc, settings.max_open_exposure_usdc - open_exposure, bankroll)
    return budget if budget >= settings.min_trade_usdc else 0.0


def evaluate(view: MarketView, est: Estimate, settings: Settings, bankroll: float, open_exposure: float) -> Decision:
    d = {"seconds_left": round(view.seconds_left, 1), "strike": view.strike, "spot": view.spot,
         "settle_mean": round(est.settle_mean, 2), "settle_sd": round(est.settle_sd, 2), "p_up_raw": round(est.p_up, 4)}

    if view.seconds_left < settings.min_seconds_left:
        return Decision(False, "too_close_to_end", details=d)
    if view.seconds_left > settings.max_seconds_left(view.timeframe):
        return Decision(False, "too_early", details=d)

    side = "Up" if est.p_up >= 0.5 else "Down"
    p_model = est.p_side(side) - settings.model_haircut
    d.update(side=side, p_model=round(p_model, 4))

    if abs(est.settle_mean - view.strike) < settings.min_distance_usd:
        return Decision(False, "too_close_to_strike", side=side, p_model=p_model, details=d)
    if p_model < settings.min_win_probability:
        return Decision(False, "probability_below_90", side=side, p_model=p_model, details=d)

    asks = view.asks.get(side) or []
    if not asks:
        return Decision(False, "no_asks", side=side, p_model=p_model, details=d)
    best_ask = min(price for price, _ in asks)
    d["best_ask"] = best_ask
    if best_ask < settings.min_entry_price:
        return Decision(False, "price_too_low_market_disagrees", side=side, p_model=p_model, details=d)
    if best_ask > settings.max_entry_price:
        return Decision(False, "price_too_high", side=side, p_model=p_model, details=d)

    # Edge at minimum size first, then size with Kelly and re-check on the real fill across book depth.
    top = fill_from_asks(asks, settings.min_trade_usdc, view.fee_schedule, settings.max_entry_price)
    top_edge = net_edge(p_model, top.avg_price, top.fee_per_share) if top.shares else -1.0
    d["edge"] = round(top_edge, 4)
    if top_edge < settings.min_net_edge:
        return Decision(False, "edge_below_min_after_fees", side=side, p_model=p_model, details=d)
    budget = kelly_budget(p_model, top.avg_price + top.fee_per_share, bankroll, settings, open_exposure)
    if budget <= 0:
        return Decision(False, "kelly_or_exposure_zero", side=side, p_model=p_model, details=d)

    for attempt in (budget, settings.min_trade_usdc):
        fill = fill_from_asks(asks, attempt, view.fee_schedule, settings.max_entry_price)
        if fill.cost_usdc < attempt * 0.99:
            continue  # book too thin at acceptable prices for an all-or-nothing order
        edge = net_edge(p_model, fill.avg_price, fill.fee_per_share)
        d.update(avg_price=round(fill.avg_price, 4), fee_per_share=round(fill.fee_per_share, 5), edge=round(edge, 4))
        if edge >= settings.min_net_edge:
            return Decision(True, "enter", side=side, p_model=p_model, budget_usdc=attempt,
                            fill=fill, edge_per_share=edge, details=d)
    return Decision(False, "edge_below_min_after_fees", side=side, p_model=p_model, details=d)
