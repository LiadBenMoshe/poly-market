"""Polymarket taker fees and edge maths.

Crypto up/down markets charge takers (makers pay nothing):

    fee_usdc = shares * rate * (p * (1 - p)) ** exponent

with rate=0.07, exponent=1 today (read from the market's `feeSchedule`).
Fees are charged in USDC, rounded to 5 decimals. Example: 100 shares @ 0.50 -> $1.75.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeeSchedule:
    rate: float = 0.07
    exponent: float = 1.0
    taker_only: bool = True

    @classmethod
    def from_market(cls, market: dict, fallback_rate: float) -> "FeeSchedule":
        if market.get("feesEnabled") is False:
            return cls(rate=0.0)
        schedule = market.get("feeSchedule") or {}
        if not schedule:
            return cls(rate=fallback_rate)
        return cls(
            rate=float(schedule.get("rate", fallback_rate)),
            exponent=float(schedule.get("exponent", 1.0)),
            taker_only=bool(schedule.get("takerOnly", True)),
        )


def fee_per_share(price: float, schedule: FeeSchedule) -> float:
    return schedule.rate * (price * (1.0 - price)) ** schedule.exponent


def fee_usdc(shares: float, price: float, schedule: FeeSchedule) -> float:
    return round(shares * fee_per_share(price, schedule), 5)


def breakeven_win_rate(avg_price: float, fee_per_share_usdc: float) -> float:
    """Win rate needed for zero expected profit: a winning share pays $1."""
    return avg_price + fee_per_share_usdc


def net_edge(p_win: float, avg_price: float, fee_per_share_usdc: float) -> float:
    """Expected profit per share after fees."""
    return p_win - breakeven_win_rate(avg_price, fee_per_share_usdc)


@dataclass(frozen=True, slots=True)
class Fill:
    shares: float
    cost_usdc: float      # sum(price * shares), excluding fees
    fee_usdc: float
    worst_price: float

    @property
    def avg_price(self) -> float:
        return self.cost_usdc / self.shares if self.shares else 0.0

    @property
    def fee_per_share(self) -> float:
        return self.fee_usdc / self.shares if self.shares else 0.0

    @property
    def total_outlay(self) -> float:
        return self.cost_usdc + self.fee_usdc


def fill_from_asks(
    asks: list[tuple[float, float]],
    budget_usdc: float,
    schedule: FeeSchedule,
    max_price: float = 0.99,
) -> Fill:
    """Walk the ask side (price ascending) spending up to `budget_usdc` (excl. fees) as a taker."""
    shares = cost = fee = 0.0
    worst = 0.0
    remaining = budget_usdc
    for price, size in sorted(asks):
        if price > max_price or remaining <= 1e-9:
            break
        take = min(size, remaining / price)
        if take <= 0:
            continue
        shares += take
        cost += take * price
        fee += take * fee_per_share(price, schedule)
        remaining -= take * price
        worst = price
    return Fill(shares=shares, cost_usdc=cost, fee_usdc=round(fee, 5), worst_price=worst)
