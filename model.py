"""Probability that a BTC up/down window settles "Up".

Polymarket settles on Chainlink BTC/USD: Up if the settle value >= the strike (price to beat).
The settle value is a TWAP over the last `twap_seconds` of the window (60s today), so near
the end part of the average is already known and only the rest is random.

Price is modelled as arithmetic Brownian motion over the remaining seconds with
per-second dollar volatility sigma_usd = sigma * S (fine for windows of a few minutes).
"""
from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass(frozen=True, slots=True)
class SettleDistribution:
    mean: float
    sd: float

    def prob_at_least(self, strike: float) -> float:
        if self.sd <= 0:
            return 1.0 if self.mean >= strike else 0.0
        return normal_cdf((self.mean - strike) / self.sd)


def settle_distribution(
    spot: float,
    seconds_left: float,
    sigma_per_sec: float,
    twap_seconds: float = 60.0,
    observed_twap_avg: float | None = None,
) -> SettleDistribution:
    """Distribution of the settle value.

    seconds_left >= twap_seconds: the averaging window has not started yet.
        settle = avg of price over [T-L, T]; var = s^2 * (tau - L + L/3)
    seconds_left <  twap_seconds: (L - tau) seconds are already observed with mean `observed_twap_avg`.
        settle = ((L-tau)*A_obs + tau*avg_future)/L; var of avg_future = s^2 * tau/3
    """
    tau = max(seconds_left, 0.0)
    sigma_usd = sigma_per_sec * spot
    L = max(twap_seconds, 0.0)
    if L == 0:
        return SettleDistribution(mean=spot, sd=sigma_usd * math.sqrt(tau))
    if tau >= L:
        return SettleDistribution(mean=spot, sd=sigma_usd * math.sqrt(tau - L + L / 3.0))
    observed = observed_twap_avg if observed_twap_avg is not None else spot
    elapsed = L - tau
    mean = (elapsed * observed + tau * spot) / L
    sd = (tau / L) * sigma_usd * math.sqrt(tau / 3.0)
    return SettleDistribution(mean=mean, sd=sd)


def realized_sigma_per_sec(prices: Sequence[float], seconds_between: float) -> float:
    """Std-dev of log returns scaled to one second. `prices` must be evenly spaced."""
    if len(prices) < 3 or seconds_between <= 0:
        return 0.0
    rets = [math.log(b / a) for a, b in zip(prices, prices[1:]) if a > 0 and b > 0]
    if len(rets) < 2:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var / seconds_between)


@dataclass(frozen=True, slots=True)
class Estimate:
    p_up: float          # raw model probability of Up
    settle_mean: float
    settle_sd: float

    def p_side(self, side: str) -> float:
        return self.p_up if side == "Up" else 1.0 - self.p_up


def estimate_up(
    *,
    spot: float,
    strike: float,
    seconds_left: float,
    sigma_per_sec: float,
    twap_seconds: float = 60.0,
    observed_twap_avg: float | None = None,
) -> Estimate:
    dist = settle_distribution(spot, seconds_left, sigma_per_sec, twap_seconds, observed_twap_avg)
    return Estimate(p_up=dist.prob_at_least(strike), settle_mean=dist.mean, settle_sd=dist.sd)
