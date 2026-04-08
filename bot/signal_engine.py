from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from bot.bybit_feed import BybitFeed


@dataclass(slots=True)
class SignalResult:
    score: float
    direction: str
    confidence: float
    signals_dict: dict[str, float]
    timestamp: datetime


class SignalEngine:
    def __init__(self, feed: BybitFeed) -> None:
        self.feed = feed

    def compute(self) -> SignalResult:
        price_now = self.feed.get_latest_price()
        history_30 = self.feed.get_price_history(seconds=30)
        history_90 = self.feed.get_price_history(seconds=90)
        velocity_30 = self._velocity(price_now, history_30[0] if history_30 else price_now)
        velocity_90 = self._velocity(price_now, history_90[0] if history_90 else price_now)
        rsi = self._compute_rsi(self.feed.get_candle_closes(limit=30))
        imbalance = self.feed.get_orderbook_imbalance()

        raw_score = (velocity_30 * 40) + (velocity_90 * 30) + ((rsi - 50) * 0.6) + (imbalance * 30)
        score = max(min(raw_score, 100.0), -100.0)
        direction = self._classify(score)
        confidence = min(abs(score) / 100.0, 1.0)
        return SignalResult(
            score=round(score, 2),
            direction=direction,
            confidence=round(confidence, 4),
            signals_dict={
                "price": round(price_now, 2),
                "velocity_30s": round(velocity_30, 4),
                "velocity_90s": round(velocity_90, 4),
                "rsi_14": round(rsi, 2),
                "imbalance": round(imbalance, 4),
            },
            timestamp=datetime.now(UTC),
        )

    @staticmethod
    def _velocity(price_now: float, previous_price: float) -> float:
        if price_now <= 0 or previous_price <= 0:
            return 0.0
        return ((price_now - previous_price) / previous_price) * 100

    @staticmethod
    def _compute_rsi(closes: list[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        series = pd.Series(np.array(closes, dtype=float))
        delta = series.diff()
        gains = delta.clip(lower=0)
        losses = (-delta).clip(lower=0)
        avg_gain = gains.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        avg_loss = losses.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        if avg_loss.iloc[-1] == 0:
            return 100.0 if avg_gain.iloc[-1] > 0 else 50.0
        rs = avg_gain.iloc[-1] / avg_loss.iloc[-1]
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _classify(score: float) -> str:
        if score > 25:
            return "STRONG_UP"
        if score > 10:
            return "WEAK_UP"
        if score < -25:
            return "STRONG_DOWN"
        if score < -10:
            return "WEAK_DOWN"
        return "NEUTRAL"
