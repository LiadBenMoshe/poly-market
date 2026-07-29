from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any

from schemas import MarketResponse, StrategyOrder


STOPWORDS = {
    "a", "an", "and", "are", "at", "be", "by", "for", "from", "if", "in", "into",
    "is", "of", "on", "or", "the", "to", "vs", "will", "with",
}
PREDICATE_BREAKS = {
    "win", "wins", "be", "become", "capture", "earn", "finish", "get", "have", "lose",
    "reach", "receive", "secure", "sweep", "take",
}


class BaseStrategy(ABC):
    name = "base"

    @abstractmethod
    def should_trade(
        self,
        market: MarketResponse,
        orderbook: dict[str, Any],
        bankroll: float,
    ) -> StrategyOrder | None:
        raise NotImplementedError

    def generate_orders(self, markets: list[MarketResponse], bankroll: float) -> list[StrategyOrder]:
        for market in markets:
            order = self.should_trade(market, {}, bankroll)
            if order is not None:
                return [order]
        return []


class MeanReversionStrategy(BaseStrategy):
    name = "mean_reversion"

    def __init__(
        self,
        *,
        volume_threshold: float = 10_000.0,
        max_trade_fraction: float = 0.05,
        kelly_fraction: float = 0.25,
    ) -> None:
        self.volume_threshold = volume_threshold
        self.max_trade_fraction = max_trade_fraction
        self.kelly_fraction = kelly_fraction

    def should_trade(
        self,
        market: MarketResponse,
        orderbook: dict[str, Any],
        bankroll: float,
    ) -> StrategyOrder | None:
        if market.volume < self.volume_threshold or bankroll <= 0:
            return None

        if market.yes_price < 0.15 and market.yes_token_id:
            size = self._position_size(bankroll, edge=1.0 - (market.yes_price * 2.0), price=market.yes_price)
            if size > 0:
                return StrategyOrder(
                    market_id=market.market_id,
                    token_id=market.yes_token_id,
                    side="BUY",
                    outcome="YES",
                    price=market.yes_price,
                    size=size,
                    reason="Mean reversion: YES is deeply discounted.",
                )

        if market.yes_price > 0.85 and market.no_token_id:
            no_price = max(market.no_price, 0.01)
            size = self._position_size(bankroll, edge=1.0 - (no_price * 2.0), price=no_price)
            if size > 0:
                return StrategyOrder(
                    market_id=market.market_id,
                    token_id=market.no_token_id,
                    side="BUY",
                    outcome="NO",
                    price=no_price,
                    size=size,
                    reason="Mean reversion: YES is overheated, buying NO.",
                )
        return None

    def _position_size(self, bankroll: float, *, edge: float, price: float) -> float:
        if price <= 0 or price >= 1:
            return 0.0
        win_prob = min(max(price + max(edge, 0.0) / 2.0, 0.01), 0.99)
        b = (1.0 - price) / price
        q = 1.0 - win_prob
        kelly = max(((b * win_prob) - q) / max(b, 1e-9), 0.0)
        allocation_fraction = min(kelly * self.kelly_fraction, self.max_trade_fraction)
        return round(bankroll * allocation_fraction, 2)


class RelatedMarketArbitrageStrategy(BaseStrategy):
    name = "related_market_arbitrage"

    def __init__(
        self,
        *,
        volume_threshold: float = 10_000.0,
        max_trade_fraction: float = 0.05,
        underround_threshold: float = 0.05,
        max_legs: int = 3,
    ) -> None:
        self.volume_threshold = volume_threshold
        self.max_trade_fraction = max_trade_fraction
        self.underround_threshold = underround_threshold
        self.max_legs = max_legs

    def should_trade(
        self,
        market: MarketResponse,
        orderbook: dict[str, Any],
        bankroll: float,
    ) -> StrategyOrder | None:
        return None

    def generate_orders(self, markets: list[MarketResponse], bankroll: float) -> list[StrategyOrder]:
        if bankroll <= 0:
            return []

        eligible = [
            market
            for market in markets
            if market.active and market.yes_token_id and market.volume >= self.volume_threshold and 0.02 <= market.yes_price <= 0.98
        ]
        buckets = self._group_related_markets(eligible)
        best_orders: list[StrategyOrder] = []
        best_edge = 0.0

        for _, bucket in buckets.items():
            if len(bucket) < 2:
                continue
            ordered = sorted(bucket, key=lambda item: item.yes_price)[: self.max_legs]
            combined_yes = sum(item.yes_price for item in ordered)
            underround = 1.0 - combined_yes
            if underround < self.underround_threshold:
                continue

            total_allocation = round(bankroll * min(self.max_trade_fraction, max(underround * 0.5, 0.01)), 2)
            if total_allocation <= 0:
                continue
            leg_size = round(total_allocation / len(ordered), 2)
            if leg_size <= 0:
                continue

            orders = [
                StrategyOrder(
                    market_id=market.market_id,
                    token_id=market.yes_token_id or "",
                    side="BUY",
                    outcome="YES",
                    price=market.yes_price,
                    size=leg_size,
                    reason=(
                        f"Related-market arbitrage basket: combined YES {combined_yes:.3f}, "
                        f"underround {underround:.3f} across {len(ordered)} related markets."
                    ),
                )
                for market in ordered
                if market.yes_token_id
            ]
            if orders and underround > best_edge:
                best_orders = orders
                best_edge = underround

        return best_orders

    def _group_related_markets(self, markets: list[MarketResponse]) -> dict[str, list[MarketResponse]]:
        buckets: dict[str, list[MarketResponse]] = {}
        for market in markets:
            key = self._related_key(market.title)
            if not key:
                continue
            buckets.setdefault(key, []).append(market)
        return {key: value for key, value in buckets.items() if len(value) >= 2}

    def _related_key(self, title: str) -> str:
        tokens = re.findall(r"[a-z0-9]+", title.lower())
        if not tokens:
            return ""

        if "will" in tokens:
            start = tokens.index("will") + 1
            for idx in range(start, len(tokens)):
                if tokens[idx] in PREDICATE_BREAKS:
                    predicate = [token for token in tokens[idx:] if token not in STOPWORDS]
                    if len(predicate) >= 3:
                        return " ".join(predicate[:6])

        tail = [token for token in tokens if token not in STOPWORDS]
        if len(tail) < 4:
            return ""
        return " ".join(tail[-4:])


class WhaleFollowStrategy(BaseStrategy):
    name = "whale_following"

    def __init__(
        self,
        *,
        whale_scanner: Any,
        min_score: float = 55.0,
        fallback_max_trade_fraction: float = 0.05,
    ) -> None:
        self.whale_scanner = whale_scanner
        self.min_score = min_score
        self.fallback_max_trade_fraction = fallback_max_trade_fraction

    def should_trade(
        self,
        market: MarketResponse,
        orderbook: dict[str, Any],
        bankroll: float,
    ) -> StrategyOrder | None:
        return None

    def generate_orders(self, markets: list[MarketResponse], bankroll: float) -> list[StrategyOrder]:
        if bankroll <= 0 or self.whale_scanner is None:
            return []
        market_map = {market.market_id: market for market in markets}
        signals = self.whale_scanner.get_actionable_signals(self.min_score)
        for signal in signals:
            market = market_map.get(str(signal.get("market_id") or ""))
            if market is None or not market.active:
                continue
            outcome = str(signal.get("side") or "YES").upper()
            token_id = market.yes_token_id if outcome == "YES" else market.no_token_id
            price = market.yes_price if outcome == "YES" else market.no_price
            if not token_id or price <= 0 or price >= 1:
                continue
            requested_fraction = float(signal.get("position_fraction") or self.fallback_max_trade_fraction)
            size = round(bankroll * max(0.0, requested_fraction), 2)
            if size <= 0:
                continue
            return [
                StrategyOrder(
                    market_id=market.market_id,
                    token_id=token_id,
                    side="BUY",
                    outcome=outcome,
                    price=price,
                    size=size,
                    reason=(
                        f"Whale follow tier {signal.get('tier')} conviction {float(signal.get('conviction_score') or 0.0):.1f} "
                        f"trade ${float(signal.get('size_usdc') or 0.0):,.0f}"
                    ),
                )
            ]
        return []
