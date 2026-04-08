from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from bot.signal_engine import SignalResult
from config import Settings
from polymarket.client import PolymarketClient


@dataclass(slots=True)
class BtcMarket:
    id: str
    title: str
    yes_price: float
    no_price: float
    closes_at: datetime
    liquidity: float
    seconds_until_close: int
    implied_probability: float
    yes_token_id: str | None
    no_token_id: str | None


@dataclass(slots=True)
class EdgeResult:
    side: str
    market_price: float
    fair_value: float
    edge: float
    kelly_fraction: float
    recommended_size_usdc: float


@dataclass(slots=True)
class ScanDiagnostics:
    source: str
    scanned: int
    matched_topic: int
    matched_timeframe: int
    within_close_window: int
    liquid_enough: int
    price_in_range: int
    accepted: int
    rejection_counts: dict[str, int]
    sample_titles: list[str]


class MarketScanner:
    def __init__(self, polymarket_client: PolymarketClient, settings: Settings) -> None:
        self.polymarket_client = polymarket_client
        self.settings = settings
        self._last_diagnostics = ScanDiagnostics(
            source="uninitialized",
            scanned=0,
            matched_topic=0,
            matched_timeframe=0,
            within_close_window=0,
            liquid_enough=0,
            price_in_range=0,
            accepted=0,
            rejection_counts={},
            sample_titles=[],
        )

    async def find_tradeable_markets(self) -> list[BtcMarket]:
        rows, source = await self._fetch_candidate_rows()
        now = datetime.now(UTC)
        markets: list[BtcMarket] = []
        matched_topic = 0
        matched_timeframe = 0
        within_close_window = 0
        liquid_enough = 0
        price_in_range = 0
        rejection_counts: dict[str, int] = {}
        sample_titles: list[str] = []
        for row in rows:
            title = str(row.get("question") or row.get("title") or "")
            slug = str(row.get("slug") or row.get("market_slug") or row.get("marketSlug") or "")
            normalized = title.lower()
            normalized_slug = slug.lower()
            if len(sample_titles) < 5 and title:
                sample_titles.append(slug or title)
            if not self._is_btc_market(normalized, normalized_slug):
                rejection_counts["topic_mismatch"] = rejection_counts.get("topic_mismatch", 0) + 1
                continue
            matched_topic += 1
            if not self._is_five_minute_market(normalized, normalized_slug):
                rejection_counts["timeframe_mismatch"] = rejection_counts.get("timeframe_mismatch", 0) + 1
                continue
            matched_timeframe += 1

            closes_at = self._parse_dt(row.get("endDate") or row.get("end_date_iso"))
            if closes_at is None:
                rejection_counts["missing_close_time"] = rejection_counts.get("missing_close_time", 0) + 1
                continue
            seconds_until_close = int((closes_at - now).total_seconds())
            if seconds_until_close < 15 or seconds_until_close > 600:
                rejection_counts["outside_window"] = rejection_counts.get("outside_window", 0) + 1
                continue
            if seconds_until_close <= 30:
                rejection_counts["too_close_to_close"] = rejection_counts.get("too_close_to_close", 0) + 1
                continue
            within_close_window += 1

            yes_price, no_price = self._outcome_prices(row)
            liquidity = float(row.get("liquidityNum") or row.get("liquidity") or 0.0)
            if liquidity < 500:
                rejection_counts["liquidity_too_low"] = rejection_counts.get("liquidity_too_low", 0) + 1
                continue
            liquid_enough += 1
            if not (0.15 <= yes_price <= 0.85):
                rejection_counts["price_out_of_range"] = rejection_counts.get("price_out_of_range", 0) + 1
                continue
            price_in_range += 1
            yes_token_id, no_token_id = self._token_ids(row)
            market_id = str(row.get("id") or row.get("conditionId") or "")
            if not market_id or not yes_token_id or not no_token_id:
                rejection_counts["missing_tokens"] = rejection_counts.get("missing_tokens", 0) + 1
                continue

            markets.append(
                BtcMarket(
                    id=market_id,
                    title=title,
                    yes_price=yes_price,
                    no_price=no_price,
                    closes_at=closes_at,
                    liquidity=liquidity,
                    seconds_until_close=seconds_until_close,
                    implied_probability=yes_price,
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                )
            )
        self._last_diagnostics = ScanDiagnostics(
            source=source,
            scanned=len(rows),
            matched_topic=matched_topic,
            matched_timeframe=matched_timeframe,
            within_close_window=within_close_window,
            liquid_enough=liquid_enough,
            price_in_range=price_in_range,
            accepted=len(markets),
            rejection_counts=rejection_counts,
            sample_titles=sample_titles,
        )
        return sorted(markets, key=lambda market: market.seconds_until_close)

    def calculate_edge(self, market: BtcMarket, signal: SignalResult, balance: float) -> EdgeResult:
        base_prob = 0.50
        signal_adjustment = (signal.score / 100.0) * 0.35
        fair_value_up = min(max(base_prob + signal_adjustment, 0.01), 0.99)
        fair_value_down = 1 - fair_value_up

        if signal.score >= 0:
            side = "YES"
            market_price = market.yes_price
            fair_value = fair_value_up
        else:
            side = "NO"
            market_price = market.no_price
            fair_value = fair_value_down

        edge = fair_value - market_price
        b = (1 / max(market_price, 1e-6)) - 1
        p = fair_value
        q = 1 - p
        kelly = max(((b * p) - q) / max(b, 1e-9), 0.0)
        half_kelly = kelly * 0.5
        size = min(half_kelly * balance, self.settings.max_trade_usdc, self.settings.arb_trade_cap_usdc)
        if side == "NO" and signal.score <= self.settings.extreme_no_signal_score:
            size *= self.settings.extreme_no_size_multiplier
        if edge < self.settings.min_edge or size < self.settings.min_trade_usdc:
            size = 0.0

        return EdgeResult(
            side=side,
            market_price=round(market_price, 4),
            fair_value=round(fair_value, 4),
            edge=round(edge, 4),
            kelly_fraction=round(half_kelly, 4),
            recommended_size_usdc=round(size, 2),
        )

    def diagnostics(self) -> dict[str, Any]:
        diag = self._last_diagnostics
        return {
            "source": diag.source,
            "scanned": diag.scanned,
            "matched_topic": diag.matched_topic,
            "matched_timeframe": diag.matched_timeframe,
            "within_close_window": diag.within_close_window,
            "liquid_enough": diag.liquid_enough,
            "price_in_range": diag.price_in_range,
            "accepted": diag.accepted,
            "rejection_counts": diag.rejection_counts,
            "sample_titles": diag.sample_titles,
        }

    async def _fetch_candidate_rows(self) -> tuple[list[dict[str, Any]], str]:
        merged: dict[str, dict[str, Any]] = {}

        slug_rows = await self._fetch_current_slug_rows()
        for row in slug_rows:
            market_id = str(row.get("id") or row.get("conditionId") or "")
            if market_id:
                merged[market_id] = row
        if merged:
            return list(merged.values()), "slug_probe_current_window"

        search_rows = await self._fetch_search_rows()
        for row in search_rows:
            market_id = str(row.get("id") or row.get("conditionId") or "")
            if market_id:
                merged[market_id] = row
        if merged:
            return list(merged.values()), "public_search_btc_updown"

        event_pages = await self._fetch_event_pages()
        for row in event_pages:
            market_id = str(row.get("id") or row.get("conditionId") or "")
            if market_id:
                merged[market_id] = row
        if merged:
            return list(merged.values()), "gamma_events_end_date"

        primary = await self.polymarket_client.get_gamma_markets(
            active="true",
            closed="false",
            archived="false",
            limit=500,
            order="end_date",
            ascending="true",
        )
        for row in primary:
            market_id = str(row.get("id") or row.get("conditionId") or "")
            if market_id:
                merged[market_id] = row

        if len(merged) < 25:
            fallback = await self.polymarket_client.get_active_markets(limit=500)
            for row in fallback:
                market_id = str(row.get("id") or row.get("conditionId") or "")
                if market_id and market_id not in merged:
                    merged[market_id] = row
            return list(merged.values()), "markets_end_date_plus_active_fallback"
        return list(merged.values()), "gamma_markets_end_date"

    async def _fetch_event_pages(self) -> list[dict[str, Any]]:
        collected: dict[str, dict[str, Any]] = {}
        for offset in (0, 100, 200):
            events = await self._fetch_events_page(offset)
            if not events:
                break
            for event in events:
                if not isinstance(event, dict):
                    continue
                event_slug = str(event.get("slug") or "")
                event_title = str(event.get("title") or event.get("question") or "")
                raw_markets = event.get("markets") or []
                if isinstance(raw_markets, list):
                    for market in raw_markets:
                        normalized_market = self._normalize_candidate_market(
                            market,
                            fallback_slug=event_slug,
                            fallback_title=event_title,
                            fallback_end_date=event.get("endDate"),
                            fallback_liquidity=event.get("liquidityNum") or event.get("liquidity"),
                        )
                        if normalized_market is None:
                            continue
                        market_id = str(normalized_market.get("id") or normalized_market.get("conditionId") or "")
                        if market_id:
                            collected[market_id] = normalized_market
                direct_market = self._normalize_candidate_market(
                    event,
                    fallback_slug=event_slug,
                    fallback_title=event_title,
                    fallback_end_date=event.get("endDate"),
                    fallback_liquidity=event.get("liquidityNum") or event.get("liquidity"),
                )
                if direct_market is not None:
                    market_id = str(direct_market.get("id") or direct_market.get("conditionId") or "")
                    if market_id:
                        collected[market_id] = direct_market
        return list(collected.values())

    async def _fetch_events_page(self, offset: int) -> list[dict[str, Any]]:
        query_variants = (
            {
                "active": "true",
                "closed": "false",
                "limit": 100,
                "offset": offset,
                "order": "end_date",
                "ascending": "true",
            },
            {
                "active": "true",
                "closed": "false",
                "limit": 100,
                "offset": offset,
            },
        )
        last_error: Exception | None = None
        for params in query_variants:
            try:
                return await self.polymarket_client.get_gamma_events(**params)
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if exc.response.status_code != 422:
                    raise
        if last_error is not None:
            raise last_error
        return []

    async def _fetch_current_slug_rows(self) -> list[dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        now = datetime.now(UTC)
        bucket_epoch = int(now.timestamp() // 300 * 300)
        candidate_epochs = [bucket_epoch + (offset * 300) for offset in range(-2, 4)]
        for epoch in candidate_epochs:
            slug = f"btc-updown-5m-{epoch}"
            try:
                market = await self.polymarket_client.get_market_by_slug(slug)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            normalized_market = self._normalize_candidate_market(
                market,
                fallback_slug=slug,
                fallback_end_date=self._slug_start_to_end_date(slug),
            )
            if normalized_market is None:
                continue
            market_id = str(normalized_market.get("id") or normalized_market.get("conditionId") or "")
            if market_id:
                rows[market_id] = normalized_market
        return list(rows.values())

    async def _fetch_search_rows(self) -> list[dict[str, Any]]:
        collected: dict[str, dict[str, Any]] = {}
        for query in ("btc-updown-5m", "btc up or down", "bitcoin up or down"):
            payload = await self.polymarket_client.public_search(
                q=query,
                events_status="active",
                limit_per_type=20,
                search_profiles="false",
                search_tags="false",
                optimized="true",
            )
            top_level_markets = payload.get("markets") or []
            for market in top_level_markets:
                normalized_market = self._normalize_candidate_market(market)
                if normalized_market is None:
                    continue
                market_id = str(normalized_market.get("id") or normalized_market.get("conditionId") or "")
                if market_id:
                    collected[market_id] = normalized_market
            events = payload.get("events") or []
            for event in events:
                if not isinstance(event, dict):
                    continue
                event_slug = str(event.get("slug") or "")
                event_title = str(event.get("title") or event.get("question") or "")
                raw_markets = event.get("markets") or []
                if isinstance(raw_markets, list):
                    for market in raw_markets:
                        normalized_market = self._normalize_candidate_market(
                            market,
                            fallback_slug=event_slug,
                            fallback_title=event_title,
                            fallback_end_date=event.get("endDate"),
                            fallback_liquidity=event.get("liquidityNum") or event.get("liquidity"),
                        )
                        if normalized_market is None:
                            continue
                        market_id = str(normalized_market.get("id") or normalized_market.get("conditionId") or "")
                        if market_id:
                            collected[market_id] = normalized_market
                direct_market = self._normalize_candidate_market(
                    event,
                    fallback_slug=event_slug,
                    fallback_title=event_title,
                    fallback_end_date=event.get("endDate"),
                    fallback_liquidity=event.get("liquidityNum") or event.get("liquidity"),
                )
                if direct_market is not None:
                    market_id = str(direct_market.get("id") or direct_market.get("conditionId") or "")
                    if market_id:
                        collected[market_id] = direct_market
        return list(collected.values())

    def _normalize_candidate_market(
        self,
        row: object,
        *,
        fallback_slug: str = "",
        fallback_title: str = "",
        fallback_end_date: object = None,
        fallback_liquidity: object = None,
    ) -> dict[str, Any] | None:
        if not isinstance(row, dict):
            return None
        slug = str(row.get("slug") or row.get("market_slug") or row.get("marketSlug") or fallback_slug or "")
        title = str(row.get("question") or row.get("title") or fallback_title or "")
        normalized_slug = slug.lower()
        normalized_title = title.lower()
        if not self._is_live_btc_5m_market(normalized_title, normalized_slug):
            return None
        normalized_market = dict(row)
        normalized_market.setdefault("slug", slug)
        normalized_market.setdefault("question", title)
        normalized_market.setdefault("title", title)
        normalized_market.setdefault(
            "endDate",
            row.get("endDate")
            or row.get("endDateIso")
            or row.get("end_date_iso")
            or self._slug_start_to_end_date(slug)
            or fallback_end_date,
        )
        normalized_market.setdefault("liquidity", row.get("liquidity") or fallback_liquidity)
        normalized_market.setdefault("liquidityNum", row.get("liquidityNum") or fallback_liquidity)
        return normalized_market

    @staticmethod
    def _is_btc_market(normalized_title: str, normalized_slug: str = "") -> bool:
        if normalized_slug.startswith("btc-updown-5m-") or "btc-updown-5m" in normalized_slug:
            return True
        return any(term in normalized_title or term in normalized_slug for term in ("bitcoin", "btc", "btc/usdt", "btc up", "btc down"))

    @staticmethod
    def _is_five_minute_market(normalized_title: str, normalized_slug: str = "") -> bool:
        if normalized_slug.startswith("btc-updown-5m-") or "btc-updown-5m" in normalized_slug:
            return True
        patterns = ("5 min", "5-min", "5 minute", "5 minutes", "5m", "five min", "up or down")
        return any(pattern in normalized_title or pattern in normalized_slug for pattern in patterns)

    @staticmethod
    def _is_live_btc_5m_market(normalized_title: str, normalized_slug: str = "") -> bool:
        if normalized_slug.startswith("btc-updown-5m-") or "btc-updown-5m" in normalized_slug:
            return True
        has_btc = "bitcoin" in normalized_title or "btc" in normalized_title or "bitcoin" in normalized_slug or "btc" in normalized_slug
        has_direction = "up or down" in normalized_title or "updown" in normalized_slug
        has_window = any(pattern in normalized_title or pattern in normalized_slug for pattern in ("5 min", "5-min", "5 minute", "5 minutes", "5m"))
        return has_btc and has_direction and has_window

    @staticmethod
    def _parse_dt(raw: object) -> datetime | None:
        if not raw:
            return None
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None

    @staticmethod
    def _slug_start_to_end_date(slug: str) -> str | None:
        if not slug.lower().startswith("btc-updown-5m-"):
            return None
        try:
            start_epoch = int(slug.rsplit("-", 1)[-1])
        except ValueError:
            return None
        end_dt = datetime.fromtimestamp(start_epoch, tz=UTC) + timedelta(minutes=5)
        return end_dt.isoformat()

    @staticmethod
    def _outcome_prices(row: dict[str, Any]) -> tuple[float, float]:
        raw = row.get("outcomePrices")
        prices = []
        if isinstance(raw, str):
            try:
                prices = json.loads(raw)
            except json.JSONDecodeError:
                prices = []
        elif isinstance(raw, list):
            prices = raw
        yes_price = float(prices[0]) if len(prices) > 0 else float(row.get("bestAsk", 0.5) or 0.5)
        no_price = float(prices[1]) if len(prices) > 1 else max(0.0, 1 - yes_price)
        return yes_price, no_price

    @staticmethod
    def _token_ids(row: dict[str, Any]) -> tuple[str | None, str | None]:
        raw = row.get("clobTokenIds") or row.get("tokenIds")
        token_ids = []
        if isinstance(raw, str):
            try:
                token_ids = json.loads(raw)
            except json.JSONDecodeError:
                token_ids = []
        elif isinstance(raw, list):
            token_ids = raw
        yes_token = str(token_ids[0]) if len(token_ids) > 0 else None
        no_token = str(token_ids[1]) if len(token_ids) > 1 else None
        return yes_token, no_token
