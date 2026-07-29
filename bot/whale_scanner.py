from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import Settings
from polymarket.client import PolymarketClient
from schemas import MarketResponse


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WhaleTrade:
    trade_id: str
    market_id: str
    title: str
    slug: str
    side: str
    size_usdc: float
    price: float
    wallet: str
    timestamp: datetime


@dataclass(slots=True)
class WhaleSignal:
    trade_id: str
    market_id: str
    title: str
    slug: str
    side: str
    price: float
    size_usdc: float
    threshold_usdc: float
    relative_size: float
    wallet: str
    wallet_win_rate: float
    cluster_count: int
    same_side_last_8: int
    momentum_alignment: bool
    crypto_alignment: bool
    oracle_alignment: float
    category: str
    keyword_hits: list[str]
    conviction_score: float
    tier: int
    position_fraction: float
    detected_at: datetime
    reasons: list[str] = field(default_factory=list)


class WhaleScanner:
    def __init__(self, client: PolymarketClient, settings: Settings) -> None:
        self.client = client
        self.settings = settings
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.state_path = Path("data") / "whale_scanner_state.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._last_snapshot: dict[str, Any] = {
            "running": False,
            "updated_at": datetime.now(UTC).isoformat(),
            "markets_scanned": 0,
            "trades_seen": 0,
            "whales_detected": 0,
            "signals": [],
            "oracles": {},
            "errors": [],
        }
        self._state = self._load_state()
        self._seen_trade_ids: deque[str] = deque(self._state.get("recent_trade_ids", []), maxlen=20_000)
        self._seen_trade_id_set: set[str] = set(self._seen_trade_ids)
        self._wallet_stats: dict[str, dict[str, float]] = dict(self._state.get("wallet_stats", {}))
        self._pending_wallet_marks: list[dict[str, Any]] = list(self._state.get("pending_wallet_marks", []))
        self._market_size_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=200))
        self._recent_signals: deque[dict[str, Any]] = deque(self._state.get("signals", []), maxlen=300)
        self._last_signal_by_market: dict[str, dict[str, Any]] = {}
        for market_id, raw_sizes in self._state.get("market_size_history", {}).items():
            history = deque(maxlen=200)
            for value in raw_sizes:
                try:
                    history.append(float(value))
                except (TypeError, ValueError):
                    continue
            if history:
                self._market_size_history[market_id] = history

    def start(self) -> None:
        if not self.scheduler.running:
            self.scheduler.start()
        if not self.scheduler.get_job("whale-scanner-loop"):
            self.scheduler.add_job(
                self.run_cycle,
                IntervalTrigger(seconds=self.settings.whale_scan_interval_seconds),
                id="whale-scanner-loop",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
        self._last_snapshot["running"] = True

    def stop(self) -> None:
        if self.scheduler.get_job("whale-scanner-loop"):
            self.scheduler.remove_job("whale-scanner-loop")
        self._last_snapshot["running"] = False
        self._save_state()

    def is_running(self) -> bool:
        return self.scheduler.running and self.scheduler.get_job("whale-scanner-loop") is not None

    def snapshot(self) -> dict[str, Any]:
        snapshot = dict(self._last_snapshot)
        snapshot["running"] = self.is_running()
        snapshot["signals"] = list(self._recent_signals)[-25:][::-1]
        return snapshot

    def get_actionable_signals(self, min_score: float | None = None) -> list[dict[str, Any]]:
        floor = self.settings.whale_min_conviction_score if min_score is None else min_score
        cutoff = datetime.now(UTC) - timedelta(minutes=10)
        deduped: dict[str, dict[str, Any]] = {}
        for row in reversed(self._recent_signals):
            try:
                detected_at = datetime.fromisoformat(str(row.get("detected_at")))
            except ValueError:
                continue
            if detected_at < cutoff or float(row.get("conviction_score") or 0.0) < floor:
                continue
            market_id = str(row.get("market_id") or "")
            if market_id and market_id not in deduped:
                deduped[market_id] = row
        return sorted(deduped.values(), key=lambda item: float(item.get("conviction_score") or 0.0), reverse=True)

    async def run_cycle(self) -> dict[str, Any]:
        started_at = datetime.now(UTC)
        errors: list[str] = []
        try:
            markets = await self._fetch_candidate_markets()
            oracle_context = await self._fetch_oracle_context()
            signals = await self._scan_markets(markets, oracle_context)
            for signal in signals:
                payload = asdict(signal) | {"detected_at": signal.detected_at.isoformat()}
                self._recent_signals.append(payload)
                self._last_signal_by_market[signal.market_id] = payload
            await self._reconcile_wallet_marks()
            self._prune_state()
            self._save_state()
            self._last_snapshot = {
                "running": self.is_running(),
                "updated_at": datetime.now(UTC).isoformat(),
                "markets_scanned": len(markets),
                "trades_seen": sum(len(rows) for rows in self._market_size_history.values()),
                "whales_detected": len(signals),
                "signals": list(self._recent_signals)[-25:][::-1],
                "oracles": oracle_context,
                "errors": errors,
                "duration_seconds": round((datetime.now(UTC) - started_at).total_seconds(), 2),
            }
            return self._last_snapshot
        except Exception as exc:  # noqa: BLE001
            logger.exception("Whale scanner cycle failed")
            errors.append(str(exc))
            self._last_snapshot = {
                "running": self.is_running(),
                "updated_at": datetime.now(UTC).isoformat(),
                "markets_scanned": 0,
                "trades_seen": 0,
                "whales_detected": 0,
                "signals": list(self._recent_signals)[-25:][::-1],
                "oracles": {},
                "errors": errors,
                "duration_seconds": round((datetime.now(UTC) - started_at).total_seconds(), 2),
            }
            return self._last_snapshot

    async def _fetch_candidate_markets(self) -> list[MarketResponse]:
        rows = await self.client.get_gamma_markets(
            active="true",
            closed="false",
            archived="false",
            limit=self.settings.whale_market_limit,
            order="volume",
            ascending="false",
        )
        markets: list[MarketResponse] = []
        for row in rows:
            outcomes = self._parse_json_list(row.get("outcomes"))
            prices = self._parse_json_list(row.get("outcomePrices"), cast=float)
            token_ids = self._parse_json_list(row.get("clobTokenIds") or row.get("tokenIds"))
            title = str(row.get("question") or row.get("title") or "")
            if not title:
                continue
            yes_index = outcomes.index("Yes") if "Yes" in outcomes else 0
            no_index = outcomes.index("No") if "No" in outcomes else 1
            yes_price = float(prices[yes_index]) if len(prices) > yes_index else float(row.get("bestAsk") or 0.5)
            no_price = float(prices[no_index]) if len(prices) > no_index else max(0.0, 1.0 - yes_price)
            markets.append(
                MarketResponse(
                    market_id=str(row.get("id") or row.get("conditionId") or ""),
                    question_id=str(row.get("questionID") or row.get("questionId") or "") or None,
                    slug=str(row.get("slug") or "") or None,
                    title=title,
                    yes_token_id=str(token_ids[yes_index]) if len(token_ids) > yes_index else None,
                    no_token_id=str(token_ids[no_index]) if len(token_ids) > no_index else None,
                    yes_price=yes_price,
                    no_price=no_price,
                    volume=float(row.get("volumeNum") or row.get("volume") or 0.0),
                    liquidity=float(row.get("liquidityNum") or row.get("liquidity") or 0.0),
                    end_date=self._parse_dt(row.get("endDate") or row.get("end_date_iso")),
                    active=bool(row.get("active", True)),
                )
            )
        return [market for market in markets if market.market_id and market.active]

    async def _scan_markets(self, markets: list[MarketResponse], oracle_context: dict[str, Any]) -> list[WhaleSignal]:
        semaphore = asyncio.Semaphore(max(1, self.settings.whale_trade_concurrency))

        async def worker(market: MarketResponse) -> list[WhaleSignal]:
            async with semaphore:
                return await self._scan_market(market, oracle_context)

        batches = await asyncio.gather(*(worker(market) for market in markets), return_exceptions=True)
        signals: list[WhaleSignal] = []
        for batch in batches:
            if isinstance(batch, Exception):
                logger.debug("Whale scanner worker failed: %s", batch)
                continue
            signals.extend(batch)
        return sorted(signals, key=lambda item: item.conviction_score, reverse=True)

    async def _scan_market(self, market: MarketResponse, oracle_context: dict[str, Any]) -> list[WhaleSignal]:
        raw_trades = await self.client.get_public_trades(
            market_id=market.market_id,
            slug=market.slug,
            limit=self.settings.whale_trade_lookback,
        )
        trades = self._normalize_trades(raw_trades, market)
        if not trades:
            return []

        ordered = sorted(trades, key=lambda item: item.timestamp)
        for trade in ordered:
            self._market_size_history[market.market_id].append(trade.size_usdc)

        baseline = max(self.settings.whale_min_trade_size_usdc, self._market_median_size(market.market_id))
        threshold = baseline * self.settings.whale_threshold_multiplier
        market_move = self._market_momentum(market)
        results: list[WhaleSignal] = []
        for trade in reversed(ordered):
            if trade.trade_id in self._seen_trade_id_set:
                continue
            self._register_trade_id(trade.trade_id)
            if trade.size_usdc < threshold:
                continue
            signal = self._build_signal(
                market=market,
                trade=trade,
                threshold=threshold,
                market_move=market_move,
                oracle_context=oracle_context,
                recent_trades=ordered,
            )
            if signal is None:
                continue
            results.append(signal)
            self._pending_wallet_marks.append(
                {
                    "wallet": trade.wallet,
                    "market_id": trade.market_id,
                    "side": trade.side,
                    "title": trade.title,
                    "closes_at": market.end_date.isoformat() if market.end_date else None,
                    "trade_id": trade.trade_id,
                    "tracked_at": datetime.now(UTC).isoformat(),
                }
            )
        return results

    def _build_signal(
        self,
        *,
        market: MarketResponse,
        trade: WhaleTrade,
        threshold: float,
        market_move: float,
        oracle_context: dict[str, Any],
        recent_trades: list[WhaleTrade],
    ) -> WhaleSignal | None:
        relative_size = trade.size_usdc / max(threshold, 1.0)
        wallet_win_rate = self._wallet_win_rate(trade.wallet)
        cluster_count = self._cluster_count(market.market_id, trade.side)
        same_side_last_8 = sum(1 for row in recent_trades[-8:] if row.side == trade.side)
        momentum_alignment = (trade.side == "YES" and market_move >= 0) or (trade.side == "NO" and market_move <= 0)
        category, keyword_hits = self._classify_market(market.title, market.slug or "")
        crypto_alignment = self._crypto_alignment(category, trade.side, oracle_context)
        oracle_alignment = self._oracle_alignment(category, market.title, trade.side, oracle_context)

        conviction = 0.0
        reasons: list[str] = []

        relative_component = min(35.0, max(0.0, (relative_size - 1.0) * 20.0 + 12.0))
        conviction += relative_component
        reasons.append(f"relative_size:{relative_size:.2f}x")

        wallet_component = min(15.0, max(0.0, (wallet_win_rate - 0.5) * 30.0))
        if wallet_component > 0:
            conviction += wallet_component
            reasons.append(f"wallet_win_rate:{wallet_win_rate:.2%}")

        cluster_component = min(12.0, float(cluster_count) * 4.0)
        if cluster_component > 0:
            conviction += cluster_component
            reasons.append(f"cluster:{cluster_count}")

        if same_side_last_8 >= 5:
            conviction += min(10.0, float(same_side_last_8))
            reasons.append(f"same_side_last_8:{same_side_last_8}")

        absolute_component = self._absolute_trade_component(trade.size_usdc)
        conviction += absolute_component
        if absolute_component > 0:
            reasons.append(f"absolute_size:${trade.size_usdc:,.0f}")

        category_component = self._category_component(category, keyword_hits)
        if category_component > 0:
            conviction += category_component
            reasons.append(f"category:{category}")

        if momentum_alignment:
            conviction += 6.0
            reasons.append("momentum_alignment")

        if crypto_alignment:
            conviction += 6.0
            reasons.append("crypto_alignment")

        if oracle_alignment > 0:
            conviction += oracle_alignment
            reasons.append(f"oracle_alignment:{oracle_alignment:.1f}")

        conviction = max(0.0, min(100.0, conviction))
        tier, position_fraction = self._tier(conviction)
        if conviction < self.settings.whale_min_conviction_score:
            return None
        return WhaleSignal(
            trade_id=trade.trade_id,
            market_id=trade.market_id,
            title=trade.title,
            slug=trade.slug,
            side=trade.side,
            price=round(trade.price, 4),
            size_usdc=round(trade.size_usdc, 2),
            threshold_usdc=round(threshold, 2),
            relative_size=round(relative_size, 2),
            wallet=trade.wallet,
            wallet_win_rate=round(wallet_win_rate, 4),
            cluster_count=cluster_count,
            same_side_last_8=same_side_last_8,
            momentum_alignment=momentum_alignment,
            crypto_alignment=crypto_alignment,
            oracle_alignment=round(oracle_alignment, 2),
            category=category,
            keyword_hits=keyword_hits,
            conviction_score=round(conviction, 2),
            tier=tier,
            position_fraction=position_fraction,
            detected_at=trade.timestamp,
            reasons=reasons,
        )

    async def _fetch_oracle_context(self) -> dict[str, Any]:
        tasks = await asyncio.gather(
            self._fetch_binance_context(),
            self._fetch_fear_greed_context(),
            self._fetch_weather_context(),
            self._fetch_economic_context(),
            self._fetch_sports_context(),
            self._fetch_polling_context(),
            return_exceptions=True,
        )
        names = ("binance", "fear_greed", "weather", "economics", "sports", "polling")
        oracles: dict[str, Any] = {}
        for name, payload in zip(names, tasks, strict=False):
            if isinstance(payload, Exception):
                oracles[name] = {"available": False, "error": str(payload)}
            else:
                oracles[name] = payload
        return {"oracles": oracles}

    async def _fetch_binance_context(self) -> dict[str, Any]:
        response = await self.client.get_external_json(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": "BTCUSDT"},
        )
        return {
            "available": True,
            "last_price": float(response.get("lastPrice") or 0.0),
            "price_change_percent": float(response.get("priceChangePercent") or 0.0),
        }

    async def _fetch_fear_greed_context(self) -> dict[str, Any]:
        response = await self.client.get_external_json("https://api.alternative.me/fng/")
        rows = response.get("data") if isinstance(response, dict) else []
        row = rows[0] if isinstance(rows, list) and rows else {}
        return {
            "available": True,
            "value": float(row.get("value") or 0.0),
            "classification": str(row.get("value_classification") or ""),
        }

    async def _fetch_weather_context(self) -> dict[str, Any]:
        response = await self.client.get_external_json(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": self.settings.whale_weather_latitude,
                "longitude": self.settings.whale_weather_longitude,
                "current": "temperature_2m,precipitation,wind_speed_10m",
            },
        )
        current = response.get("current", {}) if isinstance(response, dict) else {}
        return {
            "available": True,
            "temperature_c": float(current.get("temperature_2m") or 0.0),
            "precipitation": float(current.get("precipitation") or 0.0),
            "wind_speed": float(current.get("wind_speed_10m") or 0.0),
        }

    async def _fetch_economic_context(self) -> dict[str, Any]:
        inflation = await self._fetch_fred_series("CPIAUCSL")
        unemployment = await self._fetch_fred_series("UNRATE")
        fedfunds = await self._fetch_fred_series("FEDFUNDS")
        return {
            "available": True,
            "inflation_latest": inflation,
            "unemployment_latest": unemployment,
            "fedfunds_latest": fedfunds,
        }

    async def _fetch_sports_context(self) -> dict[str, Any]:
        response = await self.client.get_external_json(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
        )
        events = response.get("events", []) if isinstance(response, dict) else []
        live_games = 0
        for event in events:
            state = str(event.get("status", {}).get("type", {}).get("state") or "").lower()
            if state == "in":
                live_games += 1
        return {
            "available": True,
            "events": len(events),
            "live_games": live_games,
        }

    async def _fetch_polling_context(self) -> dict[str, Any]:
        response = await self.client.get_external_json(
            "https://projects.fivethirtyeight.com/polls-page/president-general/national/polls.json"
        )
        rows = response if isinstance(response, list) else []
        return {
            "available": True,
            "recent_polls": len(rows[:10]),
        }

    async def _fetch_fred_series(self, series_id: str) -> float:
        csv_text = await self.client.get_external_text(
            "https://fred.stlouisfed.org/graph/fredgraph.csv",
            params={"id": series_id},
        )
        lines = [line.strip() for line in csv_text.splitlines() if line.strip()]
        for line in reversed(lines[1:]):
            parts = line.split(",")
            if len(parts) < 2 or parts[1] == ".":
                continue
            try:
                return float(parts[1])
            except ValueError:
                continue
        return 0.0

    async def _reconcile_wallet_marks(self) -> None:
        remaining: list[dict[str, Any]] = []
        for row in self._pending_wallet_marks:
            closes_at = self._parse_dt(row.get("closes_at"))
            if closes_at is None or closes_at > datetime.now(UTC) - timedelta(minutes=2):
                remaining.append(row)
                continue
            market_id = str(row.get("market_id") or "")
            try:
                market = await self.client.get_market(market_id)
            except Exception:  # noqa: BLE001
                remaining.append(row)
                continue
            winning_outcome = self._resolved_outcome(market)
            if winning_outcome is None:
                remaining.append(row)
                continue
            wallet = str(row.get("wallet") or "")
            if not wallet:
                continue
            stats = self._wallet_stats.setdefault(wallet, {"wins": 0.0, "losses": 0.0})
            if str(row.get("side")).upper() == winning_outcome:
                stats["wins"] = float(stats.get("wins") or 0.0) + 1.0
            else:
                stats["losses"] = float(stats.get("losses") or 0.0) + 1.0
        self._pending_wallet_marks = remaining

    def _resolved_outcome(self, market: dict[str, Any]) -> str | None:
        prices = self._parse_json_list(market.get("outcomePrices"), cast=float)
        if len(prices) < 2:
            return None
        yes_price = float(prices[0])
        no_price = float(prices[1])
        if yes_price >= 0.97:
            return "YES"
        if no_price >= 0.97:
            return "NO"
        return None

    def _wallet_win_rate(self, wallet: str) -> float:
        stats = self._wallet_stats.get(wallet)
        if not stats:
            return 0.5
        wins = float(stats.get("wins") or 0.0)
        losses = float(stats.get("losses") or 0.0)
        total = wins + losses
        return 0.5 if total <= 0 else wins / total

    def _cluster_count(self, market_id: str, side: str) -> int:
        count = 0
        cutoff = datetime.now(UTC) - timedelta(minutes=5)
        for row in self._recent_signals:
            if str(row.get("market_id")) != market_id or str(row.get("side")) != side:
                continue
            try:
                detected = datetime.fromisoformat(str(row.get("detected_at")))
            except ValueError:
                continue
            if detected >= cutoff:
                count += 1
        return count

    def _market_median_size(self, market_id: str) -> float:
        history = list(self._market_size_history.get(market_id, []))
        if not history:
            return self.settings.whale_min_trade_size_usdc
        return float(median(history))

    @staticmethod
    def _market_momentum(market: MarketResponse) -> float:
        return round(market.yes_price - market.no_price, 4)

    def _tier(self, conviction: float) -> tuple[int, float]:
        if conviction >= 85:
            return 1, self.settings.whale_tier_1_fraction
        if conviction >= 70:
            return 2, self.settings.whale_tier_2_fraction
        if conviction >= 60:
            return 3, self.settings.whale_tier_3_fraction
        return 4, self.settings.whale_tier_4_fraction

    def _absolute_trade_component(self, size_usdc: float) -> float:
        if size_usdc >= self.settings.whale_absolute_tier_3_usdc:
            return 15.0
        if size_usdc >= self.settings.whale_absolute_tier_2_usdc:
            return 10.0
        if size_usdc >= self.settings.whale_absolute_tier_1_usdc:
            return 5.0
        return 0.0

    @staticmethod
    def _category_component(category: str, keyword_hits: list[str]) -> float:
        base = {
            "geopolitics": 7.0,
            "crypto": 6.0,
            "economics": 6.0,
            "sports": 4.0,
            "weather": 4.0,
            "polling": 5.0,
        }.get(category, 0.0)
        return min(8.0, base + min(len(keyword_hits), 2))

    def _crypto_alignment(self, category: str, side: str, oracle_context: dict[str, Any]) -> bool:
        if category != "crypto":
            return False
        binance = oracle_context.get("oracles", {}).get("binance", {})
        fear_greed = oracle_context.get("oracles", {}).get("fear_greed", {})
        btc_change = float(binance.get("price_change_percent") or 0.0)
        fear_value = float(fear_greed.get("value") or 0.0)
        if side == "YES":
            return btc_change > 0 and fear_value >= 45
        return btc_change < 0 or fear_value <= 40

    def _oracle_alignment(self, category: str, title: str, side: str, oracle_context: dict[str, Any]) -> float:
        score = 0.0
        lower_title = title.lower()
        oracles = oracle_context.get("oracles", {})
        if category == "weather":
            weather = oracles.get("weather", {})
            precip = float(weather.get("precipitation") or 0.0)
            wind = float(weather.get("wind_speed") or 0.0)
            if side == "YES" and ("rain" in lower_title or "storm" in lower_title) and (precip > 0 or wind > 20):
                score += 6.0
        if category == "economics":
            economics = oracles.get("economics", {})
            unemployment = float(economics.get("unemployment_latest") or 0.0)
            fedfunds = float(economics.get("fedfunds_latest") or 0.0)
            if side == "YES" and ("rate" in lower_title or "fed" in lower_title) and fedfunds >= 4:
                score += 4.0
            if side == "YES" and ("unemployment" in lower_title or "jobless" in lower_title) and unemployment >= 4:
                score += 4.0
        if category == "sports" and int(oracles.get("sports", {}).get("live_games") or 0) > 0:
            score += 3.0
        if category == "polling" and int(oracles.get("polling", {}).get("recent_polls") or 0) > 0:
            score += 3.0
        return min(8.0, score)

    @staticmethod
    def _classify_market(title: str, slug: str) -> tuple[str, list[str]]:
        text = f"{title} {slug}".lower()
        taxonomy = {
            "geopolitics": ("war", "ceasefire", "china", "taiwan", "ukraine", "nato", "tariff", "israel", "iran"),
            "crypto": ("bitcoin", "btc", "ethereum", "eth", "solana", "crypto", "doge"),
            "economics": ("inflation", "cpi", "fed", "gdp", "payroll", "unemployment", "rate", "recession"),
            "weather": ("weather", "hurricane", "storm", "rain", "snow", "temperature"),
            "sports": ("nba", "nfl", "mlb", "nhl", "finals", "match", "championship", "game"),
            "polling": ("poll", "approval", "vote share", "primary", "election"),
        }
        best_category = "general"
        hits: list[str] = []
        for category, keywords in taxonomy.items():
            matched = [keyword for keyword in keywords if keyword in text]
            if len(matched) > len(hits):
                best_category = category
                hits = matched
        return best_category, hits[:4]

    def _normalize_trades(self, rows: list[dict[str, Any]], market: MarketResponse) -> list[WhaleTrade]:
        trades: list[WhaleTrade] = []
        for row in rows:
            trade_id = str(
                row.get("id")
                or row.get("tradeID")
                or row.get("tradeId")
                or row.get("transactionHash")
                or row.get("hash")
                or ""
            )
            if not trade_id:
                continue
            timestamp = self._parse_dt(
                row.get("timestamp")
                or row.get("createdAt")
                or row.get("created_at")
                or row.get("matchedAt")
                or row.get("time")
            )
            if timestamp is None:
                continue
            size = float(
                row.get("usdcSize")
                or row.get("notional")
                or row.get("amount")
                or row.get("size")
                or row.get("quantity")
                or 0.0
            )
            price = float(row.get("price") or row.get("rate") or row.get("outcomePrice") or 0.0)
            if size and price and size < 5:
                size = size * price
            if not size and price:
                shares = float(row.get("shares") or row.get("quantity") or 0.0)
                size = shares * price
            if size <= 0:
                continue
            side = self._normalize_side(row, market)
            wallet = str(
                row.get("proxyWallet")
                or row.get("wallet")
                or row.get("makerAddress")
                or row.get("takerAddress")
                or row.get("owner")
                or "unknown"
            )
            trades.append(
                WhaleTrade(
                    trade_id=trade_id,
                    market_id=market.market_id,
                    title=market.title,
                    slug=market.slug or "",
                    side=side,
                    size_usdc=size,
                    price=price or (market.yes_price if side == "YES" else market.no_price),
                    wallet=wallet,
                    timestamp=timestamp,
                )
            )
        return trades

    def _normalize_side(self, row: dict[str, Any], market: MarketResponse) -> str:
        for key in ("side", "outcome", "position", "tokenOutcome"):
            raw = str(row.get(key) or "").upper()
            if raw in {"YES", "NO"}:
                return raw
        token_id = str(row.get("tokenID") or row.get("tokenId") or row.get("asset_id") or "")
        if token_id and market.no_token_id and token_id == market.no_token_id:
            return "NO"
        return "YES"

    def _register_trade_id(self, trade_id: str) -> None:
        if trade_id in self._seen_trade_id_set:
            return
        if self._seen_trade_ids.maxlen and len(self._seen_trade_ids) == self._seen_trade_ids.maxlen:
            removed = self._seen_trade_ids.popleft()
            self._seen_trade_id_set.discard(removed)
        self._seen_trade_ids.append(trade_id)
        self._seen_trade_id_set.add(trade_id)

    def _prune_state(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(hours=self.settings.whale_state_retention_hours)
        self._recent_signals = deque(
            [
                row
                for row in self._recent_signals
                if self._parse_dt(row.get("detected_at")) and self._parse_dt(row.get("detected_at")) >= cutoff
            ],
            maxlen=300,
        )
        self._pending_wallet_marks = [
            row
            for row in self._pending_wallet_marks
            if self._parse_dt(row.get("tracked_at")) and self._parse_dt(row.get("tracked_at")) >= cutoff
        ]

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _save_state(self) -> None:
        payload = {
            "recent_trade_ids": list(self._seen_trade_ids),
            "wallet_stats": self._wallet_stats,
            "pending_wallet_marks": self._pending_wallet_marks[-500:],
            "signals": list(self._recent_signals)[-300:],
            "market_size_history": {
                market_id: list(history)[-200:]
                for market_id, history in self._market_size_history.items()
                if history
            },
        }
        self.state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _parse_dt(raw: object) -> datetime | None:
        if not raw:
            return None
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None

    @staticmethod
    def _parse_json_list(raw: object, cast: Any = str) -> list[Any]:
        if isinstance(raw, list):
            return [cast(item) for item in raw]
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return []
            if isinstance(parsed, list):
                return [cast(item) for item in parsed]
        return []
