"""Main loop for the late-favourite strategy.  Usage:  python run.py"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime

from config import Settings, get_settings
from dashboard import serve_in_background
from fees import fee_usdc
from ledger import Ledger
from model import estimate_up
from pm_client import TIMEFRAME_SECONDS, PolymarketClient, UpDownMarket
from price_feed import BybitSpotFeed, ChainlinkTwapFeed, bybit_kline_sigma_per_sec
from strategy import MarketView, evaluate

logger = logging.getLogger("late_favourite")


class Bot:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.pm = PolymarketClient(settings)
        self.chainlink = ChainlinkTwapFeed(settings.rtds_ws_url)
        self.bybit = BybitSpotFeed(settings.bybit_ws_url)
        self.ledger = Ledger(settings.data_dir / "trades.json", settings.paper_bankroll_usdc)
        self.decisions_path = settings.data_dir / "decisions.jsonl"
        self._markets: dict[str, UpDownMarket | None] = {}
        self._kline_sigma = (0.0, 0.0)          # (fetched_at, value)
        self._live_balance = (0.0, 0.0)
        self._last_reason: dict[str, str] = {}
        self._next_settle_check: dict[str, float] = {}
        self._status_written_at = 0.0

    # --- helpers ---
    async def sigma(self) -> float:
        fetched_at, value = self._kline_sigma
        if time.time() - fetched_at > 60:
            try:
                value = await bybit_kline_sigma_per_sec(self.pm.http, self.s.bybit_rest_url)
                self._kline_sigma = (time.time(), value)
            except Exception as exc:  # noqa: BLE001
                logger.warning("kline sigma failed: %s", exc)
        tick_sigma = self.bybit.series.sigma_per_sec(lookback_seconds=900, step=5)
        return max(value, tick_sigma, self.s.min_sigma_per_sec) * self.s.vol_multiplier

    async def bankroll(self) -> float:
        if self.s.dry_run:
            return self.ledger.paper_bankroll()
        fetched_at, value = self._live_balance
        if time.time() - fetched_at > 30:
            value = await self.pm.get_usdc_balance()
            self._live_balance = (time.time(), value)
        return value

    async def write_status(self, now: float, running: bool = True) -> None:
        """Small status file for the dashboard (mode, cash). Written every few seconds."""
        if running and now - self._status_written_at < 5:
            return
        self._status_written_at = now
        try:
            cash = await self.bankroll()
        except Exception as exc:  # noqa: BLE001
            logger.warning("balance lookup failed: %s", exc)
            cash = None
        status = {"mode": "paper" if self.s.dry_run else "live", "running": running,
                  "updated_at": datetime.now(UTC).isoformat(), "cash_usdc": cash}
        path = self.s.data_dir / "status.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(status), encoding="utf-8")

    async def market(self, timeframe: str, start: int) -> UpDownMarket | None:
        slug = f"btc-updown-{timeframe}-{start}"
        if slug not in self._markets:
            self._markets[slug] = await self.pm.get_market(timeframe, start)
        return self._markets[slug]

    def log_decision(self, slug: str, reason: str, details: dict) -> None:
        if self._last_reason.get(slug) == reason and reason != "enter":
            return
        self._last_reason[slug] = reason
        row = {"ts": datetime.now(UTC).isoformat(), "slug": slug, "reason": reason, **details}
        self.decisions_path.parent.mkdir(parents=True, exist_ok=True)
        with self.decisions_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
        logger.info("%s %s %s", slug, reason, {k: details.get(k) for k in ("side", "p_model", "best_ask", "edge", "seconds_left")})

    # --- one market ---
    async def consider(self, timeframe: str, now: float) -> None:
        step = TIMEFRAME_SECONDS[timeframe]
        start = int(now // step * step)
        end = start + step
        seconds_left = end - now
        slug = f"btc-updown-{timeframe}-{start}"
        if seconds_left > self.s.max_seconds_left(timeframe) or seconds_left < self.s.min_seconds_left:
            return
        if self.ledger.has_traded(slug):
            return
        market = await self.market(timeframe, start)
        if market is None or not market.accepting_orders or market.closed:
            return
        # The price to beat is the Chainlink TWAP value at the window's first second; prefer our own
        # buffer (no HTTP call) and fall back to Polymarket's crypto-price endpoint.
        strike = self.chainlink.series.at(start)
        if strike is None:
            strike = await self.pm.get_strike(timeframe, start)
        if strike is None:
            self.log_decision(slug, "no_price_to_beat_yet", {"seconds_left": round(seconds_left, 1)})
            return

        # Map Bybit onto Chainlink's level with the basis over the last TWAP window.
        # Chainlink ticks every second, so its age is a real staleness signal. Bybit only pushes on
        # quote changes, so for it we require a live connection (any frame recently) instead.
        latest_cl = self.chainlink.series.latest()
        latest_bb = self.bybit.current(now)
        if latest_cl is None or latest_bb is None:
            return
        cl_age = self.chainlink.series.age_seconds(now)
        bb_silence = now - self.bybit.last_message_at
        if cl_age > self.s.max_price_age_seconds or bb_silence > 3 * self.s.max_price_age_seconds:
            self.log_decision(slug, "stale_price_feed", {"seconds_left": round(seconds_left, 1),
                                                         "chainlink_age": round(cl_age, 1), "bybit_silence": round(bb_silence, 1)})
            return
        L = market.twap_seconds
        cl_second, twap_now = latest_cl
        bb_avg = self.bybit.series.average(cl_second - max(L, 1), cl_second)
        if bb_avg is None or bb_avg[1] < max(L, 1) * 0.5:
            self.log_decision(slug, "warming_up_feeds", {"seconds_left": round(seconds_left, 1)})
            return
        basis = twap_now - bb_avg[0] if L else 0.0
        spot = latest_bb[1] + basis
        observed = None
        if L and seconds_left < L:
            obs = self.bybit.series.average(end - L, int(now))
            observed = obs[0] + basis if obs else None

        est = estimate_up(spot=spot, strike=strike, seconds_left=seconds_left, sigma_per_sec=await self.sigma(),
                          twap_seconds=L, observed_twap_avg=observed)

        # Only hit the order book when the model is already confident enough.
        side = "Up" if est.p_up >= 0.5 else "Down"
        if est.p_side(side) - self.s.model_haircut < self.s.min_win_probability:
            self.log_decision(slug, "probability_below_90", {"side": side, "p_model": round(est.p_side(side) - self.s.model_haircut, 4),
                                                            "seconds_left": round(seconds_left, 1), "strike": strike, "spot": round(spot, 2)})
            return
        asks = {side: await self.pm.get_asks(market.tokens[side])}
        view = MarketView(slug=slug, timeframe=timeframe, seconds_left=seconds_left, strike=strike, spot=spot,
                          fee_schedule=market.fee_schedule, asks=asks)
        decision = evaluate(view, est, self.s, await self.bankroll(), self.ledger.open_exposure())
        decision.details.update(basis=round(basis, 2), twap_now=twap_now)
        self.log_decision(slug, decision.reason, decision.details)
        if decision.enter:
            await self.enter(market, decision)

    async def enter(self, market: UpDownMarket, decision) -> None:
        fill = decision.fill
        fields = dict(slug=market.slug, timeframe=market.timeframe, side=decision.side, token_id=market.tokens[decision.side],
                      window_end=market.end, p_model=round(decision.p_model, 5), edge_per_share=round(decision.edge_per_share, 5),
                      strike=decision.details.get("strike"), spot_at_entry=decision.details.get("spot"),
                      seconds_left=decision.details.get("seconds_left"))
        if self.s.dry_run:
            self.ledger.record_entry(mode="paper", shares=round(fill.shares, 6), avg_price=round(fill.avg_price, 6),
                                     fee_usdc=fill.fee_usdc, **fields)
            logger.info("PAPER BUY %s %s %.2f sh @ %.4f fee %.4f", market.slug, decision.side, fill.shares, fill.avg_price, fill.fee_usdc)
            return
        worst = min(self.s.max_entry_price, round(fill.worst_price + market.tick_size, 2))
        try:
            resp = await self.pm.buy_fok(market.tokens[decision.side], decision.budget_usdc, worst)
        except Exception as exc:  # noqa: BLE001
            logger.warning("order failed %s: %s", market.slug, exc)
            self.log_decision(market.slug, "order_failed", {"error": str(exc)})
            return
        if not resp.get("success") or resp.get("status") != "matched":
            self.log_decision(market.slug, "order_not_filled", {"response": resp})
            return
        spent = float(resp.get("makingAmount") or decision.budget_usdc)
        shares = float(resp.get("takingAmount") or fill.shares)
        avg_price = spent / shares if shares else fill.avg_price
        self.ledger.record_entry(mode="live", shares=shares, avg_price=round(avg_price, 6),
                                 fee_usdc=fee_usdc(shares, avg_price, market.fee_schedule),
                                 order_id=resp.get("orderID"), **fields)
        self._live_balance = (0.0, 0.0)
        logger.info("LIVE BUY %s %s %.2f sh @ %.4f", market.slug, decision.side, shares, avg_price)

    async def settle_open(self, now: float) -> None:
        for trade in self.ledger.open_trades():
            if now < trade["window_end"] + 10 or now < self._next_settle_check.get(trade["id"], 0):
                continue
            self._next_settle_check[trade["id"]] = now + 20
            tf, start = trade["timeframe"], int(trade["slug"].rsplit("-", 1)[1])
            try:
                market = await self.pm.get_market(tf, start)
            except Exception as exc:  # noqa: BLE001
                logger.warning("settle lookup failed %s: %s", trade["slug"], exc)
                continue
            if market and market.winning_side:
                t = self.ledger.settle(trade["id"], market.winning_side)
                logger.info("SETTLED %s %s -> %s net %.4f", t["slug"], t["side"], t["status"], t["net_pnl"])

    # --- loop ---
    async def run(self) -> None:
        self.chainlink.start()
        self.bybit.start()
        dashboard = serve_in_background(self.s)
        logger.info("late-favourite bot started (dry_run=%s, timeframes=%s)", self.s.dry_run, self.s.timeframe_list)
        try:
            while True:
                now = time.time()
                try:
                    await self.settle_open(now)
                    await self.write_status(now)
                    if self.ledger.daily_pnl() <= -self.s.daily_loss_limit_usdc:
                        logger.warning("daily loss limit hit; not entering new trades today")
                    else:
                        for tf in self.s.timeframe_list:
                            await self.consider(tf, now)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("cycle error: %s", exc)
                if len(self._markets) > 50:
                    self._markets = dict(list(self._markets.items())[-10:])
                await asyncio.sleep(self.s.loop_interval_seconds)
        finally:
            await self.write_status(time.time(), running=False)
            if dashboard:
                dashboard.shutdown()
            await self.chainlink.stop()
            await self.bybit.stop()
            await self.pm.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(Bot(get_settings()).run())


if __name__ == "__main__":
    main()
