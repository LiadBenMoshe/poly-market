"""Calibration backtest: does "p_model >= 90%" really win >= 90% of the time?

For each past 5m/15m window it takes Polymarket's official open (price to beat) and the market's
actual resolution as ground truth, replays Binance 1-second BTC prices, and runs the exact
live model at every checkpoint inside the entry zone. The first checkpoint per window where
the model clears the threshold is what the bot would trade.

It cannot know historical order-book prices, so it reports the MAX price you could have
paid (after fees) and still broken even, per probability bucket.

Usage:  python backtest.py --days 3 [--timeframes 5m,15m]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from pathlib import Path

import httpx

from config import get_settings
from fees import FeeSchedule, fee_per_share
from ledger import CALIBRATION_BUCKETS
from model import estimate_up, realized_sigma_per_sec
from pm_client import TIMEFRAME_SECONDS, PolymarketClient

BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"
CHECKPOINT_STEP = 5


async def load_binance_seconds(http: httpx.AsyncClient, start: int, end: int, cache: Path) -> dict[int, float]:
    cache.mkdir(parents=True, exist_ok=True)
    prices: dict[int, float] = {}
    sem = asyncio.Semaphore(6)

    async def chunk(t0: int) -> None:
        path = cache / f"binance_1s_{t0}.json"
        if path.exists():
            rows = json.loads(path.read_text())
        else:
            async with sem:
                for attempt in range(5):
                    resp = await http.get(BINANCE_KLINES, params={"symbol": "BTCUSDT", "interval": "1s",
                                                                  "startTime": t0 * 1000, "limit": 1000})
                    if resp.status_code == 200:
                        break
                    await asyncio.sleep(2 ** attempt)
                resp.raise_for_status()
                rows = [[r[0] // 1000, float(r[4])] for r in resp.json()]
            if t0 + 1000 < time.time() - 60:
                path.write_text(json.dumps(rows))
        for second, close in rows:
            prices[int(second)] = close

    starts = range(start - start % 1000, end, 1000)
    await asyncio.gather(*(chunk(t0) for t0 in starts))
    return prices


async def load_official(pm: PolymarketClient, windows: list[tuple[str, int]], cache: Path) -> dict[tuple[str, int], dict]:
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / "official_prices.json"
    stored = json.loads(path.read_text()) if path.exists() else {}
    sem = asyncio.Semaphore(3)
    failed = 0

    async def one(tf: str, start: int) -> None:
        nonlocal failed
        key = f"{tf}:{start}"
        if key in stored and stored[key].get("winner"):
            return
        data: dict = {}
        winner = None
        async with sem:
            for attempt in range(6):
                try:
                    data = await pm.get_crypto_price(tf, start)
                    market = await pm.get_market(tf, start)
                    winner = market.winning_side if market else None
                    break
                except httpx.HTTPError:
                    await asyncio.sleep(1.5 * 2 ** attempt)  # rate limited: back off
        if winner and data.get("openPrice") is not None:
            stored[key] = {"open": float(data["openPrice"]), "winner": winner}
        else:
            failed += 1

    try:
        await asyncio.gather(*(one(tf, s) for tf, s in windows))
    finally:
        path.write_text(json.dumps(stored))
    if failed:
        print(f"  ({failed} windows had no official result and are skipped)")
    return {(k.split(":")[0], int(k.split(":")[1])): v for k, v in stored.items()}


def avg(prices: dict[int, float], lo: int, hi: int) -> float | None:
    """Mean over (lo, hi], forward-filled."""
    last, total, n = None, 0.0, 0
    for s in range(lo - 5, hi + 1):
        if s in prices:
            last = prices[s]
        if s > lo and last is not None:
            total += last
            n += 1
    return total / n if n else None


def sigma_at(prices: dict[int, float], t: int, settings) -> float:
    ticks = [prices[s] for s in range(t - 900, t + 1, 5) if s in prices]
    minutes = [prices[s] for s in range(t - 3600, t + 1, 60) if s in prices]
    return max(realized_sigma_per_sec(ticks, 5), realized_sigma_per_sec(minutes, 60),
               settings.min_sigma_per_sec) * settings.vol_multiplier


def max_profitable_price(win_rate: float, schedule: FeeSchedule) -> float:
    """Highest price p with p + fee_per_share(p) <= win_rate."""
    lo, hi = 0.0, 1.0
    for _ in range(50):
        mid = (lo + hi) / 2
        if mid + fee_per_share(mid, schedule) <= win_rate:
            lo = mid
        else:
            hi = mid
    return lo


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=float, default=3)
    parser.add_argument("--timeframes", default=None)
    args = parser.parse_args()
    s = get_settings()
    tfs = (args.timeframes or s.timeframes).split(",")
    cache = s.data_dir / "backtest_cache"
    end = int(time.time()) // 900 * 900 - 900
    begin = end - int(args.days * 86400)

    windows = [(tf, st) for tf in tfs for st in range(begin, end, TIMEFRAME_SECONDS[tf])]
    pm = PolymarketClient(s)
    try:
        print(f"Loading {len(windows)} official window results and {int(args.days * 86400)} seconds of BTC prices...")
        official = await load_official(pm, windows, cache)
        prices = await load_binance_seconds(pm.http, begin - 3700, end + 60, cache)
    finally:
        await pm.aclose()

    schedule = FeeSchedule(rate=s.fallback_fee_rate)
    trades: list[tuple[float, bool, str]] = []      # (p_model, won, timeframe)
    checkpoints: list[tuple[float, bool]] = []
    for tf, start in windows:
        res = official.get((tf, start))
        if not res:
            continue
        stop = start + TIMEFRAME_SECONDS[tf]
        # Ground truth is Polymarket's actual resolution (official close >= open is wrong ~1 in 10 5m windows).
        strike, up_won = res["open"], res["winner"] == "Up"
        ref = avg(prices, start - 60, start)
        if ref is None:
            continue
        basis = strike - prices.get(start, ref)   # Chainlink spot at open vs Binance spot (no lookahead)
        traded = False
        for tau in range(s.max_seconds_left(tf), s.min_seconds_left - 1, -CHECKPOINT_STEP):
            t = stop - tau
            if t not in prices:
                continue
            observed = avg(prices, stop - 60, t)
            est = estimate_up(spot=prices[t] + basis, strike=strike, seconds_left=tau,
                              sigma_per_sec=sigma_at(prices, t, s), twap_seconds=60,
                              observed_twap_avg=(observed + basis) if (observed is not None and tau < 60) else None)
            side = "Up" if est.p_up >= 0.5 else "Down"
            p_model = est.p_side(side) - s.model_haircut
            if p_model < s.min_win_probability or abs(est.settle_mean - strike) < s.min_distance_usd:
                continue
            won = (side == "Up") == up_won
            checkpoints.append((p_model, won))
            if not traded:
                trades.append((p_model, won, tf))
                traded = True

    evaluated = sum(1 for w in windows if w in official)
    print(f"\nWindows with official results: {evaluated}")
    print(f"Windows where the bot would have had a >= {s.min_win_probability:.0%} signal: {len(trades)} "
          f"({len(trades) / max(evaluated, 1):.1%})")
    if not trades:
        return
    wins = sum(w for _, w, _ in trades)
    print(f"First-signal win rate: {wins}/{len(trades)} = {wins / len(trades):.2%}  "
          f"(avg predicted {sum(p for p, _, _ in trades) / len(trades):.2%})")
    for tf in tfs:
        sub = [w for _, w, t in trades if t == tf]
        if sub:
            print(f"  {tf}: {sum(sub)}/{len(sub)} = {sum(sub) / len(sub):.2%}")

    print("\nCalibration of first signals per window (what the bot trades):")
    print("  bucket        n    predicted  realized  95% low   max price to profit (after fee)")
    for lo, hi in CALIBRATION_BUCKETS:
        b = [(p, w) for p, w, _ in trades if lo <= p < hi]
        if not b:
            continue
        n = len(b)
        rate = sum(w for _, w in b) / n
        # Wilson lower bound: a conservative win rate given the sample size
        z = 1.96
        low = (rate + z * z / (2 * n) - z * math.sqrt(rate * (1 - rate) / n + z * z / (4 * n * n))) / (1 + z * z / n)
        print(f"  {lo:.2f}-{min(hi, 1):.2f}  {n:5d}   {sum(p for p, _ in b) / n:7.2%}   {rate:7.2%}  {low:7.2%}   "
              f"{max_profitable_price(rate, schedule):.3f}  (conservative {max_profitable_price(low, schedule):.3f})")
    all_rate = sum(w for _, w in checkpoints) / len(checkpoints)
    print(f"\nAll qualifying checkpoints (correlated, for reference): n={len(checkpoints)} realized {all_rate:.2%}")


if __name__ == "__main__":
    asyncio.run(main())
