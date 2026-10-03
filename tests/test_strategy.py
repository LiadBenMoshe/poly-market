from config import Settings
from fees import FeeSchedule
from model import Estimate
from strategy import MarketView, evaluate

S = Settings(_env_file=None, min_trade_usdc=5, max_trade_usdc=25, max_open_exposure_usdc=50)
FEES = FeeSchedule(rate=0.07, exponent=1)


def view(asks, seconds_left=40, strike=85000, timeframe="5m"):
    return MarketView(slug="x", timeframe=timeframe, seconds_left=seconds_left, strike=strike, spot=85100,
                      fee_schedule=FEES, asks={"Up": asks})


def est(p_up, mean=85100):
    return Estimate(p_up=p_up, settle_mean=mean, settle_sd=10)


def test_enters_when_cheap_vs_model():
    d = evaluate(view([(0.90, 500)]), est(0.97), S, bankroll=100, open_exposure=0)
    assert d.enter and d.side == "Up"
    assert d.edge_per_share >= S.min_net_edge
    assert S.min_trade_usdc <= d.budget_usdc <= S.max_trade_usdc


def test_rejects_fair_priced_90():
    d = evaluate(view([(0.90, 500)]), est(0.92), S, bankroll=100, open_exposure=0)  # p_model 0.90
    assert not d.enter and d.reason == "edge_below_min_after_fees"


def test_rejects_below_90_probability():
    d = evaluate(view([(0.85, 500)]), est(0.90), S, bankroll=100, open_exposure=0)
    assert not d.enter and d.reason == "probability_below_90"


def test_rejects_when_market_disagrees_strongly():
    d = evaluate(view([(0.60, 500)]), est(0.99), S, bankroll=100, open_exposure=0)
    assert not d.enter and d.reason == "price_too_low_market_disagrees"


def test_rejects_near_strike_and_timing():
    assert evaluate(view([(0.9, 500)]), est(0.99, mean=85010), S, 100, 0).reason == "too_close_to_strike"
    assert evaluate(view([(0.9, 500)], seconds_left=200), est(0.99), S, 100, 0).reason == "too_early"
    assert evaluate(view([(0.9, 500)], seconds_left=200, timeframe="15m"), est(0.99), S, 100, 0).enter
    assert evaluate(view([(0.9, 500)], seconds_left=3), est(0.99), S, 100, 0).reason == "too_close_to_end"


def test_exposure_cap():
    d = evaluate(view([(0.90, 500)]), est(0.97), S, bankroll=100, open_exposure=48)
    assert not d.enter and d.reason == "kelly_or_exposure_zero"


def test_thin_book_falls_back_to_min_size_or_skips():
    d = evaluate(view([(0.90, 6)]), est(0.97), S, bankroll=1000, open_exposure=0)
    assert d.enter and d.budget_usdc == S.min_trade_usdc
    d = evaluate(view([(0.90, 2)]), est(0.97), S, bankroll=1000, open_exposure=0)
    assert not d.enter
