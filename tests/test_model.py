import pytest

from model import estimate_up, realized_sigma_per_sec, settle_distribution

SIGMA = 0.00005  # per-second log vol ~ $4/s at $85k


def test_at_the_money_is_coin_flip():
    assert estimate_up(spot=85000, strike=85000, seconds_left=120, sigma_per_sec=SIGMA).p_up == pytest.approx(0.5)


def test_symmetry():
    up = estimate_up(spot=85000, strike=84950, seconds_left=60, sigma_per_sec=SIGMA).p_up
    down = estimate_up(spot=85000, strike=85050, seconds_left=60, sigma_per_sec=SIGMA).p_up
    assert up == pytest.approx(1 - down)


def test_more_time_less_certain():
    near = estimate_up(spot=85050, strike=85000, seconds_left=20, sigma_per_sec=SIGMA).p_up
    far = estimate_up(spot=85050, strike=85000, seconds_left=200, sigma_per_sec=SIGMA).p_up
    assert near > far > 0.5


def test_twap_already_locked_in():
    # 50 of the 60 TWAP seconds observed well above strike: almost certain even if spot just dropped to strike
    est = estimate_up(spot=85000, strike=85000, seconds_left=10, sigma_per_sec=SIGMA,
                      twap_seconds=60, observed_twap_avg=85060)
    assert est.settle_mean == pytest.approx((50 * 85060 + 10 * 85000) / 60)
    assert est.p_up > 0.999


def test_twap_variance_formulas():
    s_usd = SIGMA * 85000
    before = settle_distribution(85000, 120, SIGMA, twap_seconds=60)
    assert before.sd == pytest.approx(s_usd * (120 - 60 + 20) ** 0.5)
    inside = settle_distribution(85000, 30, SIGMA, twap_seconds=60, observed_twap_avg=85000)
    assert inside.sd == pytest.approx(0.5 * s_usd * (30 / 3) ** 0.5)


def test_zero_time_is_deterministic():
    assert estimate_up(spot=85001, strike=85000, seconds_left=0, sigma_per_sec=SIGMA, twap_seconds=0).p_up == 1.0
    assert estimate_up(spot=84999, strike=85000, seconds_left=0, sigma_per_sec=SIGMA, twap_seconds=0).p_up == 0.0


def test_realized_sigma_scales_to_seconds():
    prices = [100.0, 101.0, 100.0, 101.0, 100.0]
    s60 = realized_sigma_per_sec(prices, 60)
    s1 = realized_sigma_per_sec(prices, 1)
    assert s1 == pytest.approx(s60 * 60 ** 0.5)
