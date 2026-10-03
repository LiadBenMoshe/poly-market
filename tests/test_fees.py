import pytest

from fees import FeeSchedule, breakeven_win_rate, fee_per_share, fee_usdc, fill_from_asks, net_edge

CRYPTO = FeeSchedule(rate=0.07, exponent=1)


def test_docs_example_100_shares_at_50c():
    assert fee_usdc(100, 0.50, CRYPTO) == pytest.approx(1.75)


def test_fee_is_small_near_extremes():
    assert fee_per_share(0.92, CRYPTO) == pytest.approx(0.07 * 0.92 * 0.08)


def test_buying_at_fair_price_loses_after_fees():
    # p=0.90 bought at 0.90 is negative after fees: the core reason the bot needs a real edge
    assert net_edge(0.90, 0.90, fee_per_share(0.90, CRYPTO)) < 0
    assert breakeven_win_rate(0.90, fee_per_share(0.90, CRYPTO)) == pytest.approx(0.9063)


def test_fill_walks_book_depth():
    asks = [(0.93, 10), (0.91, 10), (0.95, 100)]
    fill = fill_from_asks(asks, 20.0, CRYPTO, max_price=0.97)
    # 10 @ 0.91 = 9.10, 10 @ 0.93 = 9.30, remaining 1.60 @ 0.95
    assert fill.cost_usdc == pytest.approx(20.0)
    assert fill.shares == pytest.approx(20 + 1.6 / 0.95)
    assert fill.worst_price == 0.95


def test_fill_respects_max_price():
    fill = fill_from_asks([(0.98, 100)], 10.0, CRYPTO, max_price=0.97)
    assert fill.shares == 0


def test_schedule_from_market():
    m = {"feesEnabled": True, "feeSchedule": {"rate": 0.07, "exponent": 1, "takerOnly": True}}
    assert FeeSchedule.from_market(m, 0.1).rate == 0.07
    assert FeeSchedule.from_market({"feesEnabled": False}, 0.1).rate == 0.0
    assert FeeSchedule.from_market({}, 0.1).rate == 0.1
