import pytest

from ledger import Ledger, settle_pnl


def test_settle_pnl_includes_fees():
    assert settle_pnl(10, 9.0, 0.06, won=True) == pytest.approx((10.0, 0.94))
    assert settle_pnl(10, 9.0, 0.06, won=False) == pytest.approx((0.0, -9.06))


def test_ledger_round_trip(tmp_path):
    led = Ledger(tmp_path / "t.json", starting_bankroll=100)
    t1 = led.record_entry(slug="a", timeframe="5m", side="Up", shares=10, avg_price=0.9, fee_usdc=0.063, p_model=0.95)
    t2 = led.record_entry(slug="b", timeframe="5m", side="Down", shares=10, avg_price=0.9, fee_usdc=0.063, p_model=0.93)
    assert led.open_exposure() == pytest.approx(18.126)
    assert led.paper_bankroll() == pytest.approx(100 - 18.126)
    assert t1["breakeven_win_rate"] == pytest.approx(0.9063)
    led.settle(t1["id"], "Up")
    led.settle(t2["id"], "Up")
    st = Ledger(tmp_path / "t.json", 100).stats()  # reload from disk
    assert st["wins"] == 1 and st["losses"] == 1
    assert st["total_fees_usdc"] == pytest.approx(0.126)
    assert st["net_pnl_usdc"] == pytest.approx(0.937 - 9.063)
    assert st["gross_pnl_usdc"] == pytest.approx(10 - 18)
    assert led.has_traded("a") and not led.has_traded("c")
