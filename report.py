"""Print performance net of fees.  Usage:  python report.py"""
from __future__ import annotations

from config import get_settings
from ledger import Ledger


def main() -> None:
    s = get_settings()
    st = Ledger(s.data_dir / "trades.json", s.paper_bankroll_usdc).stats()
    n = st["closed_trades"]
    print(f"Closed trades: {n}   (open: {st['open_trades']})")
    if not n:
        return
    print(f"Wins / losses: {st['wins']} / {st['losses']}")
    print(f"Win rate:            {st['win_rate']:.2%}")
    print(f"Predicted win rate:  {st['avg_predicted_win_rate']:.2%}")
    print(f"Breakeven win rate:  {st['avg_breakeven_win_rate']:.2%}   (avg price + fee per share)")
    print(f"Spent on shares:     ${st['total_cost_usdc']:.2f}")
    print(f"Fees paid:           ${st['total_fees_usdc']:.4f}")
    print(f"Gross PnL:           ${st['gross_pnl_usdc']:+.2f}")
    print(f"Net PnL (after fees):${st['net_pnl_usdc']:+.2f}   expected ${st['expected_net_pnl_usdc']:+.2f}")
    print(f"ROI on outlay:       {st['roi_on_cost']:+.2%}")
    print(f"Avg win / avg loss:  ${st['avg_win_usdc']:+.3f} / ${st['avg_loss_usdc']:+.3f}")
    print("\nCalibration (predicted vs realized):")
    for row in st["calibration"]:
        print(f"  {row['bucket']}  n={row['trades']:4d}  predicted {row['avg_predicted']:.2%}  realized {row['realized_win_rate']:.2%}")


if __name__ == "__main__":
    main()
