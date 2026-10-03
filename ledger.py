"""Trade ledger: every entry, its fee, its settlement and PnL net of fees."""
from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CALIBRATION_BUCKETS = [(0.90, 0.92), (0.92, 0.94), (0.94, 0.96), (0.96, 0.98), (0.98, 1.01)]


def settle_pnl(shares: float, cost_usdc: float, fee_usdc: float, won: bool) -> tuple[float, float]:
    """Returns (payout, net_pnl). A winning share pays $1, a losing one $0. Fees are never refunded."""
    payout = shares if won else 0.0
    return payout, payout - cost_usdc - fee_usdc


class Ledger:
    def __init__(self, path: Path, starting_bankroll: float) -> None:
        self.path = path
        self.starting_bankroll = starting_bankroll
        self.trades: list[dict[str, Any]] = []
        if path.exists():
            self.trades = json.loads(path.read_text(encoding="utf-8")).get("trades", [])

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"trades": self.trades}, indent=1), encoding="utf-8")
        os.replace(tmp, self.path)

    # --- state ---
    def open_trades(self) -> list[dict[str, Any]]:
        return [t for t in self.trades if t["status"] == "open"]

    def has_traded(self, slug: str) -> bool:
        return any(t["slug"] == slug for t in self.trades)

    def open_exposure(self) -> float:
        return sum(t["cost_usdc"] + t["fee_usdc"] for t in self.open_trades())

    def realized_pnl(self) -> float:
        return sum(t["net_pnl"] for t in self.trades if t["status"] in ("won", "lost"))

    def paper_bankroll(self) -> float:
        """Cash available for new paper trades."""
        return self.starting_bankroll + self.realized_pnl() - self.open_exposure()

    def daily_pnl(self, now: datetime | None = None) -> float:
        day = (now or datetime.now(UTC)).date().isoformat()
        return sum(t["net_pnl"] for t in self.trades
                   if t["status"] in ("won", "lost") and (t.get("settled_at") or "")[:10] == day)

    # --- mutations ---
    def record_entry(self, **fields: Any) -> dict[str, Any]:
        shares, avg_price, fee = fields["shares"], fields["avg_price"], fields["fee_usdc"]
        trade = {
            "id": uuid.uuid4().hex[:12],
            "opened_at": datetime.now(UTC).isoformat(),
            "status": "open",
            "cost_usdc": round(shares * avg_price, 6),
            "breakeven_win_rate": round(avg_price + (fee / shares if shares else 0.0), 5),
            "expected_pnl": round(shares * fields["p_model"] - shares * avg_price - fee, 6),
            "payout": 0.0,
            "net_pnl": 0.0,
            **fields,
        }
        self.trades.append(trade)
        self.save()
        return trade

    def settle(self, trade_id: str, winning_side: str) -> dict[str, Any]:
        trade = next(t for t in self.trades if t["id"] == trade_id)
        won = trade["side"] == winning_side
        payout, pnl = settle_pnl(trade["shares"], trade["cost_usdc"], trade["fee_usdc"], won)
        trade.update(status="won" if won else "lost", winning_side=winning_side, payout=round(payout, 6),
                     net_pnl=round(pnl, 6), settled_at=datetime.now(UTC).isoformat())
        self.save()
        return trade

    # --- reporting ---
    def stats(self) -> dict[str, Any]:
        closed = [t for t in self.trades if t["status"] in ("won", "lost")]
        wins = [t for t in closed if t["status"] == "won"]
        n = len(closed)
        cost = sum(t["cost_usdc"] for t in closed)
        fees = sum(t["fee_usdc"] for t in closed)
        payout = sum(t["payout"] for t in closed)
        net = sum(t["net_pnl"] for t in closed)
        calibration = []
        for lo, hi in CALIBRATION_BUCKETS:
            bucket = [t for t in closed if lo <= t["p_model"] < hi]
            if bucket:
                calibration.append({
                    "bucket": f"{lo:.2f}-{min(hi, 1.0):.2f}",
                    "trades": len(bucket),
                    "avg_predicted": sum(t["p_model"] for t in bucket) / len(bucket),
                    "realized_win_rate": sum(t["status"] == "won" for t in bucket) / len(bucket),
                })
        return {
            "closed_trades": n,
            "open_trades": len(self.open_trades()),
            "wins": len(wins),
            "losses": n - len(wins),
            "win_rate": len(wins) / n if n else 0.0,
            "avg_breakeven_win_rate": sum(t["breakeven_win_rate"] for t in closed) / n if n else 0.0,
            "avg_predicted_win_rate": sum(t["p_model"] for t in closed) / n if n else 0.0,
            "total_cost_usdc": cost,
            "total_fees_usdc": fees,
            "gross_pnl_usdc": payout - cost,
            "net_pnl_usdc": net,
            "expected_net_pnl_usdc": sum(t["expected_pnl"] for t in closed),
            "roi_on_cost": net / (cost + fees) if cost else 0.0,
            "avg_win_usdc": sum(t["net_pnl"] for t in wins) / len(wins) if wins else 0.0,
            "avg_loss_usdc": (sum(t["net_pnl"] for t in closed if t["status"] == "lost") / (n - len(wins))) if n > len(wins) else 0.0,
            "calibration": calibration,
        }
