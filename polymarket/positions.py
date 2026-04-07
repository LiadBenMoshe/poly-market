from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from polymarket.client import PolymarketClient
from schemas import PnlBucket, PnlResponse, PositionResponse


async def fetch_positions(client: PolymarketClient) -> list[PositionResponse]:
    raw_positions = await client.get_positions()
    positions: list[PositionResponse] = []
    now = datetime.now(UTC)
    for item in raw_positions:
        cur_price = float(item.get("curPrice") or item.get("currentPrice") or 0.0)
        entry_price = float(item.get("avgPrice") or item.get("entryPrice") or 0.0)
        size = float(item.get("size") or item.get("amount") or item.get("totalBought") or 0.0)
        notional = size * cur_price
        unrealized = float(item.get("cashPnl") or item.get("unrealizedPnl") or (cur_price - entry_price) * size)
        basis = max(size * entry_price, 1e-9)
        positions.append(
            PositionResponse(
                market_id=str(item.get("conditionId") or item.get("market_id") or ""),
                market_slug=item.get("slug"),
                market_title=str(item.get("title") or "Untitled market"),
                outcome="NO" if str(item.get("outcome") or "").upper() == "NO" else "YES",
                size=size,
                entry_price=entry_price,
                current_price=cur_price,
                notional_value=notional,
                unrealized_pnl=unrealized,
                unrealized_pnl_pct=(unrealized / basis) * 100,
                realized_pnl=float(item.get("realizedPnl") or 0.0),
                updated_at=now,
            )
        )
    return positions


async def fetch_pnl(client: PolymarketClient) -> PnlResponse:
    rows = await client.get_closed_positions()
    daily_map: dict[str, float] = defaultdict(float)
    weekly_map: dict[str, float] = defaultdict(float)
    total = 0.0
    now = datetime.now(UTC)

    for item in rows:
        realized = float(item.get("realizedPnl") or 0.0)
        total += realized
        timestamp = int(item.get("timestamp") or 0)
        when = datetime.fromtimestamp(timestamp, tz=UTC) if timestamp > 0 else now
        daily_map[when.strftime("%Y-%m-%d")] += realized
        weekly_map[f"{when.strftime('%Y')}-W{when.isocalendar().week:02d}"] += realized

    daily = [PnlBucket(label=key, realized_pnl=value) for key, value in sorted(daily_map.items())[-7:]]
    weekly = [PnlBucket(label=key, realized_pnl=value) for key, value in sorted(weekly_map.items())[-8:]]
    return PnlResponse(daily=daily, weekly=weekly, total_realized_pnl=total, updated_at=now)
