from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from polymarket.client import PolymarketClient
from schemas import MarketResponse


def _parse_list(raw: Any, cast) -> list:
    if isinstance(raw, list):
        return [cast(item) for item in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [cast(item) for item in parsed]
        except json.JSONDecodeError:
            return []
    return []


async def fetch_markets(client: PolymarketClient, limit: int = 25) -> list[MarketResponse]:
    markets = await client.get_active_markets(limit=limit)
    responses: list[MarketResponse] = []
    for market in markets:
        outcomes = _parse_list(market.get("outcomes"), str)
        prices = _parse_list(market.get("outcomePrices"), float)
        token_ids = _parse_list(market.get("clobTokenIds") or market.get("tokenIds"), str)

        yes_index = outcomes.index("Yes") if "Yes" in outcomes else 0
        no_index = outcomes.index("No") if "No" in outcomes else 1
        yes_price = float(prices[yes_index]) if len(prices) > yes_index else float(market.get("bestAsk", 0.0) or 0.0)
        no_price = float(prices[no_index]) if len(prices) > no_index else max(0.0, 1.0 - yes_price)

        end_date = None
        raw_end = market.get("endDate") or market.get("end_date_iso")
        if raw_end:
            try:
                end_date = datetime.fromisoformat(str(raw_end).replace("Z", "+00:00")).astimezone(UTC)
            except ValueError:
                end_date = None

        responses.append(
            MarketResponse(
                market_id=str(market.get("id") or market.get("conditionId") or ""),
                question_id=str(market.get("questionID") or market.get("questionId") or "") or None,
                slug=market.get("slug"),
                title=str(market.get("question") or market.get("title") or "Untitled market"),
                yes_token_id=str(token_ids[yes_index]) if len(token_ids) > yes_index else None,
                no_token_id=str(token_ids[no_index]) if len(token_ids) > no_index else None,
                yes_price=yes_price,
                no_price=no_price,
                volume=float(market.get("volume") or market.get("volumeNum") or 0.0),
                liquidity=float(market.get("liquidityNum") or market.get("liquidity") or 0.0),
                end_date=end_date,
                active=bool(market.get("active", True)),
            )
        )
    return responses


async def fetch_market_map(client: PolymarketClient, limit: int = 100) -> dict[str, MarketResponse]:
    markets = await fetch_markets(client, limit=limit)
    return {market.market_id: market for market in markets if market.market_id}
