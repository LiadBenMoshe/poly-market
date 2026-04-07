from __future__ import annotations

from datetime import UTC, datetime

from polymarket.client import PolymarketClient
from polymarket.markets import fetch_market_map
from schemas import OrderResponse, PlaceOrderRequest


async def list_open_orders(client: PolymarketClient) -> list[OrderResponse]:
    raw_orders = await client.get_open_orders()
    market_map = await fetch_market_map(client)
    orders: list[OrderResponse] = []
    for item in raw_orders:
        market_id = str(item.get("market") or item.get("condition_id") or item.get("market_id") or "")
        market = market_map.get(market_id)
        outcome = str(item.get("outcome") or "YES").upper()
        orders.append(
            OrderResponse(
                id=str(item.get("id") or item.get("orderID") or ""),
                market_id=market_id,
                token_id=str(item.get("asset_id") or item.get("token_id") or "") or None,
                title=market.title if market else item.get("title"),
                side=str(item.get("side") or "BUY").upper(),
                outcome="NO" if outcome == "NO" else "YES",
                order_type=str(item.get("type") or "limit"),
                size=float(item.get("original_size") or item.get("size") or 0.0),
                price=float(item.get("price") or 0.0),
                status=str(item.get("status") or "open"),
                created_at=_parse_dt(item.get("created_at")),
            )
        )
    return orders


async def submit_order(client: PolymarketClient, payload: PlaceOrderRequest) -> OrderResponse:
    market_map = await fetch_market_map(client)
    market = market_map.get(payload.market_id)
    if not market:
        raise ValueError(f"Market {payload.market_id} was not found.")
    token_id = market.yes_token_id if payload.side == "YES" else market.no_token_id
    if not token_id:
        raise ValueError(f"Market {payload.market_id} does not expose a tradable token for {payload.side}.")

    result = await client.place_order(
        token_id=token_id,
        side="BUY",
        price=payload.price,
        size=payload.size,
        order_type=payload.order_type,
    )
    return OrderResponse(
        id=str(result.get("orderID") or result.get("id") or ""),
        market_id=payload.market_id,
        token_id=token_id,
        title=market.title,
        side="BUY",
        outcome=payload.side,
        order_type=payload.order_type,
        size=payload.size,
        price=payload.price,
        status=str(result.get("status") or "submitted"),
        created_at=datetime.now(UTC),
    )


def _parse_dt(raw: object) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None
