from __future__ import annotations

import asyncio
import json
import logging

from bot.whale_scanner import WhaleScanner
from config import get_settings
from polymarket.client import PolymarketClient


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")


async def main() -> None:
    settings = get_settings()
    client = PolymarketClient(settings)
    scanner = WhaleScanner(client, settings)
    try:
        while True:
            snapshot = await scanner.run_cycle()
            print(json.dumps(snapshot, indent=2, default=str))
            await asyncio.sleep(settings.whale_scan_interval_seconds)
    finally:
        scanner.stop()
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
