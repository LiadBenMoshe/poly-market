"""Derive Polymarket CLOB API credentials from POLYMARKET_PRIVATE_KEY in .env, then copy them into .env."""
from py_clob_client_v2 import ClobClient

from config import get_settings


def main() -> None:
    s = get_settings()
    if not s.polymarket_private_key:
        raise SystemExit("Set POLYMARKET_PRIVATE_KEY in .env first.")
    client = ClobClient(host=s.clob_base_url, chain_id=s.chain_id, key=s.polymarket_private_key)
    print(client.create_or_derive_api_key())


if __name__ == "__main__":
    main()
