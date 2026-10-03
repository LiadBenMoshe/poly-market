"""Derive Polymarket CLOB API credentials from POLYMARKET_PRIVATE_KEY in .env, then copy them into .env."""
from py_clob_client.client import ClobClient

from config import get_settings


def main() -> None:
    s = get_settings()
    if not s.polymarket_private_key:
        raise SystemExit("Set POLYMARKET_PRIVATE_KEY in .env first.")
    client = ClobClient(s.clob_base_url, key=s.polymarket_private_key, chain_id=s.chain_id,
                        signature_type=s.polymarket_signature_type, funder=s.polymarket_funder or None)
    print(client.create_or_derive_api_creds())


if __name__ == "__main__":
    main()
