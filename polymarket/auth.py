from __future__ import annotations

from dataclasses import dataclass

from eth_account import Account

from config import Settings


@dataclass(slots=True)
class WalletAuth:
    address: str
    private_key: str
    chain_id: int
    signature_type: int
    funder: str | None = None

    @property
    def trading_address(self) -> str:
        return self.funder or self.address


def build_wallet_auth(settings: Settings) -> WalletAuth:
    if not settings.polymarket_private_key:
        raise ValueError("POLYMARKET_PRIVATE_KEY is required for authenticated trading.")

    account = Account.from_key(settings.polymarket_private_key)
    address = account.address
    return WalletAuth(
        address=address,
        private_key=settings.polymarket_private_key,
        chain_id=settings.chain_id,
        signature_type=settings.polymarket_signature_type,
        funder=settings.polymarket_funder or None,
    )
