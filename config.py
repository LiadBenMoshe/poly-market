from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from eth_account import Account
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "Polymarket Bot"
    environment: Literal["development", "staging", "production"] = "development"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000

    polymarket_private_key: str = Field(default="")
    polymarket_api_key: str = Field(default="")
    polymarket_api_secret: str = Field(default="")
    polymarket_api_passphrase: str = Field(default="")
    polymarket_funder: str = Field(default="")
    polymarket_signature_type: int = Field(default=0)

    rpc_url: str = "https://polygon-rpc.com"
    chain_id: int = 137

    clob_base_url: str = "https://clob.polymarket.com"
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    data_api_base_url: str = "https://data-api.polymarket.com"

    allowed_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",
            "http://localhost:5173",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:5173",
            "http://localhost:8000",
            "http://127.0.0.1:8000",
        ]
    )

    scheduler_interval_seconds: int = 60
    min_market_volume: float = 10_000.0
    max_trade_fraction: float = 0.05
    max_total_exposure_fraction: float = 0.30
    max_market_exposure_fraction: float = 0.10
    stop_loss_fraction: float = 0.20
    dry_run: bool = True
    request_timeout_seconds: float = 15.0

    @computed_field  # type: ignore[misc]
    @property
    def wallet_address(self) -> str:
        if not self.polymarket_private_key:
            return ""
        account = Account.from_key(self.polymarket_private_key)
        return account.address


@lru_cache
def get_settings() -> Settings:
    return Settings()
