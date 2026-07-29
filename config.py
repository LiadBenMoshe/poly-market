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
    arb_scheduler_interval_seconds: int = 10
    min_market_volume: float = 10_000.0
    max_trade_fraction: float = 0.05
    max_total_exposure_fraction: float = 0.30
    max_market_exposure_fraction: float = 0.10
    stop_loss_fraction: float = 0.20
    dry_run: bool = True
    request_timeout_seconds: float = 15.0
    bybit_ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    bybit_api_key: str = ""
    bybit_api_secret: str = ""
    min_edge: float = 0.08
    max_trade_usdc: float = 50.0
    arb_trade_cap_usdc: float = 20.0
    min_trade_usdc: float = 5.0
    max_open_positions: int = 3
    min_signal_score: float = 10.0
    yes_signal_floor: float = 26.0
    no_signal_floor: float = 30.0
    yes_win_zone_max_signal: float = 40.0
    no_win_zone_floor_signal: float = -45.0
    extreme_no_signal_score: float = -40.0
    extreme_no_size_multiplier: float = 0.5
    max_trades_per_window: int = 1
    trade_cooldown_sec: int = 60
    arbot_enabled: bool = False
    daily_loss_limit: float = 100.0
    take_profit_usdc: float = 2.0
    position_stop_loss_usdc: float = 5.0
    arb_stop_loss_fraction: float = 0.35
    arb_stop_loss_cutoff_seconds: int = 45
    whale_scan_interval_seconds: int = 10
    whale_market_limit: int = 140
    whale_trade_lookback: int = 40
    whale_trade_concurrency: int = 12
    whale_min_trade_size_usdc: float = 1_000.0
    whale_threshold_multiplier: float = 1.6
    whale_absolute_tier_1_usdc: float = 10_000.0
    whale_absolute_tier_2_usdc: float = 20_000.0
    whale_absolute_tier_3_usdc: float = 50_000.0
    whale_tier_1_fraction: float = 0.30
    whale_tier_2_fraction: float = 0.20
    whale_tier_3_fraction: float = 0.10
    whale_tier_4_fraction: float = 0.05
    whale_min_conviction_score: float = 55.0
    whale_state_retention_hours: int = 24
    whale_weather_latitude: float = 40.7128
    whale_weather_longitude: float = -74.0060

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
