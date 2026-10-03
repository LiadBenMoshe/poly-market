from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    """All tunables for the single "late favourite" strategy. Values come from .env."""

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        protected_namespaces=("settings_",),
    )

    # --- credentials (only needed when DRY_RUN=false) ---
    polymarket_private_key: str = Field(default="")
    polymarket_api_key: str = Field(default="")
    polymarket_api_secret: str = Field(default="")
    polymarket_api_passphrase: str = Field(default="")
    polymarket_funder: str = Field(default="")
    polymarket_signature_type: int = 0
    chain_id: int = 137

    # --- endpoints ---
    clob_base_url: str = "https://clob.polymarket.com"
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    crypto_price_url: str = "https://polymarket.com/api/crypto/crypto-price"
    rtds_ws_url: str = "wss://ws-live-data.polymarket.com"
    bybit_rest_url: str = "https://api.bybit.com"
    bybit_ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    request_timeout_seconds: float = 10.0

    # --- run mode ---
    dry_run: bool = True
    paper_bankroll_usdc: float = 100.0
    loop_interval_seconds: float = 1.0
    timeframes: str = "5m,15m"
    data_dir: Path = BASE_DIR / "data"
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8050          # 0 disables the dashboard

    # --- entry gates ---
    min_win_probability: float = 0.90   # p_model (after haircut) must be at least this
    min_entry_price: float = 0.85       # do not buy cheaper than this (signal disagrees with market too much)
    max_entry_price: float = 0.97       # above this the upside is too thin to cover model error
    min_net_edge: float = 0.02          # p_model - avg fill price - fee per share, in $ per share
    model_haircut: float = 0.02         # subtracted from the raw model probability
    vol_multiplier: float = 1.25        # inflate measured volatility for fat tails
    min_sigma_per_sec: float = 0.00004  # floor on per-second log-return volatility
    min_distance_usd: float = 25.0      # |expected settle - strike| must exceed this
    min_seconds_left: int = 8
    max_seconds_left_5m: int = 90
    max_seconds_left_15m: int = 240
    max_price_age_seconds: float = 5.0  # skip if the Chainlink feed is stale

    # --- sizing / risk ---
    kelly_fraction: float = 0.25
    min_trade_usdc: float = 5.0
    max_trade_usdc: float = 25.0
    max_open_exposure_usdc: float = 50.0
    daily_loss_limit_usdc: float = 50.0

    # --- fees ---
    fallback_fee_rate: float = 0.07     # used if the market does not report a feeSchedule

    @property
    def timeframe_list(self) -> list[str]:
        return [tf.strip() for tf in self.timeframes.split(",") if tf.strip()]

    def max_seconds_left(self, timeframe: str) -> int:
        return self.max_seconds_left_15m if timeframe == "15m" else self.max_seconds_left_5m


@lru_cache
def get_settings() -> Settings:
    return Settings()
