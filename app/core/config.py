"""
OniQuant v6.0 — Core Configuration
====================================
Centralized settings via pydantic-settings.
All values are injectable via Railway.app environment variables.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings — all sourced from environment variables.
    Railway injects these automatically via service variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- Application ---
    app_name: str = "OniQuant v6.0"
    app_version: str = "6.0.0"
    debug: bool = False
    log_level: int = 10  # DEBUG=10, INFO=20, WARNING=30

    # --- Redis ---
    redis_url: str = "redis://redis:6379/0"
    redis_max_connections: int = 20

    # --- TimescaleDB ---
    database_url: str = "postgresql://oniquant:oniquant@timescaledb:5432/oniquant"
    db_min_pool_size: int = 5
    db_max_pool_size: int = 20

    # --- Redis Keys ---
    raw_signals_stream: str = "oniquant:raw_signals"
    pending_trades_zset: str = "oniquant:pending_trades"
    match_validation_stream: str = "oniquant:match_validation"
    active_executions_stream: str = "oniquant:active_executions"
    stream_maxlen: int = 50000

    # --- Matching Engine ---
    adverse_selection_penalty: float = 0.20
    matcher_poll_interval: float = 0.05  # 50ms

    # --- Orchestrator ---
    posterior_floor: float = 0.695

    # --- Broker: IBKR ---
    ib_gateway_host: str = "127.0.0.1"
    ib_gateway_port: int = 4001
    ib_client_id: int = 1
    ib_rate_limit: float = 45.0  # msg/sec

    # --- Broker: Bybit ---
    bybit_api_key: str = ""
    bybit_api_secret: str = ""
    bybit_testnet: bool = True


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton — parsed once at startup."""
    return Settings()
