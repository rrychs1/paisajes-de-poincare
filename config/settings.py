from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import (
    AliasChoices,
    Field,
    SecretStr,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
        env_ignore_empty=True,
    )

    binance_api_key: SecretStr = Field(..., alias="BINANCE_API_KEY")
    binance_api_secret: SecretStr = Field(..., alias="BINANCE_API_SECRET")

    trading_env: StrictStr = Field(default="TESTNET", alias="TRADING_ENV")
    use_testnet: bool = Field(default=True, alias="BINANCE_TESTNET")
    exchange_timeout_ms: StrictInt = Field(default=30000, ge=1000, le=120000)

    account_currency: StrictStr = Field(default="USDT")
    symbols: List[StrictStr] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT"])
    timeframe: StrictStr = Field(default="1m")

    max_leverage: StrictInt = Field(
        default=20,
        ge=1,
        le=125,
        validation_alias=AliasChoices("MAX_LEVERAGE", "LEVERAGE"),
    )
    risk_per_trade: StrictFloat = Field(
        default=0.005,
        ge=0.0,
        le=0.05,
        validation_alias=AliasChoices("RISK_PER_TRADE", "MAX_RISK_PER_TRADE"),
    )
    max_position_pct: StrictFloat = Field(
        default=0.25,
        ge=0.0,
        le=1.0,
        validation_alias=AliasChoices("MAX_POSITION_PCT", "MAX_POSITION_PERCENT"),
    )
    max_drawdown_pct: StrictFloat = Field(default=0.2, ge=0.0, le=1.0)
    max_daily_loss_pct: StrictFloat = Field(
        default=0.02,
        ge=0.0,
        le=1.0,
        validation_alias=AliasChoices("MAX_DAILY_LOSS", "MAX_DAILY_LOSS_PCT"),
    )
    loss_streak_limit: StrictInt = Field(
        default=3, ge=1, le=20, validation_alias=AliasChoices("LOSS_STREAK_LIMIT")
    )
    cooldown_minutes: StrictFloat = Field(
        default=30.0,
        ge=1.0,
        le=1_440.0,
        validation_alias=AliasChoices("COOLDOWN_MINUTES"),
    )
    min_notional_usd: StrictFloat = Field(default=5.0, ge=0.0)
    candles_retention_days: StrictFloat | None = Field(
        default=30.0,
        ge=1.0,
        le=3650.0,
        validation_alias=AliasChoices("CANDLES_RETENTION_DAYS"),
    )

    max_open_orders: StrictInt = Field(default=50, ge=1, le=200)
    rate_limit_per_sec: StrictInt = Field(default=10, ge=1, le=50)
    max_signals_per_symbol: StrictInt = Field(
        default=5,
        ge=1,
        le=50,
        validation_alias=AliasChoices("MAX_SIGNALS_PER_SYMBOL"),
    )
    order_retry_attempts: StrictInt = Field(
        default=2,
        ge=0,
        le=10,
        validation_alias=AliasChoices("ORDER_RETRY_ATTEMPTS"),
    )
    order_retry_backoff_seconds: StrictFloat = Field(
        default=0.5,
        ge=0.1,
        le=10.0,
        validation_alias=AliasChoices("ORDER_RETRY_BACKOFF_SECONDS"),
    )
    cancel_stale_orders_seconds: StrictFloat | None = Field(
        default=None,
        ge=30.0,
        le=86_400.0,
        validation_alias=AliasChoices("CANCEL_STALE_ORDERS_SECONDS"),
    )
    skip_duplicate_orders: bool = Field(
        default=True, validation_alias=AliasChoices("SKIP_DUPLICATE_ORDERS")
    )
    polling_interval: StrictFloat = Field(default=5.0, ge=0.25, le=60.0)
    max_runtime_seconds: StrictFloat | None = Field(
        default=None,
        ge=1.0,
        le=86_400.0,
        validation_alias=AliasChoices("MAX_RUNTIME_SECONDS", "MAX_RUNTIME"),
    )
    metrics_interval_seconds: StrictFloat = Field(
        default=60.0,
        ge=5.0,
        le=3_600.0,
        validation_alias=AliasChoices("METRICS_INTERVAL_SECONDS", "METRICS_INTERVAL"),
    )
    alert_webhook_url: StrictStr | None = Field(
        default=None, alias="ALERT_WEBHOOK_URL"
    )
    alert_cooldown_seconds: StrictFloat = Field(
        default=300.0,
        ge=30.0,
        le=3_600.0,
        validation_alias=AliasChoices("ALERT_COOLDOWN_SECONDS", "ALERT_COOLDOWN"),
    )
    log_file: StrictStr | None = Field(default="bot.log", alias="LOG_FILE")
    log_max_bytes: StrictInt = Field(
        default=5_000_000, ge=100_000, le=100_000_000, alias="LOG_MAX_BYTES"
    )
    log_backup_count: StrictInt = Field(
        default=5, ge=1, le=100, alias="LOG_BACKUP_COUNT"
    )

    @field_validator(
        "exchange_timeout_ms",
        "max_leverage",
        "max_open_orders",
        "rate_limit_per_sec",
        "loss_streak_limit",
        "max_signals_per_symbol",
        "order_retry_attempts",
        "log_max_bytes",
        "log_backup_count",
        mode="before",
    )
    @classmethod
    def _parse_int(cls, value):
        if isinstance(value, str):
            return int(value)
        return value

    @field_validator(
        "risk_per_trade",
        "max_position_pct",
        "max_drawdown_pct",
        "max_daily_loss_pct",
        "cooldown_minutes",
        "min_notional_usd",
        "polling_interval",
        "max_runtime_seconds",
        "metrics_interval_seconds",
        "alert_cooldown_seconds",
        "order_retry_backoff_seconds",
        "cancel_stale_orders_seconds",
        "candles_retention_days",
        mode="before",
    )
    @classmethod
    def _parse_float(cls, value):
        if value is None or value == "":
            return None
        if isinstance(value, str):
            return float(value)
        return value

    @field_validator("alert_webhook_url", mode="before")
    @classmethod
    def _parse_optional_str(cls, value):
        if value is None or value == "":
            return None
        return value

    @field_validator("log_file", mode="before")
    @classmethod
    def _parse_log_file(cls, value):
        if value is None or value == "":
            return None
        return value

    @model_validator(mode="after")
    def _apply_trading_env(self):
        env = self.trading_env.upper()
        if env == "LIVE":
            object.__setattr__(self, "use_testnet", False)
        elif env == "TESTNET":
            object.__setattr__(self, "use_testnet", True)
        elif env == "DEMO":
            object.__setattr__(self, "use_testnet", True)
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
