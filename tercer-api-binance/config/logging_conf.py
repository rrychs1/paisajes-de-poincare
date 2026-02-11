from __future__ import annotations

import logging
from logging.config import dictConfig

EXTRA_FIELDS = (
    "symbol",
    "regime",
    "strategy",
    "signals",
    "orders",
    "duration_ms",
    "pnl",
)


class SafeExtraFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        for key in EXTRA_FIELDS:
            if not hasattr(record, key):
                setattr(record, key, "-")
        return super().format(record)

LOGGING_CONFIG: dict[str, object] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "structured": {
            "()": "config.logging_conf.SafeExtraFormatter",
            "format": (
                "%(asctime)s %(levelname)s [%(name)s] %(message)s | "
                "symbol=%(symbol)s regime=%(regime)s strategy=%(strategy)s "
                "signals=%(signals)s orders=%(orders)s duration_ms=%(duration_ms)s "
                "pnl=%(pnl)s"
            ),
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "structured",
            "level": "DEBUG",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}


def configure_logging(
    log_level: str = "INFO",
    log_file: str | None = None,
    log_max_bytes: int = 5_000_000,
    log_backup_count: int = 5,
) -> None:
    config = dict(LOGGING_CONFIG)
    config["root"] = dict(LOGGING_CONFIG["root"])
    config["root"]["level"] = log_level.upper()
    if log_file:
        config["handlers"] = dict(LOGGING_CONFIG["handlers"])
        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "structured",
            "level": "INFO",
            "filename": log_file,
            "maxBytes": int(log_max_bytes),
            "backupCount": int(log_backup_count),
            "encoding": "utf-8",
        }
        config["root"]["handlers"] = ["console", "file"]
    dictConfig(config)
    logging.getLogger(__name__).debug("Logging configured at %s", log_level.upper())
