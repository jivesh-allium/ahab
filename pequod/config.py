from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from .env import load_dotenv


def _pick_value(values: Dict[str, str], key: str, prefer_dotenv: bool = False) -> Optional[str]:
    if prefer_dotenv:
        value = values.get(key)
        if value is not None and value != "":
            return value
        return os.environ.get(key)
    return os.environ.get(key, values.get(key))


def _to_int(values: Dict[str, str], key: str, default: int, prefer_dotenv: bool = False) -> int:
    value = _pick_value(values, key, prefer_dotenv=prefer_dotenv)
    if value is None or value == "":
        return default
    return int(value)


def _to_float(values: Dict[str, str], key: str, default: float, prefer_dotenv: bool = False) -> float:
    value = _pick_value(values, key, prefer_dotenv=prefer_dotenv)
    if value is None or value == "":
        return default
    return float(value)


def _to_str(values: Dict[str, str], key: str, default: str = "", prefer_dotenv: bool = False) -> str:
    value = _pick_value(values, key, prefer_dotenv=prefer_dotenv)
    if value is None:
        return default
    return value


def _to_bool(values: Dict[str, str], key: str, default: bool, prefer_dotenv: bool = False) -> bool:
    value = _pick_value(values, key, prefer_dotenv=prefer_dotenv)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    allium_api_key: str
    allium_base_url: str
    watchlist_path: Path
    poll_interval_seconds: int
    min_alert_usd: float
    lookback_seconds: int
    http_timeout_seconds: int
    max_addresses_per_request: int
    dedupe_db_path: Path
    telegram_bot_token: str
    telegram_chat_id: str
    discord_webhook_url: str
    generic_webhook_url: str
    run_once: bool


def load_settings(dotenv_path: str = ".env") -> Settings:
    env_values = load_dotenv(dotenv_path)

    api_key = _to_str(env_values, "ALLIUM_API_KEY", prefer_dotenv=True)
    if not api_key:
        raise ValueError("ALLIUM_API_KEY is required. Add it to .env or environment variables.")

    return Settings(
        allium_api_key=api_key,
        allium_base_url=_to_str(env_values, "ALLIUM_BASE_URL", "https://api.allium.so").rstrip("/"),
        watchlist_path=Path(_to_str(env_values, "PEQUOD_WATCHLIST_PATH", "watchlists/default.json")),
        poll_interval_seconds=_to_int(env_values, "PEQUOD_POLL_INTERVAL_SECONDS", 30),
        min_alert_usd=_to_float(env_values, "PEQUOD_MIN_ALERT_USD", 1_000_000),
        lookback_seconds=_to_int(env_values, "PEQUOD_LOOKBACK_SECONDS", 180),
        http_timeout_seconds=_to_int(env_values, "PEQUOD_HTTP_TIMEOUT_SECONDS", 20),
        max_addresses_per_request=_to_int(env_values, "PEQUOD_MAX_ADDRESSES_PER_REQUEST", 20),
        dedupe_db_path=Path(_to_str(env_values, "PEQUOD_DEDUPE_DB_PATH", "data/alerts.sqlite3")),
        telegram_bot_token=_to_str(env_values, "PEQUOD_TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=_to_str(env_values, "PEQUOD_TELEGRAM_CHAT_ID"),
        discord_webhook_url=_to_str(env_values, "PEQUOD_DISCORD_WEBHOOK_URL"),
        generic_webhook_url=_to_str(env_values, "PEQUOD_GENERIC_WEBHOOK_URL"),
        run_once=_to_bool(env_values, "PEQUOD_RUN_ONCE", False),
    )
