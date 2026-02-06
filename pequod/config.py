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
    dashboard_host: str
    dashboard_port: int
    dashboard_base_url: str
    geo_cache_path: Path
    geo_refresh_interval_seconds: int
    balance_refresh_interval_seconds: int
    auto_discover_counterparties: bool
    discover_min_usd: float
    discovered_watch_max: int
    geo_bootstrap_max_addresses: int
    dashboard_max_alerts: int
    dashboard_max_events: int


def load_settings(dotenv_path: str = ".env") -> Settings:
    env_values = load_dotenv(dotenv_path)

    api_key = _to_str(env_values, "ALLIUM_API_KEY", prefer_dotenv=True)
    if not api_key:
        raise ValueError("ALLIUM_API_KEY is required. Add it to .env or environment variables.")

    platform_port = _to_int(env_values, "PORT", 0)
    default_dashboard_host = "0.0.0.0" if platform_port > 0 else "127.0.0.1"
    default_dashboard_port = platform_port if platform_port > 0 else 8080

    dashboard_host = _to_str(env_values, "PEQUOD_DASHBOARD_HOST", default_dashboard_host)
    dashboard_port = _to_int(env_values, "PEQUOD_DASHBOARD_PORT", default_dashboard_port)
    default_base_host = dashboard_host
    if default_base_host in {"0.0.0.0", "::"}:
        default_base_host = "127.0.0.1"
    dashboard_base_url = _to_str(
        env_values,
        "PEQUOD_DASHBOARD_BASE_URL",
        f"http://{default_base_host}:{dashboard_port}",
    ).rstrip("/")

    return Settings(
        allium_api_key=api_key,
        allium_base_url=_to_str(env_values, "ALLIUM_BASE_URL", "https://api.allium.so").rstrip("/"),
        watchlist_path=Path(_to_str(env_values, "PEQUOD_WATCHLIST_PATH", "watchlists/default.json")),
        poll_interval_seconds=_to_int(env_values, "PEQUOD_POLL_INTERVAL_SECONDS", 30),
        min_alert_usd=_to_float(env_values, "PEQUOD_MIN_ALERT_USD", 10_000),
        lookback_seconds=_to_int(env_values, "PEQUOD_LOOKBACK_SECONDS", 180),
        http_timeout_seconds=_to_int(env_values, "PEQUOD_HTTP_TIMEOUT_SECONDS", 20),
        max_addresses_per_request=_to_int(env_values, "PEQUOD_MAX_ADDRESSES_PER_REQUEST", 20),
        dedupe_db_path=Path(_to_str(env_values, "PEQUOD_DEDUPE_DB_PATH", "data/alerts.sqlite3")),
        telegram_bot_token=_to_str(env_values, "PEQUOD_TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=_to_str(env_values, "PEQUOD_TELEGRAM_CHAT_ID"),
        discord_webhook_url=_to_str(env_values, "PEQUOD_DISCORD_WEBHOOK_URL"),
        generic_webhook_url=_to_str(env_values, "PEQUOD_GENERIC_WEBHOOK_URL"),
        run_once=_to_bool(env_values, "PEQUOD_RUN_ONCE", False),
        dashboard_host=dashboard_host,
        dashboard_port=dashboard_port,
        dashboard_base_url=dashboard_base_url,
        geo_cache_path=Path(_to_str(env_values, "PEQUOD_GEO_CACHE_PATH", "data/geo_cache.json")),
        geo_refresh_interval_seconds=_to_int(env_values, "PEQUOD_GEO_REFRESH_INTERVAL_SECONDS", 86_400),
        balance_refresh_interval_seconds=_to_int(env_values, "PEQUOD_BALANCE_REFRESH_INTERVAL_SECONDS", 900),
        auto_discover_counterparties=_to_bool(env_values, "PEQUOD_AUTO_DISCOVER_COUNTERPARTIES", True),
        discover_min_usd=_to_float(env_values, "PEQUOD_DISCOVER_MIN_USD", 25_000),
        discovered_watch_max=_to_int(env_values, "PEQUOD_DISCOVERED_WATCH_MAX", 500),
        geo_bootstrap_max_addresses=_to_int(env_values, "PEQUOD_GEO_BOOTSTRAP_MAX_ADDRESSES", 300),
        dashboard_max_alerts=_to_int(env_values, "PEQUOD_DASHBOARD_MAX_ALERTS", 300),
        dashboard_max_events=_to_int(env_values, "PEQUOD_DASHBOARD_MAX_EVENTS", 1500),
    )
