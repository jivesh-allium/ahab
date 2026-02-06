from __future__ import annotations

import logging
import sys

from .allium_client import AlliumClient
from .config import load_settings
from .dedupe import DedupeStore
from .poller import WhalePoller
from .sinks import build_sinks
from .watchlist import load_watchlist


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    configure_logging()
    logger = logging.getLogger("pequod")

    try:
        settings = load_settings()
    except Exception as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    try:
        watchlist = load_watchlist(settings.watchlist_path)
    except Exception as exc:
        logger.error("Watchlist error: %s", exc)
        return 1

    if not watchlist:
        logger.error("Watchlist is empty: %s", settings.watchlist_path)
        return 1

    client = AlliumClient(
        base_url=settings.allium_base_url,
        api_key=settings.allium_api_key,
        timeout_seconds=settings.http_timeout_seconds,
    )
    dedupe_store = DedupeStore(settings.dedupe_db_path)
    sinks = build_sinks(
        timeout_seconds=settings.http_timeout_seconds,
        telegram_bot_token=settings.telegram_bot_token,
        telegram_chat_id=settings.telegram_chat_id,
        discord_webhook_url=settings.discord_webhook_url,
        generic_webhook_url=settings.generic_webhook_url,
    )

    logger.info(
        "Loaded %d watched addresses. Threshold: $%.2f, poll interval: %ss",
        len(watchlist),
        settings.min_alert_usd,
        settings.poll_interval_seconds,
    )

    poller = WhalePoller(
        client=client,
        watchlist=watchlist,
        dedupe_store=dedupe_store,
        sink=sinks,
        min_alert_usd=settings.min_alert_usd,
        max_addresses_per_request=settings.max_addresses_per_request,
        poll_interval_seconds=settings.poll_interval_seconds,
        lookback_seconds=settings.lookback_seconds,
        auto_discover_counterparties=settings.auto_discover_counterparties,
        discover_min_usd=settings.discover_min_usd,
        discovered_watch_max=settings.discovered_watch_max,
        dashboard_base_url=settings.dashboard_base_url,
    )

    try:
        if settings.run_once:
            logger.info("Running a single poll cycle (PEQUOD_RUN_ONCE=true).")
            poller.run_once()
        else:
            poller.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        dedupe_store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
