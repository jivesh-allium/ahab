# Pequod Whale Watcher

Whale transaction alert service built on Allium APIs.

## What this MVP does

- Loads a curated watchlist of addresses across chains.
- Polls Allium `POST /api/v1/developer/wallet/transactions` in batches.
- Estimates USD value using transaction-provided USD values or `POST /api/v1/developer/prices`.
- Deduplicates alerts using SQLite.
- Broadcasts alerts to console, Telegram, Discord, and a generic webhook.

## Quick start

1. Copy env template and set your API key.
2. Optionally edit `watchlists/default.json`.
3. Run the app.

```bash
cp .env.example .env
python3 -m pequod.main
```

One-cycle smoke run:

```bash
PEQUOD_RUN_ONCE=true python3 -m pequod.main
```

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `ALLIUM_API_KEY` | required | Allium API key |
| `ALLIUM_BASE_URL` | `https://api.allium.so` | API base URL |
| `PEQUOD_WATCHLIST_PATH` | `watchlists/default.json` | Watchlist file |
| `PEQUOD_POLL_INTERVAL_SECONDS` | `30` | Poll interval |
| `PEQUOD_MIN_ALERT_USD` | `1000000` | Minimum USD threshold |
| `PEQUOD_LOOKBACK_SECONDS` | `180` | Startup lookback for new alerts |
| `PEQUOD_HTTP_TIMEOUT_SECONDS` | `20` | HTTP timeout |
| `PEQUOD_MAX_ADDRESSES_PER_REQUEST` | `20` | Batch size for wallet endpoint |
| `PEQUOD_DEDUPE_DB_PATH` | `data/alerts.sqlite3` | Dedupe database path |
| `PEQUOD_RUN_ONCE` | `false` | Execute one poll cycle then exit |
| `PEQUOD_TELEGRAM_BOT_TOKEN` | empty | Telegram bot token |
| `PEQUOD_TELEGRAM_CHAT_ID` | empty | Telegram chat ID |
| `PEQUOD_DISCORD_WEBHOOK_URL` | empty | Discord webhook URL |
| `PEQUOD_GENERIC_WEBHOOK_URL` | empty | Generic webhook endpoint |

## Watchlist formats supported

Nested format:

```json
{
  "ethereum": {
    "exchanges": {
      "binance_hot_1": "0x28C6c06298d514Db089934071355E5743bf21d60"
    },
    "whales": {
      "vitalik": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    }
  }
}
```

Flat format:

```json
[
  {"chain": "ethereum", "address": "0x...", "label": "Binance Hot"},
  {"chain": "solana", "address": "So11111111111111111111111111111111111111112", "label": "SOL"}
]
```

## Run tests

```bash
python3 -m unittest discover -s tests
```

## Notes

- Allium rate limit is 1 request/sec. The client enforces it.
- Alerts include the required attribution: `Powered by Allium`.
