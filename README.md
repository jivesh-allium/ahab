# Pequod Whale Watcher

Whale transaction alert service built on Allium APIs.

## What this MVP does

- Loads a curated watchlist of addresses across chains.
- Polls Allium `POST /api/v1/developer/wallet/transactions` in batches.
- Estimates USD value using transaction-provided USD values or `POST /api/v1/developer/prices`.
- Deduplicates alerts using SQLite.
- Broadcasts alerts to console, Telegram, Discord, and a generic webhook.
- Serves a live frontend with an ocean map and whale markers backed by Allium geo data.

## Quick start

1. Copy env template and set your API key.
2. Optionally edit `watchlists/default.json`.
3. Run everything (dashboard + API + live poller).

```bash
cp .env.example .env
python3 -m pequod
```

You can also use:

```bash
make run
```

One-cycle smoke run:

```bash
PEQUOD_RUN_ONCE=true python3 -m pequod.main
```

Run dashboard/frontend:

```bash
python3 -m pequod dashboard
```

Open `http://127.0.0.1:8080` (or your configured host/port).

Dashboard UX tips:
- Use `Scout Now` to force an immediate poll cycle (no need to wait for interval).
- Increase the `Window` slider to pull more real historical events onto the map.
- Route filters default to `Geo + Anchored` to reduce pseudo-route noise; enable `Pseudo` when needed.
- Use `Save` / `Delete` / `Copy Link` in `Saved Views` to persist and share investigative presets.

Run poller only (no frontend):

```bash
python3 -m pequod poller
```

## Docker

One command for anyone with Docker:

```bash
docker compose up --build
```

Then open `http://127.0.0.1:8080`.

Notes:
- `docker-compose.yml` reads `ALLIUM_API_KEY` from your local `.env`.
- Container data is persisted to `./data`.
- Watchlist is mounted from `./watchlists` (read-only).

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `ALLIUM_API_KEY` | required | Allium API key |
| `ALLIUM_BASE_URL` | `https://api.allium.so` | API base URL |
| `PEQUOD_WATCHLIST_PATH` | `watchlists/default.json` | Watchlist file |
| `PEQUOD_POLL_INTERVAL_SECONDS` | `30` | Poll interval |
| `PEQUOD_MIN_ALERT_USD` | `10000` | Minimum USD threshold |
| `PEQUOD_LOOKBACK_SECONDS` | `180` | Startup lookback for new alerts |
| `PEQUOD_HTTP_TIMEOUT_SECONDS` | `20` | HTTP timeout |
| `PEQUOD_MAX_ADDRESSES_PER_REQUEST` | `20` | Batch size for wallet endpoint |
| `PEQUOD_DEDUPE_DB_PATH` | `data/alerts.sqlite3` | Dedupe database path |
| `PEQUOD_RUN_ONCE` | `false` | Execute one poll cycle then exit |
| `PEQUOD_DASHBOARD_HOST` | `127.0.0.1` | Dashboard server bind host |
| `PEQUOD_DASHBOARD_PORT` | `8080` | Dashboard server port |
| `PEQUOD_DASHBOARD_BASE_URL` | `http://127.0.0.1:8080` | Public dashboard URL used for alert deep links |
| `PEQUOD_DASHBOARD_MAX_ALERTS` | `300` | In-memory alert history size |
| `PEQUOD_DASHBOARD_MAX_EVENTS` | `1500` | In-memory cinematic event history size |
| `PEQUOD_GEO_CACHE_PATH` | `data/geo_cache.json` | Cached geo attribution data |
| `PEQUOD_GEO_REFRESH_INTERVAL_SECONDS` | `86400` | Geo refresh cadence (daily default) |
| `PEQUOD_BALANCE_REFRESH_INTERVAL_SECONDS` | `900` | Wallet holdings refresh cadence |
| `PEQUOD_AUTO_DISCOVER_COUNTERPARTIES` | `true` | Auto-add large-transfer counterparties to watch set |
| `PEQUOD_DISCOVER_MIN_USD` | `25000` | Minimum transfer USD to discover unknown counterparties |
| `PEQUOD_DISCOVERED_WATCH_MAX` | `500` | Cap for discovered counterparty addresses |
| `PEQUOD_GEO_BOOTSTRAP_MAX_ADDRESSES` | `300` | Append top geo addresses to watchlist (set `0` to disable) |
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
- Geo attribution is fetched from `allium_identity.geo.addresses_geography` and cached locally.
- Wallet portfolio snapshots are fetched from `POST /api/v1/developer/wallet/balances`.
- Unknown high-value counterparties are auto-discovered and added into the runtime watch set (bounded by config).
- Moby-Dick tooltip/header quotes are loaded from `frontend/moby_quotes.json` (edit this file to add/remove lines).
- `/api/state` includes live stream metrics (`events_ingested`, `events_usable`, `price_miss_rate`, `events_per_min`, `active_whales_5m`) to validate animation density.
- Alerts now include explainable score breakdowns and entity context (watchlist-derived + heuristic fallback).
- Deep links in alerts open directly into focused event replay state in the dashboard.
- Saved views are local to your browser (`localStorage`) and can be shared using `Copy Link`.
