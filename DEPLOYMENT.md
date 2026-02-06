# Deployment Guide

This app runs a long-lived Python process (`python -m pequod`) that serves HTTP and continuously polls Allium. Use a host that supports persistent processes.

Vercel is not a good primary target for this repository because Vercel functions are request-driven and scale to zero, while Pequod expects a continuously running poller.

## Recommended: Render (Blueprint)

1. Push this repo to GitHub/GitLab.
2. In Render, create a new Blueprint and point it at this repo.
3. Render will pick up `render.yaml` automatically.
4. In the service environment variables, set:
   - `ALLIUM_API_KEY` (required)
   - `PEQUOD_DASHBOARD_BASE_URL` (optional but recommended, use your public service URL)
   - Any optional sink vars (`PEQUOD_TELEGRAM_BOT_TOKEN`, `PEQUOD_DISCORD_WEBHOOK_URL`, etc.)
5. Deploy and open your service URL.
6. Verify health at `/api/health`.

## Also Works: Railway

1. Create a new Railway project from this repo.
2. Railway will build from the included `Dockerfile`.
3. Set environment variables:
   - `ALLIUM_API_KEY` (required)
   - `PEQUOD_DASHBOARD_BASE_URL` (optional but recommended)
   - Optional sink vars as needed
4. Deploy and confirm `/api/health` is healthy.

## Persistence Notes

- SQLite and geo cache default to `data/` (`PEQUOD_DEDUPE_DB_PATH`, `PEQUOD_GEO_CACHE_PATH`).
- If you want data to survive restarts/redeploys, attach persistent storage and mount it at `/app/data` (or set those paths to your mounted volume).
