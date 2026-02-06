from __future__ import annotations

import json
import logging
import mimetypes
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .allium_client import AlliumClient, AlliumError
from .config import Settings, load_settings
from .dashboard_state import DashboardSink, DashboardState
from .dedupe import DedupeStore
from .geo import GeoResolver
from .poller import WhalePoller
from .sinks import MultiSink
from .types import WatchAddress
from .balances import extract_wallet_balance_summary
from .utils import chunked
from .watchlist import load_watchlist

LOG = logging.getLogger(__name__)


class DashboardRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = AlliumClient(
            base_url=settings.allium_base_url,
            api_key=settings.allium_api_key,
            timeout_seconds=settings.http_timeout_seconds,
        )
        watchlist = load_watchlist(settings.watchlist_path)
        self.geo = GeoResolver(
            client=self.client,
            cache_path=settings.geo_cache_path,
            refresh_interval_seconds=settings.geo_refresh_interval_seconds,
        )
        if settings.geo_bootstrap_max_addresses > 0:
            watchlist = self._merge_geo_bootstrap(watchlist, settings.geo_bootstrap_max_addresses)
        self.watchlist = watchlist
        self.state = DashboardState(
            watchlist=watchlist,
            max_alerts=settings.dashboard_max_alerts,
            max_events=settings.dashboard_max_events,
        )
        self.dedupe = DedupeStore(settings.dedupe_db_path)
        self.sink = MultiSink([DashboardSink(self.state)])
        self.poller = WhalePoller(
            client=self.client,
            watchlist=self.watchlist,
            dedupe_store=self.dedupe,
            sink=self.sink,
            min_alert_usd=settings.min_alert_usd,
            max_addresses_per_request=settings.max_addresses_per_request,
            poll_interval_seconds=settings.poll_interval_seconds,
            lookback_seconds=settings.lookback_seconds,
            auto_discover_counterparties=settings.auto_discover_counterparties,
            discover_min_usd=settings.discover_min_usd,
            discovered_watch_max=settings.discovered_watch_max,
            on_discovered_watch_addresses=self._register_discovered_watch_addresses,
            dashboard_base_url=settings.dashboard_base_url,
        )
        self._stop_event = threading.Event()
        self._poll_lock = threading.Lock()
        self._poll_thread: Optional[threading.Thread] = None
        self._geo_thread: Optional[threading.Thread] = None
        self._balance_thread: Optional[threading.Thread] = None
        self._geo_last_refresh_at = 0
        self._balance_last_refresh_at = 0

    def _merge_geo_bootstrap(self, current: List[WatchAddress], limit: int) -> List[WatchAddress]:
        base = list(current)
        existing = {item.address.lower() for item in base}
        try:
            extra = self.geo.bootstrap_watchlist(limit=limit, chain="ethereum")
        except AlliumError as exc:
            LOG.warning("Geo bootstrap failed: %s", exc)
            return base
        added = 0
        for item in extra:
            if item.address.lower() in existing:
                continue
            base.append(item)
            existing.add(item.address.lower())
            added += 1
        if added:
            LOG.info("Added %d geo bootstrap addresses to watchlist.", added)
        return base

    def start(self) -> None:
        self.refresh_geo(force=False)
        self.refresh_balances(force=False)
        self._poll_thread = threading.Thread(target=self._poll_loop, name="pequod-poller", daemon=True)
        self._geo_thread = threading.Thread(target=self._geo_loop, name="pequod-geo", daemon=True)
        self._balance_thread = threading.Thread(target=self._balance_loop, name="pequod-balances", daemon=True)
        self._poll_thread.start()
        self._geo_thread.start()
        self._balance_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self.dedupe.close()

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            started = time.time()
            try:
                self.poll_now()
            except Exception:
                LOG.exception("Poll loop iteration failed.")
            elapsed = time.time() - started
            sleep_for = max(1.0, self.settings.poll_interval_seconds - elapsed)
            self._stop_event.wait(sleep_for)

    def poll_now(self) -> None:
        with self._poll_lock:
            self.poller.run_once()

    def _geo_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.refresh_geo(force=False)
            except Exception:
                LOG.exception("Geo refresh loop failed.")
            self._stop_event.wait(max(300, self.settings.geo_refresh_interval_seconds))

    def _balance_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.refresh_balances(force=False)
            except Exception:
                LOG.exception("Balance refresh loop failed.")
            self._stop_event.wait(max(120, self.settings.balance_refresh_interval_seconds))

    def refresh_geo(self, force: bool) -> None:
        geo_by_address = self.geo.get_geo_for_watchlist(self.watchlist, force=force)
        self.state.update_geo(geo_by_address)
        self._geo_last_refresh_at = int(time.time())

    def _register_discovered_watch_addresses(self, addresses: List[WatchAddress]) -> None:
        if not addresses:
            return
        added = self.state.add_watch_addresses(addresses)
        if added <= 0:
            return
        geo_rows = self.geo.get_geo_for_watchlist(addresses, force=False)
        self.state.update_geo(geo_rows)
        self._geo_last_refresh_at = int(time.time())
        LOG.info("Discovered and registered %d new watch addresses.", added)

    def refresh_balances(self, force: bool) -> None:
        now_ts = int(time.time())
        if (
            not force
            and self._balance_last_refresh_at > 0
            and now_ts - self._balance_last_refresh_at < self.settings.balance_refresh_interval_seconds
        ):
            return
        payload_addresses = [{"chain": item.chain, "address": item.address} for item in self.watchlist]
        by_address: Dict[str, Dict[str, Any]] = {}
        for batch in chunked(payload_addresses, self.settings.max_addresses_per_request):
            try:
                raw = self.client.wallet_balances(batch)
            except AlliumError as exc:
                LOG.warning("wallet/balances failed for batch of %d: %s", len(batch), exc)
                continue
            parsed = extract_wallet_balance_summary(raw)
            for address, summary in parsed.items():
                by_address[address.lower()] = summary
        self.state.update_balances(by_address=by_address, updated_at=now_ts)
        self._balance_last_refresh_at = now_ts

    def snapshot(self) -> Dict[str, Any]:
        base = self.state.snapshot()
        base["config"] = {
            "min_alert_usd": self.settings.min_alert_usd,
            "poll_interval_seconds": self.settings.poll_interval_seconds,
            "dashboard_base_url": self.settings.dashboard_base_url,
            "geo_refresh_interval_seconds": self.settings.geo_refresh_interval_seconds,
            "balance_refresh_interval_seconds": self.settings.balance_refresh_interval_seconds,
            "auto_discover_counterparties": self.settings.auto_discover_counterparties,
            "discover_min_usd": self.settings.discover_min_usd,
            "discovered_watch_max": self.settings.discovered_watch_max,
        }
        base["geo_last_refresh_at"] = self._geo_last_refresh_at
        base["balance_last_refresh_at"] = self._balance_last_refresh_at
        metrics = self.poller.metrics_snapshot()
        base["metrics"] = metrics
        base["events_ingested"] = metrics.get("events_ingested", 0)
        base["events_usable"] = metrics.get("events_usable", 0)
        base["price_miss_rate"] = metrics.get("price_miss_rate", 0.0)
        base["events_per_min"] = metrics.get("events_per_min", 0)
        base["active_whales_5m"] = metrics.get("active_whales_5m", 0)
        return base

    def set_filters(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.state.set_filters(payload)


class DashboardHandler(BaseHTTPRequestHandler):
    runtime: DashboardRuntime
    static_root: Path

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._json_response(self.runtime.snapshot())
            return
        if parsed.path == "/api/state/filters":
            self._json_response({"ok": True, "filters": self.runtime.snapshot().get("filters", {})})
            return
        if parsed.path == "/api/health":
            self._json_response({"ok": True, "ts": int(time.time())})
            return
        self._serve_static(parsed.path)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/refresh-geo":
            try:
                self.runtime.refresh_geo(force=True)
                self._json_response({"ok": True, "refreshed_at": int(time.time())})
            except Exception as exc:
                self._json_response({"ok": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/refresh-balances":
            try:
                self.runtime.refresh_balances(force=True)
                self._json_response({"ok": True, "refreshed_at": int(time.time())})
            except Exception as exc:
                self._json_response({"ok": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/poll-now":
            try:
                self.runtime.poll_now()
                self._json_response({"ok": True, "polled_at": int(time.time())})
            except Exception as exc:
                self._json_response({"ok": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/state/filters":
            try:
                payload = self._read_json_body()
                filters = self.runtime.set_filters(payload if isinstance(payload, dict) else {})
                self._json_response({"ok": True, "filters": filters})
            except Exception as exc:
                self._json_response({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._json_response({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.info("%s - %s", self.address_string(), fmt % args)

    def _json_response(self, payload: Dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, raw_path: str) -> None:
        request_path = raw_path or "/"
        if request_path == "/":
            target = self.static_root / "index.html"
        else:
            target = (self.static_root / request_path.lstrip("/")).resolve()
            if not str(target).startswith(str(self.static_root.resolve())):
                self.send_error(HTTPStatus.FORBIDDEN)
                return
        if not target.exists() or not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        data = target.read_bytes()
        ctype, _ = mimetypes.guess_type(str(target))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json_body(self) -> Dict[str, Any]:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        parsed = json.loads(raw.decode("utf-8"))
        if isinstance(parsed, dict):
            return parsed
        return {}


def run_dashboard() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = load_settings()
    runtime = DashboardRuntime(settings)
    runtime.start()

    static_root = Path(__file__).resolve().parent.parent / "frontend"
    if not static_root.exists():
        raise FileNotFoundError(f"Frontend directory not found: {static_root}")

    class _Handler(DashboardHandler):
        pass

    _Handler.runtime = runtime
    _Handler.static_root = static_root

    server = ThreadingHTTPServer((settings.dashboard_host, settings.dashboard_port), _Handler)
    LOG.info(
        "Dashboard serving on http://%s:%s with %d watched addresses.",
        settings.dashboard_host,
        settings.dashboard_port,
        len(runtime.watchlist),
    )
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        LOG.info("Dashboard shutting down.")
    finally:
        runtime.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(run_dashboard())
