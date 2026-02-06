from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List

from .event_engine import build_map_event
from .sinks import AlertSink
from .types import Alert, WatchAddress


class DashboardState:
    def __init__(self, watchlist: List[WatchAddress], max_alerts: int = 300, max_events: int = 1500) -> None:
        self._lock = threading.Lock()
        self._watch_by_address: Dict[str, WatchAddress] = {w.address.lower(): w for w in watchlist}
        self._geo_by_address: Dict[str, Dict[str, Any]] = {}
        self._alerts: Deque[Dict[str, Any]] = deque(maxlen=max_alerts)
        self._events: Deque[Dict[str, Any]] = deque(maxlen=max_events)
        self._metrics_by_address: Dict[str, Dict[str, Any]] = {
            address: self._default_metric_row()
            for address in self._watch_by_address
        }
        self._started_at = int(time.time())
        self._filters: Dict[str, Any] = {
            "types": [],
            "chains": [],
            "min_usd": 0.0,
            "window_seconds": 3600,
            "replay_offset_seconds": 0,
        }
        self._event_types_seen: set[str] = set()
        self._chains_seen: set[str] = {w.chain.lower() for w in watchlist}

    @staticmethod
    def _default_metric_row() -> Dict[str, Any]:
        return {
            "last_alert_usd": None,
            "last_alert_at": None,
            "alerts_24h": 0,
            "alert_count_total": 0,
            "holdings_total_usd": None,
            "holdings_token_count": 0,
            "top_holdings": [],
            "holdings_updated_at": None,
        }

    def update_geo(self, geo_by_address: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            for address, value in geo_by_address.items():
                self._geo_by_address[address.lower()] = value

    def add_watch_addresses(self, addresses: List[WatchAddress]) -> int:
        added = 0
        with self._lock:
            for watch in addresses:
                normalized = watch.address.lower()
                if normalized in self._watch_by_address:
                    continue
                self._watch_by_address[normalized] = watch
                self._metrics_by_address.setdefault(normalized, self._default_metric_row())
                self._chains_seen.add(watch.chain.lower())
                added += 1
        return added

    def set_filters(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if "types" in payload and isinstance(payload["types"], list):
                self._filters["types"] = sorted(
                    {str(item).strip().lower() for item in payload["types"] if str(item).strip()}
                )
            if "chains" in payload and isinstance(payload["chains"], list):
                self._filters["chains"] = sorted(
                    {str(item).strip().lower() for item in payload["chains"] if str(item).strip()}
                )
            if "min_usd" in payload:
                try:
                    self._filters["min_usd"] = max(0.0, float(payload["min_usd"]))
                except (TypeError, ValueError):
                    pass
            if "window_seconds" in payload:
                try:
                    window = int(payload["window_seconds"])
                    self._filters["window_seconds"] = max(60, min(24 * 60 * 60, window))
                except (TypeError, ValueError):
                    pass
            if "replay_offset_seconds" in payload:
                try:
                    offset = int(payload["replay_offset_seconds"])
                    self._filters["replay_offset_seconds"] = max(0, min(24 * 60 * 60, offset))
                except (TypeError, ValueError):
                    pass
            return dict(self._filters)

    def update_balances(self, by_address: Dict[str, Dict[str, Any]], updated_at: int) -> None:
        with self._lock:
            for address in self._watch_by_address:
                metric = self._metrics_by_address.setdefault(address, self._default_metric_row())
                metric["holdings_updated_at"] = updated_at

            for address, payload in by_address.items():
                normalized = address.lower()
                if normalized not in self._watch_by_address:
                    continue
                metric = self._metrics_by_address.setdefault(normalized, self._default_metric_row())
                total_usd = payload.get("holdings_total_usd")
                metric["holdings_total_usd"] = float(total_usd) if isinstance(total_usd, (int, float)) else None
                token_count = payload.get("holdings_token_count")
                metric["holdings_token_count"] = int(token_count) if isinstance(token_count, (int, float)) else 0
                top_holdings = payload.get("top_holdings")
                if isinstance(top_holdings, list):
                    metric["top_holdings"] = [item for item in top_holdings if isinstance(item, dict)][:3]
                else:
                    metric["top_holdings"] = []

    def ingest_alert(self, alert: Alert) -> None:
        with self._lock:
            addresses = self._addresses_for_alert(alert)
            now = int(time.time())
            for address in addresses:
                metric = self._metrics_by_address.setdefault(address, self._default_metric_row())
                metric["last_alert_usd"] = alert.usd_value
                metric["last_alert_at"] = alert.timestamp or now
                metric["alert_count_total"] = int(metric.get("alert_count_total") or 0) + 1
            event = build_map_event(
                alert=alert,
                now_ts=now,
                geo_by_address=self._geo_by_address,
                watch_by_address=self._watch_by_address,
            )
            self._event_types_seen.add(str(event.get("event_type", "")).lower())
            self._chains_seen.add(alert.chain.lower())

            alert_row = {
                "dedupe_key": alert.dedupe_key,
                "text": alert.text,
                "usd_value": alert.usd_value,
                "score": alert.score,
                "score_reasons": alert.score_reasons,
                "score_breakdown": alert.score_breakdown,
                "tx_id": alert.tx_id,
                "chain": alert.chain,
                "tx_type": alert.tx_type,
                "timestamp": alert.timestamp,
                "watch_address": alert.watch_address,
                "from_address": alert.from_address,
                "to_address": alert.to_address,
                "token_symbol": alert.token_symbol,
                "token_address": alert.token_address,
                "amount": alert.amount,
                "addresses": addresses,
                "event_type": event.get("event_type"),
                "entities": alert.entities,
                "deep_link": alert.deep_link,
            }
            self._alerts.appendleft(alert_row)
            self._events.appendleft(event)
            self._recompute_alert_counts_24h(now)

    def _addresses_for_alert(self, alert: Alert) -> List[str]:
        out: List[str] = []
        candidates = [alert.watch_address, alert.from_address, alert.to_address]
        raw_addr = alert.raw.get("address")
        if isinstance(raw_addr, str):
            candidates.append(raw_addr)
        for value in candidates:
            if not isinstance(value, str) or not value:
                continue
            normalized = value.lower()
            if normalized in self._watch_by_address and normalized not in out:
                out.append(normalized)
        return out

    def _recompute_alert_counts_24h(self, now_ts: int) -> None:
        cutoff = now_ts - (24 * 60 * 60)
        counts: Dict[str, int] = {addr: 0 for addr in self._watch_by_address}
        for alert in self._alerts:
            ts = alert.get("timestamp")
            if not isinstance(ts, int):
                continue
            if ts < cutoff:
                continue
            for addr in alert.get("addresses", []):
                if isinstance(addr, str) and addr in counts:
                    counts[addr] += 1
        for address, metric in self._metrics_by_address.items():
            metric["alerts_24h"] = counts.get(address, 0)

    def _apply_filters(self, events: List[Dict[str, Any]], now_ts: int) -> List[Dict[str, Any]]:
        selected_types = {value.lower() for value in self._filters.get("types", [])}
        selected_chains = {value.lower() for value in self._filters.get("chains", [])}
        min_usd = float(self._filters.get("min_usd", 0.0) or 0.0)
        window_seconds = int(self._filters.get("window_seconds", 3600) or 3600)
        replay_offset_seconds = int(self._filters.get("replay_offset_seconds", 0) or 0)
        pivot_ts = now_ts - replay_offset_seconds
        start_ts = pivot_ts - window_seconds

        filtered: List[Dict[str, Any]] = []
        for event in events:
            usd = event.get("usd_value")
            if isinstance(usd, (int, float)) and usd < min_usd:
                continue
            if selected_types and str(event.get("event_type", "")).lower() not in selected_types:
                continue
            if selected_chains and str(event.get("chain", "")).lower() not in selected_chains:
                continue
            ts = event.get("timestamp")
            if isinstance(ts, int):
                if ts < start_ts or ts > pivot_ts:
                    continue
            filtered.append(event)
        return filtered

    def _apply_filters_to_alerts(self, alerts: List[Dict[str, Any]], now_ts: int) -> List[Dict[str, Any]]:
        selected_types = {value.lower() for value in self._filters.get("types", [])}
        selected_chains = {value.lower() for value in self._filters.get("chains", [])}
        min_usd = float(self._filters.get("min_usd", 0.0) or 0.0)
        window_seconds = int(self._filters.get("window_seconds", 3600) or 3600)
        replay_offset_seconds = int(self._filters.get("replay_offset_seconds", 0) or 0)
        pivot_ts = now_ts - replay_offset_seconds
        start_ts = pivot_ts - window_seconds

        out: List[Dict[str, Any]] = []
        for alert in alerts:
            usd = alert.get("usd_value")
            if isinstance(usd, (int, float)) and usd < min_usd:
                continue
            if selected_types and str(alert.get("event_type", "")).lower() not in selected_types:
                continue
            if selected_chains and str(alert.get("chain", "")).lower() not in selected_chains:
                continue
            ts = alert.get("timestamp")
            if isinstance(ts, int):
                if ts < start_ts or ts > pivot_ts:
                    continue
            out.append(alert)
        return out

    @staticmethod
    def _sea_state(events: List[Dict[str, Any]], now_ts: int) -> Dict[str, Any]:
        score_15m = 0.0
        count_5m = 0
        for event in events:
            ts = event.get("timestamp")
            if not isinstance(ts, int):
                continue
            if ts >= now_ts - 15 * 60:
                score_15m += float(event.get("score") or 0.0)
            if ts >= now_ts - 5 * 60:
                count_5m += 1
        if score_15m >= 300:
            tier = "storm"
        elif score_15m >= 120:
            tier = "rough"
        else:
            tier = "calm"
        return {
            "tier": tier,
            "score_15m": round(score_15m, 2),
            "events_5m": count_5m,
            "updated_at": now_ts,
        }

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now_ts = int(time.time())
            raw_events = list(self._events)
            raw_alerts = list(self._alerts)
            filtered_events = self._apply_filters(raw_events, now_ts=now_ts)
            filtered_alerts = self._apply_filters_to_alerts(raw_alerts, now_ts=now_ts)
            whales: List[Dict[str, Any]] = []
            for address, watch in self._watch_by_address.items():
                metric = self._metrics_by_address.get(address, {})
                geo = self._geo_by_address.get(address, {})
                whales.append(
                    {
                        "address": address,
                        "label": watch.label,
                        "chain": watch.chain,
                        "category": watch.category,
                        "primary_country": geo.get("primary_country"),
                        "primary_region": geo.get("primary_region"),
                        "confidence": geo.get("confidence"),
                        "geo_score": geo.get("score"),
                        "lat": geo.get("lat"),
                        "lon": geo.get("lon"),
                        "last_alert_usd": metric.get("last_alert_usd"),
                        "last_alert_at": metric.get("last_alert_at"),
                        "alerts_24h": metric.get("alerts_24h", 0),
                        "alert_count_total": metric.get("alert_count_total", 0),
                        "holdings_total_usd": metric.get("holdings_total_usd"),
                        "holdings_token_count": metric.get("holdings_token_count", 0),
                        "top_holdings": metric.get("top_holdings", []),
                        "holdings_updated_at": metric.get("holdings_updated_at"),
                    }
                )
            whales.sort(
                key=lambda item: (
                    item.get("last_alert_usd") or 0,
                    item.get("alerts_24h") or 0,
                    item.get("alert_count_total") or 0,
                ),
                reverse=True,
            )
            sea_state = self._sea_state(filtered_events, now_ts=now_ts)
            available_event_types = sorted(value for value in self._event_types_seen if value)
            available_chains = sorted(value for value in self._chains_seen if value)
            return {
                "generated_at": now_ts,
                "started_at": self._started_at,
                "whales": whales,
                "alerts": filtered_alerts[:240],
                "events": filtered_events[:900],
                "sea_state": sea_state,
                "filters": dict(self._filters),
                "filters_meta": {
                    "available_event_types": available_event_types,
                    "available_chains": available_chains,
                    "max_replay_offset_seconds": 24 * 60 * 60,
                },
                "watch_count": len(self._watch_by_address),
                "geo_count": len([1 for row in self._geo_by_address.values() if row]),
                "event_count": len(filtered_events),
            }


class DashboardSink(AlertSink):
    def __init__(self, state: DashboardState) -> None:
        self._state = state

    def send(self, alert: Alert) -> None:
        self._state.ingest_alert(alert)
