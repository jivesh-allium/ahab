from __future__ import annotations

import threading
import logging
import math
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

from .alerts import build_alert
from .allium_client import AlliumClient, AlliumError
from .dedupe import DedupeStore
from .sinks import MultiSink
from .tx_extractors import normalize_transactions
from .types import NormalizedTransaction, WatchAddress
from .utils import chunked

LOG = logging.getLogger(__name__)


class WhalePoller:
    def __init__(
        self,
        client: AlliumClient,
        watchlist: List[WatchAddress],
        dedupe_store: DedupeStore,
        sink: MultiSink,
        min_alert_usd: float,
        max_addresses_per_request: int,
        poll_interval_seconds: int,
        lookback_seconds: int,
        auto_discover_counterparties: bool = False,
        discover_min_usd: float = 0.0,
        discovered_watch_max: int = 0,
        on_discovered_watch_addresses: Optional[Callable[[List[WatchAddress]], None]] = None,
        dashboard_base_url: str = "",
    ) -> None:
        self._client = client
        self._watchlist = watchlist
        self._dedupe_store = dedupe_store
        self._sink = sink
        self._min_alert_usd = min_alert_usd
        self._max_addresses_per_request = max(1, min(20, max_addresses_per_request))
        self._poll_interval_seconds = max(5, poll_interval_seconds)
        self._address_to_chain: Dict[str, str] = {item.address.lower(): item.chain for item in watchlist}
        self._address_labels: Dict[str, WatchAddress] = {item.address.lower(): item for item in watchlist}
        self._auto_discover_counterparties = auto_discover_counterparties
        self._discover_min_usd = max(0.0, float(discover_min_usd))
        self._discovered_watch_max = max(0, int(discovered_watch_max))
        self._on_discovered_watch_addresses = on_discovered_watch_addresses
        self._dashboard_base_url = dashboard_base_url.strip().rstrip("/")
        self._dynamic_watch_count = 0
        self._discovered_watch_total = 0
        cutoff = int(time.time()) - max(0, lookback_seconds)
        self._latest_timestamp_by_watch_address: Dict[str, int] = {item.address.lower(): cutoff for item in watchlist}
        self._metrics_lock = threading.Lock()
        self._started_at = int(time.time())
        self._events_ingested_total = 0
        self._events_new_total = 0
        self._events_usable_total = 0
        self._alerts_sent_total = 0
        self._price_items_requested_total = 0
        self._price_items_quoted_total = 0
        self._price_missing_total = 0
        self._price_errors_total = 0
        self._price_request_calls_total = 0
        self._recent_alerts: Deque[Tuple[int, str]] = deque(maxlen=8000)
        self._score_history_by_watch: Dict[str, Dict[str, Deque[Any]]] = {}
        self._last_cycle: Dict[str, Any] = {
            "started_at": self._started_at,
            "completed_at": self._started_at,
            "events_ingested": 0,
            "events_new": 0,
            "events_usable": 0,
            "alerts_sent": 0,
            "price_items_requested": 0,
            "price_items_quoted": 0,
            "price_missing": 0,
            "price_errors": 0,
            "price_request_calls": 0,
            "discovered_watch_addresses": 0,
        }

    def run_forever(self) -> None:
        LOG.info("Starting poller with %d watched addresses.", len(self._watchlist))
        while True:
            started = time.time()
            self.run_once()
            elapsed = time.time() - started
            sleep_for = max(0.0, self._poll_interval_seconds - elapsed)
            time.sleep(sleep_for)

    def run_once(self) -> None:
        payload_addresses = [{"chain": item.chain, "address": item.address} for item in self._watchlist]
        cycle_started = int(time.time())
        normalized_all: List[NormalizedTransaction] = []
        for batch in chunked(payload_addresses, self._max_addresses_per_request):
            try:
                raw = self._client.wallet_transactions(batch)
            except AlliumError as exc:
                LOG.error("wallet/transactions failed: %s", exc)
                continue

            normalized = normalize_transactions(raw, self._address_to_chain)
            normalized_all.extend(normalized)

        cycle = self._process_transactions(normalized_all)
        cycle["started_at"] = cycle_started
        cycle["completed_at"] = int(time.time())
        self._commit_cycle_metrics(cycle)

    @staticmethod
    def _normalize_address(value: Optional[str]) -> str:
        if not isinstance(value, str):
            return ""
        return value.strip().lower()

    @staticmethod
    def _short_addr(value: str) -> str:
        if not value:
            return "unknown"
        if len(value) <= 14:
            return value
        return f"{value[:8]}...{value[-6:]}"

    @staticmethod
    def _looks_like_exchange(label: str) -> bool:
        value = label.lower()
        return any(
            token in value
            for token in (
                "binance",
                "coinbase",
                "kraken",
                "okx",
                "bybit",
                "bitfinex",
                "kucoin",
                "exchange",
            )
        )

    def _watch_key_for_tx(self, tx: NormalizedTransaction) -> str:
        for candidate in (tx.watch_address, tx.from_address, tx.to_address):
            normalized = self._normalize_address(candidate)
            if normalized and normalized in self._address_labels:
                return normalized
        return self._normalize_address(tx.watch_address) or self._normalize_address(tx.from_address) or self._normalize_address(tx.to_address)

    def _counterparty_for_tx(self, tx: NormalizedTransaction) -> str:
        watch = self._watch_key_for_tx(tx)
        from_addr = self._normalize_address(tx.from_address)
        to_addr = self._normalize_address(tx.to_address)
        if watch:
            if from_addr and from_addr != watch:
                return from_addr
            if to_addr and to_addr != watch:
                return to_addr
        return to_addr or from_addr

    def _entity_for_address(self, address: Optional[str], chain: str) -> Dict[str, Any]:
        normalized = self._normalize_address(address)
        if not normalized:
            return {
                "address": None,
                "display_name": "unknown",
                "kind": "unknown",
                "confidence": "low",
                "source": "none",
                "tags": [],
                "chain": chain.lower(),
            }
        watch = self._address_labels.get(normalized)
        if watch is None:
            return {
                "address": normalized,
                "display_name": self._short_addr(normalized),
                "kind": "unknown",
                "confidence": "low",
                "source": "heuristic",
                "tags": [],
                "chain": chain.lower(),
            }
        category = str(watch.category or "watchlist").strip().lower()
        tags: List[str] = ["watchlist"]
        if category:
            tags.append(category)
        is_exchange = category in {"exchange", "exchanges"} or self._looks_like_exchange(watch.label)
        if is_exchange:
            tags.append("exchange")
        confidence = "medium" if category == "discovered" else "high"
        return {
            "address": normalized,
            "display_name": watch.label,
            "kind": category or "watchlist",
            "confidence": confidence,
            "source": "watchlist",
            "tags": sorted(set(tags)),
            "chain": chain.lower(),
        }

    def _enrich_entities(self, tx: NormalizedTransaction) -> Dict[str, Dict[str, Any]]:
        watch_key = self._watch_key_for_tx(tx)
        counterparty = self._counterparty_for_tx(tx)
        entities: Dict[str, Dict[str, Any]] = {
            "watch": self._entity_for_address(watch_key, tx.chain),
            "from": self._entity_for_address(tx.from_address, tx.chain),
            "to": self._entity_for_address(tx.to_address, tx.chain),
            "counterparty": self._entity_for_address(counterparty, tx.chain),
        }
        entities["watch"]["role"] = "watch"
        entities["from"]["role"] = "from"
        entities["to"]["role"] = "to"
        entities["counterparty"]["role"] = "counterparty"
        return entities

    def _history_row(self, watch_key: str) -> Dict[str, Deque[Any]]:
        row = self._score_history_by_watch.get(watch_key)
        if row is not None:
            return row
        created: Dict[str, Deque[Any]] = {
            "usd_samples": deque(maxlen=120),
            "recent_alert_ts": deque(maxlen=360),
            "counterparties": deque(maxlen=720),
        }
        self._score_history_by_watch[watch_key] = created
        return created

    @staticmethod
    def _median(values: Deque[Any]) -> Optional[float]:
        numeric = [float(item) for item in values if isinstance(item, (int, float))]
        if not numeric:
            return None
        ordered = sorted(numeric)
        mid = len(ordered) // 2
        if len(ordered) % 2 == 1:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2.0

    def _prune_score_history(self, watch_key: str, now_ts: int) -> Dict[str, Deque[Any]]:
        row = self._history_row(watch_key)
        while row["recent_alert_ts"] and int(row["recent_alert_ts"][0]) < now_ts - 24 * 60 * 60:
            row["recent_alert_ts"].popleft()
        while row["counterparties"] and int(row["counterparties"][0][0]) < now_ts - 7 * 24 * 60 * 60:
            row["counterparties"].popleft()
        return row

    def _score_alert(
        self,
        tx: NormalizedTransaction,
        usd_value: float,
        entities: Dict[str, Dict[str, Any]],
        now_ts: int,
    ) -> Dict[str, Any]:
        watch_key = self._watch_key_for_tx(tx)
        history = self._prune_score_history(watch_key, now_ts=now_ts) if watch_key else None
        baseline = self._median(history["usd_samples"]) if history else None
        counterparty = self._counterparty_for_tx(tx)

        breakdown: Dict[str, float] = {}
        reasons: List[Dict[str, Any]] = []

        magnitude = min(45.0, math.log10(max(0.0, float(usd_value)) + 10.0) * 9.0)
        breakdown["magnitude"] = round(magnitude, 2)
        reasons.append(
            {
                "key": "magnitude",
                "label": "Large USD flow magnitude",
                "impact": round(magnitude, 2),
                "detail": f"{usd_value:,.2f} USD",
            }
        )

        anomaly = 0.0
        anomaly_detail = ""
        if baseline and baseline > 0:
            ratio = usd_value / baseline
            if ratio >= 8.0:
                anomaly = 18.0
            elif ratio >= 4.0:
                anomaly = 12.0
            elif ratio >= 2.0:
                anomaly = 7.0
            if anomaly > 0:
                anomaly_detail = f"{ratio:.1f}x vs watch median"
        elif usd_value >= 1_000_000:
            anomaly = 6.0
            anomaly_detail = "no baseline yet; absolute size is high"
        if anomaly > 0:
            breakdown["size_anomaly"] = anomaly
            reasons.append(
                {
                    "key": "size_anomaly",
                    "label": "Size anomaly vs recent watch flow",
                    "impact": anomaly,
                    "detail": anomaly_detail,
                }
            )

        novelty = 0.0
        if counterparty and history:
            seen_recently = any(
                str(item[1]).lower() == counterparty
                for item in history["counterparties"]
                if isinstance(item, tuple) and len(item) == 2
            )
            if not seen_recently:
                novelty = 10.0
                breakdown["counterparty_novelty"] = novelty
                reasons.append(
                    {
                        "key": "counterparty_novelty",
                        "label": "New counterparty for this watched wallet",
                        "impact": novelty,
                        "detail": self._short_addr(counterparty),
                    }
                )

        bridge = 0.0
        tx_type_value = (tx.tx_type or "").lower()
        if "bridge" in tx_type_value:
            bridge = 8.0
            breakdown["bridge_interaction"] = bridge
            reasons.append(
                {
                    "key": "bridge_interaction",
                    "label": "Bridge interaction",
                    "impact": bridge,
                    "detail": tx.tx_type,
                }
            )

        cex = 0.0
        has_exchange = False
        for key in ("watch", "from", "to", "counterparty"):
            entity = entities.get(key) or {}
            tags = entity.get("tags")
            if isinstance(tags, list) and any(str(tag).lower() == "exchange" for tag in tags):
                has_exchange = True
                break
        if has_exchange:
            cex = 7.0
            breakdown["cex_interaction"] = cex
            reasons.append(
                {
                    "key": "cex_interaction",
                    "label": "Exchange-linked wallet involved",
                    "impact": cex,
                    "detail": "watchlist or label suggests exchange entity",
                }
            )

        burst = 0.0
        if history:
            burst_count = 0
            for value in history["recent_alert_ts"]:
                if int(value) >= now_ts - 5 * 60:
                    burst_count += 1
            if burst_count >= 4:
                burst = 8.0
            elif burst_count >= 2:
                burst = 4.0
            if burst > 0:
                breakdown["burst_activity"] = burst
                reasons.append(
                    {
                        "key": "burst_activity",
                        "label": "Burst of recent alerts on this watch",
                        "impact": burst,
                        "detail": f"{burst_count} alerts in 5m",
                    }
                )

        total = max(0.0, min(100.0, magnitude + anomaly + novelty + bridge + cex + burst))
        ranked = sorted(
            [item for item in reasons if float(item.get("impact") or 0.0) > 0],
            key=lambda item: float(item.get("impact") or 0.0),
            reverse=True,
        )
        return {
            "score": round(total, 2),
            "breakdown": {key: round(float(value), 2) for key, value in breakdown.items()},
            "reasons": ranked[:4],
            "watch_key": watch_key,
            "counterparty": counterparty,
        }

    def _record_alert_history(
        self,
        watch_key: str,
        counterparty: str,
        usd_value: float,
        ts: int,
    ) -> None:
        if not watch_key:
            return
        row = self._prune_score_history(watch_key, now_ts=ts)
        row["usd_samples"].append(float(usd_value))
        row["recent_alert_ts"].append(int(ts))
        if counterparty:
            row["counterparties"].append((int(ts), counterparty))

    def _process_transactions(self, transactions: List[NormalizedTransaction]) -> Dict[str, int]:
        cycle = {
            "events_ingested": len(transactions),
            "events_new": 0,
            "events_usable": 0,
            "alerts_sent": 0,
            "price_items_requested": 0,
            "price_items_quoted": 0,
            "price_missing": 0,
            "price_errors": 0,
            "price_request_calls": 0,
            "discovered_watch_addresses": 0,
        }
        discovered_in_cycle: List[WatchAddress] = []
        new_transactions: List[NormalizedTransaction] = []
        for tx in transactions:
            if not self._is_new_enough(tx):
                continue
            new_transactions.append(tx)
        cycle["events_new"] = len(new_transactions)

        prefetch = self._prefetch_prices(new_transactions)
        cycle["price_items_requested"] = prefetch["price_items_requested"]
        cycle["price_items_quoted"] = prefetch["price_items_quoted"]
        cycle["price_errors"] = prefetch["price_errors"]
        cycle["price_request_calls"] = prefetch["price_request_calls"]

        for tx in new_transactions:
            usd_value = self._resolve_usd_value(tx)
            if usd_value is None:
                if self._requires_price_lookup(tx):
                    cycle["price_missing"] += 1
                self._bump_watermark(tx)
                continue
            cycle["events_usable"] += 1
            if usd_value < self._min_alert_usd:
                self._bump_watermark(tx)
                continue

            discovered = self._discover_counterparties(tx=tx, usd_value=usd_value)
            if discovered:
                cycle["discovered_watch_addresses"] += len(discovered)
                discovered_in_cycle.extend(discovered)

            now_ts = int(time.time())
            entities = self._enrich_entities(tx)
            score_meta = self._score_alert(
                tx=tx,
                usd_value=usd_value,
                entities=entities,
                now_ts=now_ts,
            )
            alert = build_alert(
                tx=tx,
                usd_value=usd_value,
                label_by_address=self._address_labels,
                score=float(score_meta["score"]),
                score_reasons=list(score_meta["reasons"]),
                score_breakdown=dict(score_meta["breakdown"]),
                entities=entities,
                dashboard_base_url=self._dashboard_base_url,
            )
            if self._dedupe_store.has_seen(alert.dedupe_key):
                self._bump_watermark(tx)
                continue

            self._sink.send(alert)
            self._dedupe_store.mark_seen(alert.dedupe_key)
            cycle["alerts_sent"] += 1
            self._record_alert_history(
                watch_key=str(score_meta.get("watch_key") or ""),
                counterparty=str(score_meta.get("counterparty") or ""),
                usd_value=usd_value,
                ts=alert.timestamp if isinstance(alert.timestamp, int) else now_ts,
            )
            self._mark_alert_activity(alert)
            self._bump_watermark(tx)
        if discovered_in_cycle and self._on_discovered_watch_addresses:
            try:
                self._on_discovered_watch_addresses(discovered_in_cycle)
            except Exception:
                LOG.exception("Discovered-watch callback failed for %d addresses.", len(discovered_in_cycle))
        return cycle

    def _is_new_enough(self, tx: NormalizedTransaction) -> bool:
        if not tx.watch_address:
            return True
        current = self._latest_timestamp_by_watch_address.get(tx.watch_address.lower())
        if current is None:
            return True
        if tx.timestamp is None:
            return True
        return tx.timestamp > current

    def _bump_watermark(self, tx: NormalizedTransaction) -> None:
        if not tx.watch_address or tx.timestamp is None:
            return
        key = tx.watch_address.lower()
        current = self._latest_timestamp_by_watch_address.get(key, 0)
        if tx.timestamp > current:
            self._latest_timestamp_by_watch_address[key] = tx.timestamp

    def _resolve_usd_value(self, tx: NormalizedTransaction) -> Optional[float]:
        if tx.usd_value is not None and tx.usd_value >= 0:
            return tx.usd_value

        if tx.amount is None or tx.token_address is None:
            return None

        cached = self._client.get_cached_price(tx.chain, tx.token_address)

        if cached is None:
            return None
        return abs(tx.amount) * cached.price

    @staticmethod
    def _requires_price_lookup(tx: NormalizedTransaction) -> bool:
        return tx.usd_value is None and tx.amount is not None and tx.token_address is not None

    def _prefetch_prices(self, transactions: List[NormalizedTransaction]) -> Dict[str, int]:
        unique_tokens: Set[Tuple[str, str]] = set()
        for tx in transactions:
            if not self._requires_price_lookup(tx):
                continue
            chain = tx.chain.lower()
            token_address = tx.token_address.lower() if tx.token_address else ""
            if not chain or not token_address:
                continue
            if self._client.get_cached_price(chain, token_address) is not None:
                continue
            unique_tokens.add((chain, token_address))

        stats = {
            "price_items_requested": len(unique_tokens),
            "price_items_quoted": 0,
            "price_errors": 0,
            "price_request_calls": 0,
        }
        if not unique_tokens:
            return stats

        payload = [{"chain": chain, "token_address": token} for chain, token in sorted(unique_tokens)]
        for chunk in chunked(payload, 50):
            stats["price_request_calls"] += 1
            try:
                quotes = self._client.prices(chunk)
                stats["price_items_quoted"] += len(quotes)
            except AlliumError as exc:
                stats["price_errors"] += len(chunk)
                LOG.warning("price batch lookup failed for %d tokens: %s", len(chunk), exc)
        return stats

    def _mark_alert_activity(self, alert: Any) -> None:
        ts = alert.timestamp if isinstance(alert.timestamp, int) else int(time.time())
        address = (alert.watch_address or alert.from_address or alert.to_address or "").lower()
        if not address:
            return
        with self._metrics_lock:
            self._recent_alerts.append((ts, address))

    def _prune_recent_alerts(self, now_ts: int) -> None:
        cutoff = now_ts - 3600
        while self._recent_alerts and self._recent_alerts[0][0] < cutoff:
            self._recent_alerts.popleft()

    def _commit_cycle_metrics(self, cycle: Dict[str, int]) -> None:
        with self._metrics_lock:
            self._events_ingested_total += int(cycle.get("events_ingested", 0))
            self._events_new_total += int(cycle.get("events_new", 0))
            self._events_usable_total += int(cycle.get("events_usable", 0))
            self._alerts_sent_total += int(cycle.get("alerts_sent", 0))
            self._price_items_requested_total += int(cycle.get("price_items_requested", 0))
            self._price_items_quoted_total += int(cycle.get("price_items_quoted", 0))
            self._price_missing_total += int(cycle.get("price_missing", 0))
            self._price_errors_total += int(cycle.get("price_errors", 0))
            self._price_request_calls_total += int(cycle.get("price_request_calls", 0))
            self._discovered_watch_total += int(cycle.get("discovered_watch_addresses", 0))
            self._last_cycle = dict(cycle)

    @staticmethod
    def _is_probably_evm_address(address: str) -> bool:
        normalized = address.lower()
        if not normalized.startswith("0x") or len(normalized) != 42:
            return False
        return all(ch in "0123456789abcdef" for ch in normalized[2:])

    def _is_valid_counterparty(self, address: str, chain: str) -> bool:
        if not address:
            return False
        if chain.lower() in {"ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche"}:
            return self._is_probably_evm_address(address)
        return len(address) >= 20

    def _discover_counterparties(self, tx: NormalizedTransaction, usd_value: float) -> List[WatchAddress]:
        if not self._auto_discover_counterparties:
            return []
        if usd_value < self._discover_min_usd:
            return []
        if self._dynamic_watch_count >= self._discovered_watch_max:
            return []

        watched = (tx.watch_address or "").lower()
        candidates: List[str] = []
        for value in (tx.from_address, tx.to_address):
            if not isinstance(value, str):
                continue
            normalized = value.strip().lower()
            if not normalized:
                continue
            if watched and normalized == watched:
                continue
            if normalized in self._address_to_chain:
                continue
            candidates.append(normalized)

        discovered: List[WatchAddress] = []
        for candidate in candidates:
            if self._dynamic_watch_count >= self._discovered_watch_max:
                break
            if not self._is_valid_counterparty(candidate, tx.chain):
                continue
            label = f"{candidate[:6]}..{candidate[-4:]}"
            watch = WatchAddress(chain=tx.chain.lower(), address=candidate, label=label, category="discovered")
            self._watchlist.append(watch)
            self._address_to_chain[candidate] = tx.chain.lower()
            self._address_labels[candidate] = watch
            self._latest_timestamp_by_watch_address.setdefault(candidate, int(time.time()) - 60)
            self._dynamic_watch_count += 1
            discovered.append(watch)
        if discovered:
            LOG.info("Discovered %d counterpart watch addresses (total discovered: %d).", len(discovered), self._dynamic_watch_count)
        return discovered

    def metrics_snapshot(self) -> Dict[str, Any]:
        now_ts = int(time.time())
        with self._metrics_lock:
            self._prune_recent_alerts(now_ts)
            events_1m = 0
            active_whales_5m: Set[str] = set()
            for ts, address in self._recent_alerts:
                if ts >= now_ts - 60:
                    events_1m += 1
                if ts >= now_ts - 300:
                    active_whales_5m.add(address)
            requested = self._price_items_requested_total
            price_miss_rate = 0.0 if requested <= 0 else (self._price_missing_total / requested)
            return {
                "started_at": self._started_at,
                "events_ingested": self._events_ingested_total,
                "events_new": self._events_new_total,
                "events_usable": self._events_usable_total,
                "alerts_sent": self._alerts_sent_total,
                "price_items_requested": self._price_items_requested_total,
                "price_items_quoted": self._price_items_quoted_total,
                "price_missing": self._price_missing_total,
                "price_errors": self._price_errors_total,
                "price_request_calls": self._price_request_calls_total,
                "discovered_watch_addresses": self._discovered_watch_total,
                "price_miss_rate": round(price_miss_rate, 4),
                "events_per_min": events_1m,
                "active_whales_5m": len(active_whales_5m),
                "last_cycle": dict(self._last_cycle),
            }
