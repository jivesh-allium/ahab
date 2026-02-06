from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

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
        cutoff = int(time.time()) - max(0, lookback_seconds)
        self._latest_timestamp_by_watch_address: Dict[str, int] = {item.address.lower(): cutoff for item in watchlist}

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

        for batch in chunked(payload_addresses, self._max_addresses_per_request):
            try:
                raw = self._client.wallet_transactions(batch)
            except AlliumError as exc:
                LOG.error("wallet/transactions failed: %s", exc)
                continue

            normalized = normalize_transactions(raw, self._address_to_chain)
            self._process_transactions(normalized)

    def _process_transactions(self, transactions: List[NormalizedTransaction]) -> None:
        for tx in transactions:
            if not self._is_new_enough(tx):
                continue

            usd_value = self._resolve_usd_value(tx)
            if usd_value is None or usd_value < self._min_alert_usd:
                self._bump_watermark(tx)
                continue

            alert = build_alert(tx=tx, usd_value=usd_value, label_by_address=self._address_labels)
            if self._dedupe_store.has_seen(alert.dedupe_key):
                self._bump_watermark(tx)
                continue

            self._sink.send(alert)
            self._dedupe_store.mark_seen(alert.dedupe_key)
            self._bump_watermark(tx)

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
            try:
                quotes = self._client.prices([{"token_address": tx.token_address, "chain": tx.chain}])
                if quotes:
                    cached = quotes[0]
            except AlliumError as exc:
                LOG.warning("price lookup failed for %s on %s: %s", tx.token_address, tx.chain, exc)
                return None

        if cached is None:
            return None
        return abs(tx.amount) * cached.price
