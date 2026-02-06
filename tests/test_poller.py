import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, List, Optional

from pequod.allium_client import PriceQuote
from pequod.dedupe import DedupeStore
from pequod.poller import WhalePoller
from pequod.sinks import AlertSink, MultiSink
from pequod.types import Alert, WatchAddress


class RecordingSink(AlertSink):
    def __init__(self) -> None:
        self.alerts: List[Alert] = []

    def send(self, alert: Alert) -> None:
        self.alerts.append(alert)


class FakeClient:
    def __init__(self, transactions_payload: Any, prices_by_key: Dict[str, float]) -> None:
        self._transactions_payload = transactions_payload
        self._prices_by_key = prices_by_key
        self._cache: Dict[str, float] = {}
        self.price_calls: List[List[Dict[str, str]]] = []

    def wallet_transactions(self, addresses: List[Dict[str, str]]) -> Any:
        return self._transactions_payload

    def prices(self, tokens: List[Dict[str, str]]) -> List[PriceQuote]:
        self.price_calls.append(tokens)
        quotes: List[PriceQuote] = []
        for item in tokens:
            chain = item.get("chain", "").lower()
            token = item.get("token_address", "").lower()
            price = self._prices_by_key.get(f"{chain}:{token}")
            if price is None:
                continue
            self._cache[f"{chain}:{token}"] = float(price)
            quotes.append(PriceQuote(chain=chain, token_address=token, price=float(price), symbol=None))
        return quotes

    def get_cached_price(self, chain: str, token_address: str, ttl_seconds: int = 60) -> Optional[PriceQuote]:
        key = f"{chain.lower()}:{token_address.lower()}"
        price = self._cache.get(key)
        if price is None:
            return None
        return PriceQuote(chain=chain.lower(), token_address=token_address.lower(), price=price, symbol=None)


class PollerTests(unittest.TestCase):
    def _build_poller(
        self,
        tmp_dir: Path,
        client: FakeClient,
        sink: RecordingSink,
        min_alert_usd: float = 1.0,
        auto_discover_counterparties: bool = False,
        discover_min_usd: float = 0.0,
        discovered_watch_max: int = 0,
        on_discovered_watch_addresses: Optional[Callable[[List[WatchAddress]], None]] = None,
    ) -> WhalePoller:
        return WhalePoller(
            client=client,  # type: ignore[arg-type]
            watchlist=[WatchAddress(chain="ethereum", address="0x1111111111111111111111111111111111111111", label="Watch Whale")],
            dedupe_store=DedupeStore(tmp_dir / "dedupe.sqlite3"),
            sink=MultiSink([sink]),
            min_alert_usd=min_alert_usd,
            max_addresses_per_request=20,
            poll_interval_seconds=20,
            lookback_seconds=3600,
            auto_discover_counterparties=auto_discover_counterparties,
            discover_min_usd=discover_min_usd,
            discovered_watch_max=discovered_watch_max,
            on_discovered_watch_addresses=on_discovered_watch_addresses,
        )

    def test_batches_price_lookups_once_per_unique_token(self) -> None:
        now = int(time.time())
        payload = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "items": [
                    {
                        "transaction_hash": "0xtx1",
                        "chain": "ethereum",
                        "activity_type": "asset_transfer",
                        "from_address": "0xa",
                        "to_address": "0xb",
                        "token_address": "0xtoken",
                        "amount": 10,
                        "block_timestamp": now - 5,
                    },
                    {
                        "transaction_hash": "0xtx2",
                        "chain": "ethereum",
                        "activity_type": "asset_transfer",
                        "from_address": "0xc",
                        "to_address": "0xd",
                        "token_address": "0xtoken",
                        "amount": 20,
                        "block_timestamp": now - 4,
                    },
                ],
            }
        ]
        client = FakeClient(payload, {"ethereum:0xtoken": 100.0})
        sink = RecordingSink()
        with TemporaryDirectory() as tmp:
            poller = self._build_poller(Path(tmp), client, sink, min_alert_usd=1.0)
            poller.run_once()
            metrics = poller.metrics_snapshot()

        self.assertEqual(1, len(client.price_calls))
        self.assertEqual(1, len(client.price_calls[0]))
        self.assertEqual(2, len(sink.alerts))
        self.assertGreater(sink.alerts[0].score, 0.0)
        self.assertTrue(len(sink.alerts[0].score_reasons) >= 1)
        self.assertIn("watch", sink.alerts[0].entities)
        self.assertIn("counterparty", sink.alerts[0].entities)
        self.assertEqual(2, metrics["events_ingested"])
        self.assertEqual(2, metrics["events_usable"])
        self.assertEqual(1, metrics["price_items_requested"])
        self.assertEqual(0, metrics["price_missing"])
        self.assertGreaterEqual(metrics["events_per_min"], 2)

    def test_records_price_miss_when_quote_unavailable(self) -> None:
        now = int(time.time())
        payload = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "items": [
                    {
                        "transaction_hash": "0xtx-miss",
                        "chain": "ethereum",
                        "activity_type": "asset_transfer",
                        "from_address": "0xa",
                        "to_address": "0xb",
                        "token_address": "0xunknown",
                        "amount": 42,
                        "block_timestamp": now - 8,
                    }
                ],
            }
        ]
        client = FakeClient(payload, {})
        sink = RecordingSink()
        with TemporaryDirectory() as tmp:
            poller = self._build_poller(Path(tmp), client, sink, min_alert_usd=1.0)
            poller.run_once()
            metrics = poller.metrics_snapshot()

        self.assertEqual(1, metrics["events_ingested"])
        self.assertEqual(0, metrics["events_usable"])
        self.assertEqual(1, metrics["price_items_requested"])
        self.assertEqual(1, metrics["price_missing"])
        self.assertEqual(1.0, metrics["price_miss_rate"])
        self.assertEqual(0, len(sink.alerts))

    def test_discovers_unknown_counterparty_into_watch_set(self) -> None:
        now = int(time.time())
        payload = [
            {
                "address": "0x1111111111111111111111111111111111111111",
                "items": [
                    {
                        "transaction_hash": "0xtx-discover",
                        "chain": "ethereum",
                        "activity_type": "asset_transfer",
                        "from_address": "0x1111111111111111111111111111111111111111",
                        "to_address": "0x2222222222222222222222222222222222222222",
                        "token_address": "0xtoken",
                        "amount": 10,
                        "usd_value": 120000,
                        "block_timestamp": now - 3,
                    }
                ],
            }
        ]
        client = FakeClient(payload, {})
        sink = RecordingSink()
        discovered: List[WatchAddress] = []
        with TemporaryDirectory() as tmp:
            poller = self._build_poller(
                Path(tmp),
                client,
                sink,
                min_alert_usd=1000.0,
                auto_discover_counterparties=True,
                discover_min_usd=50_000.0,
                discovered_watch_max=20,
                on_discovered_watch_addresses=lambda rows: discovered.extend(rows),
            )
            poller.run_once()
            metrics = poller.metrics_snapshot()

        self.assertEqual(1, len(discovered))
        self.assertEqual("0x2222222222222222222222222222222222222222", discovered[0].address)
        self.assertEqual("discovered", discovered[0].category)
        self.assertEqual(1, metrics["discovered_watch_addresses"])


if __name__ == "__main__":
    unittest.main()
