import unittest
import time

from pequod.dashboard_state import DashboardState
from pequod.types import Alert, WatchAddress


class DashboardStateTests(unittest.TestCase):
    def test_ingest_alert_updates_metrics_and_feed(self) -> None:
        now = int(time.time())
        watchlist = [WatchAddress(chain="ethereum", address="0xwatch", label="Whale 1")]
        state = DashboardState(watchlist=watchlist, max_alerts=10, max_events=10)
        alert = Alert(
            dedupe_key="eth:tx:transfer",
            text="alert",
            usd_value=12345.0,
            tx_id="0xtx",
            chain="ethereum",
            tx_type="asset_transfer",
            timestamp=now - 30,
            watch_address="0xwatch",
            from_address="0xwatch",
            to_address="0xother",
            token_symbol="USDC",
            token_address="0xa0b8",
            amount=12345.0,
            raw={"address": "0xwatch"},
            score=66.5,
            score_reasons=[{"key": "size_anomaly", "label": "Size anomaly vs recent watch flow", "impact": 12.0}],
            score_breakdown={"magnitude": 30.0, "size_anomaly": 12.0},
            entities={"watch": {"display_name": "Whale 1"}},
            deep_link="http://localhost:8080/?event=eth%3Atx%3Atransfer",
        )
        state.ingest_alert(alert)
        snapshot = state.snapshot()

        self.assertEqual(1, len(snapshot["alerts"]))
        self.assertEqual(1, len(snapshot["events"]))
        self.assertAlmostEqual(66.5, snapshot["alerts"][0]["score"])
        self.assertEqual("http://localhost:8080/?event=eth%3Atx%3Atransfer", snapshot["alerts"][0]["deep_link"])
        self.assertAlmostEqual(66.5, snapshot["events"][0]["score"])
        whale = snapshot["whales"][0]
        self.assertEqual("0xwatch", whale["address"])
        self.assertEqual(12345.0, whale["last_alert_usd"])
        self.assertGreaterEqual(whale["alert_count_total"], 1)
        self.assertIn("sea_state", snapshot)

    def test_filters_apply_to_event_and_alert_streams(self) -> None:
        now = int(time.time())
        watchlist = [WatchAddress(chain="ethereum", address="0xwatch", label="Whale 1")]
        state = DashboardState(watchlist=watchlist, max_alerts=10, max_events=10)
        high = Alert(
            dedupe_key="eth:tx:bridge",
            text="bridge alert",
            usd_value=2_000_000.0,
            tx_id="0xbridge",
            chain="ethereum",
            tx_type="asset_bridge",
            timestamp=now - 20,
            watch_address="0xwatch",
            from_address="0xwatch",
            to_address="0xother",
            token_symbol="ETH",
            token_address="0xeth",
            amount=1000.0,
            raw={"address": "0xwatch"},
        )
        low = Alert(
            dedupe_key="eth:tx:swap",
            text="swap alert",
            usd_value=1_000.0,
            tx_id="0xswap",
            chain="ethereum",
            tx_type="dex_trade",
            timestamp=now - 10,
            watch_address="0xwatch",
            from_address="0xwatch",
            to_address="0xother",
            token_symbol="USDC",
            token_address="0xusdc",
            amount=1000.0,
            raw={"address": "0xwatch"},
        )
        state.ingest_alert(high)
        state.ingest_alert(low)
        state.set_filters({"types": ["bridge_move"], "min_usd": 100000})
        snapshot = state.snapshot()
        self.assertEqual(1, len(snapshot["events"]))
        self.assertEqual("bridge_move", snapshot["events"][0]["event_type"])
        self.assertEqual(1, len(snapshot["alerts"]))
        self.assertEqual("asset_bridge", snapshot["alerts"][0]["tx_type"])

    def test_update_balances_includes_portfolio_fields(self) -> None:
        now = int(time.time())
        watchlist = [WatchAddress(chain="ethereum", address="0xwatch", label="Whale 1")]
        state = DashboardState(watchlist=watchlist, max_alerts=10, max_events=10)
        state.update_balances(
            {
                "0xwatch": {
                    "holdings_total_usd": 12_345_678.9,
                    "holdings_token_count": 2,
                    "top_holdings": [
                        {"symbol": "ETH", "usd_value": 11_000_000.0},
                        {"symbol": "USDC", "usd_value": 1_345_678.9},
                    ],
                }
            },
            updated_at=now,
        )
        snapshot = state.snapshot()
        whale = snapshot["whales"][0]
        self.assertAlmostEqual(12_345_678.9, whale["holdings_total_usd"])
        self.assertEqual(2, whale["holdings_token_count"])
        self.assertEqual("ETH", whale["top_holdings"][0]["symbol"])
        self.assertEqual(now, whale["holdings_updated_at"])

    def test_add_watch_addresses_registers_new_rows(self) -> None:
        watchlist = [WatchAddress(chain="ethereum", address="0xwatch", label="Whale 1")]
        state = DashboardState(watchlist=watchlist, max_alerts=10, max_events=10)
        added = state.add_watch_addresses(
            [WatchAddress(chain="ethereum", address="0xnew", label="Discovered", category="discovered")]
        )
        self.assertEqual(1, added)
        snapshot = state.snapshot()
        addresses = [row["address"] for row in snapshot["whales"]]
        self.assertIn("0xnew", addresses)


if __name__ == "__main__":
    unittest.main()
