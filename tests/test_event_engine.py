import unittest

from pequod.event_engine import build_map_event, classify_event_type, event_score, severity_for_usd
from pequod.types import Alert, WatchAddress


class EventEngineTests(unittest.TestCase):
    def test_classification(self) -> None:
        self.assertEqual("bridge_move", classify_event_type("asset_bridge"))
        self.assertEqual("dex_swap", classify_event_type("dex_trade"))
        self.assertEqual("mint", classify_event_type("mint"))
        self.assertEqual("burn", classify_event_type("burn"))
        self.assertEqual("transfer_large", classify_event_type("asset_transfer"))

    def test_severity_and_scoring(self) -> None:
        self.assertEqual("storm", severity_for_usd(2_000_000))
        self.assertEqual("rough", severity_for_usd(200_000))
        self.assertEqual("calm", severity_for_usd(1000))
        high = event_score("bridge_move", 2_000_000, now_ts=1000, event_ts=995)
        low = event_score("transfer_large", 1000, now_ts=1000, event_ts=995)
        self.assertGreater(high, low)

    def test_unknown_counterparty_is_anchored_near_known_geo(self) -> None:
        alert = Alert(
            dedupe_key="eth:tx:test",
            text="test",
            usd_value=150000.0,
            tx_id="0xtx",
            chain="ethereum",
            tx_type="asset_transfer",
            timestamp=1_770_000_000,
            watch_address="0xwatch",
            from_address="0xwatch",
            to_address="0xunknown",
            token_symbol="USDC",
            token_address="0xa0b8",
            amount=150000.0,
            raw={},
            score=88.0,
            score_reasons=[{"key": "counterparty_novelty", "label": "New counterparty", "impact": 10.0}],
            score_breakdown={"counterparty_novelty": 10.0},
            entities={"from": {"display_name": "whale"}, "to": {"display_name": "new_wallet"}},
        )
        geo = {"0xwatch": {"lat": 37.7, "lon": -122.4, "primary_country": "United States"}}
        watch_by_address = {"0xwatch": WatchAddress(chain="ethereum", address="0xwatch", label="whale")}
        event = build_map_event(alert=alert, now_ts=1_770_000_010, geo_by_address=geo, watch_by_address=watch_by_address)
        source = event["source"]
        target = event["target"]
        self.assertEqual("geo", source.get("geo_source"))
        self.assertEqual("anchored", target.get("geo_source"))
        self.assertLess(abs(float(target["lat"]) - float(source["lat"])), 8.5)
        self.assertLess(abs(float(target["lon"]) - float(source["lon"])), 12.5)
        self.assertAlmostEqual(88.0, event["score"])
        self.assertEqual("new_wallet", event["target"]["label"])


if __name__ == "__main__":
    unittest.main()
