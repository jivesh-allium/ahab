import unittest

from pequod.alerts import build_alert
from pequod.types import NormalizedTransaction, WatchAddress


class AlertTests(unittest.TestCase):
    def test_build_alert_includes_required_attribution(self) -> None:
        tx = NormalizedTransaction(
            tx_id="0xabc",
            chain="ethereum",
            tx_type="asset_transfer",
            from_address="0xfrom",
            to_address="0xto",
            token_address="0xtoken",
            token_symbol="USDC",
            amount=2_000_000.0,
            usd_value=2_000_000.0,
            timestamp=1_706_000_000,
            watch_address="0xwatch",
            raw={"transaction_hash": "0xabc"},
        )
        labels = {
            "0xfrom": WatchAddress(chain="ethereum", address="0xfrom", label="Binance"),
            "0xto": WatchAddress(chain="ethereum", address="0xto", label="Whale Wallet"),
        }
        alert = build_alert(tx, 2_000_000.0, labels)

        self.assertIn("Powered by Allium", alert.text)
        self.assertIn("$2,000,000.00", alert.text)


if __name__ == "__main__":
    unittest.main()

