import json
import tempfile
import unittest
from pathlib import Path

from pequod.watchlist import load_watchlist


class WatchlistTests(unittest.TestCase):
    def test_loads_nested_watchlist(self) -> None:
        payload = {
            "ethereum": {
                "exchanges": {"binance": "0xabc"},
                "whales": [{"address": "0xdef", "label": "Whale X"}],
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watchlist.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            items = load_watchlist(path)

        self.assertEqual(2, len(items))
        labels = {item.label for item in items}
        self.assertIn("binance", labels)
        self.assertIn("Whale X", labels)

    def test_loads_flat_watchlist(self) -> None:
        payload = [
            {"chain": "ethereum", "address": "0xabc", "label": "Binance"},
            {"chain": "solana", "address": "So111", "label": "SOL Whale"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watchlist.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            items = load_watchlist(path)

        self.assertEqual(2, len(items))


if __name__ == "__main__":
    unittest.main()

