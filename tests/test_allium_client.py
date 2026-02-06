import unittest

from pequod.allium_client import AlliumClient


class DummyClient(AlliumClient):
    def __init__(self, response):
        super().__init__(base_url="https://api.allium.so", api_key="x")
        self._response = response

    def _request(self, method, path, payload=None):  # type: ignore[override]
        return self._response


class AlliumClientTests(unittest.TestCase):
    def test_prices_parses_items_envelope(self) -> None:
        client = DummyClient(
            {
                "items": [
                    {
                        "chain": "ethereum",
                        "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "symbol": "USDC",
                        "price": 1.0019,
                    }
                ]
            }
        )
        quotes = client.prices(
            [{"token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "chain": "ethereum"}]
        )
        self.assertEqual(1, len(quotes))
        self.assertEqual("ethereum", quotes[0].chain)
        self.assertEqual("USDC", quotes[0].symbol)
        self.assertAlmostEqual(1.0019, quotes[0].price)

    def test_prices_parses_legacy_list_shape(self) -> None:
        client = DummyClient(
            [
                {
                    "chain": "ethereum",
                    "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "price": "1.0",
                    "info": {"symbol": "USDC"},
                }
            ]
        )
        quotes = client.prices(
            [{"token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "chain": "ethereum"}]
        )
        self.assertEqual(1, len(quotes))
        self.assertEqual("USDC", quotes[0].symbol)
        self.assertAlmostEqual(1.0, quotes[0].price)


if __name__ == "__main__":
    unittest.main()

