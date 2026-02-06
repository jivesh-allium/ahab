import unittest

from pequod.balances import extract_wallet_balance_summary


class BalanceParserTests(unittest.TestCase):
    def test_extracts_nested_items_shape(self) -> None:
        payload = [
            {
                "address": "0xWatch",
                "items": [
                    {
                        "symbol": "USDC",
                        "token_address": "0xusdc",
                        "amount": "1000",
                        "usd_value": "1000",
                    },
                    {
                        "asset": {"symbol": "ETH", "address": "0xeth"},
                        "amount": {"amount": "2", "usd_value": 6000},
                    },
                ],
            }
        ]
        parsed = extract_wallet_balance_summary(payload)
        self.assertIn("0xwatch", parsed)
        self.assertAlmostEqual(7000.0, parsed["0xwatch"]["holdings_total_usd"])
        self.assertEqual(2, parsed["0xwatch"]["holdings_token_count"])
        self.assertEqual("ETH", parsed["0xwatch"]["top_holdings"][0]["symbol"])

    def test_estimates_usd_from_amount_times_price_when_missing(self) -> None:
        payload = {
            "items": [
                {
                    "wallet_address": "0xWhale",
                    "token": {"symbol": "ARB", "address": "0xarb"},
                    "balance": "100",
                    "price_usd": 2.5,
                }
            ]
        }
        parsed = extract_wallet_balance_summary(payload)
        self.assertIn("0xwhale", parsed)
        self.assertAlmostEqual(250.0, parsed["0xwhale"]["holdings_total_usd"])
        self.assertEqual("ARB", parsed["0xwhale"]["top_holdings"][0]["symbol"])

    def test_parses_allium_raw_balance_shape(self) -> None:
        payload = {
            "items": [
                {
                    "chain": "ethereum",
                    "address": "0xWatch",
                    "raw_balance": "1500000000",
                    "token": {
                        "address": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "decimals": 6,
                        "price": 1.0,
                        "info": {"symbol": "USDC"},
                    },
                }
            ]
        }
        parsed = extract_wallet_balance_summary(payload)
        self.assertIn("0xwatch", parsed)
        self.assertAlmostEqual(1500.0, parsed["0xwatch"]["holdings_total_usd"])
        self.assertEqual("USDC", parsed["0xwatch"]["top_holdings"][0]["symbol"])

    def test_uses_none_for_unpriced_wallets(self) -> None:
        payload = {
            "items": [
                {
                    "wallet_address": "0xNoPrice",
                    "token": {"symbol": "UNK", "address": "0xunk"},
                    "balance": "12345",
                }
            ]
        }
        parsed = extract_wallet_balance_summary(payload)
        self.assertIn("0xnoprice", parsed)
        self.assertIsNone(parsed["0xnoprice"]["holdings_total_usd"])
        self.assertEqual(0, parsed["0xnoprice"]["holdings_token_count"])
        self.assertEqual([], parsed["0xnoprice"]["top_holdings"])


if __name__ == "__main__":
    unittest.main()
