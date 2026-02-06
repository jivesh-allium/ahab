import unittest

from pequod.tx_extractors import normalize_transactions


class TxExtractorTests(unittest.TestCase):
    def test_normalizes_list_with_items(self) -> None:
        payload = [
            {
                "address": "0xwatch",
                "items": [
                    {
                        "transaction_hash": "0xtx1",
                        "chain": "ethereum",
                        "activity_type": "asset_transfer",
                        "from_address": "0xfrom",
                        "to_address": "0xto",
                        "token_address": "0xToken",
                        "token_symbol": "USDC",
                        "amount": "1000000",
                        "usd_value": 1000000,
                        "block_timestamp": "2026-02-06T00:00:00Z",
                    }
                ],
            }
        ]
        txs = normalize_transactions(payload, {"0xwatch": "ethereum"})
        self.assertEqual(1, len(txs))
        tx = txs[0]
        self.assertEqual("0xtx1", tx.tx_id)
        self.assertEqual("ethereum", tx.chain)
        self.assertEqual("asset_transfer", tx.tx_type)
        self.assertEqual(1_000_000.0, tx.usd_value)

    def test_falls_back_to_hash_when_no_tx_id(self) -> None:
        payload = [{"address": "0xwatch", "items": [{"chain": "ethereum", "amount": "1"}]}]
        txs = normalize_transactions(payload, {"0xwatch": "ethereum"})
        self.assertTrue(txs[0].tx_id.startswith("tx_"))

    def test_extracts_nested_asset_transfer_fields(self) -> None:
        payload = {
            "items": [
                {
                    "address": "0xwatch",
                    "chain": "ethereum",
                    "hash": "0xtx2",
                    "type": "transfer",
                    "block_timestamp": 1770349115,
                    "asset_transfers": [
                        {
                            "from_address": "0xfrom",
                            "to_address": "0xto",
                            "amount": {"amount": 0.21862549, "raw_amount": "21862549"},
                            "asset": {
                                "address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                                "symbol": "WBTC",
                            },
                        }
                    ],
                }
            ]
        }
        txs = normalize_transactions(payload, {"0xwatch": "ethereum"})
        self.assertEqual(1, len(txs))
        tx = txs[0]
        self.assertEqual("0xtx2", tx.tx_id)
        self.assertEqual("0xwatch", tx.watch_address)
        self.assertEqual("0xfrom", tx.from_address)
        self.assertEqual("0xto", tx.to_address)
        self.assertEqual("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", tx.token_address)
        self.assertEqual("WBTC", tx.token_symbol)
        self.assertAlmostEqual(0.21862549, tx.amount)

    def test_explodes_multiple_asset_transfers_into_multiple_records(self) -> None:
        payload = {
            "items": [
                {
                    "address": "0xwatch",
                    "chain": "ethereum",
                    "hash": "0xtx-multi",
                    "type": "asset_transfer",
                    "block_timestamp": 1770349115,
                    "asset_transfers": [
                        {
                            "from_address": "0xfrom1",
                            "to_address": "0xto1",
                            "amount": {"amount": "2.5", "usd_value": 5000},
                            "asset": {"address": "0xtoken1", "symbol": "AAA"},
                        },
                        {
                            "from_address": "0xfrom2",
                            "to_address": "0xto2",
                            "amount": {"amount": "7", "usd_value": 14000},
                            "asset": {"address": "0xtoken2", "symbol": "BBB"},
                        },
                    ],
                }
            ]
        }
        txs = normalize_transactions(payload, {"0xwatch": "ethereum"})
        self.assertEqual(2, len(txs))
        self.assertEqual(["0xfrom1", "0xfrom2"], [tx.from_address for tx in txs])
        self.assertEqual(["0xto1", "0xto2"], [tx.to_address for tx in txs])
        self.assertEqual(["0xtoken1", "0xtoken2"], [tx.token_address for tx in txs])
        self.assertEqual([0, 1], [tx.raw.get("asset_transfer_index") for tx in txs])


if __name__ == "__main__":
    unittest.main()
