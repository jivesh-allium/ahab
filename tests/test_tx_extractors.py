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


if __name__ == "__main__":
    unittest.main()

