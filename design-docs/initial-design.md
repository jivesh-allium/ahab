# Pequod 🐋

**Whale transaction alerts powered by Allium APIs**

*"From hell's heart I stab at thee, for the sake of transparency into whale movements"*

---

## Overview

Pequod is a whale alert service that monitors large cryptocurrency transactions and broadcasts them to social channels (Twitter, Telegram, Discord). Similar to [Whale Alert](https://whale-alert.io/), but built on Allium's data infrastructure.

## What Whale Alert Does

1. **Real-time large tx alerts** — Push notifications when transactions exceed a USD threshold (typically $100K-$1M+)
2. **Transaction types** — Transfers, mints, burns, freezes, locks
3. **Address attribution** — "transferred from Binance to unknown wallet"
4. **Human-readable text** — "🐋 50,000,000 USDT ($50M) transferred from Binance to Coinbase"
5. **Multi-chain** — BTC, ETH, SOL, TRX, etc.

## Allium API Surface for Pequod

### What Already Exists

| Need | Allium Endpoint | Notes |
|------|-----------------|-------|
| Transaction data | `POST /wallet/transactions` | Rich activity data |
| Asset transfers | ↑ | Includes mint/burn detection |
| DEX trades | ↑ | With project/protocol attribution |
| NFT trades | ↑ | With marketplace info |
| Bridge transactions | ↑ | With source/dest chain |
| LP events | ↑ | Mint/burn liquidity |
| Token prices | `POST /prices` | For USD value calculation |
| Real-time prices | `WS /state-prices` | WebSocket streaming |
| Multi-chain | ✓ | EVM, Solana, Bitcoin, Stellar, Near |

### Activity Types from `/wallet/transactions`

```python
# Already supported:
- asset_transfers     # Sent/received, with mint/burn operations
- dex_trade          # DEX swaps with project/protocol
- nft_trade          # NFT trades with marketplace
- asset_bridge       # Cross-chain bridges with src/dest chain
- dex_liquidity_pool_mint   # LP additions
- dex_liquidity_pool_burn   # LP removals
- asset_approval     # Token approvals/revocations
```

### What's Missing

| Need | Status | Workaround |
|------|--------|------------|
| Scan all txs by value | ❌ | Watchlist approach (see below) |
| Real-time tx stream | ❌ | Polling |
| Address labels | ❌ | Curated registry or external API |

## The Watchlist Problem

The `/wallet/transactions` endpoint requires specific addresses as input:

```python
addresses: list[PayloadAddress]  # min_length=1, max_length=20
```

You can't query "all transactions over $X" — you must specify *which* wallets to watch.

### Options

**Option 1: Curated Watchlist (MVP)**
- Compile list of ~100-500 known big wallets
- Exchange hot/cold wallets, protocol treasuries, known whales
- Poll their transactions periodically
- Pros: Works today, no API changes needed
- Cons: Miss unknown whales, requires watchlist maintenance

**Option 2: Block Scanner (Expensive)**
- Poll recent blocks via `/evm/{chain}/raw/transactions`
- Fetch all txs, enrich with prices, filter client-side
- Pros: Catches everything
- Cons: Hammers API, expensive, probably not viable

**Option 3: New Streaming Endpoint (Build It)**
- Add server-side: `WS /v2/transactions/stream?min_value_usd=1000000`
- Filter at data layer, only push qualifying txs
- Pros: Real-time, efficient, scalable
- Cons: Requires new infrastructure

**Recommendation:** Start with Option 1 for MVP, build Option 3 as the real product.

## MVP Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         WATCHLIST                                │
│  exchanges.json + treasuries.json + whales.json (~500 addrs)   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                          POLLER                                  │
│                                                                  │
│   for each address in watchlist:                                │
│     txs = allium.get_transactions(address, since=last_poll)    │
│     for tx in txs:                                              │
│       usd_value = calculate_value(tx, prices)                  │
│       if usd_value > THRESHOLD:                                 │
│         queue_alert(tx, usd_value)                             │
│                                                                  │
│   Poll interval: ~30-60 seconds                                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FORMATTER                                 │
│                                                                  │
│   Input:  tx + usd_value + labels                               │
│   Output: "🐋 50,000 ETH ($160M) transferred from               │
│            Binance to unknown wallet"                           │
│                                                                  │
│   - Lookup from/to addresses in label registry                  │
│   - Format amounts with appropriate precision                   │
│   - Add emoji based on tx type (🔥 burn, 🌊 mint, etc.)        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       BROADCASTERS                               │
│                                                                  │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │
│   │ Twitter  │    │ Telegram │    │ Discord  │    │ Webhook  │ │
│   │   Bot    │    │ Channel  │    │ Channel  │    │   API    │ │
│   └──────────┘    └──────────┘    └──────────┘    └──────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Watchlist Bootstrap Sources

### Exchange Addresses
- Etherscan labeled addresses (scrapeable)
- Exchange documentation (some publish deposit addresses)
- Arkham Intelligence (if you want to pay)
- Nansen labels (if you want to pay)

### Protocol Treasuries
- Published in protocol docs/governance
- DAO multisigs are public
- Examples: Uniswap treasury, Aave treasury, etc.

### Known Whales
- Public figures: vitalik.eth, etc.
- Historic whale addresses from on-chain analysis
- Previous Whale Alert posts (they tag addresses)

### Starting List (~100 addresses)
```json
{
  "ethereum": {
    "exchanges": {
      "binance_hot_1": "0x28C6c06298d514Db089934071355E5743bf21d60",
      "binance_hot_2": "0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549",
      "coinbase_hot": "0x71660c4005BA85c37ccec55d0C4493E66Fe775d3",
      "kraken_hot": "0x2910543Af39abA0Cd09dBb2D50200b3E800A63D2"
    },
    "treasuries": {
      "uniswap": "0x1a9C8182C09F50C8318d769245beA52c32BE35BC",
      "aave": "0x25F2226B597E8F9514B3F68F00f494cF4f286491"
    },
    "whales": {
      "vitalik": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    }
  },
  "solana": {
    // ...
  }
}
```

## Alert Format Examples

### Transfer
```
🐋 50,000 ETH ($162,500,000) transferred from Binance to unknown wallet

Chain: Ethereum
Tx: 0xabc...def
```

### Mint
```
🌊 1,000,000,000 USDT ($1,000,000,000) minted at Tether Treasury

Chain: Ethereum
Tx: 0xabc...def
```

### Burn
```
🔥 500,000,000 USDC ($500,000,000) burned at Circle

Chain: Ethereum
Tx: 0xabc...def
```

### Bridge
```
🌉 10,000 ETH ($32,500,000) bridged from Ethereum to Arbitrum

From: 0xabc...def
To: 0x123...456
Tx: 0xabc...def
```

### DEX Trade
```
🔄 Whale swapped 5,000 ETH ($16,250,000) for 16,250,000 USDC on Uniswap

Chain: Ethereum
Tx: 0xabc...def
```

## Implementation Phases

### Phase 1: MVP (1-2 weeks)
- [ ] Curate initial watchlist (100 addresses)
- [ ] Build poller service
- [ ] Integrate with Allium `/wallet/transactions` + `/prices`
- [ ] Build alert formatter
- [ ] Deploy Telegram bot
- [ ] Manual monitoring, tune thresholds

### Phase 2: Expand (2-4 weeks)
- [ ] Add Twitter bot
- [ ] Add Discord bot
- [ ] Expand watchlist to 500+ addresses
- [ ] Add multi-chain support (Solana, Bitcoin)
- [ ] Add basic address labels
- [ ] Public webhook API for subscribers

### Phase 3: Real-time (Future)
- [ ] Build `/v2/transactions/stream` endpoint in Allium
- [ ] Replace polling with WebSocket subscription
- [ ] Add value-based server-side filtering
- [ ] Support custom alert thresholds per user

## Differentiation from Whale Alert

| Feature | Whale Alert | Pequod |
|---------|-------------|--------|
| Years of data | ✓ | Starting fresh |
| Label database | Extensive | Bootstrap needed |
| Real-time | ✓ (WebSocket) | Polling initially |
| Price | $15-30/mo | Free tier? |
| API access | Paid | Could offer |
| **Built on Allium** | ✗ | ✓ (dogfooding) |
| **Open source?** | ✗ | Possible |

### Why Build It?

1. **Dogfooding** — Use Allium's own APIs, find pain points
2. **Marketing** — Viral potential, brand awareness
3. **Showcase** — Demonstrates Allium's capabilities
4. **Product insight** — Reveals need for streaming tx endpoint
5. **Fun** — It's a cool project

## Tech Stack Suggestion

```
- Python or TypeScript
- Allium API client
- Redis for deduplication (don't re-alert same tx)
- Twitter API v2
- Telegram Bot API
- Discord webhooks
- Vercel/Railway/Fly for hosting
```

## Open Questions

1. **Threshold tuning** — What's the right USD threshold per chain? ETH $1M? SOL $500K?
2. **Rate limiting** — How often can we poll 500 addresses without hitting limits?
3. **Deduplication** — How to handle txs that appear in multiple watched addresses?
4. **Label sourcing** — Build own registry vs integrate Arkham/Nansen?
5. **Monetization** — Free with premium tiers? API access fees?

---

*Document: Pequod Whale Alerts*
*Author: Jiv + Lorbgod 🐙*
*Date: 2026-02-05*
