from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from .types import Alert, NormalizedTransaction, WatchAddress


def _short_addr(value: Optional[str]) -> str:
    if not value:
        return "unknown"
    if len(value) <= 14:
        return value
    return f"{value[:8]}...{value[-6:]}"


def _tx_type_emoji(tx_type: str) -> str:
    value = tx_type.lower()
    if "burn" in value:
        return "🔥"
    if "mint" in value:
        return "🌊"
    if "bridge" in value:
        return "🌉"
    if "trade" in value or "swap" in value:
        return "🔄"
    return "🐋"


def _tx_type_phrase(tx_type: str) -> str:
    value = tx_type.lower()
    if "burn" in value:
        return "burn detected"
    if "mint" in value:
        return "mint detected"
    if "bridge" in value:
        return "bridge transfer detected"
    if "trade" in value or "swap" in value:
        return "large swap detected"
    return "large transfer detected"


def _format_amount(amount: Optional[float], symbol: Optional[str]) -> str:
    if amount is None:
        return "unknown amount"
    if abs(amount) >= 1:
        text = f"{amount:,.4f}".rstrip("0").rstrip(".")
    else:
        text = f"{amount:.8f}".rstrip("0").rstrip(".")
    if symbol:
        return f"{text} {symbol}"
    return text


def _format_usd(value: float) -> str:
    return f"${value:,.2f}"


def _format_time(timestamp: Optional[int]) -> str:
    if timestamp is None:
        return "unknown"
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%SZ")


def build_alert(
    tx: NormalizedTransaction,
    usd_value: float,
    label_by_address: Dict[str, WatchAddress],
) -> Alert:
    from_label = label_by_address.get((tx.from_address or "").lower())
    to_label = label_by_address.get((tx.to_address or "").lower())

    from_text = from_label.label if from_label else _short_addr(tx.from_address)
    to_text = to_label.label if to_label else _short_addr(tx.to_address)

    amount_text = _format_amount(tx.amount, tx.token_symbol)
    tx_link_id = _short_addr(tx.tx_id)

    line1 = f"{_tx_type_emoji(tx.tx_type)} {amount_text} ({_format_usd(usd_value)}) {_tx_type_phrase(tx.tx_type)}"
    line2 = f"From: {from_text} -> To: {to_text}"
    line3 = f"Chain: {tx.chain} | Tx: {tx_link_id} | Time: {_format_time(tx.timestamp)}"
    line4 = "Powered by Allium"
    text = "\n".join([line1, line2, line3, line4])

    dedupe_key = f"{tx.chain}:{tx.tx_id}:{tx.tx_type}"
    return Alert(
        dedupe_key=dedupe_key,
        text=text,
        usd_value=usd_value,
        tx_id=tx.tx_id,
        chain=tx.chain,
        timestamp=tx.timestamp,
        raw=tx.raw,
    )

