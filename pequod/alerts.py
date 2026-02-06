from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

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


def _dedupe_suffix(tx: NormalizedTransaction) -> str:
    transfer_index = tx.raw.get("asset_transfer_index")
    if isinstance(transfer_index, int):
        return str(transfer_index)
    material = "|".join(
        [
            tx.from_address or "",
            tx.to_address or "",
            tx.token_address or "",
            str(tx.amount) if tx.amount is not None else "",
            tx.watch_address or "",
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


def _dashboard_link(
    dashboard_base_url: str,
    dedupe_key: str,
    tx_id: str,
    chain: str,
) -> Optional[str]:
    base = dashboard_base_url.strip().rstrip("/")
    if not base:
        return None
    query = urlencode(
        {
            "event": dedupe_key,
            "tx": tx_id,
            "chain": chain.lower(),
        }
    )
    return f"{base}/?{query}"


def build_alert(
    tx: NormalizedTransaction,
    usd_value: float,
    label_by_address: Dict[str, WatchAddress],
    score: float = 0.0,
    score_reasons: Optional[List[Dict[str, Any]]] = None,
    score_breakdown: Optional[Dict[str, float]] = None,
    entities: Optional[Dict[str, Dict[str, Any]]] = None,
    dashboard_base_url: str = "",
) -> Alert:
    from_label = label_by_address.get((tx.from_address or "").lower())
    to_label = label_by_address.get((tx.to_address or "").lower())

    from_text = from_label.label if from_label else _short_addr(tx.from_address)
    to_text = to_label.label if to_label else _short_addr(tx.to_address)

    amount_text = _format_amount(tx.amount, tx.token_symbol)
    tx_link_id = _short_addr(tx.tx_id)

    dedupe_key = f"{tx.chain}:{tx.tx_id}:{tx.tx_type}:{_dedupe_suffix(tx)}"
    deep_link = _dashboard_link(
        dashboard_base_url=dashboard_base_url,
        dedupe_key=dedupe_key,
        tx_id=tx.tx_id,
        chain=tx.chain,
    )
    reasons = [item for item in (score_reasons or []) if isinstance(item, dict)]
    reason_labels = [str(item.get("label") or item.get("key") or "").strip() for item in reasons if item]
    reason_text = ", ".join([label for label in reason_labels if label][:3]) or "flow magnitude"

    line1 = f"{_tx_type_emoji(tx.tx_type)} {amount_text} ({_format_usd(usd_value)}) {_tx_type_phrase(tx.tx_type)}"
    line2 = f"Score: {float(score):.1f}/100 | Drivers: {reason_text}"
    line3 = f"From: {from_text} -> To: {to_text}"
    line4 = f"Chain: {tx.chain} | Tx: {tx_link_id} | Time: {_format_time(tx.timestamp)}"
    lines = [line1, line2, line3, line4]
    if deep_link:
        lines.append(f"Dashboard: {deep_link}")
    lines.append("Powered by Allium")
    text = "\n".join(lines)

    return Alert(
        dedupe_key=dedupe_key,
        text=text,
        usd_value=usd_value,
        tx_id=tx.tx_id,
        chain=tx.chain,
        tx_type=tx.tx_type,
        timestamp=tx.timestamp,
        watch_address=tx.watch_address,
        from_address=tx.from_address,
        to_address=tx.to_address,
        token_symbol=tx.token_symbol,
        token_address=tx.token_address,
        amount=tx.amount,
        raw=tx.raw,
        score=float(score),
        score_reasons=reasons,
        score_breakdown=dict(score_breakdown or {}),
        entities=dict(entities or {}),
        deep_link=deep_link,
    )
