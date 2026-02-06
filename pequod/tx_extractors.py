from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from .types import NormalizedTransaction
from .utils import parse_timestamp, short_hash, to_float


def _flatten_transactions(payload: Any) -> Iterable[Tuple[Optional[str], Dict[str, Any]]]:
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            watched_address = item.get("address") if isinstance(item.get("address"), str) else None
            nested = None
            for key in ("items", "transactions", "data", "activities", "events"):
                value = item.get(key)
                if isinstance(value, list):
                    nested = value
                    break
            if nested is None:
                yield watched_address, item
            else:
                for tx in nested:
                    if isinstance(tx, dict):
                        yield watched_address, tx
    elif isinstance(payload, dict):
        for key in ("items", "transactions", "data", "activities", "events"):
            value = payload.get(key)
            if isinstance(value, list):
                for tx in value:
                    if isinstance(tx, dict):
                        yield None, tx
                return
        yield None, payload


def _pick_first_str(data: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _pick_first_float(data: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        value = data.get(key)
        parsed = to_float(value)
        if parsed is not None:
            return parsed
    return None


def _infer_tx_type(tx: Dict[str, Any]) -> str:
    tx_type = _pick_first_str(tx, ["activity_type", "type", "event_type", "operation", "kind"])
    return (tx_type or "transfer").lower()


def _extract_chain(tx: Dict[str, Any], fallback: Optional[str]) -> str:
    chain = _pick_first_str(tx, ["chain", "network", "source_chain"])
    return (chain or fallback or "unknown").lower()


def _extract_token_address(tx: Dict[str, Any]) -> Optional[str]:
    direct = _pick_first_str(tx, ["token_address", "mint", "asset_address", "contract_address"])
    if direct:
        return direct
    token = tx.get("token")
    if isinstance(token, dict):
        nested = _pick_first_str(token, ["address", "token_address", "mint"])
        if nested:
            return nested
    return None


def _extract_symbol(tx: Dict[str, Any]) -> Optional[str]:
    direct = _pick_first_str(tx, ["token_symbol", "symbol", "asset_symbol", "currency"])
    if direct:
        return direct
    token = tx.get("token")
    if isinstance(token, dict):
        nested = _pick_first_str(token, ["symbol", "ticker"])
        if nested:
            return nested
    return None


def _extract_tx_id(tx: Dict[str, Any]) -> str:
    tx_id = _pick_first_str(tx, ["transaction_hash", "tx_hash", "hash", "signature", "id"])
    if tx_id:
        return tx_id
    return f"tx_{short_hash(tx)}"


def _extract_timestamp(tx: Dict[str, Any]) -> Optional[int]:
    for key in ("block_timestamp", "timestamp", "time", "created_at"):
        parsed = parse_timestamp(tx.get(key))
        if parsed is not None:
            return parsed
    return None


def _extract_usd_value(tx: Dict[str, Any]) -> Optional[float]:
    direct = _pick_first_float(tx, ["usd_value", "value_usd", "amount_usd", "valueUsd", "usd"])
    if direct is not None:
        return direct
    token = tx.get("token")
    if isinstance(token, dict):
        nested = _pick_first_float(token, ["usd_value", "value_usd", "price_usd"])
        if nested is not None:
            return nested
    return None


def _extract_amount(tx: Dict[str, Any]) -> Optional[float]:
    direct = _pick_first_float(
        tx,
        [
            "token_amount",
            "amount",
            "quantity",
            "value",
            "raw_amount",
            "amount_raw",
        ],
    )
    if direct is not None:
        return direct

    token = tx.get("token")
    if isinstance(token, dict):
        nested = _pick_first_float(token, ["amount", "quantity", "balance_change"])
        if nested is not None:
            return nested
    return None


def normalize_transactions(payload: Any, default_chain_by_address: Dict[str, str]) -> List[NormalizedTransaction]:
    records: List[NormalizedTransaction] = []
    for watched_address, tx in _flatten_transactions(payload):
        from_address = _pick_first_str(tx, ["from_address", "sender", "from", "source_address"])
        to_address = _pick_first_str(tx, ["to_address", "receiver", "to", "destination_address"])

        fallback_chain = None
        if watched_address:
            fallback_chain = default_chain_by_address.get(watched_address.lower())

        normalized = NormalizedTransaction(
            tx_id=_extract_tx_id(tx),
            chain=_extract_chain(tx, fallback_chain),
            tx_type=_infer_tx_type(tx),
            from_address=from_address,
            to_address=to_address,
            token_address=_extract_token_address(tx),
            token_symbol=_extract_symbol(tx),
            amount=_extract_amount(tx),
            usd_value=_extract_usd_value(tx),
            timestamp=_extract_timestamp(tx),
            watch_address=watched_address,
            raw=tx,
        )
        records.append(normalized)
    return records

