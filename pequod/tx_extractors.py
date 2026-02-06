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
                        watched_address = tx.get("address") if isinstance(tx.get("address"), str) else None
                        yield watched_address, tx
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


def _transfer_entries(tx: Dict[str, Any]) -> List[Tuple[Optional[int], Optional[Dict[str, Any]]]]:
    transfers = tx.get("asset_transfers")
    if not isinstance(transfers, list):
        return [(None, None)]
    rows: List[Tuple[Optional[int], Optional[Dict[str, Any]]]] = []
    for index, item in enumerate(transfers):
        if isinstance(item, dict):
            rows.append((index, item))
    if rows:
        return rows
    return [(None, None)]


def _first_transfer(tx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for _, transfer in _transfer_entries(tx):
        if isinstance(transfer, dict):
            return transfer
    return None


def _transfer_asset(transfer: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(transfer, dict):
        return None
    asset = transfer.get("asset")
    if isinstance(asset, dict):
        return asset
    return None


def _transfer_amount_obj(transfer: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(transfer, dict):
        return None
    amount = transfer.get("amount")
    if isinstance(amount, dict):
        return amount
    return None


def _infer_tx_type(tx: Dict[str, Any], transfer: Optional[Dict[str, Any]] = None) -> str:
    tx_type = None
    if isinstance(transfer, dict):
        tx_type = _pick_first_str(transfer, ["activity_type", "type", "event_type", "operation", "kind"])
    if not tx_type:
        tx_type = _pick_first_str(tx, ["activity_type", "type", "event_type", "operation", "kind"])
    return (tx_type or "transfer").lower()


def _extract_chain(tx: Dict[str, Any], fallback: Optional[str]) -> str:
    chain = _pick_first_str(tx, ["chain", "network", "source_chain"])
    return (chain or fallback or "unknown").lower()


def _extract_token_address(tx: Dict[str, Any], transfer: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(transfer, dict):
        transfer_direct = _pick_first_str(transfer, ["token_address", "mint", "asset_address", "contract_address"])
        if transfer_direct:
            return transfer_direct
        transfer_asset = _transfer_asset(transfer)
        if transfer_asset:
            nested = _pick_first_str(transfer_asset, ["address", "token_address", "mint"])
            if nested:
                return nested

    direct = _pick_first_str(tx, ["token_address", "mint", "asset_address", "contract_address"])
    if direct:
        return direct
    token = tx.get("token")
    if isinstance(token, dict):
        nested = _pick_first_str(token, ["address", "token_address", "mint"])
        if nested:
            return nested

    transfer = _first_transfer(tx)
    if transfer:
        asset = transfer.get("asset")
        if isinstance(asset, dict):
            nested = _pick_first_str(asset, ["address", "token_address", "mint"])
            if nested:
                return nested
    return None


def _extract_symbol(tx: Dict[str, Any], transfer: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(transfer, dict):
        transfer_direct = _pick_first_str(transfer, ["token_symbol", "symbol", "asset_symbol", "currency"])
        if transfer_direct:
            return transfer_direct
        transfer_asset = _transfer_asset(transfer)
        if transfer_asset:
            nested = _pick_first_str(transfer_asset, ["symbol", "ticker"])
            if nested:
                return nested

    direct = _pick_first_str(tx, ["token_symbol", "symbol", "asset_symbol", "currency"])
    if direct:
        return direct
    token = tx.get("token")
    if isinstance(token, dict):
        nested = _pick_first_str(token, ["symbol", "ticker"])
        if nested:
            return nested

    transfer = _first_transfer(tx)
    if transfer:
        asset = transfer.get("asset")
        if isinstance(asset, dict):
            nested = _pick_first_str(asset, ["symbol", "ticker"])
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


def _extract_usd_value(tx: Dict[str, Any], transfer: Optional[Dict[str, Any]] = None) -> Optional[float]:
    transfer_amount = _transfer_amount_obj(transfer)
    if transfer_amount:
        nested = _pick_first_float(transfer_amount, ["usd_value", "value_usd", "amount_usd", "usd"])
        if nested is not None:
            return nested
    if isinstance(transfer, dict):
        nested_direct = _pick_first_float(transfer, ["usd_value", "value_usd", "amount_usd", "usd"])
        if nested_direct is not None:
            return nested_direct

    direct = _pick_first_float(tx, ["usd_value", "value_usd", "amount_usd", "valueUsd", "usd"])
    if direct is not None:
        return direct
    token = tx.get("token")
    if isinstance(token, dict):
        nested = _pick_first_float(token, ["usd_value", "value_usd", "price_usd"])
        if nested is not None:
            return nested

    transfer = _first_transfer(tx)
    if transfer:
        amount = transfer.get("amount")
        if isinstance(amount, dict):
            nested = _pick_first_float(amount, ["usd_value", "value_usd", "amount_usd", "usd"])
            if nested is not None:
                return nested
    return None


def _extract_amount(tx: Dict[str, Any], transfer: Optional[Dict[str, Any]] = None) -> Optional[float]:
    transfer_amount = _transfer_amount_obj(transfer)
    if transfer_amount:
        nested = _pick_first_float(transfer_amount, ["amount", "raw_amount"])
        if nested is not None:
            return nested
    if isinstance(transfer, dict):
        transfer_direct = _pick_first_float(transfer, ["amount", "quantity", "value"])
        if transfer_direct is not None:
            return transfer_direct

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

    transfer = _first_transfer(tx)
    if transfer:
        amount = transfer.get("amount")
        if isinstance(amount, dict):
            nested = _pick_first_float(amount, ["amount", "raw_amount"])
            if nested is not None:
                return nested
        nested_direct = _pick_first_float(transfer, ["amount", "quantity", "value"])
        if nested_direct is not None:
            return nested_direct
    return None


def _extract_from_address(tx: Dict[str, Any], transfer: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(transfer, dict):
        nested = _pick_first_str(transfer, ["from_address", "sender", "from", "source_address"])
        if nested:
            return nested

    direct = _pick_first_str(tx, ["from_address", "sender", "from", "source_address"])
    if direct:
        return direct
    transfer = _first_transfer(tx)
    if transfer:
        nested = _pick_first_str(transfer, ["from_address", "sender", "from", "source_address"])
        if nested:
            return nested
    return None


def _extract_to_address(tx: Dict[str, Any], transfer: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(transfer, dict):
        nested = _pick_first_str(transfer, ["to_address", "receiver", "to", "destination_address"])
        if nested:
            return nested

    direct = _pick_first_str(tx, ["to_address", "receiver", "to", "destination_address"])
    if direct:
        return direct
    transfer = _first_transfer(tx)
    if transfer:
        nested = _pick_first_str(transfer, ["to_address", "receiver", "to", "destination_address"])
        if nested:
            return nested
    return None


def normalize_transactions(payload: Any, default_chain_by_address: Dict[str, str]) -> List[NormalizedTransaction]:
    records: List[NormalizedTransaction] = []
    for watched_address, tx in _flatten_transactions(payload):
        fallback_chain = None
        if watched_address:
            fallback_chain = default_chain_by_address.get(watched_address.lower())
        chain = _extract_chain(tx, fallback_chain)
        tx_id = _extract_tx_id(tx)
        timestamp = _extract_timestamp(tx)

        for transfer_index, transfer in _transfer_entries(tx):
            raw = dict(tx)
            if transfer_index is not None:
                raw["asset_transfer_index"] = transfer_index
            if isinstance(transfer, dict):
                raw["asset_transfer"] = transfer

            normalized = NormalizedTransaction(
                tx_id=tx_id,
                chain=chain,
                tx_type=_infer_tx_type(tx, transfer=transfer),
                from_address=_extract_from_address(tx, transfer=transfer),
                to_address=_extract_to_address(tx, transfer=transfer),
                token_address=_extract_token_address(tx, transfer=transfer),
                token_symbol=_extract_symbol(tx, transfer=transfer),
                amount=_extract_amount(tx, transfer=transfer),
                usd_value=_extract_usd_value(tx, transfer=transfer),
                timestamp=timestamp,
                watch_address=watched_address,
                raw=raw,
            )
            records.append(normalized)
    return records
