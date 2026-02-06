from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from .utils import to_float

MAX_REASONABLE_TOKEN_USD = 100_000_000_000.0
MIN_TRACKED_TOKEN_USD = 0.01
MAX_REASONABLE_TOKEN_PRICE_USD = 1_000_000.0
MAX_REASONABLE_TOKEN_AMOUNT = 1_000_000_000_000_000.0


def _normalize_address(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    return normalized


def _pick_first_str(data: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _pick_first_float(data: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        parsed = to_float(data.get(key))
        if parsed is not None:
            return parsed
    return None


def _flatten_balance_rows(payload: Any) -> Iterable[Tuple[Optional[str], Dict[str, Any]]]:
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            watched_address = _normalize_address(item.get("address"))
            nested = None
            for key in ("items", "balances", "data", "tokens", "assets", "holdings"):
                value = item.get(key)
                if isinstance(value, list):
                    nested = value
                    break
            if nested is None:
                yield watched_address, item
            else:
                for token in nested:
                    if isinstance(token, dict):
                        yield watched_address, token
    elif isinstance(payload, dict):
        for key in ("items", "balances", "data", "wallets", "accounts"):
            value = payload.get(key)
            if isinstance(value, list):
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    watched_address = _normalize_address(item.get("address"))
                    nested = None
                    for nested_key in ("items", "balances", "data", "tokens", "assets", "holdings"):
                        nested_value = item.get(nested_key)
                        if isinstance(nested_value, list):
                            nested = nested_value
                            break
                    if nested is None:
                        yield watched_address, item
                    else:
                        for token in nested:
                            if isinstance(token, dict):
                                yield watched_address, token
                return
        yield None, payload


def _wallet_address(watched_address: Optional[str], row: Dict[str, Any]) -> Optional[str]:
    if watched_address:
        return watched_address
    return _normalize_address(
        _pick_first_str(
            row,
            [
                "wallet_address",
                "owner_address",
                "holder_address",
                "account_address",
                "address",
            ],
        )
    )


def _token_symbol(row: Dict[str, Any]) -> Optional[str]:
    direct = _pick_first_str(row, ["token_symbol", "symbol", "ticker", "currency"])
    if direct:
        return direct
    token = row.get("token")
    if isinstance(token, dict):
        nested = _pick_first_str(token, ["symbol", "ticker"])
        if nested:
            return nested
        info = token.get("info")
        if isinstance(info, dict):
            info_symbol = _pick_first_str(info, ["symbol", "ticker"])
            if info_symbol:
                return info_symbol
    asset = row.get("asset")
    if isinstance(asset, dict):
        nested = _pick_first_str(asset, ["symbol", "ticker"])
        if nested:
            return nested
        info = asset.get("info")
        if isinstance(info, dict):
            info_symbol = _pick_first_str(info, ["symbol", "ticker"])
            if info_symbol:
                return info_symbol
    return None


def _token_address(row: Dict[str, Any]) -> Optional[str]:
    direct = _pick_first_str(row, ["token_address", "asset_address", "contract_address", "mint"])
    if direct:
        return direct
    token = row.get("token")
    if isinstance(token, dict):
        nested = _pick_first_str(token, ["address", "token_address", "mint"])
        if nested:
            return nested
    asset = row.get("asset")
    if isinstance(asset, dict):
        nested = _pick_first_str(asset, ["address", "token_address", "mint"])
        if nested:
            return nested
    return None


def _token_amount(row: Dict[str, Any]) -> Optional[float]:
    direct = _pick_first_float(row, ["amount", "balance", "quantity", "token_amount"])
    if direct is not None:
        return direct
    amount = row.get("amount")
    if isinstance(amount, dict):
        nested = _pick_first_float(amount, ["amount", "raw_amount"])
        if nested is not None:
            return nested
    raw_balance = _pick_first_float(row, ["raw_balance", "raw_balance_str"])
    if raw_balance is not None:
        decimals = _token_decimals(row)
        if decimals is not None and 0 <= decimals <= 36:
            return raw_balance / (10 ** decimals)
        return raw_balance
    return None


def _token_decimals(row: Dict[str, Any]) -> Optional[int]:
    direct = _pick_first_float(row, ["decimals"])
    if direct is not None:
        return int(direct)
    token = row.get("token")
    if isinstance(token, dict):
        nested = _pick_first_float(token, ["decimals"])
        if nested is not None:
            return int(nested)
    asset = row.get("asset")
    if isinstance(asset, dict):
        nested = _pick_first_float(asset, ["decimals"])
        if nested is not None:
            return int(nested)
    return None


def _token_price_usd(row: Dict[str, Any]) -> Optional[float]:
    direct = _pick_first_float(row, ["price_usd", "usd_price", "price"])
    if direct is not None:
        return direct
    token = row.get("token")
    if isinstance(token, dict):
        nested = _pick_first_float(token, ["price", "price_usd", "usd_price"])
        if nested is not None:
            return nested
        attributes = token.get("attributes")
        if isinstance(attributes, dict):
            nested_attr = _pick_first_float(attributes, ["price", "price_usd", "usd_price"])
            if nested_attr is not None:
                return nested_attr
    asset = row.get("asset")
    if isinstance(asset, dict):
        nested = _pick_first_float(asset, ["price", "price_usd", "usd_price"])
        if nested is not None:
            return nested
        attributes = asset.get("attributes")
        if isinstance(attributes, dict):
            nested_attr = _pick_first_float(attributes, ["price", "price_usd", "usd_price"])
            if nested_attr is not None:
                return nested_attr
    return None


def _token_usd_value(row: Dict[str, Any]) -> Optional[float]:
    direct = _pick_first_float(
        row,
        [
            "usd_value",
            "balance_usd",
            "value_usd",
            "amount_usd",
            "usd",
            "usd_balance",
            "usdBalance",
        ],
    )
    if direct is not None:
        return direct
    amount_obj = row.get("amount")
    if isinstance(amount_obj, dict):
        nested = _pick_first_float(amount_obj, ["usd_value", "value_usd", "amount_usd", "usd"])
        if nested is not None:
            return nested
    amount = _token_amount(row)
    price = _token_price_usd(row)
    if amount is not None and price is not None:
        return amount * price
    return None


def extract_wallet_balance_summary(payload: Any) -> Dict[str, Dict[str, Any]]:
    by_address: Dict[str, Dict[str, Any]] = {}
    for watched_address, row in _flatten_balance_rows(payload):
        if not isinstance(row, dict):
            continue
        wallet = _wallet_address(watched_address, row)
        if not wallet:
            continue
        summary = by_address.setdefault(wallet, {"holdings_total_usd": 0.0, "tokens": []})

        usd_value = _token_usd_value(row)
        amount = _token_amount(row)
        price_usd = _token_price_usd(row)
        symbol = _token_symbol(row)
        token_address = _token_address(row)

        if (
            usd_value is not None
            and MIN_TRACKED_TOKEN_USD <= usd_value <= MAX_REASONABLE_TOKEN_USD
            and (amount is None or (0 <= amount <= MAX_REASONABLE_TOKEN_AMOUNT))
            and (price_usd is None or (0 < price_usd <= MAX_REASONABLE_TOKEN_PRICE_USD))
        ):
            summary["holdings_total_usd"] = float(summary["holdings_total_usd"]) + float(usd_value)
            summary["tokens"].append(
                {
                    "symbol": symbol,
                    "token_address": token_address,
                    "usd_value": float(usd_value),
                    "amount": amount,
                    "price_usd": price_usd,
                }
            )

    result: Dict[str, Dict[str, Any]] = {}
    for address, summary in by_address.items():
        tokens = summary.get("tokens", [])
        if not isinstance(tokens, list):
            tokens = []
        ranked = sorted(
            [token for token in tokens if isinstance(token, dict)],
            key=lambda token: float(token.get("usd_value") or 0.0),
            reverse=True,
        )
        total_usd = round(float(summary.get("holdings_total_usd") or 0.0), 2)
        if total_usd <= 0 or not ranked:
            total_value: Optional[float] = None
        else:
            total_value = total_usd
        result[address] = {
            "holdings_total_usd": total_value,
            "holdings_token_count": len(ranked),
            "top_holdings": ranked[:3],
        }
    return result
