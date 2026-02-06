from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from .types import WatchAddress


def _normalize_chain(chain: str) -> str:
    return chain.strip().lower()


def _from_flat_list(data: List[Dict[str, Any]]) -> List[WatchAddress]:
    records: List[WatchAddress] = []
    for item in data:
        chain = _normalize_chain(str(item.get("chain", "")))
        address = str(item.get("address", "")).strip()
        if not chain or not address:
            continue
        label = str(item.get("label") or item.get("name") or address)
        category = item.get("category")
        records.append(WatchAddress(chain=chain, address=address, label=label, category=category))
    return records


def _from_nested_map(data: Dict[str, Any]) -> List[WatchAddress]:
    records: List[WatchAddress] = []
    for chain, groups in data.items():
        chain_name = _normalize_chain(str(chain))
        if isinstance(groups, dict):
            for category, entries in groups.items():
                if isinstance(entries, dict):
                    for label, address in entries.items():
                        address_value = str(address).strip()
                        if not address_value:
                            continue
                        records.append(
                            WatchAddress(
                                chain=chain_name,
                                address=address_value,
                                label=str(label),
                                category=str(category),
                            )
                        )
                elif isinstance(entries, list):
                    for entry in entries:
                        if isinstance(entry, dict):
                            address_value = str(entry.get("address", "")).strip()
                            label = str(entry.get("label") or entry.get("name") or address_value)
                        else:
                            address_value = str(entry).strip()
                            label = address_value
                        if not address_value:
                            continue
                        records.append(
                            WatchAddress(
                                chain=chain_name,
                                address=address_value,
                                label=label,
                                category=str(category),
                            )
                        )
        elif isinstance(groups, list):
            for entry in groups:
                if isinstance(entry, dict):
                    address_value = str(entry.get("address", "")).strip()
                    label = str(entry.get("label") or entry.get("name") or address_value)
                    category = entry.get("category")
                else:
                    address_value = str(entry).strip()
                    label = address_value
                    category = None
                if not address_value:
                    continue
                records.append(WatchAddress(chain=chain_name, address=address_value, label=label, category=category))
    return records


def load_watchlist(path: Path) -> List[WatchAddress]:
    if not path.exists():
        raise FileNotFoundError(f"Watchlist not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        result = _from_flat_list(payload)
    elif isinstance(payload, dict):
        result = _from_nested_map(payload)
    else:
        raise ValueError("Watchlist must be a JSON object or list.")

    deduped: Dict[str, WatchAddress] = {}
    for item in result:
        key = f"{item.chain}:{item.address.lower()}"
        deduped[key] = item
    return list(deduped.values())

