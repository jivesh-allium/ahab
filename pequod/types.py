from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class WatchAddress:
    chain: str
    address: str
    label: str
    category: Optional[str] = None


@dataclass
class NormalizedTransaction:
    tx_id: str
    chain: str
    tx_type: str
    from_address: Optional[str]
    to_address: Optional[str]
    token_address: Optional[str]
    token_symbol: Optional[str]
    amount: Optional[float]
    usd_value: Optional[float]
    timestamp: Optional[int]
    watch_address: Optional[str]
    raw: Dict[str, Any]


@dataclass
class Alert:
    dedupe_key: str
    text: str
    usd_value: float
    tx_id: str
    chain: str
    timestamp: Optional[int]
    raw: Dict[str, Any]

