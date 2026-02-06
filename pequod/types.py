from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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
    tx_type: str
    timestamp: Optional[int]
    watch_address: Optional[str]
    from_address: Optional[str]
    to_address: Optional[str]
    token_symbol: Optional[str]
    token_address: Optional[str]
    amount: Optional[float]
    raw: Dict[str, Any]
    score: float = 0.0
    score_reasons: List[Dict[str, Any]] = field(default_factory=list)
    score_breakdown: Dict[str, float] = field(default_factory=dict)
    entities: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    deep_link: Optional[str] = None
