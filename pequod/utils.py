from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return None
    return None


def parse_timestamp(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = int(value)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return ts
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return parse_timestamp(int(raw))
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None
    return None


def short_hash(data: Dict[str, Any]) -> str:
    digest = hashlib.sha256(repr(sorted(data.items())).encode("utf-8")).hexdigest()
    return digest[:16]

