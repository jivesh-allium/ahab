from __future__ import annotations

import hashlib
import math
from typing import Any, Dict, Optional, Tuple

from .types import Alert, WatchAddress


def _normalize_address(address: Optional[str]) -> Optional[str]:
    if not isinstance(address, str):
        return None
    value = address.strip().lower()
    if not value:
        return None
    return value


def pseudo_latlon_for_address(address: str) -> Tuple[float, float]:
    digest = hashlib.sha256(address.encode("utf-8")).digest()
    lat = (digest[0] / 255.0) * 140.0 - 70.0
    lon = (digest[1] / 255.0) * 360.0 - 180.0
    return (lat, lon)


def anchored_latlon_for_address(address: str, anchor_lat: float, anchor_lon: float) -> Tuple[float, float]:
    digest = hashlib.sha256(f"{address}:anchor".encode("utf-8")).digest()
    lat_offset = (digest[0] / 255.0 - 0.5) * 8.0
    lon_offset = (digest[1] / 255.0 - 0.5) * 12.0
    lat = max(-84.0, min(84.0, anchor_lat + lat_offset))
    lon = anchor_lon + lon_offset
    if lon > 180:
        lon -= 360
    if lon < -180:
        lon += 360
    return (lat, lon)


def classify_event_type(tx_type: str) -> str:
    value = (tx_type or "").lower()
    if "bridge" in value:
        return "bridge_move"
    if "trade" in value or "swap" in value:
        return "dex_swap"
    if "burn" in value:
        return "burn"
    if "mint" in value:
        return "mint"
    if "liquidity" in value or "lp_" in value:
        return "lp_event"
    return "transfer_large"


def effect_for_event_type(event_type: str) -> str:
    return {
        "bridge_move": "arc",
        "dex_swap": "spiral",
        "burn": "collapse",
        "mint": "fountain",
        "lp_event": "surge",
        "transfer_large": "ripple",
    }.get(event_type, "ripple")


def severity_for_usd(usd_value: float) -> str:
    if usd_value >= 1_000_000:
        return "storm"
    if usd_value >= 100_000:
        return "rough"
    return "calm"


def event_score(event_type: str, usd_value: float, now_ts: int, event_ts: Optional[int]) -> float:
    usd = max(0.0, float(usd_value))
    magnitude = math.log10(usd + 10.0) * 10.0
    multiplier = {
        "bridge_move": 1.25,
        "dex_swap": 1.2,
        "burn": 1.15,
        "mint": 1.1,
        "lp_event": 1.05,
        "transfer_large": 1.0,
    }.get(event_type, 1.0)
    score = magnitude * multiplier
    if isinstance(event_ts, int) and event_ts > 0:
        age = max(0, now_ts - event_ts)
        decay = max(0.35, 1.0 - (age / 3600.0))
        score *= decay
    return round(score, 2)


def address_geo(
    address: Optional[str],
    geo_by_address: Dict[str, Dict[str, Any]],
    watch_by_address: Dict[str, WatchAddress],
) -> Optional[Dict[str, Any]]:
    normalized = _normalize_address(address)
    if not normalized:
        return None
    geo = geo_by_address.get(normalized, {})
    lat = geo.get("lat")
    lon = geo.get("lon")
    geo_source = "geo"
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        lat, lon = pseudo_latlon_for_address(normalized)
        geo_source = "pseudo"
    watch = watch_by_address.get(normalized)
    return {
        "address": normalized,
        "label": watch.label if watch else None,
        "lat": float(lat),
        "lon": float(lon),
        "country": geo.get("primary_country"),
        "region": geo.get("primary_region"),
        "confidence": geo.get("confidence"),
        "geo_source": geo_source,
    }


def build_map_event(
    alert: Alert,
    now_ts: int,
    geo_by_address: Dict[str, Dict[str, Any]],
    watch_by_address: Dict[str, WatchAddress],
) -> Dict[str, Any]:
    event_type = classify_event_type(alert.tx_type)
    source = address_geo(alert.from_address or alert.watch_address, geo_by_address, watch_by_address)
    target = address_geo(alert.to_address, geo_by_address, watch_by_address)
    if source is None:
        source = address_geo(alert.watch_address or alert.from_address or alert.to_address, geo_by_address, watch_by_address)
    if target is None:
        target = source
    if (
        source
        and target
        and source.get("geo_source") == "pseudo"
        and target.get("geo_source") == "geo"
        and isinstance(target.get("lat"), (int, float))
        and isinstance(target.get("lon"), (int, float))
        and isinstance(source.get("address"), str)
    ):
        lat, lon = anchored_latlon_for_address(
            address=str(source["address"]),
            anchor_lat=float(target["lat"]),
            anchor_lon=float(target["lon"]),
        )
        source["lat"] = lat
        source["lon"] = lon
        source["geo_source"] = "anchored"
    if (
        source
        and target
        and target.get("geo_source") == "pseudo"
        and source.get("geo_source") in {"geo", "anchored"}
        and isinstance(source.get("lat"), (int, float))
        and isinstance(source.get("lon"), (int, float))
        and isinstance(target.get("address"), str)
    ):
        lat, lon = anchored_latlon_for_address(
            address=str(target["address"]),
            anchor_lat=float(source["lat"]),
            anchor_lon=float(source["lon"]),
        )
        target["lat"] = lat
        target["lon"] = lon
        target["geo_source"] = "anchored"
    entities = alert.entities if isinstance(alert.entities, dict) else {}
    from_entity = entities.get("from") if isinstance(entities.get("from"), dict) else {}
    to_entity = entities.get("to") if isinstance(entities.get("to"), dict) else {}
    if source and isinstance(from_entity.get("display_name"), str) and from_entity.get("display_name"):
        source["label"] = str(from_entity["display_name"])
    if target and isinstance(to_entity.get("display_name"), str) and to_entity.get("display_name"):
        target["label"] = str(to_entity["display_name"])
    model_score = event_score(event_type, alert.usd_value, now_ts=now_ts, event_ts=alert.timestamp)
    final_score = float(alert.score) if isinstance(alert.score, (int, float)) and alert.score > 0 else model_score
    return {
        "event_id": alert.dedupe_key,
        "timestamp": alert.timestamp,
        "chain": alert.chain,
        "tx_id": alert.tx_id,
        "tx_type": alert.tx_type,
        "event_type": event_type,
        "effect": effect_for_event_type(event_type),
        "severity": severity_for_usd(alert.usd_value),
        "score": round(final_score, 2),
        "score_model": model_score,
        "score_breakdown": alert.score_breakdown if isinstance(alert.score_breakdown, dict) else {},
        "score_reasons": alert.score_reasons if isinstance(alert.score_reasons, list) else [],
        "usd_value": alert.usd_value,
        "token_symbol": alert.token_symbol,
        "token_address": alert.token_address,
        "amount": alert.amount,
        "from_address": alert.from_address,
        "to_address": alert.to_address,
        "watch_address": alert.watch_address,
        "source": source,
        "target": target,
        "entities": entities,
        "deep_link": alert.deep_link,
        "text": alert.text,
    }
