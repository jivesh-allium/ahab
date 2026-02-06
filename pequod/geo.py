from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .allium_client import AlliumClient, AlliumError
from .types import WatchAddress
from .utils import to_float

LOG = logging.getLogger(__name__)

DAY_SECONDS = 24 * 60 * 60


COUNTRY_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "united states": (39.5, -98.35),
    "usa": (39.5, -98.35),
    "canada": (56.13, -106.34),
    "mexico": (23.63, -102.55),
    "brazil": (-14.23, -51.93),
    "argentina": (-38.42, -63.62),
    "chile": (-35.67, -71.54),
    "peru": (-9.19, -75.02),
    "colombia": (4.57, -74.30),
    "united kingdom": (55.38, -3.43),
    "uk": (55.38, -3.43),
    "ireland": (53.41, -8.24),
    "france": (46.23, 2.21),
    "germany": (51.17, 10.45),
    "spain": (40.46, -3.75),
    "portugal": (39.40, -8.22),
    "italy": (41.87, 12.57),
    "netherlands": (52.13, 5.29),
    "belgium": (50.50, 4.47),
    "switzerland": (46.82, 8.23),
    "austria": (47.52, 14.55),
    "poland": (51.92, 19.15),
    "czech republic": (49.82, 15.47),
    "sweden": (60.13, 18.64),
    "norway": (60.47, 8.47),
    "finland": (61.92, 25.75),
    "denmark": (56.26, 9.50),
    "iceland": (64.96, -19.02),
    "russia": (61.52, 105.32),
    "ukraine": (48.38, 31.17),
    "turkey": (38.96, 35.24),
    "israel": (31.05, 34.85),
    "saudi arabia": (23.89, 45.08),
    "uae": (23.42, 53.85),
    "united arab emirates": (23.42, 53.85),
    "south africa": (-30.56, 22.94),
    "nigeria": (9.08, 8.68),
    "kenya": (-0.02, 37.91),
    "egypt": (26.82, 30.80),
    "morocco": (31.79, -7.09),
    "india": (20.59, 78.96),
    "pakistan": (30.38, 69.35),
    "bangladesh": (23.68, 90.36),
    "china": (35.86, 104.20),
    "hong kong": (22.32, 114.17),
    "taiwan": (23.70, 121.00),
    "japan": (36.20, 138.25),
    "south korea": (35.91, 127.77),
    "singapore": (1.35, 103.82),
    "thailand": (15.87, 100.99),
    "vietnam": (14.06, 108.28),
    "indonesia": (-0.79, 113.92),
    "philippines": (12.88, 121.77),
    "australia": (-25.27, 133.78),
    "new zealand": (-40.90, 174.89),
}


def _normalize_address(address: str) -> str:
    return address.strip().lower()


def _country_to_latlon(country: Optional[str], address: str) -> Tuple[float, float]:
    if country:
        centroid = COUNTRY_CENTROIDS.get(country.strip().lower())
        if centroid:
            return centroid
    digest = hashlib.sha256(address.encode("utf-8")).digest()
    lat = (digest[0] / 255.0) * 140.0 - 70.0
    lon = (digest[1] / 255.0) * 360.0 - 180.0
    return (lat, lon)


class GeoResolver:
    def __init__(
        self,
        client: AlliumClient,
        cache_path: Path,
        refresh_interval_seconds: int = DAY_SECONDS,
        query_timeout_seconds: int = 180,
    ) -> None:
        self._client = client
        self._cache_path = cache_path
        self._refresh_interval_seconds = refresh_interval_seconds
        self._query_timeout_seconds = query_timeout_seconds
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache_updated_at = 0
        self._cache_rows: Dict[str, Dict[str, Any]] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        if not self._cache_path.exists():
            return
        try:
            payload = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        self._cache_updated_at = int(payload.get("updated_at") or 0)
        rows = payload.get("rows")
        if isinstance(rows, dict):
            for address, row in rows.items():
                if isinstance(row, dict):
                    self._cache_rows[_normalize_address(address)] = row

    def _save_cache(self) -> None:
        payload = {"updated_at": self._cache_updated_at, "rows": self._cache_rows}
        self._cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _query_with_wait(self, sql: str, limit: int) -> List[Dict[str, Any]]:
        query_id = self._client.explorer_create_query(
            title=f"pequod_geo_{int(time.time())}",
            sql=sql,
            limit=limit,
        )
        run_id = self._client.explorer_run_query_async(query_id=query_id, parameters={})
        deadline = time.time() + max(30, self._query_timeout_seconds)
        while time.time() < deadline:
            status = self._client.explorer_query_status(run_id)
            if status == "success":
                results = self._client.explorer_query_results(run_id)
                rows = results.get("data")
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
                return []
            if status == "failed":
                raise AlliumError(f"Explorer query failed for run_id={run_id}")
            time.sleep(2)
        raise AlliumError(f"Explorer query timed out for run_id={run_id}")

    @staticmethod
    def _escape_sql_literal(value: str) -> str:
        return value.replace("'", "''")

    def get_geo_for_watchlist(self, watchlist: List[WatchAddress], force: bool = False) -> Dict[str, Dict[str, Any]]:
        addresses = sorted({_normalize_address(item.address) for item in watchlist if item.address})
        if not addresses:
            return {}
        needs_refresh = force or (time.time() - self._cache_updated_at > self._refresh_interval_seconds)
        missing = [addr for addr in addresses if addr not in self._cache_rows]
        if needs_refresh or missing:
            target = addresses if needs_refresh else missing
            self._refresh_geo_rows(target)
        return {address: self._cache_rows.get(address, {}) for address in addresses}

    def _refresh_geo_rows(self, addresses: List[str]) -> None:
        if not addresses:
            return
        literals = ", ".join(f"'{self._escape_sql_literal(addr)}'" for addr in addresses)
        sql = (
            "SELECT LOWER(address) AS address, primary_country, primary_region, score, confidence, reasoning "
            "FROM allium_identity.geo.addresses_geography "
            f"WHERE LOWER(address) IN ({literals}) "
            "LIMIT 20000"
        )
        rows = self._query_with_wait(sql=sql, limit=max(1000, len(addresses)))
        by_address: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            address_value = row.get("address")
            if not isinstance(address_value, str):
                continue
            address = _normalize_address(address_value)
            existing = by_address.get(address)
            score = to_float(row.get("score")) or 0.0
            if existing is None or score > (to_float(existing.get("score")) or 0.0):
                lat, lon = _country_to_latlon(row.get("primary_country"), address=address)
                by_address[address] = {
                    "address": address,
                    "primary_country": row.get("primary_country"),
                    "primary_region": row.get("primary_region"),
                    "score": row.get("score"),
                    "confidence": row.get("confidence"),
                    "reasoning": row.get("reasoning"),
                    "lat": lat,
                    "lon": lon,
                    "source": "allium_identity.geo.addresses_geography",
                }
        for address in addresses:
            fallback_lat, fallback_lon = _country_to_latlon(None, address=address)
            self._cache_rows[address] = by_address.get(
                address,
                {
                    "address": address,
                    "primary_country": None,
                    "primary_region": None,
                    "score": None,
                    "confidence": None,
                    "reasoning": None,
                    "lat": fallback_lat,
                    "lon": fallback_lon,
                    "source": "fallback",
                },
            )
        self._cache_updated_at = int(time.time())
        self._save_cache()
        LOG.info("Geo cache refreshed for %d addresses.", len(addresses))

    def bootstrap_watchlist(self, limit: int, chain: str = "ethereum") -> List[WatchAddress]:
        if limit <= 0:
            return []
        sql = (
            "SELECT LOWER(address) AS address, MAX(score) AS score "
            "FROM allium_identity.geo.addresses_geography "
            "GROUP BY LOWER(address) "
            "ORDER BY score DESC NULLS LAST "
            f"LIMIT {int(limit)}"
        )
        rows = self._query_with_wait(sql=sql, limit=max(100, limit))
        result: List[WatchAddress] = []
        for idx, row in enumerate(rows):
            address = row.get("address")
            if not isinstance(address, str):
                continue
            result.append(
                WatchAddress(
                    chain=chain,
                    address=_normalize_address(address),
                    label=f"geo_whale_{idx + 1}",
                    category="geo_bootstrap",
                )
            )
        return result
