from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class AlliumError(RuntimeError):
    pass


@dataclass
class PriceQuote:
    chain: str
    token_address: str
    price: float
    symbol: Optional[str]


class AlliumClient:
    def __init__(self, base_url: str, api_key: str, timeout_seconds: int = 20) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._lock = threading.Lock()
        self._last_request_at = 0.0
        self._price_cache: Dict[str, tuple[float, float, Optional[str]]] = {}

    def _rate_limit(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_for = 1.0 - (now - self._last_request_at)
            if wait_for > 0:
                time.sleep(wait_for)
            self._last_request_at = time.monotonic()

    def _request(self, method: str, path: str, payload: Optional[Any] = None) -> Any:
        self._rate_limit()
        url = f"{self._base_url}{path}"
        data = None
        headers = {"X-API-KEY": self._api_key}

        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, data=data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise AlliumError(f"Allium HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise AlliumError(f"Allium request failed: {exc}") from exc

    def wallet_transactions(self, addresses: List[Dict[str, str]]) -> Any:
        return self._request("POST", "/api/v1/developer/wallet/transactions", payload=addresses)

    def prices(self, tokens: List[Dict[str, str]]) -> List[PriceQuote]:
        if not tokens:
            return []
        response = self._request("POST", "/api/v1/developer/prices", payload=tokens)
        if not isinstance(response, list):
            return []

        quotes: List[PriceQuote] = []
        now = time.time()
        for item in response:
            if not isinstance(item, dict):
                continue
            chain = str(item.get("chain", "")).lower()
            token_address = str(item.get("address") or item.get("token_address") or "").lower()
            price = item.get("price")
            symbol = None
            info = item.get("info")
            if isinstance(info, dict):
                symbol = info.get("symbol")
            if not chain or not token_address or not isinstance(price, (int, float)):
                continue
            self._price_cache[f"{chain}:{token_address}"] = (float(price), now, symbol)
            quotes.append(PriceQuote(chain=chain, token_address=token_address, price=float(price), symbol=symbol))
        return quotes

    def get_cached_price(self, chain: str, token_address: str, ttl_seconds: int = 60) -> Optional[PriceQuote]:
        key = f"{chain.lower()}:{token_address.lower()}"
        cached = self._price_cache.get(key)
        if not cached:
            return None
        price, fetched_at, symbol = cached
        if time.time() - fetched_at > ttl_seconds:
            return None
        return PriceQuote(chain=chain.lower(), token_address=token_address.lower(), price=price, symbol=symbol)

