from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any, Optional

import httpx


class BinanceRestClient:
    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        spot_base_url: str = "https://api.binance.com",
        futures_base_url: str = "https://fapi.binance.com",
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.spot_base_url = spot_base_url
        self.futures_base_url = futures_base_url
        self._client = httpx.Client(timeout=5.0)

    def close(self) -> None:
        self._client.close()

    def get_spot_account(self) -> Optional[dict[str, Any]]:
        return self._signed_get(self.spot_base_url, "/api/v3/account")

    def get_margin_account(self) -> Optional[dict[str, Any]]:
        return self._signed_get(self.spot_base_url, "/sapi/v1/margin/account")

    def get_futures_account(self) -> Optional[dict[str, Any]]:
        return self._signed_get(self.futures_base_url, "/fapi/v2/account")

    def get_futures_balance(self) -> Optional[list[dict[str, Any]]]:
        data = self._signed_get(self.futures_base_url, "/fapi/v2/balance")
        if isinstance(data, list):
            return data
        return None

    def get_depth(self, symbol: str, limit: int = 20) -> Optional[dict[str, Any]]:
        try:
            resp = self._client.get(
                f"{self.spot_base_url}/api/v3/depth",
                params={"symbol": symbol, "limit": limit},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def _signed_get(self, base_url: str, path: str) -> Optional[dict[str, Any]]:
        if not self.api_key or not self.api_secret:
            return None
        try:
            params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
            query_string = "&".join(f"{key}={value}" for key, value in params.items())
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                query_string.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            params["signature"] = signature
            headers = {"X-MBX-APIKEY": self.api_key}
            resp = self._client.get(f"{base_url}{path}", params=params, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None
