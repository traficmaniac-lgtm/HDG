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
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.spot_base_url = spot_base_url
        self._client = httpx.Client(timeout=5.0)
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[int] = None

    def close(self) -> None:
        self._client.close()

    def get_spot_account(self) -> Optional[dict[str, Any]]:
        return self._signed_get(self.spot_base_url, "/api/v3/account")

    def get_margin_account(self) -> Optional[dict[str, Any]]:
        return self._signed_get(self.spot_base_url, "/sapi/v1/margin/account")

    def get_open_margin_orders(self, symbol: str) -> Optional[list[dict[str, Any]]]:
        data = self._signed_get(
            self.spot_base_url,
            "/sapi/v1/margin/openOrders",
            {"symbol": symbol},
        )
        if isinstance(data, list):
            return data
        return None

    def place_margin_order(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        return self._signed_post(self.spot_base_url, "/sapi/v1/margin/order", payload)

    def cancel_margin_order(self, symbol: str, order_id: int) -> Optional[dict[str, Any]]:
        payload = {"symbol": symbol, "orderId": order_id}
        return self._signed_delete(self.spot_base_url, "/sapi/v1/margin/order", payload)

    def borrow_margin_asset(self, asset: str, amount: float) -> Optional[dict[str, Any]]:
        payload = {"asset": asset, "amount": f"{amount:.8f}"}
        return self._signed_post(self.spot_base_url, "/sapi/v1/margin/loan", payload)

    def repay_margin_asset(self, asset: str, amount: float) -> Optional[dict[str, Any]]:
        payload = {"asset": asset, "amount": f"{amount:.8f}"}
        return self._signed_post(self.spot_base_url, "/sapi/v1/margin/repay", payload)

    def get_exchange_info(self, symbol: str) -> Optional[dict[str, Any]]:
        try:
            resp = self._client.get(
                f"{self.spot_base_url}/api/v3/exchangeInfo", params={"symbol": symbol}
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
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

    def _signed_get(
        self, base_url: str, path: str, params: Optional[dict[str, Any]] = None
    ) -> Optional[dict[str, Any]]:
        return self._signed_request("GET", base_url, path, params or {})

    def _signed_post(
        self, base_url: str, path: str, params: Optional[dict[str, Any]] = None
    ) -> Optional[dict[str, Any]]:
        return self._signed_request("POST", base_url, path, params or {})

    def _signed_delete(
        self, base_url: str, path: str, params: Optional[dict[str, Any]] = None
    ) -> Optional[dict[str, Any]]:
        return self._signed_request("DELETE", base_url, path, params or {})

    def _signed_request(
        self, method: str, base_url: str, path: str, params: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        if not self.api_key or not self.api_secret:
            self.last_error = "Missing API key/secret"
            self.last_error_code = None
            return None
        self.last_error = None
        self.last_error_code = None
        signed_params = {
            **params,
            "timestamp": int(time.time() * 1000),
            "recvWindow": 5000,
        }
        query_string = "&".join(f"{key}={value}" for key, value in signed_params.items())
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed_params["signature"] = signature
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            resp = self._client.request(
                method, f"{base_url}{path}", params=signed_params, headers=headers
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            try:
                payload = exc.response.json()
                self.last_error_code = int(payload.get("code")) if "code" in payload else None
                self.last_error = str(payload.get("msg", "HTTP error"))
            except Exception:
                self.last_error = str(exc)
                self.last_error_code = None
            return None
        except Exception as exc:
            self.last_error = str(exc)
            self.last_error_code = None
            return None
