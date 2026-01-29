from __future__ import annotations

import hashlib
import hmac
import time
import urllib.parse

import httpx


class BinanceRestClient:
    def __init__(
        self, api_key: str = "", api_secret: str = "", timeout_s: float = 10.0
    ) -> None:
        self._timeout = httpx.Timeout(timeout_s, connect=min(5.0, timeout_s))
        self._client = httpx.Client(timeout=self._timeout)
        self._base_url = "https://api.binance.com"
        self.api_key = api_key
        self.api_secret = api_secret

    def get_server_time(self) -> dict:
        response = self._client.get(f"{self._base_url}/api/v3/time")
        response.raise_for_status()
        return response.json()

    def get_exchange_info(self, symbol: str) -> dict:
        response = self._client.get(
            f"{self._base_url}/api/v3/exchangeInfo", params={"symbol": symbol}
        )
        response.raise_for_status()
        return response.json()

    def get_book_ticker(self, symbol: str) -> dict:
        response = self._client.get(
            f"{self._base_url}/api/v3/ticker/bookTicker", params={"symbol": symbol}
        )
        response.raise_for_status()
        return response.json()

    def create_margin_order(self, params: dict) -> dict:
        return self.signed_sapi_request("POST", "/sapi/v1/margin/order", params)

    def cancel_margin_order(self, params: dict) -> dict:
        return self.signed_sapi_request("DELETE", "/sapi/v1/margin/order", params)

    def get_margin_open_orders(self, symbol: str) -> list[dict]:
        return self.signed_sapi_request(
            "GET", "/sapi/v1/margin/openOrders", {"symbol": symbol}
        )

    def get_margin_order(self, symbol: str, order_id: int) -> dict:
        return self.signed_sapi_request(
            "GET",
            "/sapi/v1/margin/order",
            {"symbol": symbol, "orderId": order_id},
        )

    def get_margin_my_trades(self, symbol: str, limit: int = 50) -> list[dict]:
        return self.signed_sapi_request(
            "GET",
            "/sapi/v1/margin/myTrades",
            {"symbol": symbol, "limit": limit},
        )

    def get_margin_account(self) -> dict:
        return self.signed_sapi_request("GET", "/sapi/v1/margin/account", {})

    def borrow_margin_asset(self, params: dict) -> dict:
        return self.signed_sapi_request("POST", "/sapi/v1/margin/loan", params)

    def repay_margin_asset(self, params: dict) -> dict:
        return self.signed_sapi_request("POST", "/sapi/v1/margin/repay", params)

    def probe_margin_borrow_access(self, asset: str) -> dict:
        params = {"asset": asset, "amount": "0"}
        return self.signed_sapi_request("POST", "/sapi/v1/margin/loan", params)

    def signed_sapi_request(self, method: str, path: str, params: dict) -> dict:
        if not path.startswith("/sapi/"):
            raise ValueError("SAPI path must start with /sapi/")
        return self._signed_request(method, path, params)

    def _signed_request(self, method: str, path: str, params: dict) -> dict:
        payload = dict(params)
        payload["timestamp"] = int(time.time() * 1000)
        payload.setdefault("recvWindow", 5000)
        signature = self._sign(payload)
        payload["signature"] = signature
        headers = {"X-MBX-APIKEY": self.api_key}
        response = self._client.request(
            method, f"{self._base_url}{path}", params=payload, headers=headers
        )
        response.raise_for_status()
        return response.json()

    def _sign(self, params: dict) -> str:
        query = urllib.parse.urlencode(params)
        secret = self.api_secret.encode("utf-8")
        return hmac.new(secret, query.encode("utf-8"), hashlib.sha256).hexdigest()

    def close(self) -> None:
        self._client.close()
