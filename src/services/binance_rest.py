from __future__ import annotations

import hashlib
import hmac
import time
from urllib.parse import urlencode
from typing import Any, Optional

import httpx

from src.services.time_sync import TimeSync


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
        self._time_sync = TimeSync(self._client, self.spot_base_url)
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[int] = None
        self.last_error_status: Optional[int] = None
        self.last_error_path: Optional[str] = None
        self.last_error_params: Optional[dict[str, Any]] = None

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

    def get_time_offset_ms(self) -> int:
        return self._time_sync.offset_ms

    def sync_time(self) -> bool:
        return self._time_sync.sync()

    def place_order_margin(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        return self.place_margin_order(payload)

    def get_order(self, symbol: str, order_id: int) -> Optional[dict[str, Any]]:
        payload = {"symbol": symbol, "orderId": order_id}
        return self._signed_get(self.spot_base_url, "/sapi/v1/margin/order", payload)

    def cancel_open_orders(self, symbol: str) -> None:
        orders = self.get_open_margin_orders(symbol) or []
        for order in orders:
            order_id = order.get("orderId")
            if order_id:
                self.cancel_margin_order(symbol, int(order_id))

    def margin_account(self) -> Optional[dict[str, Any]]:
        return self.get_margin_account()

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
            self.last_error_status = None
            self.last_error_path = path
            self.last_error_params = params
            return None
        self.last_error = None
        self.last_error_code = None
        self.last_error_status = None
        self.last_error_path = None
        self.last_error_params = None
        retries = 2
        resynced = False
        for attempt in range(retries + 1):
            if attempt == 0 and self._time_sync.offset_ms == 0:
                self._time_sync.sync()
            signed_params = {
                **params,
                "timestamp": self._time_sync.timestamp_ms(),
                "recvWindow": 5000,
            }
            ordered_params = self._build_query_params(signed_params)
            query_string = urlencode(ordered_params, doseq=True)
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                query_string.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            ordered_params.append(("signature", signature))
            headers = {"X-MBX-APIKEY": self.api_key}
            try:
                resp = self._client.request(
                    method,
                    f"{base_url}{path}",
                    params=ordered_params,
                    headers=headers,
                )
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                self.last_error_status = status
                self.last_error_path = path
                self.last_error_params = params
                try:
                    payload = exc.response.json()
                    self.last_error_code = (
                        int(payload.get("code")) if "code" in payload else None
                    )
                    self.last_error = str(payload.get("msg", "HTTP error"))
                except Exception:
                    self.last_error = str(exc)
                    self.last_error_code = None
                self._log_signed_failure(
                    method=method,
                    path=path,
                    status=status,
                    code=self.last_error_code,
                    msg=self.last_error or "",
                    params=params,
                )
                if self.last_error_code == -1021:
                    if resynced:
                        return None
                    resynced = True
                    self._time_sync.sync()
                    continue
                if self.last_error_code == -1002:
                    return None
                if status >= 500 and attempt < retries:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                return None
            except httpx.RequestError as exc:
                self.last_error = str(exc)
                self.last_error_code = None
                self.last_error_status = None
                self.last_error_path = path
                self.last_error_params = params
                if attempt < retries:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                return None
            except Exception as exc:
                self.last_error = str(exc)
                self.last_error_code = None
                self.last_error_status = None
                self.last_error_path = path
                self.last_error_params = params
                return None
        return None

    def _build_query_params(self, params: dict[str, Any]) -> list[tuple[str, Any]]:
        return sorted(params.items(), key=lambda item: item[0])

    def _log_signed_failure(
        self,
        method: str,
        path: str,
        status: int,
        code: Optional[int],
        msg: str,
        params: dict[str, Any],
    ) -> None:
        safe_params = {
            key: value
            for key, value in params.items()
            if key.lower() not in {"signature", "api_key", "apikey"}
        }
        tag = "signed_fail" if code == -1002 else "signed_error"
        print(
            f"[REST] {tag} method={method} path={path} status={status} "
            f"code={code} msg={msg} params={safe_params}"
        )
