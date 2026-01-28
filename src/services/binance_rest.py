from __future__ import annotations

import httpx


class BinanceRestClient:
    def __init__(self, api_key: str = "", api_secret: str = "") -> None:
        self._client = httpx.Client(timeout=10.0)
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

    def close(self) -> None:
        self._client.close()
