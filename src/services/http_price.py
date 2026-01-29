from __future__ import annotations

import httpx


class HttpPriceService:
    def __init__(self, timeout_s: float = 5.0) -> None:
        self._client = httpx.Client(timeout=httpx.Timeout(timeout_s))
        self._base_url = "https://api.binance.com"

    def fetch_book_ticker(self, symbol: str) -> dict:
        response = self._client.get(
            f"{self._base_url}/api/v3/ticker/bookTicker", params={"symbol": symbol}
        )
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self._client.close()
