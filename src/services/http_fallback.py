from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import httpx


class HttpFallback:
    def __init__(self, base_url: str = "https://api.binance.com") -> None:
        self.base_url = base_url
        self._client = httpx.Client(timeout=5.0)

    def get_book_ticker(self, symbol: str) -> Optional[dict]:
        try:
            resp = self._client.get(
                f"{self.base_url}/api/v3/ticker/bookTicker", params={"symbol": symbol}
            )
            resp.raise_for_status()
            data = resp.json()
            bid = float(data.get("bidPrice", 0.0))
            ask = float(data.get("askPrice", 0.0))
            if bid <= 0 or ask <= 0:
                return None
            mid = (bid + ask) / 2
            spread_bps = (ask / bid - 1) * 10_000
            now = datetime.now(timezone.utc)
            return {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_bps": spread_bps,
                "event_time": now,
                "rx_time": now,
            }
        except Exception:
            return None

    def close(self) -> None:
        self._client.close()
