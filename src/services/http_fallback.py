from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx


class HttpFallback:
    def __init__(self, base_url: str = "https://api.binance.com") -> None:
        self.base_url = base_url
        self._client = httpx.Client(timeout=5.0)
        self.last_http_tick_time: Optional[datetime] = None
        self._last_http_tick_ms: Optional[float] = None
        self.http_age_ms: float = 9999.0
        self.consecutive_http_failures = 0
        self._closed = False
        self._logger = logging.getLogger("dhs")

    def get_book_ticker(self, symbol: str) -> Optional[dict]:
        if self._closed:
            return None
        now_ms = time.monotonic() * 1000
        if self._last_http_tick_ms is None:
            self.http_age_ms = 9999.0
        else:
            self.http_age_ms = max(0.0, now_ms - self._last_http_tick_ms)
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
            spread_bps = (ask - bid) / mid * 10_000
            now = datetime.now(timezone.utc)
            rx_time_ms = time.monotonic() * 1000
            self.last_http_tick_time = now
            self._last_http_tick_ms = rx_time_ms
            self.http_age_ms = 0.0
            self.consecutive_http_failures = 0
            return {
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_bps": spread_bps,
                "event_time": now,
                "rx_time": now,
                "rx_time_ms": rx_time_ms,
                "source": "HTTP",
            }
        except Exception:
            self.consecutive_http_failures += 1
            if self._last_http_tick_ms is not None:
                self.http_age_ms = max(0.0, now_ms - self._last_http_tick_ms)
            self._logger.exception("HTTP fallback request failed for %s", symbol)
            return None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._client.close()
