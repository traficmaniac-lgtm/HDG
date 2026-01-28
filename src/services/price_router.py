from __future__ import annotations

import time
from typing import Optional

from src.core.models import HealthState, PriceState, Settings


class PriceRouter:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ws_bid: Optional[float] = None
        self._ws_ask: Optional[float] = None
        self._http_bid: Optional[float] = None
        self._http_ask: Optional[float] = None
        self._last_ws_ts: Optional[float] = None
        self._last_http_ts: Optional[float] = None
        self._last_mid_ts: Optional[float] = None
        self._last_source = "NONE"
        self._last_switch_reason = ""

    def update_ws(self, bid: float, ask: float) -> None:
        self._ws_bid = bid
        self._ws_ask = ask
        self._last_ws_ts = time.monotonic()

    def update_http(self, bid: float, ask: float) -> None:
        self._http_bid = bid
        self._http_ask = ask
        self._last_http_ts = time.monotonic()

    def get_best_quote(self) -> tuple[
        Optional[float], Optional[float], Optional[float], str, Optional[int], Optional[int], Optional[int]
    ]:
        now = time.monotonic()
        ws_age_ms = (
            int((now - self._last_ws_ts) * 1000) if self._last_ws_ts is not None else None
        )
        http_age_ms = (
            int((now - self._last_http_ts) * 1000)
            if self._last_http_ts is not None
            else None
        )
        ws_fresh = ws_age_ms is not None and ws_age_ms <= self._settings.ws_fresh_ms
        http_fresh = http_age_ms is not None and http_age_ms <= self._settings.http_fresh_ms

        bid = None
        ask = None
        source = "NONE"
        mid_ts = None

        if ws_fresh and self._ws_bid is not None and self._ws_ask is not None:
            bid = self._ws_bid
            ask = self._ws_ask
            source = "WS"
            mid_ts = self._last_ws_ts
        elif http_fresh and self._http_bid is not None and self._http_ask is not None:
            bid = self._http_bid
            ask = self._http_ask
            source = "HTTP"
            mid_ts = self._last_http_ts

        mid = (bid + ask) / 2.0 if bid is not None and ask is not None else None
        self._last_mid_ts = mid_ts if mid is not None else self._last_mid_ts
        mid_age_ms = (
            int((now - self._last_mid_ts) * 1000) if self._last_mid_ts is not None else None
        )
        return bid, ask, mid, source, ws_age_ms, http_age_ms, mid_age_ms

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        bid, ask, mid, source, ws_age_ms, http_age_ms, mid_age_ms = self.get_best_quote()

        if source != self._last_source:
            if source == "WS":
                self._last_switch_reason = "WS fresh"
            elif source == "HTTP":
                self._last_switch_reason = "HTTP fallback"
            else:
                self._last_switch_reason = "No fresh data"
            self._last_source = source

        price_state = PriceState(
            bid=bid,
            ask=ask,
            mid=mid,
            source=source,
            mid_age_ms=mid_age_ms,
        )
        health_state = HealthState(
            ws_connected=False,
            ws_age_ms=ws_age_ms,
            http_age_ms=http_age_ms,
            last_switch_reason=self._last_switch_reason,
        )
        return price_state, health_state
