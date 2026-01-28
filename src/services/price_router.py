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
        self._last_mid_ts = self._last_ws_ts

    def update_http(self, bid: float, ask: float) -> None:
        self._http_bid = bid
        self._http_ask = ask
        self._last_http_ts = time.monotonic()
        self._last_mid_ts = self._last_http_ts

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        now = time.monotonic()
        ws_age_ms = (
            int((now - self._last_ws_ts) * 1000) if self._last_ws_ts is not None else None
        )
        http_age_ms = (
            int((now - self._last_http_ts) * 1000)
            if self._last_http_ts is not None
            else None
        )

        source = "NONE"
        if ws_age_ms is not None and ws_age_ms <= self._settings.ws_fresh_ms:
            source = "WS"
        elif http_age_ms is not None and http_age_ms <= self._settings.http_fresh_ms:
            source = "HTTP"

        if source != self._last_source:
            if source == "WS":
                self._last_switch_reason = "WS fresh"
            elif source == "HTTP":
                self._last_switch_reason = "HTTP fallback"
            else:
                self._last_switch_reason = "No fresh data"
            self._last_source = source

        bid = None
        ask = None
        mid = None
        if source == "WS":
            bid = self._ws_bid
            ask = self._ws_ask
        elif source == "HTTP":
            bid = self._http_bid
            ask = self._http_ask

        if bid is not None and ask is not None:
            mid = (bid + ask) / 2.0

        mid_age_ms = (
            int((now - self._last_mid_ts) * 1000) if self._last_mid_ts is not None else None
        )

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
