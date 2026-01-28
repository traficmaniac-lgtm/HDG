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
        self._stable_bid: Optional[float] = None
        self._stable_ask: Optional[float] = None
        self._stable_mid: Optional[float] = None
        self._stable_ts: Optional[float] = None
        self._stable_source = "NONE"
        self._last_source = "NONE"
        self._last_switch_reason = ""
        self._ws_fresh_streak = 0
        self._ws_stable_since: Optional[float] = None

    def update_ws(self, bid: float, ask: float) -> None:
        self._ws_bid = bid
        self._ws_ask = ask
        now = time.monotonic()
        if self._last_ws_ts is None:
            self._ws_fresh_streak = 1
            self._ws_stable_since = now
        else:
            gap_ms = (now - self._last_ws_ts) * 1000.0
            if gap_ms > self._settings.ws_fresh_ms:
                self._ws_fresh_streak = 1
                self._ws_stable_since = now
            else:
                self._ws_fresh_streak += 1
                if self._ws_stable_since is None:
                    self._ws_stable_since = now
        self._last_ws_ts = now

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

        ws_stable = False
        if ws_fresh:
            stable_ms = (
                int((now - self._ws_stable_since) * 1000)
                if self._ws_stable_since is not None
                else 0
            )
            ws_stable = self._ws_fresh_streak >= 3 or (
                stable_ms >= self._settings.source_switch_hysteresis_ms
            )

        effective_source = self._stable_source
        if effective_source == "WS":
            if not ws_fresh:
                if http_fresh:
                    effective_source = "HTTP"
                else:
                    effective_source = "NONE"
        elif effective_source == "HTTP":
            if ws_stable:
                effective_source = "WS"
            elif not http_fresh:
                effective_source = "NONE"
        else:
            if ws_stable:
                effective_source = "WS"
            elif http_fresh:
                effective_source = "HTTP"

        bid = None
        ask = None
        if effective_source == "WS" and ws_fresh:
            bid = self._ws_bid
            ask = self._ws_ask
        elif effective_source == "HTTP" and http_fresh:
            bid = self._http_bid
            ask = self._http_ask

        mid = (bid + ask) / 2.0 if bid is not None and ask is not None else None
        min_interval_s = 0.2
        if mid is not None:
            should_update = (
                self._stable_ts is None or (now - self._stable_ts) >= min_interval_s
            )
            if should_update:
                self._stable_bid = bid
                self._stable_ask = ask
                self._stable_mid = mid
                self._stable_ts = now
                self._stable_source = effective_source

        mid_age_ms = (
            int((now - self._stable_ts) * 1000) if self._stable_ts is not None else None
        )
        return (
            self._stable_bid,
            self._stable_ask,
            self._stable_mid,
            self._stable_source,
            ws_age_ms,
            http_age_ms,
            mid_age_ms,
        )

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
