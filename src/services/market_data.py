from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import replace
from decimal import Decimal
from typing import Any, Deque, Optional

from src.core.models import MarketDataMode, MarketSnapshot


class MarketDataService:
    def __init__(
        self,
        ws_fresh_ms: int = 500,
        http_fresh_ms: int = 1200,
        http_hold_ms: int = 1500,
        ws_recovery_ticks: int = 3,
    ) -> None:
        self.ws_fresh_ms = ws_fresh_ms
        self.http_fresh_ms = http_fresh_ms
        self.http_hold_ms = http_hold_ms
        self.ws_recovery_ticks = ws_recovery_ticks
        self.mode = MarketDataMode.HYBRID
        self._lock = threading.Lock()
        self._snapshot = MarketSnapshot()
        self._last_ws_tick: Optional[dict[str, Any]] = None
        self._last_http_tick: Optional[dict[str, Any]] = None
        self._last_effective_tick: Optional[dict[str, Any]] = None
        self._last_effective_source = "NONE"
        self._last_source_change_ms = time.monotonic() * 1000
        self._ws_fresh_streak = 0
        self._http_hold_until_ms = 0.0
        self._ws_connected = False
        self._ws_rx_times: Deque[float] = deque()
        self._ws_mid_buffer: Deque[Decimal] = deque(maxlen=5)
        self._prev_ws_mid_raw: Optional[Decimal] = None
        self._last_ws_mid_raw: Optional[Decimal] = None

    def update_tick(self, payload: dict[str, Any]) -> None:
        source = str(payload.get("source", "WS"))
        rx_time_ms = payload.get("rx_time_ms")
        now_ms = time.monotonic() * 1000
        if rx_time_ms is None:
            rx_time_ms = now_ms
            payload["rx_time_ms"] = rx_time_ms
        with self._lock:
            if source == "WS":
                self._last_ws_tick = dict(payload)
                self._snapshot.last_ws_tick_ms = float(rx_time_ms)
                self._ws_rx_times.append(float(rx_time_ms))
                mid_raw = payload.get("mid_raw")
                if mid_raw is None:
                    mid_raw = Decimal(str(payload.get("mid") or "0"))
                if mid_raw > 0 and (not self._ws_mid_buffer or mid_raw != self._ws_mid_buffer[-1]):
                    self._ws_mid_buffer.append(mid_raw)
                    self._prev_ws_mid_raw = self._last_ws_mid_raw
                    self._last_ws_mid_raw = mid_raw
            elif source == "HTTP":
                self._last_http_tick = dict(payload)
                self._snapshot.last_http_tick_ms = float(rx_time_ms)
            self._update_snapshot_prices(payload)
            self._update_effective_tick(now_ms)

    def set_mode(self, mode: MarketDataMode) -> None:
        with self._lock:
            self.mode = mode
            self._update_effective_tick(time.monotonic() * 1000)

    def set_ws_connected(self, connected: bool) -> None:
        with self._lock:
            self._ws_connected = connected
            self._update_effective_tick(time.monotonic() * 1000)

    def get_snapshot(self) -> MarketSnapshot:
        with self._lock:
            return replace(self._snapshot)

    def get_last_ws_tick(self) -> Optional[dict[str, Any]]:
        with self._lock:
            return dict(self._last_ws_tick) if self._last_ws_tick else None

    def get_ws_mid_buffer(self) -> list[Decimal]:
        with self._lock:
            return list(self._ws_mid_buffer)

    def set_detect_window_ticks(self, detect_window_ticks: int) -> None:
        maxlen = max(5, int(detect_window_ticks))
        with self._lock:
            if self._ws_mid_buffer.maxlen == maxlen:
                return
            self._ws_mid_buffer = deque(list(self._ws_mid_buffer)[-maxlen:], maxlen=maxlen)

    def get_ws_mid_raws(self) -> tuple[Optional[Decimal], Optional[Decimal]]:
        with self._lock:
            return self._prev_ws_mid_raw, self._last_ws_mid_raw

    def get_last_http_tick(self) -> Optional[dict[str, Any]]:
        with self._lock:
            return dict(self._last_http_tick) if self._last_http_tick else None

    def get_last_effective_tick(self) -> Optional[dict[str, Any]]:
        with self._lock:
            return dict(self._last_effective_tick) if self._last_effective_tick else None

    def get_last_ws_rx_monotonic_ms(self) -> Optional[float]:
        with self._lock:
            return self._snapshot.last_ws_tick_ms

    def get_ws_rx_count_1s(self, now_ms: Optional[float] = None) -> int:
        if now_ms is None:
            now_ms = time.monotonic() * 1000
        cutoff_ms = now_ms - 1000.0
        with self._lock:
            while self._ws_rx_times and self._ws_rx_times[0] < cutoff_ms:
                self._ws_rx_times.popleft()
            return int(len(self._ws_rx_times))

    def is_ws_connected(self) -> bool:
        with self._lock:
            return self._ws_connected

    def _ws_age_ms(self, now_ms: float) -> float:
        last_tick_ms = self._snapshot.last_ws_tick_ms
        if last_tick_ms is None:
            return 9999.0
        return max(0.0, now_ms - last_tick_ms)

    def _http_age_ms(self, now_ms: float) -> float:
        last_tick_ms = self._snapshot.last_http_tick_ms
        if last_tick_ms is None:
            return 9999.0
        return max(0.0, now_ms - last_tick_ms)

    def _is_ws_fresh(self, now_ms: float) -> bool:
        if not self._ws_connected:
            return False
        last_tick_ms = self._snapshot.last_ws_tick_ms
        if last_tick_ms is None:
            return False
        return (now_ms - last_tick_ms) <= self.ws_fresh_ms

    def _is_http_fresh(self, now_ms: float) -> bool:
        last_tick_ms = self._snapshot.last_http_tick_ms
        if last_tick_ms is None:
            return False
        return (now_ms - last_tick_ms) <= self.http_fresh_ms

    def _effective_source(self, now_ms: float) -> str:
        ws_fresh = self._is_ws_fresh(now_ms)
        http_fresh = self._is_http_fresh(now_ms)
        if self.mode == MarketDataMode.WS_ONLY:
            return "WS" if ws_fresh else "NONE"
        if self.mode == MarketDataMode.HTTP_ONLY:
            return "HTTP" if http_fresh else "NONE"
        if self._last_effective_source == "HTTP":
            if http_fresh and now_ms < self._http_hold_until_ms:
                return "HTTP"
            if ws_fresh and self._ws_fresh_streak >= self.ws_recovery_ticks:
                return "WS"
            if http_fresh:
                return "HTTP"
            if ws_fresh:
                return "WS"
            return "NONE"
        if ws_fresh:
            return "WS"
        if http_fresh:
            return "HTTP"
        return "NONE"

    def _update_snapshot_prices(self, payload: dict[str, Any]) -> None:
        self._snapshot.bid = float(payload.get("bid", self._snapshot.bid) or 0.0)
        self._snapshot.ask = float(payload.get("ask", self._snapshot.ask) or 0.0)
        self._snapshot.mid = float(payload.get("mid", self._snapshot.mid) or 0.0)

    def _update_effective_tick(self, now_ms: float) -> None:
        ws_fresh = self._is_ws_fresh(now_ms)
        if ws_fresh and self._last_ws_tick:
            self._ws_fresh_streak += 1
        else:
            self._ws_fresh_streak = 0
        source = self._effective_source(now_ms)
        if source != self._last_effective_source:
            self._last_effective_source = source
            self._last_source_change_ms = now_ms
            if source == "HTTP":
                self._http_hold_until_ms = now_ms + self.http_hold_ms
        if source == "WS":
            self._last_effective_tick = self._last_ws_tick
        elif source == "HTTP":
            self._last_effective_tick = self._last_http_tick
        else:
            self._last_effective_tick = None
        self._snapshot.ws_age_ms = self._ws_age_ms(now_ms)
        self._snapshot.http_age_ms = self._http_age_ms(now_ms)
        self._snapshot.effective_source = self._last_effective_source
        if self._snapshot.effective_source == "WS":
            self._snapshot.effective_age_ms = self._snapshot.ws_age_ms
        elif self._snapshot.effective_source == "HTTP":
            self._snapshot.effective_age_ms = self._snapshot.http_age_ms
        else:
            self._snapshot.effective_age_ms = 9999.0
        self._snapshot.data_stale = self._snapshot.effective_source == "NONE"
