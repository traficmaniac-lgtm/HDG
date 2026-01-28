from __future__ import annotations

import logging
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
        ws_fresh_ms: int = 700,
        http_fresh_ms: int = 1500,
        http_hold_ms: int = 3000,
        ws_recovery_ticks: int = 5,
        ws_min_connected_ms: int = 1000,
        switch_cooldown_ms: int = 800,
        ws_grace_ms: int = 300,
    ) -> None:
        self._symbol: Optional[str] = None
        self.ws_fresh_ms = ws_fresh_ms
        self.http_fresh_ms = http_fresh_ms
        self.http_hold_ms = http_hold_ms
        self.ws_recovery_ticks = ws_recovery_ticks
        self.ws_min_connected_ms = ws_min_connected_ms
        self.switch_cooldown_ms = switch_cooldown_ms
        self.ws_grace_ms = ws_grace_ms
        self.mode = MarketDataMode.HYBRID
        self._lock = threading.Lock()
        self._logger = logging.getLogger("dhs")
        self._snapshot = MarketSnapshot()
        self._last_ws_tick: Optional[dict[str, Any]] = None
        self._last_http_tick: Optional[dict[str, Any]] = None
        self._last_effective_tick: Optional[dict[str, Any]] = None
        self._last_effective_source = "NONE"
        self._last_source_change_ms = time.monotonic() * 1000
        self._ws_good_streak = 0
        self._ws_bad_streak = 0
        self._http_hold_until_ms = 0.0
        self._ws_connected = False
        self._ws_connected_since_ms: Optional[float] = None
        self._ws_rx_times: Deque[float] = deque()
        self._ws_mid_buffer: Deque[Decimal] = deque(maxlen=5)
        self._prev_ws_mid_raw: Optional[Decimal] = None
        self._last_ws_mid_raw: Optional[Decimal] = None
        self._last_status_log_ms = 0.0

    def set_symbol(self, symbol: str) -> None:
        normalized = symbol.upper().strip()
        with self._lock:
            if self._symbol == normalized:
                return
            self._symbol = normalized
            self._snapshot = MarketSnapshot()
            self._last_ws_tick = None
            self._last_http_tick = None
            self._last_effective_tick = None
            self._last_effective_source = "NONE"
            self._last_source_change_ms = time.monotonic() * 1000
            self._ws_good_streak = 0
            self._ws_bad_streak = 0
            self._http_hold_until_ms = 0.0
            self._ws_connected = False
            self._ws_connected_since_ms = None
            self._ws_rx_times.clear()
            self._ws_mid_buffer.clear()
            self._prev_ws_mid_raw = None
            self._last_ws_mid_raw = None
            self._last_status_log_ms = 0.0

    def get_symbol(self) -> Optional[str]:
        with self._lock:
            return self._symbol

    def update_tick(self, payload: dict[str, Any]) -> None:
        source = str(payload.get("source", "WS"))
        rx_time_ms = payload.get("rx_time_ms")
        now_ms = time.monotonic() * 1000
        if rx_time_ms is None:
            rx_time_ms = now_ms
            payload["rx_time_ms"] = rx_time_ms
        with self._lock:
            payload_symbol = payload.get("symbol")
            if self._symbol and payload_symbol and str(payload_symbol).upper() != self._symbol:
                return
            ws_tick_received = False
            if source == "WS":
                self._last_ws_tick = dict(payload)
                self._snapshot.last_ws_tick_ms = float(rx_time_ms)
                self._ws_rx_times.append(float(rx_time_ms))
                ws_tick_received = True
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
            self._update_effective_tick(now_ms, ws_tick_received=ws_tick_received)

    def set_mode(self, mode: MarketDataMode) -> None:
        with self._lock:
            self.mode = mode
            self._update_effective_tick(time.monotonic() * 1000)

    def set_ws_connected(self, connected: bool) -> None:
        with self._lock:
            self._ws_connected = connected
            now_ms = time.monotonic() * 1000
            if connected:
                self._ws_connected_since_ms = now_ms
            else:
                self._ws_connected_since_ms = None
                self._ws_good_streak = 0
                self._ws_bad_streak = 0
            self._update_effective_tick(now_ms, ws_tick_received=False)

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

    def get_mid_raw(self) -> Optional[Decimal]:
        with self._lock:
            tick = self._last_effective_tick or {}
            mid_raw = tick.get("mid_raw")
            if mid_raw is not None:
                return Decimal(str(mid_raw))
            bid_raw = tick.get("bid_raw")
            ask_raw = tick.get("ask_raw")
            if bid_raw is not None and ask_raw is not None:
                bid = Decimal(str(bid_raw))
                ask = Decimal(str(ask_raw))
                if bid > 0 and ask > 0:
                    return (bid + ask) / Decimal("2")
            mid = tick.get("mid")
            if mid is None:
                return None
            mid_dec = Decimal(str(mid))
            if mid_dec > 0:
                return mid_dec
            return None

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

    def _update_snapshot_prices(self, payload: dict[str, Any]) -> None:
        self._snapshot.bid = float(payload.get("bid", self._snapshot.bid) or 0.0)
        self._snapshot.ask = float(payload.get("ask", self._snapshot.ask) or 0.0)
        self._snapshot.mid = float(payload.get("mid", self._snapshot.mid) or 0.0)

    def _update_effective_tick(self, now_ms: float, ws_tick_received: bool) -> None:
        ws_fresh = self._is_ws_fresh(now_ms)
        if ws_tick_received and ws_fresh and self._last_ws_tick:
            self._ws_good_streak += 1
            self._ws_bad_streak = 0
        elif ws_tick_received:
            self._ws_good_streak = 0
            self._ws_bad_streak += 1
        elif not ws_fresh and self._ws_connected:
            self._ws_bad_streak += 1
            self._ws_good_streak = 0
        source, reason = self._evaluate_source(now_ms)
        if source != self._last_effective_source:
            self._log_switch(
                from_source=self._last_effective_source,
                to_source=source,
                reason=reason,
                now_ms=now_ms,
            )
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
        self._maybe_log_status(now_ms)

    def _is_ws_recovered(self, now_ms: float) -> bool:
        if not self._ws_connected:
            return False
        if self._ws_connected_since_ms is None:
            return False
        if now_ms - self._ws_connected_since_ms < self.ws_min_connected_ms:
            return False
        return self._ws_good_streak >= self.ws_recovery_ticks

    def _evaluate_source(self, now_ms: float) -> tuple[str, str]:
        ws_fresh = self._is_ws_fresh(now_ms)
        http_fresh = self._is_http_fresh(now_ms)
        ws_age_ms = self._ws_age_ms(now_ms)
        if self.mode == MarketDataMode.WS_ONLY:
            return ("WS", "ws_only") if ws_fresh else ("NONE", "ws_only_stale")
        if self.mode == MarketDataMode.HTTP_ONLY:
            return ("HTTP", "http_only") if http_fresh else ("NONE", "http_only_stale")
        if self._last_effective_source == "WS":
            if ws_fresh:
                return "WS", "ws_fresh"
            if http_fresh and ws_age_ms > self.ws_fresh_ms + self.ws_grace_ms:
                return "HTTP", "ws_stale"
            if http_fresh:
                return "HTTP", "ws_stale"
            return "NONE", "ws_stale"
        if self._last_effective_source == "HTTP":
            if http_fresh and now_ms < self._http_hold_until_ms:
                return "HTTP", "hold"
            if ws_fresh and self._is_ws_recovered(now_ms):
                if now_ms - self._last_source_change_ms >= self.switch_cooldown_ms:
                    return "WS", "recovered"
            if http_fresh:
                return "HTTP", "http_fresh"
            return "NONE", "http_stale"
        if http_fresh:
            return "HTTP", "http_fresh"
        if ws_fresh and self._is_ws_recovered(now_ms):
            if now_ms - self._last_source_change_ms >= self.switch_cooldown_ms:
                return "WS", "recovered"
        return "NONE", "no_fresh"

    def _log_switch(
        self, from_source: str, to_source: str, reason: str, now_ms: float
    ) -> None:
        if from_source == to_source:
            return
        ws_age_ms = int(self._ws_age_ms(now_ms))
        http_age_ms = int(self._http_age_ms(now_ms))
        if to_source == "HTTP":
            hold_ms = int(self.http_hold_ms)
            self._logger.info(
                "[DATA] switch from=%s to=%s reason=%s http_age_ms=%s ws_age_ms=%s hold_ms=%s",
                from_source,
                to_source,
                reason,
                http_age_ms,
                ws_age_ms,
                hold_ms,
            )
        elif to_source == "WS":
            self._logger.info(
                "[DATA] switch from=%s to=%s reason=%s ws_good_streak=%s ws_age_ms=%s http_age_ms=%s",
                from_source,
                to_source,
                reason,
                self._ws_good_streak,
                ws_age_ms,
                http_age_ms,
            )
        else:
            self._logger.info(
                "[DATA] switch from=%s to=%s reason=%s ws_age_ms=%s http_age_ms=%s",
                from_source,
                to_source,
                reason,
                ws_age_ms,
                http_age_ms,
            )

    def _maybe_log_status(self, now_ms: float) -> None:
        if now_ms - self._last_status_log_ms < 5000:
            return
        self._last_status_log_ms = now_ms
        hold_left = max(0.0, self._http_hold_until_ms - now_ms)
        self._logger.info(
            "[DATA] status source=%s ws_age_ms=%s http_age_ms=%s ws_good_streak=%s hold_left_ms=%s",
            self._last_effective_source,
            int(self._ws_age_ms(now_ms)),
            int(self._http_age_ms(now_ms)),
            self._ws_good_streak,
            int(hold_left),
        )
