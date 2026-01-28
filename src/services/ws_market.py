from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional

import websockets
from PySide6.QtCore import QThread, Signal

from src.services.market_data import MarketDataService


class MarketDataThread(QThread):
    price_update = Signal(dict)
    depth_update = Signal(dict)
    status_update = Signal(str)

    def __init__(self, symbol: str, market_data: MarketDataService | None = None) -> None:
        super().__init__()
        self.symbol = symbol.lower()
        self.symbol_upper = symbol.upper()
        self._symbols = {self.symbol}
        self._symbols_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._status = "DISCONNECTED"
        self._market_data = market_data
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._reconnect_event: Optional[asyncio.Event] = None
        self._stop_async_event: Optional[asyncio.Event] = None
        self._reconnect_reason: Optional[str] = None
        self._logger = logging.getLogger("dhs")
        self._first_tick_deadline_ms = 2500
        self._stale_tick_ms = 2000
        self._backoff_steps = [0.5, 1.0, 2.0, 4.0, 8.0, 10.0]

    def run(self) -> None:
        asyncio.run(self._runner())

    def stop(self) -> None:
        self._stop_event.set()
        if self._loop and not self._loop.is_closed() and self._stop_async_event:
            try:
                self._loop.call_soon_threadsafe(self._stop_async_event.set)
            except RuntimeError:
                pass

    def is_stopped(self) -> bool:
        return self._stop_event.is_set()

    def set_symbols(self, symbols: Iterable[str]) -> None:
        if self._stop_event.is_set():
            return
        normalized = {symbol.lower() for symbol in symbols if symbol}
        if not normalized:
            return
        with self._symbols_lock:
            if normalized == self._symbols:
                return
            self._symbols = normalized
            first = sorted(self._symbols)[0]
            self.symbol = first
            self.symbol_upper = first.upper()
        self._request_reconnect("symbols_changed")

    def set_symbol(self, symbol: str) -> None:
        self.set_symbols([symbol])

    async def _runner(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._reconnect_event = asyncio.Event()
        self._stop_async_event = asyncio.Event()
        backoff_idx = 0
        while not self._stop_event.is_set() and not self._stop_async_event.is_set():
            url = self._build_stream_url()
            self._emit_status("CONNECTING")
            reconnect_reason = None
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self._emit_status("CONNECTED")
                    if self._market_data:
                        self._market_data.set_ws_connected(True)
                    backoff_idx = 0
                    reconnect_reason = await self._consume_messages(ws)
            except asyncio.CancelledError:
                reconnect_reason = "exception"
            except Exception:
                reconnect_reason = "exception"
            finally:
                if self._market_data:
                    self._market_data.set_ws_connected(False)
            if self._stop_event.is_set() or self._stop_async_event.is_set():
                break
            self._emit_status("DEGRADED")
            if reconnect_reason:
                self._logger.info("[DATA] ws_reconnect reason=%s", reconnect_reason)
            await asyncio.sleep(self._next_backoff(backoff_idx))
            backoff_idx = min(backoff_idx + 1, len(self._backoff_steps) - 1)

        self._emit_status("DISCONNECTED")

    async def _consume_messages(self, ws: websockets.WebSocketClientProtocol) -> str:
        tick_times = deque()
        last_tick_ms: Optional[float] = None
        first_tick_deadline = time.monotonic() * 1000 + self._first_tick_deadline_ms
        while not self._stop_event.is_set():
            if self._stop_async_event and self._stop_async_event.is_set():
                return "socket_closed"
            if self._reconnect_event and self._reconnect_event.is_set():
                self._reconnect_event.clear()
                return self._reconnect_reason or "manual"
            now_ms = time.monotonic() * 1000
            if last_tick_ms is None and now_ms > first_tick_deadline:
                return "no_first_tick"
            if last_tick_ms is not None:
                while tick_times and tick_times[0] < now_ms - 1000.0:
                    tick_times.popleft()
                if now_ms - last_tick_ms > self._stale_tick_ms and len(tick_times) == 0:
                    return "stale_ticks"
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed:
                return "socket_closed"
            payload = json.loads(message)
            data = payload.get("data", payload)
            stream_name = payload.get("stream", "")
            if "bookTicker" in stream_name or data.get("e") == "bookTicker":
                last_tick_ms = time.monotonic() * 1000
                tick_times.append(last_tick_ms)
                self._handle_book_ticker(data)
            elif "depth" in stream_name or data.get("e") == "depthUpdate":
                self._handle_depth_update(data)
        return "socket_closed"

    def _request_reconnect(self, reason: str) -> None:
        self._reconnect_reason = reason
        if self._loop and not self._loop.is_closed() and self._reconnect_event:
            try:
                self._loop.call_soon_threadsafe(self._reconnect_event.set)
            except RuntimeError:
                pass

    def _next_backoff(self, idx: int) -> float:
        base = self._backoff_steps[idx]
        jitter = random.uniform(-0.1, 0.1)
        return max(0.1, base * (1 + jitter))

    def _build_stream_url(self) -> str:
        with self._symbols_lock:
            symbols = sorted(self._symbols) if self._symbols else [self.symbol]
        stream_parts = []
        for symbol in symbols:
            stream_parts.append(f"{symbol}@bookTicker")
            stream_parts.append(f"{symbol}@depth10@100ms")
        stream = "/".join(stream_parts)
        return f"wss://stream.binance.com:9443/stream?streams={stream}"

    def _handle_book_ticker(self, data: dict) -> None:
        try:
            bid_raw = Decimal(str(data.get("b", "0")))
            ask_raw = Decimal(str(data.get("a", "0")))
        except (InvalidOperation, TypeError):
            return
        if bid_raw <= 0 or ask_raw <= 0:
            return
        mid_raw = (bid_raw + ask_raw) / Decimal("2")
        if mid_raw <= 0:
            return
        spread_bps_raw = (ask_raw - bid_raw) / mid_raw * Decimal("10000")
        bid = float(bid_raw)
        ask = float(ask_raw)
        mid = float(mid_raw)
        spread_bps = float(spread_bps_raw)
        now = datetime.now(timezone.utc)
        rx_time_ms = time.monotonic() * 1000
        event_ts = data.get("E")
        event_time = (
            datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc) if event_ts else now
        )
        payload = {
            "symbol": self.symbol_upper,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread_bps": spread_bps,
            "bid_raw": bid_raw,
            "ask_raw": ask_raw,
            "mid_raw": mid_raw,
            "spread_bps_raw": spread_bps_raw,
            "event_time": event_time,
            "rx_time": now,
            "rx_time_ms": rx_time_ms,
            "source": "WS",
        }
        self.price_update.emit(payload)
        if self._market_data:
            self._market_data.update_tick(payload)

    def _handle_depth_update(self, data: dict) -> None:
        bids = data.get("b") or data.get("bids") or []
        asks = data.get("a") or data.get("asks") or []
        now = datetime.now(timezone.utc)
        self.depth_update.emit(
            {
                "symbol": self.symbol_upper,
                "bids": bids,
                "asks": asks,
                "event_time": now,
                "rx_time": now,
            }
        )

    def _emit_status(self, status: str) -> None:
        if self._status != status:
            self._status = status
            self.status_update.emit(status)
