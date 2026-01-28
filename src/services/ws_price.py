from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from typing import Optional

import websockets
from PySide6.QtCore import QObject, Signal, Slot


class WsPriceWorker(QObject):
    tick = Signal(float, float)
    status = Signal(bool)
    log = Signal(str)
    issue = Signal(str)
    finished = Signal()

    def __init__(self, symbol: str, reconnect_dedup_ms: int = 10000) -> None:
        super().__init__()
        self._symbol = symbol.lower()
        self._stop_event = threading.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._reconnect_dedup_s = reconnect_dedup_ms / 1000.0
        self._reconnect_cooldown_s = 2.0
        self._last_reconnect_ts = 0.0
        self._reconnect_log_ts: dict[str, float] = {}

    @Slot()
    def run(self) -> None:
        asyncio.run(self._run())

    def stop(self) -> None:
        self._stop_event.set()
        if self._loop:
            self._loop.call_soon_threadsafe(self._request_close)

    def _request_close(self) -> None:
        if self._ws:
            asyncio.create_task(self._ws.close())

    async def _run(self) -> None:
        self._loop = asyncio.get_running_loop()
        backoff = 0.5
        while not self._stop_event.is_set():
            url = f"wss://stream.binance.com:9443/ws/{self._symbol}@bookTicker"
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self._ws = ws
                    self._set_connected(True)
                    backoff = 0.5
                    last_tick = None
                    first_deadline = time.monotonic() + 3.0
                    while not self._stop_event.is_set():
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            now = time.monotonic()
                            if last_tick is None and now > first_deadline:
                                raise TimeoutError("no first tick")
                            if last_tick is not None and (now - last_tick) > 2.0:
                                raise TimeoutError("stale ticks")
                            continue
                        payload = json.loads(message)
                        bid = float(payload.get("b", 0.0))
                        ask = float(payload.get("a", 0.0))
                        last_tick = time.monotonic()
                        self.tick.emit(bid, ask)
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                self._set_connected(False)
                now = time.monotonic()
                reason = str(exc) or "unknown"
                if reason in {"no first tick", "stale ticks"}:
                    self.issue.emit(reason)
                last_log_ts = self._reconnect_log_ts.get(reason, 0.0)
                if now - last_log_ts >= self._reconnect_dedup_s:
                    self._reconnect_log_ts[reason] = now
                    self.log.emit(f"WS reconnect: {reason}")
                jitter = random.uniform(0.0, 0.2)
                cooldown = max(0.0, self._reconnect_cooldown_s - (now - self._last_reconnect_ts))
                await asyncio.sleep(max(backoff + jitter, cooldown))
                self._last_reconnect_ts = time.monotonic()
                backoff = min(backoff * 2.0, 10.0)
            finally:
                self._ws = None
        self._set_connected(False)
        self.finished.emit()

    def _set_connected(self, value: bool) -> None:
        if self._connected != value:
            self._connected = value
            self.status.emit(value)
