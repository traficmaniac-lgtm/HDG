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
    finished = Signal()

    def __init__(self, symbol: str) -> None:
        super().__init__()
        self._symbol = symbol.lower()
        self._stop_event = threading.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False

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
                jitter = random.uniform(0.0, 0.2)
                self.log.emit(f"WS reconnect: {exc}")
                await asyncio.sleep(backoff + jitter)
                backoff = min(backoff * 2.0, 10.0)
            finally:
                self._ws = None
        self._set_connected(False)
        self.finished.emit()

    def _set_connected(self, value: bool) -> None:
        if self._connected != value:
            self._connected = value
            self.status.emit(value)
