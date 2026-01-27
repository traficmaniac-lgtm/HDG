from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timezone
from typing import Optional

import websockets
from PySide6.QtCore import QThread, Signal


class MarketDataThread(QThread):
    price_update = Signal(dict)
    depth_update = Signal(dict)
    status_update = Signal(str)

    def __init__(self, symbol: str = "btcusdt") -> None:
        super().__init__()
        self.symbol = symbol.lower()
        self._stop_event = threading.Event()
        self._status = "DISCONNECTED"

    def run(self) -> None:
        asyncio.run(self._runner())

    def stop(self) -> None:
        self._stop_event.set()

    def is_stopped(self) -> bool:
        return self._stop_event.is_set()

    async def _runner(self) -> None:
        stream = f"{self.symbol}@bookTicker/{self.symbol}@depth10@100ms"
        url = f"wss://stream.binance.com:9443/stream?streams={stream}"
        backoff = 1
        while not self._stop_event.is_set():
            try:
                self._emit_status("CONNECTING")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self._emit_status("CONNECTED")
                    backoff = 1
                    async for message in ws:
                        if self._stop_event.is_set():
                            break
                        payload = json.loads(message)
                        data = payload.get("data", payload)
                        stream_name = payload.get("stream", "")
                        if "bookTicker" in stream_name or data.get("e") == "bookTicker":
                            self._handle_book_ticker(data)
                        elif "depth" in stream_name or data.get("e") == "depthUpdate":
                            self._handle_depth_update(data)
            except Exception:
                if self._stop_event.is_set():
                    break
                self._emit_status("DEGRADED")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5)

        self._emit_status("DISCONNECTED")

    def _handle_book_ticker(self, data: dict) -> None:
        bid = float(data.get("b", 0.0))
        ask = float(data.get("a", 0.0))
        if bid <= 0 or ask <= 0:
            return
        mid = (bid + ask) / 2
        spread_bps = (ask / bid - 1) * 10_000
        now = datetime.now(timezone.utc)
        event_ts = data.get("E")
        event_time = (
            datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc) if event_ts else now
        )
        self.price_update.emit(
            {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_bps": spread_bps,
                "event_time": event_time,
                "rx_time": now,
                "source": "WS",
            }
        )

    def _handle_depth_update(self, data: dict) -> None:
        bids = data.get("b") or data.get("bids") or []
        asks = data.get("a") or data.get("asks") or []
        now = datetime.now(timezone.utc)
        self.depth_update.emit(
            {
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
