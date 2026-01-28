from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
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
        self._stop_event = threading.Event()
        self._status = "DISCONNECTED"
        self._market_data = market_data

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
        self.price_update.emit(
            {
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
        )
        if self._market_data:
            self._market_data.update_tick(
                {
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
            )

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
