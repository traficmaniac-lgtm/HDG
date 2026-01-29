from __future__ import annotations

import time
from typing import Callable

from PySide6.QtCore import QObject, QTimer, Signal, Slot

from src.services.binance_rest import BinanceRestClient


class OrderTracker(QObject):
    order_filled = Signal(int, str, float, float, "qint64")
    order_partial = Signal(int, str, float, float, "qint64")
    order_done = Signal(int, str, "qint64")

    def __init__(
        self,
        rest: BinanceRestClient,
        symbol: str,
        poll_ms: int,
        logger: Callable[[str], None],
        owns_rest: bool = False,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._rest = rest
        self._symbol = symbol
        self._logger = logger
        self._poll_ms = poll_ms
        self._owns_rest = owns_rest
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._poll_orders)
        self._active_orders: dict[int, dict] = {}
        self._last_error_ts = 0.0

    @Slot()
    def start(self) -> None:
        self._timer.start(self._poll_ms)

    @Slot()
    def stop(self) -> None:
        self._timer.stop()
        self._active_orders.clear()
        if self._owns_rest:
            self._rest.close()

    @Slot(list)
    def sync_active_orders(self, orders: list[dict]) -> None:
        active_ids: set[int] = set()
        for order in orders:
            order_id = order.get("orderId")
            if not isinstance(order_id, int):
                continue
            active_ids.add(order_id)
            state = self._active_orders.get(order_id)
            if state is None:
                self._active_orders[order_id] = {
                    "side": str(order.get("side", "UNKNOWN")).upper(),
                    "price": float(order.get("price", 0.0) or 0.0),
                    "qty": float(order.get("qty", 0.0) or 0.0),
                    "status": str(order.get("status", "NEW")).upper(),
                    "cum_qty": float(order.get("executedQty", 0.0) or 0.0),
                    "avg_fill_price": 0.0,
                    "last_status": str(order.get("status", "NEW")).upper(),
                }
            else:
                state["side"] = str(order.get("side", state["side"])).upper()
                state["price"] = float(order.get("price", state["price"]) or state["price"])
                state["qty"] = float(order.get("qty", state["qty"]) or state["qty"])
        stale_ids = [order_id for order_id in self._active_orders if order_id not in active_ids]
        for order_id in stale_ids:
            self._active_orders.pop(order_id, None)

    @Slot(str, str)
    def update_api_credentials(self, api_key: str, api_secret: str) -> None:
        self._rest.api_key = api_key
        self._rest.api_secret = api_secret

    def _poll_orders(self) -> None:
        if not self._active_orders:
            return
        for order_id, entry in list(self._active_orders.items()):
            try:
                payload = self._rest.get_margin_order(self._symbol, order_id)
            except Exception as exc:
                now = time.monotonic()
                if now - self._last_error_ts >= 5.0:
                    self._last_error_ts = now
                    self._logger(f"[ORDER_TRACKER] poll error: {exc}")
                continue
            status = str(payload.get("status", entry.get("status", "NEW"))).upper()
            prev_cum_qty = float(entry.get("cum_qty", 0.0) or 0.0)
            executed_qty = float(payload.get("executedQty", prev_cum_qty) or prev_cum_qty)
            cumulative_quote = payload.get("cumulativeQuoteQty", payload.get("cummulativeQuoteQty"))
            cumulative_quote_qty = (
                float(cumulative_quote)
                if cumulative_quote is not None
                else float(payload.get("cumulativeQuoteQty", 0.0) or 0.0)
            )
            avg_fill_price = float(entry.get("avg_fill_price", 0.0) or 0.0)
            avg_price_payload = float(payload.get("avgPrice", 0.0) or 0.0)
            if executed_qty > 0:
                if cumulative_quote_qty > 0:
                    avg_fill_price = cumulative_quote_qty / executed_qty
                elif avg_price_payload > 0:
                    avg_fill_price = avg_price_payload
            entry["cum_qty"] = executed_qty
            entry["avg_fill_price"] = avg_fill_price
            entry["last_status"] = status
            if status != entry.get("status"):
                entry["status"] = status
                self._logger(
                    f"[ORDER_TRACKER] status change id={order_id} "
                    f"side={entry.get('side')} status={status}"
                )
            if status == "PARTIALLY_FILLED":
                if executed_qty > 0 and (executed_qty != prev_cum_qty or avg_fill_price):
                    ts_ms = int(time.time() * 1000)
                    self.order_partial.emit(
                        order_id,
                        entry.get("side", "UNKNOWN"),
                        executed_qty,
                        avg_fill_price,
                        ts_ms,
                    )
            if status == "FILLED":
                ts_ms = int(time.time() * 1000)
                fill_qty = executed_qty if executed_qty > 0 else float(entry.get("qty", 0.0))
                fill_price = avg_fill_price if avg_fill_price > 0 else float(entry.get("price", 0.0))
                self.order_filled.emit(
                    order_id,
                    entry.get("side", "UNKNOWN"),
                    fill_price,
                    fill_qty,
                    ts_ms,
                )
                self._active_orders.pop(order_id, None)
            elif status in {"CANCELED", "REJECTED", "EXPIRED"}:
                ts_ms = int(time.time() * 1000)
                self.order_done.emit(order_id, status, ts_ms)
                self._active_orders.pop(order_id, None)
