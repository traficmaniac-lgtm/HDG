from __future__ import annotations

import logging
import time
from typing import Any, Optional

from src.services.binance_rest import BinanceRestClient


class BinanceMarginExecution:
    _VALID_SIDE_EFFECT_TYPES = {
        "NO_SIDE_EFFECT",
        "MARGIN_BUY",
        "AUTO_REPAY",
        "AUTO_BORROW_REPAY",
    }

    def __init__(self, client: BinanceRestClient, symbol: str = "BTCUSDT") -> None:
        self._client = client
        self.symbol = symbol
        self._logger = logging.getLogger("dhs")
        self._side_effect_warned = False

    @property
    def last_error(self) -> Optional[str]:
        return self._client.last_error

    @property
    def last_error_code(self) -> Optional[int]:
        return self._client.last_error_code

    def get_margin_account(self) -> Optional[dict[str, Any]]:
        return self._client.get_margin_account()

    def get_spot_account(self) -> Optional[dict[str, Any]]:
        return self._client.get_spot_account()

    def get_open_orders(self) -> list[dict[str, Any]]:
        return self._client.get_open_margin_orders(self.symbol) or []

    def cancel_open_orders(self) -> None:
        self._client.cancel_open_orders(self.symbol)

    def cancel_order(self, order_id: int) -> Optional[dict[str, Any]]:
        return self._client.cancel_margin_order(self.symbol, order_id)

    def get_order(self, order_id: int) -> Optional[dict[str, Any]]:
        return self._client.get_order(self.symbol, order_id)

    def repay_asset(self, asset: str, amount: float) -> Optional[dict[str, Any]]:
        return self._client.repay_margin_asset(asset, amount)

    def place_order(
        self,
        side: str,
        quantity: float,
        order_mode: str,
        price: Optional[float] = None,
        side_effect_type: Optional[str] = None,
        time_in_force: Optional[str] = None,
        is_isolated: bool = False,
    ) -> Optional[dict[str, Any]]:
        resolved_side_effect = side_effect_type
        if resolved_side_effect not in self._VALID_SIDE_EFFECT_TYPES:
            if not self._side_effect_warned:
                self._logger.warning(
                    "MARGIN_ORDER sideEffectType invalid or missing; defaulting",
                    extra={
                        "sideEffectType": resolved_side_effect,
                        "default": "AUTO_BORROW_REPAY",
                    },
                )
                self._side_effect_warned = True
            resolved_side_effect = "AUTO_BORROW_REPAY"
        payload: dict[str, Any] = {
            "symbol": self.symbol,
            "side": side,
            "quantity": f"{quantity:.6f}",
            "isIsolated": "TRUE" if is_isolated else "FALSE",
            "sideEffectType": resolved_side_effect,
        }
        if order_mode == "aggressive_limit":
            if price is None:
                raise ValueError("price required for aggressive_limit")
            payload["type"] = "LIMIT"
            payload["price"] = f"{price:.8f}"
            payload["timeInForce"] = time_in_force or "IOC"
        else:
            payload["type"] = "MARKET"
        self._logger.info(
            "MARGIN_ORDER_REQUEST symbol=%s side=%s type=%s qty=%s price=%s sideEffectType=%s isIsolated=%s leverage=%s clientOrderId=%s",
            payload.get("symbol"),
            payload.get("side"),
            payload.get("type"),
            payload.get("quantity"),
            payload.get("price"),
            payload.get("sideEffectType"),
            payload.get("isIsolated"),
            payload.get("leverage"),
            payload.get("newClientOrderId"),
        )
        return self._client.place_order_margin(payload)

    def wait_for_fill(
        self, order_id: Optional[int], timeout_s: float = 3.0
    ) -> Optional[tuple[float, float, dict[str, Any]]]:
        if not order_id:
            return None
        start = time.monotonic()
        while time.monotonic() - start < timeout_s:
            order = self.get_order(int(order_id))
            if order and order.get("status") == "FILLED":
                executed_qty = float(order.get("executedQty", 0.0) or 0.0)
                cumm_quote = float(order.get("cummulativeQuoteQty", 0.0) or 0.0)
                if executed_qty > 0 and cumm_quote > 0:
                    avg_price = cumm_quote / executed_qty
                else:
                    avg_price = float(order.get("price", 0.0) or 0.0)
                return avg_price, executed_qty, order
            if order and order.get("status") in {"CANCELED", "REJECTED", "EXPIRED"}:
                return None
            time.sleep(0.1)
        return None
