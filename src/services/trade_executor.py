from __future__ import annotations

import math
import re
import time
from typing import Callable, Optional

import httpx

from src.core.models import Settings, SymbolProfile
from src.services.binance_rest import BinanceRestClient
from src.services.price_router import PriceRouter


class TradeExecutor:
    TAG = "TEST_V0_5_2"

    def __init__(
        self,
        rest: BinanceRestClient,
        router: PriceRouter,
        settings: Settings,
        profile: SymbolProfile,
        logger: Callable[[str], None],
    ) -> None:
        self._rest = rest
        self._router = router
        self._settings = settings
        self._profile = profile
        self._logger = logger
        self.active_test_orders: list[dict] = []
        self._client_tag = self._sanitize_client_order_id(self.TAG)

    def place_test_orders_margin(self) -> int:
        price_state, _ = self._router.build_price_state()
        mid = price_state.mid
        if mid is None:
            self._logger(
                f"[TRADE] place_test_orders | no mid price available tag={self.TAG}"
            )
            return 0
        if not self._profile.tick_size or not self._profile.step_size:
            self._logger(
                f"[TRADE] place_test_orders | missing tick/step sizes tag={self.TAG}"
            )
            return 0
        if not self._profile.min_qty:
            self._logger(
                f"[TRADE] place_test_orders | missing minQty tag={self.TAG}"
            )
            return 0

        tick_offset = self._settings.test_tick_offset
        buy_price = mid - tick_offset * self._profile.tick_size
        sell_price = mid + tick_offset * self._profile.tick_size
        buy_price = self._round_to_step(buy_price, self._profile.tick_size)
        sell_price = self._round_to_step(sell_price, self._profile.tick_size)
        qty = self._round_down(self._settings.test_notional_usd / mid, self._profile.step_size)

        if qty <= 0:
            self._logger(f"[TRADE] place_test_orders | qty <= 0 tag={self.TAG}")
            return 0
        if qty < self._profile.min_qty:
            self._logger(
                "[TRADE] place_test_orders | "
                f"qty below minQty ({qty} < {self._profile.min_qty}) tag={self.TAG}"
            )
            return 0

        notional = qty * mid
        if self._profile.min_notional is None:
            self._logger(
                f"[TRADE] place_test_orders | minNotional not available tag={self.TAG}"
            )
        elif notional < self._profile.min_notional:
            self._logger(
                "[TRADE] place_test_orders | "
                f"notional below minNotional ({notional} < {self._profile.min_notional}) tag={self.TAG}"
            )
            return 0

        self._logger(
            "[TRADE] place_test_orders | "
            f"mode={self._settings.account_mode} "
            f"lev_hint={self._settings.max_leverage_hint} "
            f"buy={buy_price:.5f} sell={sell_price:.5f} qty={qty:.5f} "
            f"tag={self.TAG}"
        )

        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        buy_side_effect = "MARGIN_BUY"
        sell_side_effect = "AUTO_BORROW_REPAY"
        timestamp = int(time.time() * 1000)
        buy_client_id = self._build_client_order_id("BUY", timestamp)
        sell_client_id = self._build_client_order_id("SELL", timestamp + 1)

        buy_order = self._place_margin_order(
            symbol=self._settings.symbol,
            side="BUY",
            quantity=qty,
            price=buy_price,
            is_isolated=is_isolated,
            client_order_id=buy_client_id,
            side_effect=buy_side_effect,
        )
        if not buy_order:
            return 0

        sell_order = self._place_margin_order(
            symbol=self._settings.symbol,
            side="SELL",
            quantity=qty,
            price=sell_price,
            is_isolated=is_isolated,
            client_order_id=sell_client_id,
            side_effect=sell_side_effect,
        )
        if not sell_order:
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=buy_order.get("orderId"),
                is_isolated=is_isolated,
                tag=self.TAG,
            )
            return 1

        now = time.time()
        self.active_test_orders = [
            {
                "orderId": buy_order.get("orderId"),
                "side": "BUY",
                "price": buy_price,
                "qty": qty,
                "created_ts": now,
                "tag": self.TAG,
            },
            {
                "orderId": sell_order.get("orderId"),
                "side": "SELL",
                "price": sell_price,
                "qty": qty,
                "created_ts": now,
                "tag": self.TAG,
            },
        ]
        return 2

    def cancel_test_orders_margin(self) -> int:
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        cancelled = 0
        for order in list(self.active_test_orders):
            order_id = order.get("orderId")
            if not order_id:
                continue
            if self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=order.get("tag", self.TAG),
            ):
                cancelled += 1

        cancelled += self._cancel_tagged_open_orders(is_isolated=is_isolated)

        self.active_test_orders = []
        self._logger(f"[TRADE] cancel_test_orders | cancelled={cancelled} tag={self.TAG}")
        return cancelled

    def _cancel_tagged_open_orders(self, is_isolated: str) -> int:
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("openOrders", exc, {"symbol": self._settings.symbol})
            return 0
        except Exception as exc:
            self._logger(f"[TRADE] openOrders error: {exc} tag={self.TAG}")
            return 0

        cancelled = 0
        for order in open_orders:
            client_order_id = order.get("clientOrderId", "")
            if not client_order_id.startswith(self._client_tag):
                continue
            order_id = order.get("orderId")
            if not order_id:
                continue
            if self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=self.TAG,
            ):
                cancelled += 1
        return cancelled

    def _place_margin_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        is_isolated: str,
        client_order_id: Optional[str],
        side_effect: str,
    ) -> Optional[dict]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": f"{quantity:.8f}",
            "price": f"{price:.8f}",
            "isIsolated": is_isolated,
            "sideEffectType": side_effect,
        }
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        try:
            return self._rest.create_margin_order(params)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("place_order", exc, params)
        except Exception as exc:
            self._logger(f"[TRADE] place_order error: {exc} tag={self.TAG}")
        return None

    def _cancel_margin_order(
        self,
        symbol: str,
        order_id: Optional[int],
        is_isolated: str,
        tag: str,
    ) -> bool:
        if not order_id:
            return False
        params = {
            "symbol": symbol,
            "orderId": order_id,
            "isIsolated": is_isolated,
        }
        try:
            self._rest.cancel_margin_order(params)
            return True
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("cancel_order", exc, params)
        except Exception as exc:
            self._logger(f"[TRADE] cancel_order error: {exc} tag={tag}")
        return False

    def _log_binance_error(self, action: str, exc: httpx.HTTPStatusError, params: dict) -> None:
        status = exc.response.status_code if exc.response else "?"
        code = None
        msg = None
        try:
            payload = exc.response.json() if exc.response else {}
            code = payload.get("code")
            msg = payload.get("msg")
        except Exception:
            pass
        if code == -2014:
            self._logger(
                "[AUTH] invalid api key format (-2014). "
                "Check API settings in меню -> Настройки API."
            )
            return
        self._logger(
            "[TRADE] binance_error | "
            f"action={action} status={status} code={code} msg={msg} params={params} tag={self.TAG}"
        )

    def _build_client_order_id(self, side: str, timestamp: int) -> str:
        raw = f"{self._client_tag}_{side}_{timestamp}"
        sanitized = self._sanitize_client_order_id(raw)
        if len(sanitized) > 36:
            sanitized = sanitized[-36:]
        return sanitized

    @staticmethod
    def _sanitize_client_order_id(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9-_]", "_", value)

    @staticmethod
    def _round_down(value: float, step: float) -> float:
        return math.floor(value / step) * step

    @staticmethod
    def _round_to_step(value: float, step: float) -> float:
        return round(value / step) * step
