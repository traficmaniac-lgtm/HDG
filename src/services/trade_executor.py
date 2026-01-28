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
    TAG = "TEST_V0_5_7"

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
        self.borrowed_assets: dict[str, float] = {}
        self.last_action = "idle"
        self.orders_count = 0
        self._inflight_trade_action = False

    def place_test_orders_margin(self) -> int:
        if self._inflight_trade_action:
            self.last_action = "inflight"
            return 0
        self._inflight_trade_action = True
        try:
            price_state, health_state = self._router.build_price_state()
            mid = price_state.mid
            if mid is None:
                self._logger(
                    "[TRADE] blocked: no_price "
                    f"(ws_age={health_state.ws_age_ms} http_age={health_state.http_age_ms})"
                )
                self.last_action = "no_price"
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
            qty = self._round_down(
                self._settings.test_notional_usd / mid, self._profile.step_size
            )

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
                    f"notional below minNotional ({notional} < {self._profile.min_notional}) "
                    f"tag={self.TAG}"
                )
                return 0

            base_asset, quote_asset = self._split_symbol(self._settings.symbol)
            base_state = self._get_margin_asset(base_asset)
            quote_state = self._get_margin_asset(quote_asset)
            if base_state is None or quote_state is None:
                self._logger(
                    f"[PRECHECK] margin asset info unavailable symbol={self._settings.symbol}"
                )
                self.last_action = "precheck_failed"
                self.orders_count = 0
                return 0

            buy_cost = qty * buy_price
            if quote_state["free"] < buy_cost:
                self._logger(
                    f"[PRECHECK] insufficient {quote_asset} for BUY"
                )
                self.last_action = "precheck_failed"
                self.orders_count = 0
                return 0

            borrowed_amount = 0.0
            if base_state["free"] < qty:
                need = qty - base_state["free"]
                borrowed_amount = self._round_up(need, self._profile.step_size)
                if borrowed_amount <= 0:
                    self._logger(
                        f"[PRECHECK] insufficient {base_asset} for SELL"
                    )
                    self.last_action = "precheck_failed"
                    self.orders_count = 0
                    return 0
                if not self._borrow_margin_asset(base_asset, borrowed_amount):
                    self.last_action = "borrow_failed"
                    self.orders_count = 0
                    return 0
                self.borrowed_assets[base_asset] = (
                    self.borrowed_assets.get(base_asset, 0.0) + borrowed_amount
                )
                self._logger(
                    f"[BORROW] asset={base_asset} amount={borrowed_amount:.8f} ok"
                )

            self._logger(
                "[TRADE] place_test_orders | "
                f"mode={self._settings.account_mode} "
                f"lev_hint={self._settings.max_leverage_hint} "
                f"buy={buy_price:.5f} sell={sell_price:.5f} qty={qty:.5f} "
                f"tag={self.TAG}"
            )

            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
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
                side_effect=None,
            )
            if not buy_order:
                self._rollback_after_failure(
                    base_asset=base_asset,
                    borrowed_amount=borrowed_amount,
                    reason="buy_failed",
                )
                self.last_action = "place_failed"
                self.orders_count = 0
                return 0
            self._logger(
                f"[ORDER] placed side=BUY qty={qty:.8f} price={buy_price:.8f} "
                f"id={buy_order.get('orderId')}"
            )

            sell_order = self._place_margin_order(
                symbol=self._settings.symbol,
                side="SELL",
                quantity=qty,
                price=sell_price,
                is_isolated=is_isolated,
                client_order_id=sell_client_id,
                side_effect=None,
            )
            if not sell_order:
                self._cancel_margin_order(
                    symbol=self._settings.symbol,
                    order_id=buy_order.get("orderId"),
                    is_isolated=is_isolated,
                    tag=self.TAG,
                )
                self._rollback_after_failure(
                    base_asset=base_asset,
                    borrowed_amount=borrowed_amount,
                    reason="sell_failed",
                )
                self.last_action = "place_failed"
                self.orders_count = 0
                return 0
            self._logger(
                f"[ORDER] placed side=SELL qty={qty:.8f} price={sell_price:.8f} "
                f"id={sell_order.get('orderId')}"
            )

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
            self.last_action = "placed_test_orders"
            self.orders_count = 2
            return 2
        finally:
            self._inflight_trade_action = False

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
        self._repay_borrowed_assets()
        self.last_action = "cancelled"
        self.orders_count = 0
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
        side_effect: Optional[str],
    ) -> Optional[dict]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": f"{quantity:.8f}",
            "price": f"{price:.8f}",
            "isIsolated": is_isolated,
        }
        if side_effect:
            params["sideEffectType"] = side_effect
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
        path = "?"
        try:
            payload = exc.response.json() if exc.response else {}
            code = payload.get("code")
            msg = payload.get("msg")
        except Exception:
            pass
        if exc.request and exc.request.url:
            path = exc.request.url.path
        if code == -2014:
            self._logger(
                "[AUTH] invalid api key format (-2014). "
                "Check API settings in меню -> Настройки API."
            )
            return
        self._logger(
            f"[BINANCE_ERROR] action={action} http={status} code={code} msg={msg} path={path}"
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

    @staticmethod
    def _round_up(value: float, step: float) -> float:
        return math.ceil(value / step) * step

    @staticmethod
    def _split_symbol(symbol: str) -> tuple[str, str]:
        if symbol.endswith("USDT"):
            return symbol[:-4], "USDT"
        if symbol.endswith("BUSD"):
            return symbol[:-4], "BUSD"
        if symbol.endswith("USDC"):
            return symbol[:-4], "USDC"
        return symbol[:-3], symbol[-3:]

    def _get_margin_asset(self, asset: str) -> Optional[dict]:
        try:
            account = self._rest.get_margin_account()
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("margin_account", exc, {"asset": asset})
            return None
        except Exception as exc:
            self._logger(f"[TRADE] margin_account error: {exc} tag={self.TAG}")
            return None
        assets = account.get("userAssets", [])
        for entry in assets:
            if entry.get("asset") == asset:
                return {
                    "free": self._safe_float(entry.get("free")),
                    "borrowed": self._safe_float(entry.get("borrowed")),
                    "interest": self._safe_float(entry.get("interest")),
                    "netAsset": self._safe_float(entry.get("netAsset")),
                }
        return None

    def _borrow_margin_asset(self, asset: str, amount: float) -> bool:
        params = {"asset": asset, "amount": f"{amount:.8f}"}
        try:
            self._rest.borrow_margin_asset(params)
            return True
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("borrow", exc, {"asset": asset})
        except Exception as exc:
            self._logger(f"[TRADE] borrow error: {exc} tag={self.TAG}")
        return False

    def _repay_margin_asset(self, asset: str, amount: float) -> bool:
        params = {"asset": asset, "amount": f"{amount:.8f}"}
        try:
            self._rest.repay_margin_asset(params)
            return True
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("repay", exc, {"asset": asset})
        except Exception as exc:
            self._logger(f"[TRADE] repay error: {exc} tag={self.TAG}")
        return False

    def _rollback_after_failure(
        self,
        base_asset: str,
        borrowed_amount: float,
        reason: str,
    ) -> None:
        if borrowed_amount > 0:
            self._repay_after_borrow(base_asset, borrowed_amount)
        self._logger(f"[ROLLBACK] reason={reason}")

    def _repay_after_borrow(self, asset: str, borrowed_amount: float) -> None:
        asset_state = self._get_margin_asset(asset)
        free_amount = asset_state["free"] if asset_state else 0.0
        repay_amount = min(borrowed_amount, free_amount)
        if repay_amount <= 0:
            self._logger(f"[REPAY] asset={asset} amount=0.00000000")
            return
        success = self._repay_margin_asset(asset, repay_amount)
        if success:
            self.borrowed_assets[asset] = max(
                0.0, self.borrowed_assets.get(asset, 0.0) - repay_amount
            )
        status = "ok" if success else "failed"
        self._logger(f"[REPAY] asset={asset} amount={repay_amount:.8f} {status}")

    def _repay_borrowed_assets(self) -> None:
        for asset, amount in list(self.borrowed_assets.items()):
            if amount <= 0:
                continue
            self._repay_after_borrow(asset, amount)

    @staticmethod
    def _safe_float(value: Optional[str]) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
