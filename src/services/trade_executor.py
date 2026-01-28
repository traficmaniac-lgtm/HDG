from __future__ import annotations

import math
import re
import time
from enum import Enum
from typing import Callable, Optional

import httpx

from src.core.models import Settings, SymbolProfile
from src.services.binance_rest import BinanceRestClient
from src.services.price_router import PriceRouter


class TradeState(Enum):
    STATE_IDLE = "IDLE"
    STATE_PLACE_BUY = "PLACE_BUY"
    STATE_WAIT_BUY = "WAIT_BUY"
    STATE_POSITION_OPEN = "POSITION_OPEN"
    STATE_PLACE_SELL = "PLACE_SELL"
    STATE_WAIT_SELL = "WAIT_SELL"
    STATE_CLOSED = "CLOSED"
    STATE_ERROR = "ERROR"


class TradeExecutor:
    TAG = "TEST_V0_5_9"

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
        self._error_dedup: dict[str, float] = {}
        self._last_no_price_log_ts = 0.0
        self._borrow_allowed_by_api: Optional[bool] = None
        self._borrow_hint_ts = 0.0
        self.position: Optional[dict] = None
        self.pnl_cycle: Optional[float] = None
        self.pnl_session: float = 0.0
        self._last_buy_price: Optional[float] = None
        self.state = TradeState.STATE_IDLE

    def get_state_label(self) -> str:
        return self.state.value

    def _transition_state(self, next_state: TradeState) -> None:
        if self.state == next_state:
            return
        previous = self.state
        self.state = next_state
        self._logger(f"[STATE] {previous.value} → {next_state.value}")

    def set_margin_capabilities(
        self, margin_api_access: bool, borrow_allowed_by_api: Optional[bool]
    ) -> None:
        self._borrow_allowed_by_api = borrow_allowed_by_api

    def place_test_orders_margin(self) -> int:
        if self._inflight_trade_action:
            self.last_action = "inflight"
            return 0
        self._inflight_trade_action = True
        try:
            if self.state != TradeState.STATE_IDLE:
                self._logger(
                    f"[TRADE] START ignored: state={self.state.value}"
                )
                self.last_action = "state_blocked"
                return 0
            if self._has_active_order("BUY") or self.position is not None:
                self._logger("[TRADE] place_test_orders | BUY already active")
                self.last_action = "buy_active"
                return 0
            price_state, health_state = self._router.build_price_state()
            mid = price_state.mid
            if mid is None:
                now = time.monotonic()
                if now - self._last_no_price_log_ts >= 2.0:
                    self._last_no_price_log_ts = now
                    self._logger(
                        "[TRADE] blocked: no_price "
                        f"(ws_age={health_state.ws_age_ms} http_age={health_state.http_age_ms})"
                    )
                self.last_action = "NO_PRICE"
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

            tick_offset = self._settings.offset_ticks
            buy_price = mid - tick_offset * self._profile.tick_size
            buy_price = self._round_to_step(buy_price, self._profile.tick_size)
            qty = self._round_down(
                self._settings.nominal_usd / mid, self._profile.step_size
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
                self._logger("[TRADE] place_test_orders | minNotional unknown")
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
            can_buy = True
            if quote_state["free"] < buy_cost:
                self._logger(f"[PRECHECK] insufficient {quote_asset} for BUY")
                can_buy = False

            if not can_buy:
                self.last_action = "precheck_failed"
                self.orders_count = 0
                return 0

            self._logger(
                "[TRADE] place_test_orders | "
                f"mode={self._settings.account_mode} "
                f"lev_hint={self._settings.leverage_hint} "
                f"buy={buy_price:.5f} qty={qty:.5f} "
                f"tag={self.TAG}"
            )

            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            buy_client_id = self._build_client_order_id("BUY", timestamp)
            order_type = self._normalize_order_type(self._settings.order_type)
            side_effect_type = self._normalize_side_effect_type(
                self._settings.side_effect_type
            )

            self._transition_state(TradeState.STATE_PLACE_BUY)
            self.last_action = "placing_buy"
            buy_order = self._place_margin_order(
                symbol=self._settings.symbol,
                side="BUY",
                quantity=qty,
                price=buy_price,
                is_isolated=is_isolated,
                client_order_id=buy_client_id,
                side_effect=side_effect_type,
                order_type=order_type,
            )
            if not buy_order:
                self.last_action = "place_failed"
                self._transition_state(TradeState.STATE_ERROR)
                self.orders_count = 0
                return 0
            self._logger(
                f"[ORDER] BUY placed qty={qty:.8f} price={buy_price:.8f} "
                f"id={buy_order.get('orderId')}"
            )
            self._transition_state(TradeState.STATE_WAIT_BUY)
            now_ms = int(time.time() * 1000)
            self.active_test_orders = [
                {
                    "orderId": buy_order.get("orderId"),
                    "side": "BUY",
                    "price": buy_price,
                    "qty": qty,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": buy_order.get("clientOrderId", buy_client_id),
                }
            ]
            self.orders_count = len(self.active_test_orders)
            self.pnl_cycle = None
            self._last_buy_price = buy_price
            return self.orders_count
        finally:
            self._inflight_trade_action = False

    def close_position(self) -> int:
        if self._inflight_trade_action:
            self.last_action = "inflight"
            return 0
        if self.state != TradeState.STATE_POSITION_OPEN:
            self._logger(
                f"[TRADE] close_position | blocked state={self.state.value}"
            )
            self.last_action = "state_blocked"
            return 0
        return self._place_sell_order()

    def _place_sell_order(self) -> int:
        if self._has_active_order("SELL"):
            self._logger("[TRADE] close_position | SELL already active")
            self.last_action = "sell_active"
            return 0
        if not self._has_active_order("BUY") and self.position is None:
            self._logger("[TRADE] close_position | no BUY to close")
            self.last_action = "no_buy"
            return 0
        self._inflight_trade_action = True
        try:
            price_state, health_state = self._router.build_price_state()
            mid = price_state.mid
            if mid is None:
                now = time.monotonic()
                if now - self._last_no_price_log_ts >= 2.0:
                    self._last_no_price_log_ts = now
                    self._logger(
                        "[TRADE] blocked: no_price "
                        f"(ws_age={health_state.ws_age_ms} http_age={health_state.http_age_ms})"
                    )
                self.last_action = "NO_PRICE"
                return 0
            if not self._profile.tick_size or not self._profile.step_size:
                self._logger(
                    f"[TRADE] close_position | missing tick/step sizes tag={self.TAG}"
                )
                return 0
            qty = self._resolve_close_qty()
            if qty <= 0:
                self._logger("[TRADE] close_position | qty <= 0")
                self.last_action = "precheck_failed"
                return 0

            tick_offset = self._settings.offset_ticks
            sell_price = mid + tick_offset * self._profile.tick_size
            sell_price = self._round_to_step(sell_price, self._profile.tick_size)

            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            sell_client_id = self._build_client_order_id("SELL", timestamp + 1)
            side_effect_type = (
                "AUTO_REPAY"
                if self._settings.allow_borrow and self._borrow_allowed_by_api is not False
                else None
            )
            order_type = "LIMIT"

            self._transition_state(TradeState.STATE_PLACE_SELL)
            self.last_action = "placing_sell"
            sell_order = self._place_margin_order(
                symbol=self._settings.symbol,
                side="SELL",
                quantity=qty,
                price=sell_price,
                is_isolated=is_isolated,
                client_order_id=sell_client_id,
                side_effect=side_effect_type,
                order_type=order_type,
            )
            if not sell_order:
                self._logger("[ORDER] SELL rejected")
                self.last_action = "place_failed"
                self._transition_state(TradeState.STATE_ERROR)
                return 0
            self._logger(
                f"[ORDER] SELL placed qty={qty:.8f} price={sell_price:.8f} "
                f"id={sell_order.get('orderId')}"
            )
            self._transition_state(TradeState.STATE_WAIT_SELL)
            now_ms = int(time.time() * 1000)
            self.active_test_orders.append(
                {
                    "orderId": sell_order.get("orderId"),
                    "side": "SELL",
                    "price": sell_price,
                    "qty": qty,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": sell_order.get("clientOrderId", sell_client_id),
                }
            )
            self.orders_count = len(self.active_test_orders)
            return 1
        finally:
            self._inflight_trade_action = False

    def sync_open_orders(self, open_orders: list[dict]) -> None:
        if not self.active_test_orders:
            self.orders_count = 0
            return
        open_map = {order.get("orderId"): order for order in open_orders}
        now_ms = int(time.time() * 1000)
        close_filled = False
        for order in self.active_test_orders:
            order_id = order.get("orderId")
            if order_id in open_map:
                status = str(open_map[order_id].get("status", "NEW")).upper()
                order["status"] = status
                order["updated_ts"] = now_ms
                continue
            if order.get("status") == "FILLED":
                continue
            order["status"] = "FILLED"
            order["updated_ts"] = now_ms
            side = order.get("side")
            if side == "BUY":
                self._logger(
                    f"[ORDER] BUY filled qty={order.get('qty'):.8f} "
                    f"price={order.get('price'):.8f}"
                )
                if self.position is None:
                    self.position = {
                        "buy_price": order.get("price"),
                        "qty": order.get("qty"),
                        "opened_ts": now_ms,
                    }
                self._transition_state(TradeState.STATE_POSITION_OPEN)
                if self._settings.auto_close:
                    self._place_sell_order()
            elif side == "SELL":
                self._logger(
                    f"[ORDER] SELL filled qty={order.get('qty'):.8f} "
                    f"price={order.get('price'):.8f}"
                )
                pnl = self._finalize_close(order)
                realized = "—" if pnl is None else f"{pnl:.8f}"
                self._logger(f"[PNL] realized={realized}")
                self._transition_state(TradeState.STATE_CLOSED)
                close_filled = True
        if close_filled:
            self.active_test_orders = []
            self.orders_count = 0
            self._transition_state(TradeState.STATE_IDLE)
        else:
            self.orders_count = len(self.active_test_orders)

    def get_orders_snapshot(self) -> list[dict]:
        return [dict(order) for order in self.active_test_orders]

    def get_unrealized_pnl(self, mid: Optional[float]) -> Optional[float]:
        if mid is None:
            return None
        if self.position is not None:
            return (mid - self.position.get("buy_price", mid)) * self.position.get(
                "qty", 0.0
            )
        buy_order = self._find_order("BUY")
        if buy_order and buy_order.get("status") == "NEW":
            return (mid - buy_order.get("price", mid)) * buy_order.get("qty", 0.0)
        return None

    def _finalize_close(self, sell_order: dict) -> Optional[float]:
        sell_price = sell_order.get("price")
        qty = sell_order.get("qty")
        buy_price = None
        if self.position is not None:
            buy_price = self.position.get("buy_price")
        if buy_price is None:
            buy_price = self._last_buy_price
        pnl = None
        if buy_price is not None and sell_price is not None and qty is not None:
            pnl = (sell_price - buy_price) * qty
        self.pnl_cycle = pnl
        if pnl is not None:
            self.pnl_session += pnl
        self.position = None
        self.last_action = "closed"
        return pnl

    def _resolve_close_qty(self) -> float:
        if self.position is not None:
            return self.position.get("qty", 0.0)
        buy_order = self._find_order("BUY")
        if buy_order:
            return buy_order.get("qty", 0.0)
        return 0.0

    def _find_order(self, side: str) -> Optional[dict]:
        for order in self.active_test_orders:
            if order.get("side") == side and order.get("status") != "FILLED":
                return order
        for order in self.active_test_orders:
            if order.get("side") == side:
                return order
        return None

    def _has_active_order(self, side: str) -> bool:
        return any(
            order.get("side") == side and order.get("status") != "FILLED"
            for order in self.active_test_orders
        )

    def abort_cycle(self) -> int:
        cancelled = self.cancel_test_orders_margin(reason="aborted")
        self._transition_state(TradeState.STATE_IDLE)
        self.last_action = "aborted"
        return cancelled

    def cancel_test_orders_margin(self, reason: str = "cancelled") -> int:
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
        self.last_action = reason
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
        order_type: str,
    ) -> Optional[dict]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": f"{quantity:.8f}",
            "isIsolated": is_isolated,
        }
        if order_type == "LIMIT":
            params["timeInForce"] = "GTC"
            params["price"] = f"{price:.8f}"
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

    def _log_binance_error(
        self, action: str, exc: httpx.HTTPStatusError, params: dict
    ) -> None:
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
        if action == "borrow" and (status == 401 or code == -1002):
            key = f"{action}:{code or status}:{path}"
            if self._should_dedup_log(key, 10.0):
                return
            self._borrow_allowed_by_api = False
            self._log_borrow_unavailable()
            return
        if code == -2014:
            self._logger(
                "[AUTH] invalid api key format (-2014). "
                "Check API settings in меню -> Настройки API."
            )
            return
        self._logger(
            f"[BINANCE_ERROR] action={action} http={status} code={code} msg={msg} path={path}"
        )

    def _should_dedup_log(self, key: str, window_s: float) -> bool:
        now = time.monotonic()
        last = self._error_dedup.get(key, 0.0)
        if now - last < window_s:
            return True
        self._error_dedup[key] = now
        return False

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

    @staticmethod
    def _normalize_order_type(value: str) -> str:
        upper = value.upper()
        if upper in {"LIMIT", "MARKET"}:
            return upper
        return "LIMIT"

    @staticmethod
    def _normalize_side_effect_type(value: str) -> Optional[str]:
        upper = value.upper()
        if upper == "NONE":
            return None
        if upper in {"AUTO_BORROW_REPAY", "MARGIN_BUY"}:
            return upper
        return "AUTO_BORROW_REPAY"

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

    def _log_borrow_unavailable(self) -> None:
        now = time.monotonic()
        if now - self._borrow_hint_ts < 10.0:
            return
        self._borrow_hint_ts = now
        self._logger(
            "BORROW недоступен для API-ключа. Проверь: включена маржинальная торговля для ключа, "
            "разрешение Spot&Margin, нет IP-ограничений, и аккаунт имеет доступ к Cross Margin Borrow."
        )

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
