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
    TAG = "PRE_V0_8_0"

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
        self.last_exit_reason: Optional[str] = None
        self._last_buy_price: Optional[float] = None
        self._buy_wait_started_ts: Optional[float] = None
        self._buy_retry_count = 0
        self._sell_wait_started_ts: Optional[float] = None
        self._sell_retry_count = 0
        self.sell_active_order_id: Optional[int] = None
        self.sell_cancel_pending = False
        self.sell_place_inflight = False
        self.sell_last_attempt_ts = 0.0
        self.sell_backoff_ms = 500
        self._pending_sell_retry_reason: Optional[str] = None
        self._aggregate_sold_qty = 0.0
        self.position_qty_base: Optional[float] = None
        self.exit_intent: Optional[str] = None
        self.exit_intent_set_ts: Optional[float] = None
        self._last_place_error_code: Optional[int] = None
        self.state = TradeState.STATE_IDLE
        self.cycles_target = self._normalize_cycle_target(
            getattr(settings, "cycle_count", 1)
        )
        self.cycles_done = 0
        self.run_active = False
        self._next_cycle_ready_ts: Optional[float] = None
        self._cycle_cooldown_s = 0.3
        self._exits_frozen = False
        self._entry_price_wait_started_ts: Optional[float] = None
        self._exit_price_wait_started_ts: Optional[float] = None

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
            bid = price_state.bid
            if price_state.data_blind:
                self._logger("[TRADE] blocked: data_blind")
                self.last_action = "DATA_BLIND"
                return 0
            if mid is None or bid is None:
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

            tick_offset = max(0, int(self._settings.entry_offset_ticks))
            buy_price = bid + tick_offset * self._profile.tick_size
            buy_price = self._round_to_step(buy_price, self._profile.tick_size)
            order_quote = float(self._settings.order_quote)
            max_budget = float(self._settings.max_budget)
            budget_reserve = float(self._settings.budget_reserve)
            free_quote = float(quote_state["free"])
            self._logger(f"[ORDER_UNIT] quote={order_quote:.8f}")
            self._logger(
                f"[BUDGET_CHECK] free={free_quote:.8f} max={max_budget:.8f} "
                f"reserve={budget_reserve:.8f}"
            )
            allowed_budget = max(0.0, max_budget - budget_reserve)
            min_notional = self._profile.min_notional or 0.0
            min_order_quote = min_notional * 1.2 if min_notional > 0 else 0.0
            if min_order_quote > 0 and order_quote < min_order_quote:
                self._logger(
                    "[ORDER_UNIT_INVALID] "
                    f"quote={order_quote:.8f} min_required={min_order_quote:.8f}"
                )
                self.last_action = "order_unit_invalid"
                self.orders_count = 0
                return 0
            if order_quote > allowed_budget:
                self._logger(
                    f"[BUDGET_BLOCKED] free={free_quote:.8f} required={order_quote:.8f}"
                )
                self.last_action = "budget_blocked"
                self.orders_count = 0
                return 0
            if free_quote < order_quote:
                self._logger(
                    f"[BUDGET_BLOCKED] free={free_quote:.8f} required={order_quote:.8f}"
                )
                self.last_action = "budget_blocked"
                self.orders_count = 0
                return 0
            qty = self._round_down(order_quote / buy_price, self._profile.step_size)

            if qty <= 0:
                self._logger(f"[TRADE] place_test_orders | qty <= 0 tag={self.TAG}")
                return 0
            if qty < self._profile.min_qty:
                self._logger(
                    "[TRADE] place_test_orders | "
                    f"qty below minQty ({qty} < {self._profile.min_qty}) tag={self.TAG}"
                )
                return 0

            notional = qty * buy_price
            if self._profile.min_notional is None:
                self._logger("[TRADE] place_test_orders | minNotional unknown")
            elif notional < self._profile.min_notional:
                self._logger(
                    "[TRADE] place_test_orders | "
                    f"notional below minNotional ({notional} < {self._profile.min_notional}) "
                    f"tag={self.TAG}"
                )
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
            if order_type == "MARKET":
                self._logger("[TRADE] BUY rejected: MARKET disabled")
                self.last_action = "market_buy_rejected"
                self._transition_state(TradeState.STATE_IDLE)
                return 0
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
                self._handle_cycle_completion(reason="ERROR")
                self._transition_state(TradeState.STATE_CLOSED)
                self._transition_state(TradeState.STATE_IDLE)
                self.orders_count = 0
                return 0
            self._logger(
                f"[ORDER] BUY placed qty={qty:.8f} price={buy_price:.8f} "
                f"id={buy_order.get('orderId')}"
            )
            self._transition_state(TradeState.STATE_WAIT_BUY)
            self._reset_buy_retry()
            self._start_buy_wait()
            now_ms = int(time.time() * 1000)
            self.active_test_orders = [
                {
                    "orderId": buy_order.get("orderId"),
                    "side": "BUY",
                    "price": buy_price,
                    "qty": qty,
                    "cum_qty": 0.0,
                    "avg_fill_price": 0.0,
                    "last_fill_price": None,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": buy_order.get("clientOrderId", buy_client_id),
                }
            ]
            self.orders_count = len(self.active_test_orders)
            self.pnl_cycle = None
            self.last_exit_reason = None
            self._last_buy_price = buy_price
            self._reset_exit_intent()
            return self.orders_count
        finally:
            self._inflight_trade_action = False

    def start_cycle_run(self) -> int:
        self.run_active = True
        self.cycles_done = 0
        self.cycles_target = self._normalize_cycle_target(self._settings.cycle_count)
        self._next_cycle_ready_ts = None
        return self._attempt_cycle_start()

    def stop_run_by_user(self, reason: str = "user") -> None:
        self.run_active = False
        self._next_cycle_ready_ts = None
        cancel_wait = self._cancel_open_orders_wait(timeout_s=2.0)
        self._logger(f"[STOP] cancel_wait={cancel_wait:.1f}s")
        if self.position is not None and self._settings.auto_exit_enabled:
            self._transition_state(TradeState.STATE_POSITION_OPEN)
            self._logger("[STOP] auto_exit closing position")
            qty = self._round_down(
                self._resolve_remaining_qty_raw(), self._profile.step_size
            )
            self._logger(f"[STOP] flatten_start qty={qty:.8f}")
            if self.exit_intent is None:
                self._set_exit_intent("MANUAL_CLOSE", mid=None)
            self._place_sell_order(reason="STOP")
            self._logger(f"[CYCLE_STOPPED_BY_USER] reason={reason}")
            return
        self._transition_state(TradeState.STATE_CLOSED)
        self._transition_state(TradeState.STATE_IDLE)
        self._logger(f"[CYCLE_STOPPED_BY_USER] reason={reason}")

    def process_cycle_flow(self) -> int:
        if not self.run_active:
            return 0
        if self.cycles_done >= self.cycles_target:
            return 0
        if self._next_cycle_ready_ts is None:
            return 0
        if time.monotonic() < self._next_cycle_ready_ts:
            return 0
        if self.state != TradeState.STATE_IDLE:
            return 0
        self._next_cycle_ready_ts = None
        placed = self._attempt_cycle_start()
        if not placed:
            self._next_cycle_ready_ts = time.monotonic() + self._cycle_cooldown_s
        return placed

    def _attempt_cycle_start(self) -> int:
        if self.state != TradeState.STATE_IDLE:
            return 0
        cycle_index = self.cycles_done + 1
        placed = self.place_test_orders_margin()
        if placed:
            self._logger(
                f"[CYCLE_START] idx={cycle_index} target={self.cycles_target}"
            )
        return placed

    def _handle_cycle_completion(self, reason: str, critical: bool = False) -> None:
        if not self.run_active:
            return
        self.cycles_done += 1
        pnl_label = "—" if self.pnl_cycle is None else f"{self.pnl_cycle:.8f}"
        self._logger(
            f"[CYCLE_DONE] idx={self.cycles_done} pnl={pnl_label} reason={reason}"
        )
        if critical:
            self.run_active = False
            self._next_cycle_ready_ts = None
            self._logger(
                f"[CYCLE_STOP] done={self.cycles_done} target={self.cycles_target}"
            )
            return
        if self.run_active and self.cycles_done < self.cycles_target:
            self._next_cycle_ready_ts = time.monotonic() + self._cycle_cooldown_s
            self._logger(
                f"[CYCLE_NEXT] done={self.cycles_done} target={self.cycles_target}"
            )
        else:
            self.run_active = False
            self._next_cycle_ready_ts = None
            self._logger(
                f"[CYCLE_STOP] done={self.cycles_done} target={self.cycles_target}"
            )

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
        if self.exit_intent is None:
            self._set_exit_intent("MANUAL_CLOSE", mid=None)
        return self._place_sell_order(reason="manual")

    def evaluate_exit_conditions(self) -> None:
        if self.state != TradeState.STATE_POSITION_OPEN:
            return
        if not self._settings.auto_exit_enabled:
            return
        price_state, health_state = self._router.build_price_state()
        mid = price_state.mid
        ws_age_ms = health_state.ws_age_ms
        http_age_ms = health_state.http_age_ms
        mid_fresh_ms = int(self._settings.mid_fresh_ms)
        ws_fresh = ws_age_ms is not None and ws_age_ms <= mid_fresh_ms
        http_fresh = http_age_ms is not None and http_age_ms <= mid_fresh_ms
        if not (ws_fresh or http_fresh):
            if not self._exits_frozen:
                ws_age_label = ws_age_ms if ws_age_ms is not None else "?"
                http_age_label = http_age_ms if http_age_ms is not None else "?"
                window_s = max(self._settings.price_wait_log_every_ms, 1) / 1000.0
                if not self._should_dedup_log("exits_frozen:blackout", window_s):
                    self._logger(
                        "[EXITS_FROZEN] "
                        f"reason=blackout ws_age={ws_age_label} http_age={http_age_label}"
                    )
            self._exits_frozen = True
            return
        if self._exits_frozen:
            self._exits_frozen = False
        exit_source = "WS" if ws_fresh else "HTTP"
        exit_age_ms = ws_age_ms if ws_fresh else http_age_ms
        if not self._profile.tick_size:
            return
        buy_price = self.get_buy_price()
        if buy_price is None:
            return
        tick_size = self._profile.tick_size
        take_profit_ticks = int(self._settings.take_profit_ticks)
        stop_loss_ticks = int(self._settings.stop_loss_ticks)
        tp_price = (
            buy_price + take_profit_ticks * tick_size
            if take_profit_ticks > 0
            else None
        )
        sl_price = (
            buy_price - stop_loss_ticks * tick_size if stop_loss_ticks > 0 else None
        )
        tp_ready = tp_price is not None and mid is not None and mid >= tp_price
        sl_ready = sl_price is not None and mid is not None and mid <= sl_price
        if self.exit_intent is None:
            if tp_ready:
                self._set_exit_intent("TP", mid=mid, ticks=take_profit_ticks)
            elif sl_ready:
                self._set_exit_intent("SL", mid=mid, ticks=stop_loss_ticks)
        else:
            if tp_ready or sl_ready:
                if not self._should_dedup_log(
                    f"exit_intent_hold:{self.exit_intent}", 2.0
                ):
                    self._logger(
                        f"[EXIT_INTENT_HOLD] intent={self.exit_intent}"
                    )
        if self.exit_intent is None:
            return
        if self.sell_place_inflight or self.sell_cancel_pending or self.sell_active_order_id:
            if self.sell_place_inflight:
                reason = "sell_inflight"
            elif self.sell_cancel_pending:
                reason = "cancel_pending"
            else:
                reason = "active_sell"
            if not self._should_dedup_log(f"exit_suppressed:{reason}", 2.0):
                self._logger(f"[EXIT_SUPPRESSED] reason={reason}")
            return
        if self._has_active_order("SELL"):
            return
        if not self._should_dedup_log(
            f"exit_allowed:{exit_source}", 1.0
        ):
            age_label = exit_age_ms if exit_age_ms is not None else "?"
            self._logger(
                f"[EXIT_ALLOWED] src={exit_source} age={age_label}"
            )
        self._place_sell_order(reason=self.exit_intent, mid_override=mid)

    def _start_price_wait(self, stage: str) -> None:
        now = time.monotonic()
        if stage == "ENTRY":
            if self._entry_price_wait_started_ts is None:
                self._entry_price_wait_started_ts = now
        elif stage == "EXIT":
            if self._exit_price_wait_started_ts is None:
                self._exit_price_wait_started_ts = now

    def _clear_price_wait(self, stage: str) -> None:
        if stage == "ENTRY":
            self._entry_price_wait_started_ts = None
        elif stage == "EXIT":
            self._exit_price_wait_started_ts = None

    def _price_wait_elapsed_ms(self, stage: str) -> Optional[int]:
        ts = None
        if stage == "ENTRY":
            ts = self._entry_price_wait_started_ts
        elif stage == "EXIT":
            ts = self._exit_price_wait_started_ts
        if ts is None:
            return None
        return int((time.monotonic() - ts) * 1000.0)

    def _log_price_wait(self, stage: str, age_ms: Optional[int], source: str) -> None:
        window_s = max(self._settings.price_wait_log_every_ms, 1) / 1000.0
        if self._should_dedup_log(f"price_wait:{stage}", window_s):
            return
        age_label = age_ms if age_ms is not None else "?"
        self._logger(f"[PRICE_WAIT] stage={stage} age_ms={age_label} source={source}")

    def _maybe_abort_entry_wait(self) -> bool:
        elapsed_ms = self._price_wait_elapsed_ms("ENTRY")
        if elapsed_ms is None:
            return False
        if elapsed_ms < int(self._settings.max_wait_price_ms):
            return False
        self._logger(f"[PRICE_WAIT_TIMEOUT] stage=ENTRY waited_ms={elapsed_ms}")
        self._clear_price_wait("ENTRY")
        self.abort_cycle()
        return True

    def get_buy_price(self) -> Optional[float]:
        if self.position is not None:
            return self.position.get("buy_price")
        return self._last_buy_price

    def _place_sell_order(
        self,
        reason: str = "manual",
        mid_override: Optional[float] = None,
        reset_retry: bool = True,
    ) -> int:
        if self.sell_place_inflight:
            self.last_action = "sell_place_inflight"
            return 0
        if self.sell_cancel_pending:
            self.last_action = "sell_cancel_pending"
            return 0
        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            self._logger("[TRADE] close_position | SELL already active")
            self.last_action = "sell_active"
            return 0
        now = time.monotonic()
        if (
            self.sell_last_attempt_ts
            and (now - self.sell_last_attempt_ts)
            < (self.sell_backoff_ms / 1000.0)
        ):
            self.last_action = "sell_backoff"
            return 0
        if not self._has_active_order("BUY") and self.position is None:
            self._logger("[TRADE] close_position | no BUY to close")
            self.last_action = "no_buy"
            return 0
        self.sell_place_inflight = True
        self.sell_last_attempt_ts = now
        self._inflight_trade_action = True
        try:
            price_state, health_state = self._router.build_price_state()
            mid = mid_override if mid_override is not None else price_state.mid
            ask = price_state.ask
            exit_order_type = self._normalize_order_type(
                self._settings.exit_order_type
            )
            reason_upper = reason.upper()
            if exit_order_type == "MARKET" and reason_upper != "EMERGENCY":
                self._logger("[TRADE] SELL rejected: MARKET disabled")
                self.last_action = "market_sell_rejected"
                return 0
            if exit_order_type == "LIMIT" and ask is None:
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
            remaining_qty_raw = self._resolve_remaining_qty_raw()
            qty = self._round_down(remaining_qty_raw, self._profile.step_size)
            if qty <= 0 or remaining_qty_raw <= self._get_step_size_tolerance():
                self._logger("[SELL_ALREADY_CLOSED_BY_PARTIAL]")
                self._finalize_partial_close(reason="PARTIAL")
                return 0
            if self._profile.min_qty and qty < self._profile.min_qty:
                self._logger(
                    f"[PARTIAL_TOO_SMALL] qty={qty:.8f} minQty={self._profile.min_qty}"
                )
                if qty <= self._get_step_size_tolerance():
                    self._logger("[SELL_ALREADY_CLOSED_BY_PARTIAL]")
                    self._finalize_partial_close(reason="PARTIAL")
                    return 0
                if exit_order_type == "MARKET":
                    return self._place_emergency_market_sell(qty)
                self.last_action = "partial_too_small"
                return 0

            balance = self._get_base_balance_snapshot()
            if balance is None:
                if not self._should_dedup_log("sell_balance_unavailable", 2.0):
                    self._logger("[SELL_BALANCE_UNAVAILABLE]")
                self.last_action = "sell_balance_unavailable"
                return 0
            base_free = balance["free"]
            base_locked = balance["locked"]
            qty_to_sell = qty
            if base_free < qty_to_sell:
                if base_locked >= qty_to_sell:
                    if not self._should_dedup_log("sell_blocked_locked", 2.0):
                        self._logger(
                            "[SELL_BLOCKED_LOCKED] "
                            f"free={base_free:.8f} locked={base_locked:.8f} "
                            f"need={qty_to_sell:.8f}"
                        )
                    self.last_action = "sell_blocked_locked"
                    if self.state != TradeState.STATE_WAIT_SELL:
                        self._transition_state(TradeState.STATE_WAIT_SELL)
                        self._start_sell_wait()
                    return 0
                if not self._should_dedup_log("sell_blocked_no_balance", 2.0):
                    self._logger(
                        "[SELL_BLOCKED_NO_BALANCE] "
                        f"free={base_free:.8f} locked={base_locked:.8f} "
                        f"need={qty_to_sell:.8f}"
                    )
                self.last_action = "sell_blocked_no_balance"
                return 0

            sell_price = None
            if exit_order_type == "LIMIT":
                tick_offset = max(0, int(self._settings.exit_offset_ticks))
                sell_price = ask - tick_offset * self._profile.tick_size
                sell_price = self._round_to_step(sell_price, self._profile.tick_size)

            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            sell_client_id = self._build_client_order_id("SELL", timestamp + 1)
            side_effect_type = (
                "AUTO_REPAY"
                if self._settings.allow_borrow and self._borrow_allowed_by_api is not False
                else None
            )
            order_type = exit_order_type

            self._transition_state(TradeState.STATE_PLACE_SELL)
            self.last_action = "placing_sell_tp" if reason_upper == "TP" else "placing_sell"
            price_label = "MARKET" if sell_price is None else f"{sell_price:.8f}"
            self._logger(f"[SELL_PLACE] qty={qty:.8f} price={price_label}")
            if self.position is not None and self.position.get("partial"):
                self._logger(f"[SELL_FOR_PARTIAL] qty={qty:.8f}")
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
                if self._last_place_error_code == -2010:
                    return self._handle_sell_insufficient_balance(reason)
                self._logger("[ORDER] SELL rejected")
                self.last_action = "place_failed"
                self._transition_state(TradeState.STATE_ERROR)
                self._handle_cycle_completion(reason="ERROR", critical=True)
                self._transition_state(TradeState.STATE_CLOSED)
                self._transition_state(TradeState.STATE_IDLE)
                return 0
            price_label = (
                f"{mid:.8f}" if sell_price is None else f"{sell_price:.8f}"
            )
            if reason_upper in {"TP", "SL"}:
                self._logger(
                    "[ORDER] SELL placed "
                    f"reason={reason_upper} type={order_type} price={price_label} "
                    f"qty={qty:.8f} id={sell_order.get('orderId')}"
                )
            else:
                self._logger(
                    f"[ORDER] SELL placed qty={qty:.8f} price={price_label} "
                    f"id={sell_order.get('orderId')}"
                )
            self._transition_state(TradeState.STATE_WAIT_SELL)
            if reset_retry:
                self._reset_sell_retry()
            self._start_sell_wait()
            now_ms = int(time.time() * 1000)
            self.sell_active_order_id = sell_order.get("orderId")
            self.sell_cancel_pending = False
            self.active_test_orders.append(
                {
                    "orderId": sell_order.get("orderId"),
                    "side": "SELL",
                    "price": sell_price,
                    "qty": qty,
                    "cum_qty": 0.0,
                    "sell_executed_qty": 0.0,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "sell_status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": sell_order.get("clientOrderId", sell_client_id),
                    "reason": reason_upper,
                }
            )
            self.orders_count = len(self.active_test_orders)
            return 1
        finally:
            self._inflight_trade_action = False
            self.sell_place_inflight = False

    def _place_emergency_market_sell(self, qty: float) -> int:
        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            self.last_action = "sell_active"
            return 0
        if self.position is None:
            self.last_action = "no_position"
            return 0
        self.sell_place_inflight = True
        self.sell_last_attempt_ts = time.monotonic()
        try:
            self._transition_state(TradeState.STATE_PLACE_SELL)
            self.last_action = "placing_sell"
            if self.position.get("partial"):
                self._logger(f"[SELL_FOR_PARTIAL] qty={qty:.8f}")
            self._logger(f"[SELL_PLACE] qty={qty:.8f} price=MARKET")
            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            sell_client_id = self._build_client_order_id("SELL", timestamp + 1)
            side_effect_type = (
                "AUTO_REPAY"
                if self._settings.allow_borrow and self._borrow_allowed_by_api is not False
                else None
            )
            sell_order = self._place_margin_order(
                symbol=self._settings.symbol,
                side="SELL",
                quantity=qty,
                price=None,
                is_isolated=is_isolated,
                client_order_id=sell_client_id,
                side_effect=side_effect_type,
                order_type="MARKET",
            )
            if not sell_order:
                self._logger("[ORDER] SELL rejected")
                self.last_action = "place_failed"
                self._transition_state(TradeState.STATE_POSITION_OPEN)
                return 0
            self._logger(
                f"[ORDER] SELL placed reason=EMERGENCY type=MARKET qty={qty:.8f} "
                f"id={sell_order.get('orderId')}"
            )
            self._transition_state(TradeState.STATE_WAIT_SELL)
            self._reset_sell_retry()
            self._start_sell_wait()
            now_ms = int(time.time() * 1000)
            self.sell_active_order_id = sell_order.get("orderId")
            self.sell_cancel_pending = False
            self.active_test_orders.append(
                {
                    "orderId": sell_order.get("orderId"),
                    "side": "SELL",
                    "price": None,
                    "qty": qty,
                    "cum_qty": 0.0,
                    "sell_executed_qty": 0.0,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "sell_status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": sell_order.get("clientOrderId", sell_client_id),
                    "reason": "EMERGENCY",
                }
            )
            self.orders_count = len(self.active_test_orders)
            return 1
        finally:
            self.sell_place_inflight = False

    def _reset_sell_retry(self) -> None:
        self._sell_wait_started_ts = None
        self._sell_retry_count = 0
        self._clear_price_wait("EXIT")
        self._pending_sell_retry_reason = None

    def _start_sell_wait(self) -> None:
        self._sell_wait_started_ts = time.monotonic()

    def _reset_buy_retry(self) -> None:
        self._buy_wait_started_ts = None
        self._buy_retry_count = 0
        self._clear_price_wait("ENTRY")

    def _start_buy_wait(self) -> None:
        self._buy_wait_started_ts = time.monotonic()

    def _maybe_handle_buy_ttl(self, open_orders: dict[int, dict]) -> None:
        if self.state != TradeState.STATE_WAIT_BUY:
            return
        ttl_ms = int(self._settings.buy_ttl_ms)
        if ttl_ms <= 0:
            return
        buy_order = self._find_order("BUY")
        if not buy_order or buy_order.get("status") == "FILLED":
            return
        if self._buy_wait_started_ts is None:
            self._start_buy_wait()
            return
        elapsed_ms = (time.monotonic() - self._buy_wait_started_ts) * 1000.0
        if elapsed_ms < ttl_ms:
            return
        self._logger("[BUY_TTL_EXPIRED]")
        order_id = buy_order.get("orderId")
        if order_id:
            live_order = self._get_margin_order_snapshot(order_id)
            if live_order:
                status = str(live_order.get("status") or "").upper()
                executed_qty = float(
                    live_order.get("executedQty", 0.0)
                    or live_order.get("executedQuantity", 0.0)
                    or 0.0
                )
                cum_quote = float(live_order.get("cummulativeQuoteQty", 0.0) or 0.0)
                avg_price = 0.0
                if executed_qty > 0 and cum_quote > 0:
                    avg_price = cum_quote / executed_qty
                else:
                    avg_price = float(
                        live_order.get("avgPrice", 0.0)
                        or live_order.get("price", 0.0)
                        or 0.0
                    )
                if status:
                    buy_order["status"] = status
                if executed_qty > 0:
                    buy_order["cum_qty"] = executed_qty
                    buy_order["avg_fill_price"] = avg_price
                    buy_order["last_fill_price"] = avg_price
                if status == "FILLED":
                    buy_order["qty"] = (
                        executed_qty if executed_qty > 0 else buy_order.get("qty")
                    )
                    if avg_price > 0:
                        buy_order["price"] = avg_price
                    self._apply_order_filled(buy_order)
                    self.active_test_orders = [
                        entry
                        for entry in self.active_test_orders
                        if entry.get("orderId") != order_id
                    ]
                    self.orders_count = len(self.active_test_orders)
                    return
        cum_qty = float(buy_order.get("cum_qty", 0.0) or 0.0)
        avg_fill_price = float(buy_order.get("avg_fill_price", 0.0) or 0.0)
        last_fill_price = buy_order.get("last_fill_price")
        if cum_qty > 0:
            order_id = buy_order.get("orderId")
            if order_id and order_id in open_orders:
                is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
                self._cancel_margin_order(
                    symbol=self._settings.symbol,
                    order_id=order_id,
                    is_isolated=is_isolated,
                    tag=buy_order.get("tag", self.TAG),
                )
            self.active_test_orders = [
                entry for entry in self.active_test_orders if entry.get("orderId") != order_id
            ]
            self.orders_count = len(self.active_test_orders)
            price_fallback = (
                avg_fill_price
                if avg_fill_price > 0
                else float(last_fill_price or buy_order.get("price") or 0.0)
            )
            avg_label = avg_fill_price if avg_fill_price > 0 else price_fallback
            self.position = {
                "buy_price": price_fallback if price_fallback > 0 else None,
                "qty": cum_qty,
                "opened_ts": int(time.time() * 1000),
                "partial": True,
                "initial_qty": cum_qty,
            }
            self._set_position_qty_base(cum_qty)
            self._reset_exit_intent()
            self._last_buy_price = self.position.get("buy_price") or self._last_buy_price
            self._reset_buy_retry()
            self._transition_state(TradeState.STATE_POSITION_OPEN)
            self._logger(
                f"[BUY_PARTIAL_COMMIT] qty={cum_qty:.8f} avg={avg_label:.8f}"
            )
            if not self._settings.auto_exit_enabled:
                self._logger("[PARTIAL_HELD_NO_AUTO_EXIT]")
            return
        max_retries = int(self._settings.max_buy_retries)
        if self._buy_retry_count >= max_retries:
            self._logger("[BUY_ABORT retries_exceeded]")
            self.abort_cycle()
            return
        snapshot = self._router.get_mid_snapshot(self._settings.mid_fresh_ms)
        if not snapshot[0]:
            self._start_price_wait("ENTRY")
            self._log_price_wait("ENTRY", snapshot[2], snapshot[3])
            self._maybe_abort_entry_wait()
            return
        self._clear_price_wait("ENTRY")
        order_id = buy_order.get("orderId")
        if order_id and order_id in open_orders:
            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            cancelled = self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=buy_order.get("tag", self.TAG),
            )
            if not cancelled:
                return
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        price_state, _ = self._router.build_price_state()
        mid = price_state.mid
        bid = price_state.bid
        if price_state.data_blind or mid is None or bid is None:
            self._start_price_wait("ENTRY")
            self._log_price_wait("ENTRY", price_state.mid_age_ms, price_state.source)
            self._maybe_abort_entry_wait()
            return
        self._clear_price_wait("ENTRY")
        if not self._profile.tick_size or not self._profile.step_size or not self._profile.min_qty:
            self._logger("[BUY_ABORT retries_exceeded]")
            self.abort_cycle()
            return
        self._buy_retry_count += 1
        retry_index = self._buy_retry_count
        tick_offset = max(0, int(self._settings.entry_offset_ticks))
        buy_price = bid + tick_offset * self._profile.tick_size
        buy_price = self._round_to_step(buy_price, self._profile.tick_size)
        order_quote = float(self._settings.order_quote)
        qty = self._round_down(order_quote / buy_price, self._profile.step_size)
        if qty <= 0 or qty < self._profile.min_qty:
            self._logger("[BUY_ABORT retries_exceeded]")
            self.abort_cycle()
            return
        if self._profile.min_notional is not None and qty * buy_price < self._profile.min_notional:
            self._logger("[BUY_ABORT retries_exceeded]")
            self.abort_cycle()
            return
        retry_price = f"{buy_price:.8f}"
        self._logger(
            f"[BUY_RETRY {retry_index}/{max_retries} price={retry_price}]"
        )
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        timestamp = int(time.time() * 1000)
        buy_client_id = self._build_client_order_id("BUY", timestamp)
        side_effect_type = self._normalize_side_effect_type(
            self._settings.side_effect_type
        )
        buy_order = self._place_margin_order(
            symbol=self._settings.symbol,
            side="BUY",
            quantity=qty,
            price=buy_price,
            is_isolated=is_isolated,
            client_order_id=buy_client_id,
            side_effect=side_effect_type,
            order_type="LIMIT",
        )
        if not buy_order:
            self._logger("[BUY_ABORT retries_exceeded]")
            self.abort_cycle()
            return
        self._transition_state(TradeState.STATE_WAIT_BUY)
        self._start_buy_wait()
        now_ms = int(time.time() * 1000)
        self.active_test_orders = [
            {
                "orderId": buy_order.get("orderId"),
                "side": "BUY",
                "price": buy_price,
                "qty": qty,
                "cum_qty": 0.0,
                "avg_fill_price": 0.0,
                "last_fill_price": None,
                "created_ts": now_ms,
                "updated_ts": now_ms,
                "status": "NEW",
                "tag": self.TAG,
                "clientOrderId": buy_order.get("clientOrderId", buy_client_id),
            }
        ]
        self.orders_count = len(self.active_test_orders)

    def _maybe_handle_sell_ttl(self, open_orders: dict[int, dict]) -> None:
        if self.state != TradeState.STATE_WAIT_SELL:
            return
        ttl_ms = int(self._settings.sell_ttl_ms)
        if ttl_ms <= 0:
            return
        sell_order = self._find_order("SELL")
        if sell_order and self.sell_active_order_id is None:
            self.sell_active_order_id = sell_order.get("orderId")
        if not sell_order:
            if self._pending_sell_retry_reason:
                self._retry_sell_after_cancel(
                    {"reason": self._pending_sell_retry_reason}
                )
                return
            if self.sell_cancel_pending:
                self._finalize_sell_cancel(self.sell_active_order_id)
            return
        if sell_order.get("status") == "FILLED":
            return
        if self.sell_cancel_pending:
            self._maybe_confirm_sell_cancel(sell_order, open_orders)
            return
        if self._sell_wait_started_ts is None:
            self._start_sell_wait()
            return
        elapsed_ms = (time.monotonic() - self._sell_wait_started_ts) * 1000.0
        if elapsed_ms < ttl_ms:
            return
        order_id = sell_order.get("orderId")
        self._logger(f"[SELL_TTL_EXPIRED] id={order_id}")
        max_retries = int(self._settings.max_sell_retries)
        if self._sell_retry_count >= max_retries:
            self._force_close_after_ttl()
            return
        if order_id and order_id in open_orders:
            if not self._should_dedup_log(f"sell_cancel_request:{order_id}", 2.0):
                self._logger(f"[SELL_CANCEL_REQUEST] id={order_id}")
            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            cancelled, error_code = self._cancel_margin_order_with_code(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=sell_order.get("tag", self.TAG),
            )
            if cancelled:
                self.sell_cancel_pending = True
                return
            if error_code == -2011:
                if order_id not in open_orders:
                    self._finalize_sell_cancel(order_id)
                    self._retry_sell_after_cancel(sell_order)
                else:
                    self.sell_cancel_pending = True
                    if not self._should_dedup_log(f"sell_cancel_wait:{order_id}", 2.0):
                        self._logger(f"[SELL_CANCEL_WAIT] id={order_id}")
                return
            return
        self._finalize_sell_cancel(order_id)
        self._retry_sell_after_cancel(sell_order)

    def _force_close_after_ttl(self, reason: str = "TTL_EXCEEDED") -> None:
        self._logger(f"[SELL_FORCE_CLOSE reason={reason}]")
        self._sell_wait_started_ts = None
        self._sell_retry_count = 0
        self.sell_active_order_id = None
        self.sell_cancel_pending = False
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        for order in list(self.active_test_orders):
            if order.get("side") != "SELL":
                continue
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order.get("orderId"),
                is_isolated=is_isolated,
                tag=order.get("tag", self.TAG),
            )
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("side") != "SELL"
        ]
        self.orders_count = len(self.active_test_orders)
        if not self._settings.force_close_on_ttl:
            self.last_exit_reason = "TTL_EXCEEDED"
            self._transition_state(TradeState.STATE_CLOSED)
            self._transition_state(TradeState.STATE_IDLE)
            self._handle_cycle_completion(reason="TTL_EXCEEDED")
            return
        qty = self._round_down(
            self._resolve_remaining_qty_raw(), self._profile.step_size
        )
        if qty <= 0:
            self.last_exit_reason = "TTL_EXCEEDED"
            self._transition_state(TradeState.STATE_CLOSED)
            self._transition_state(TradeState.STATE_IDLE)
            self._handle_cycle_completion(reason="TTL_EXCEEDED")
            return
        timestamp = int(time.time() * 1000)
        sell_client_id = self._build_client_order_id("SELL", timestamp + 1)
        side_effect_type = (
            "AUTO_REPAY"
            if self._settings.allow_borrow and self._borrow_allowed_by_api is not False
            else None
        )
        sell_order = self._place_margin_order(
            symbol=self._settings.symbol,
            side="SELL",
            quantity=qty,
            price=None,
            is_isolated=is_isolated,
            client_order_id=sell_client_id,
            side_effect=side_effect_type,
            order_type="MARKET",
        )
        if not sell_order:
            self.last_exit_reason = "TTL_EXCEEDED"
            self._transition_state(TradeState.STATE_CLOSED)
            self._transition_state(TradeState.STATE_IDLE)
            return
        now_ms = int(time.time() * 1000)
        self.active_test_orders.append(
            {
                "orderId": sell_order.get("orderId"),
                "side": "SELL",
                "price": None,
                "qty": qty,
                "created_ts": now_ms,
                "updated_ts": now_ms,
                "status": "NEW",
                "tag": self.TAG,
                "clientOrderId": sell_order.get("clientOrderId", sell_client_id),
                "reason": "TTL_EXCEEDED",
            }
        )
        self.orders_count = len(self.active_test_orders)
        self._transition_state(TradeState.STATE_WAIT_SELL)
        self._start_sell_wait()

    def sync_open_orders(self, open_orders: list[dict]) -> None:
        if not self.active_test_orders:
            self.orders_count = 0
            return
        open_map = {order.get("orderId"): order for order in open_orders}
        now_ms = int(time.time() * 1000)
        for order in self.active_test_orders:
            order_id = order.get("orderId")
            if order_id in open_map:
                status = str(open_map[order_id].get("status", "NEW")).upper()
                order["status"] = status
                if order.get("side") == "SELL":
                    order["sell_status"] = status
                order["updated_ts"] = now_ms
        self.orders_count = len(self.active_test_orders)
        if self.sell_active_order_id is None:
            sell_order = self._find_order("SELL")
            if sell_order:
                self.sell_active_order_id = sell_order.get("orderId")
        self._maybe_handle_buy_ttl(open_map)
        self._maybe_handle_sell_ttl(open_map)

    def handle_order_filled(
        self, order_id: int, side: str, price: float, qty: float, ts_ms: int
    ) -> None:
        order = next(
            (entry for entry in self.active_test_orders if entry.get("orderId") == order_id),
            None,
        )
        if order is None:
            return
        if order.get("status") == "FILLED":
            return
        order["status"] = "FILLED"
        if order.get("side") == "SELL":
            order["sell_status"] = "FILLED"
        order["updated_ts"] = ts_ms
        order["price"] = price
        order["qty"] = qty
        self._apply_order_filled(order)
        self.orders_count = len(self.active_test_orders)

    def handle_order_partial(
        self,
        order_id: int,
        side: str,
        cum_qty: float,
        avg_price: float,
        ts_ms: int,
    ) -> None:
        order = next(
            (entry for entry in self.active_test_orders if entry.get("orderId") == order_id),
            None,
        )
        if order is None:
            return
        side_upper = str(side).upper()
        if side_upper == "BUY":
            order["cum_qty"] = cum_qty
            if avg_price > 0:
                order["avg_fill_price"] = avg_price
                order["last_fill_price"] = avg_price
            order["updated_ts"] = ts_ms
            self._logger(
                f"[BUY_PARTIAL] cum_qty={cum_qty:.8f} avg={avg_price:.8f}"
            )
            return
        if side_upper == "SELL":
            self._update_sell_execution(order, cum_qty)
            if avg_price > 0:
                order["avg_fill_price"] = avg_price
                order["last_fill_price"] = avg_price
            order["updated_ts"] = ts_ms
            if self.position is None:
                return
            remaining_qty = self._resolve_remaining_qty_raw()
            self.position["qty"] = remaining_qty
            self._logger(
                "[SELL_PARTIAL] "
                f"order_id={order_id} executed={cum_qty:.8f} "
                f"agg_sold={self._aggregate_sold_qty:.8f} "
                f"remaining={remaining_qty:.8f}"
            )

    def handle_order_done(self, order_id: int, status: str, ts_ms: int) -> None:
        order = next(
            (entry for entry in self.active_test_orders if entry.get("orderId") == order_id),
            None,
        )
        if order is None:
            return
        if order.get("status") == status:
            return
        order["status"] = status
        if order.get("side") == "SELL":
            order["sell_status"] = status
        order["updated_ts"] = ts_ms
        side = str(order.get("side", "UNKNOWN")).upper()
        self._logger(f"[ORDER] {side} {status} id={order_id}")
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        if side == "BUY":
            self._reset_buy_retry()
            if self.position is None:
                self._transition_state(TradeState.STATE_IDLE)
        elif side == "SELL":
            self._reset_sell_retry()
            self.sell_active_order_id = None
            self.sell_cancel_pending = False
            if self.position is not None:
                self._transition_state(TradeState.STATE_POSITION_OPEN)
            else:
                self._transition_state(TradeState.STATE_IDLE)

    def _apply_order_filled(self, order: dict) -> None:
        side = order.get("side")
        if side == "BUY":
            self._logger(
                f"[ORDER] BUY filled qty={order.get('qty'):.8f} "
                f"price={order.get('price'):.8f}"
            )
            self._reset_buy_retry()
            if self.position is None:
                self.position = {
                    "buy_price": order.get("price"),
                    "qty": order.get("qty"),
                    "opened_ts": order.get("updated_ts"),
                    "partial": False,
                    "initial_qty": order.get("qty"),
                }
                self._set_position_qty_base(order.get("qty"))
                self._reset_exit_intent()
            self._transition_state(TradeState.STATE_POSITION_OPEN)
        elif side == "SELL":
            if order.get("qty") is not None:
                self._update_sell_execution(order, float(order.get("qty") or 0.0))
            self._reset_sell_retry()
            self.sell_active_order_id = None
            self.sell_cancel_pending = False
            reason = str(order.get("reason") or "MANUAL").upper()
            fill_price = order.get("price")
            qty = order.get("qty")
            buy_price = self.position.get("buy_price") if self.position else self._last_buy_price
            if reason in {"TP", "SL"}:
                fill_price_label = "—" if fill_price is None else f"{fill_price:.8f}"
                qty_label = "—" if qty is None else f"{qty:.8f}"
                self._logger(
                    "[ORDER] SELL filled "
                    f"reason={reason} fill_price={fill_price_label} qty={qty_label}"
                )
            else:
                fill_price_label = "—" if fill_price is None else f"{fill_price:.8f}"
                qty_label = "—" if qty is None else f"{qty:.8f}"
                self._logger(
                    f"[ORDER] SELL filled qty={qty_label} "
                    f"price={fill_price_label}"
                )
            pnl = self._finalize_close(order)
            self.last_exit_reason = reason if reason in {"TP", "SL"} else "MANUAL"
            if reason in {"TP", "SL"}:
                pnl_quote = "—" if pnl is None else f"{pnl:.8f}"
                pnl_bps = "—"
                if pnl is not None and buy_price and qty:
                    notion = buy_price * qty
                    if notion > 0:
                        pnl_bps = f"{(pnl / notion) * 10000:.2f}"
                self._logger(
                    f"[PNL] realized reason={reason} pnl_quote={pnl_quote} pnl_bps={pnl_bps}"
                )
            else:
                realized = "—" if pnl is None else f"{pnl:.8f}"
                self._logger(f"[PNL] realized={realized}")
            self._transition_state(TradeState.STATE_CLOSED)
            self._handle_cycle_completion(reason=reason)
            self.active_test_orders = []
            self.orders_count = 0
            self._transition_state(TradeState.STATE_IDLE)

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
        self._reset_position_tracking()
        self.last_action = "closed"
        return pnl

    def _resolve_close_qty(self) -> float:
        return self._resolve_remaining_qty_raw()

    def _resolve_remaining_qty_raw(self) -> float:
        if self.position_qty_base is None:
            if self.position is not None:
                self._set_position_qty_base(self.position.get("initial_qty"))
            else:
                return 0.0
        base_qty = float(self.position_qty_base or 0.0)
        remaining_qty = base_qty - self._aggregate_sold_qty
        return max(remaining_qty, 0.0)

    def _get_step_size_tolerance(self) -> float:
        if not self._profile.step_size:
            return 0.0
        return max(1e-8, self._profile.step_size * 0.1)

    def _set_position_qty_base(self, qty: Optional[float]) -> None:
        if qty is None:
            return
        if self.position_qty_base is not None:
            return
        base_qty = float(qty)
        if self._profile.step_size:
            base_qty = self._round_down(base_qty, self._profile.step_size)
        self.position_qty_base = base_qty
        self._aggregate_sold_qty = 0.0
        if self.position is not None:
            self.position["qty"] = base_qty
            self.position["initial_qty"] = base_qty

    def _reset_position_tracking(self) -> None:
        self.position_qty_base = None
        self._aggregate_sold_qty = 0.0
        self._reset_exit_intent()

    def _reset_exit_intent(self) -> None:
        self.exit_intent = None
        self.exit_intent_set_ts = None

    def _set_exit_intent(
        self, intent: str, mid: Optional[float], ticks: Optional[int] = None
    ) -> None:
        if self.exit_intent is not None:
            return
        self.exit_intent = intent
        self.exit_intent_set_ts = time.monotonic()
        ticks_label = "?" if ticks is None else str(ticks)
        mid_label = "—" if mid is None else f"{mid:.8f}"
        self._logger(
            f"[EXIT_INTENT_SET] intent={intent} ticks={ticks_label} mid={mid_label}"
        )

    def _update_sell_execution(self, order: dict, cum_qty: float) -> None:
        prev_cum = float(order.get("cum_qty") or 0.0)
        delta = max(cum_qty - prev_cum, 0.0)
        if delta > 0:
            self._aggregate_sold_qty += delta
        order["cum_qty"] = cum_qty
        order["sell_executed_qty"] = cum_qty
        if self.position is not None:
            remaining_qty = self._resolve_remaining_qty_raw()
            self.position["qty"] = remaining_qty

    def _handle_sell_insufficient_balance(self, reason: str) -> int:
        balance = self._get_base_balance_snapshot()
        if balance is not None:
            base_free = balance["free"]
            base_locked = balance["locked"]
            qty_needed = self._round_down(
                self._resolve_remaining_qty_raw(), self._profile.step_size
            )
            if base_locked > 0 and qty_needed > 0:
                self.sell_backoff_ms = 1000
                if not self._should_dedup_log("sell_place_2010", 0.5):
                    self._logger(
                        "[SELL_PLACE_2010] "
                        f"free={base_free:.8f} locked={base_locked:.8f} "
                        f"need={qty_needed:.8f} action=wait_cancel"
                    )
                if self.state != TradeState.STATE_WAIT_SELL:
                    self._transition_state(TradeState.STATE_WAIT_SELL)
                    self._start_sell_wait()
                return 0
        remaining_qty_raw = self._resolve_remaining_qty_raw()
        min_qty = self._profile.min_qty or 0.0
        if (
            remaining_qty_raw <= self._get_step_size_tolerance()
            or remaining_qty_raw < min_qty
        ):
            self._logger("[SELL_ALREADY_CLOSED_BY_PARTIAL]")
            self._finalize_partial_close(reason="PARTIAL")
            return 0
        max_retries = int(self._settings.max_sell_retries)
        if self._sell_retry_count >= max_retries:
            self._logger(
                f"[SELL_PLACE_FAILED_INSUFFICIENT] remaining={remaining_qty_raw:.8f}"
            )
            self._transition_state(TradeState.STATE_POSITION_OPEN)
            return 0
        self._sell_retry_count += 1
        retry_index = self._sell_retry_count
        qty_to_sell = self._round_down(
            remaining_qty_raw, self._profile.step_size
        )
        self._logger(
            f"[SELL_PLACE_FAILED_INSUFFICIENT] remaining={remaining_qty_raw:.8f}"
        )
        self._logger(
            "[SELL_RETRY_REMAINING] "
            f"qty={qty_to_sell:.8f} remaining={remaining_qty_raw:.8f} "
            f"n={retry_index}/{max_retries}"
        )
        return self._place_sell_order(reason=reason, reset_retry=False)

    def _finalize_partial_close(self, reason: str) -> None:
        self.last_exit_reason = reason
        self.active_test_orders = []
        self.orders_count = 0
        self.position = None
        self._reset_position_tracking()
        self._transition_state(TradeState.STATE_CLOSED)
        self._handle_cycle_completion(reason=reason)
        self._transition_state(TradeState.STATE_IDLE)

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

    def _get_active_sell_order(self) -> Optional[dict]:
        if self.sell_active_order_id is None:
            return self._find_order("SELL")
        return next(
            (
                entry
                for entry in self.active_test_orders
                if entry.get("orderId") == self.sell_active_order_id
            ),
            None,
        )

    @staticmethod
    def _sell_order_is_final(order: dict) -> bool:
        status = str(order.get("status") or "").upper()
        return status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}

    def _maybe_confirm_sell_cancel(
        self, sell_order: dict, open_orders: dict[int, dict]
    ) -> None:
        order_id = sell_order.get("orderId")
        if order_id is None:
            self.sell_cancel_pending = False
            return
        status = str(sell_order.get("status") or "").upper()
        if status in {"CANCELED", "REJECTED", "EXPIRED"}:
            self._finalize_sell_cancel(order_id)
            return
        if order_id not in open_orders:
            self._finalize_sell_cancel(order_id)
            return
        if not self._should_dedup_log(f"sell_cancel_wait:{order_id}", 2.0):
            self._logger(f"[SELL_CANCEL_WAIT] id={order_id}")

    def _finalize_sell_cancel(self, order_id: Optional[int]) -> None:
        if order_id is not None:
            self._logger(f"[SELL_CANCEL_CONFIRMED] id={order_id}")
        self.sell_cancel_pending = False
        self.sell_active_order_id = None
        if order_id is not None:
            self.active_test_orders = [
                entry
                for entry in self.active_test_orders
                if entry.get("orderId") != order_id
            ]
            self.orders_count = len(self.active_test_orders)
        self._sell_wait_started_ts = None

    def _retry_sell_after_cancel(self, sell_order: dict) -> None:
        max_retries = int(self._settings.max_sell_retries)
        if self._sell_retry_count >= max_retries:
            self._force_close_after_ttl()
            return
        reason = str(sell_order.get("reason") or "MANUAL").upper()
        snapshot = self._router.get_mid_snapshot(self._settings.mid_fresh_ms)
        if not snapshot[0]:
            self._start_price_wait("EXIT")
            self._log_price_wait("EXIT", snapshot[2], snapshot[3])
            self._pending_sell_retry_reason = reason
            return
        self._clear_price_wait("EXIT")
        self._pending_sell_retry_reason = None
        price_state, _ = self._router.build_price_state()
        exit_order_type = self._normalize_order_type(self._settings.exit_order_type)
        if exit_order_type == "LIMIT":
            ask = price_state.ask
            if ask is None or not self._profile.tick_size:
                self._force_close_after_ttl(reason="NO_PRICE")
                return
            tick_offset = max(0, int(self._settings.exit_offset_ticks))
            sell_price = ask - tick_offset * self._profile.tick_size
            sell_price = self._round_to_step(sell_price, self._profile.tick_size)
            retry_price = f"{sell_price:.8f}"
        else:
            retry_price = "MARKET"
        self._sell_retry_count += 1
        retry_index = self._sell_retry_count
        self._logger(
            f"[SELL_RETRY {retry_index}/{max_retries} price={retry_price}]"
        )
        remaining_qty_raw = self._resolve_remaining_qty_raw()
        remaining_qty = self._round_down(
            remaining_qty_raw, self._profile.step_size
        )
        if remaining_qty > 0:
            self._logger(
                "[SELL_RETRY_REMAINING] "
                f"qty={remaining_qty:.8f} remaining={remaining_qty_raw:.8f} "
                f"n={retry_index}/{max_retries}"
            )
        placed = self._place_sell_order(reason=reason, reset_retry=False)
        if placed:
            self._pending_sell_retry_reason = None

    def abort_cycle(self) -> int:
        return self.abort_cycle_with_reason(reason="ABORT")

    def abort_cycle_with_reason(self, reason: str, critical: bool = False) -> int:
        cancelled = self.cancel_test_orders_margin(reason="aborted")
        self._transition_state(TradeState.STATE_CLOSED)
        self._transition_state(TradeState.STATE_IDLE)
        self.last_action = "aborted"
        self._handle_cycle_completion(reason=reason, critical=critical)
        return cancelled

    def _cancel_open_orders_wait(self, timeout_s: float) -> float:
        start = time.monotonic()
        self.cancel_test_orders_margin(reason="stopped")
        while time.monotonic() - start < timeout_s:
            try:
                open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
            except httpx.HTTPStatusError as exc:
                self._log_binance_error("openOrders", exc, {"symbol": self._settings.symbol})
                break
            except Exception as exc:
                self._logger(f"[TRADE] openOrders error: {exc} tag={self.TAG}")
                break
            has_tagged = False
            for order in open_orders:
                client_order_id = order.get("clientOrderId", "")
                if client_order_id.startswith(self._client_tag):
                    has_tagged = True
                    break
            if not has_tagged:
                break
            time.sleep(0.2)
        return time.monotonic() - start

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
        self._reset_buy_retry()
        self._reset_sell_retry()
        self._reset_exit_intent()
        self.sell_active_order_id = None
        self.sell_cancel_pending = False
        self.sell_place_inflight = False
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
        price: Optional[float],
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
            if price is None:
                self._logger("[TRADE] place_order error: missing price for LIMIT")
                return None
            params["price"] = f"{price:.8f}"
        if side_effect:
            params["sideEffectType"] = side_effect
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        self._last_place_error_code = None
        try:
            return self._rest.create_margin_order(params)
        except httpx.HTTPStatusError as exc:
            code = None
            try:
                payload = exc.response.json() if exc.response else {}
                code = payload.get("code")
            except Exception:
                pass
            self._last_place_error_code = code
            self._log_binance_error("place_order", exc, params)
        except Exception as exc:
            self._logger(f"[TRADE] place_order error: {exc} tag={self.TAG}")
        return None

    def _get_margin_order_snapshot(self, order_id: int) -> Optional[dict]:
        try:
            return self._rest.get_margin_order(self._settings.symbol, order_id)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error(
                "get_order", exc, {"symbol": self._settings.symbol, "orderId": order_id}
            )
        except Exception as exc:
            self._logger(f"[TRADE] get_order error: {exc} tag={self.TAG}")
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
            code = None
            try:
                payload = exc.response.json() if exc.response else {}
                code = payload.get("code")
            except Exception:
                pass
            if code == -2011:
                if not self._should_dedup_log("cancel_order:-2011", 10.0):
                    self._logger("[CANCEL_IGNORED_UNKNOWN_ORDER]")
                return True
            self._log_binance_error("cancel_order", exc, params)
        except Exception as exc:
            self._logger(f"[TRADE] cancel_order error: {exc} tag={tag}")
        return False

    def _cancel_margin_order_with_code(
        self,
        symbol: str,
        order_id: Optional[int],
        is_isolated: str,
        tag: str,
    ) -> tuple[bool, Optional[int]]:
        if not order_id:
            return False, None
        params = {
            "symbol": symbol,
            "orderId": order_id,
            "isIsolated": is_isolated,
        }
        try:
            self._rest.cancel_margin_order(params)
            return True, None
        except httpx.HTTPStatusError as exc:
            code = None
            try:
                payload = exc.response.json() if exc.response else {}
                code = payload.get("code")
            except Exception:
                pass
            if code == -2011:
                if not self._should_dedup_log("cancel_order:-2011", 10.0):
                    self._logger("[CANCEL_IGNORED_UNKNOWN_ORDER]")
                return False, code
            self._log_binance_error("cancel_order", exc, params)
            return False, code
        except Exception as exc:
            self._logger(f"[TRADE] cancel_order error: {exc} tag={tag}")
        return False, None

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
                    "locked": self._safe_float(entry.get("locked")),
                    "borrowed": self._safe_float(entry.get("borrowed")),
                    "interest": self._safe_float(entry.get("interest")),
                    "netAsset": self._safe_float(entry.get("netAsset")),
                }
        return None

    def _get_base_balance_snapshot(self) -> Optional[dict]:
        base_asset, _ = self._split_symbol(self._settings.symbol)
        base_state = self._get_margin_asset(base_asset)
        if base_state is None:
            return None
        free = float(base_state.get("free", 0.0))
        locked = float(base_state.get("locked", 0.0))
        total = free + locked
        return {
            "asset": base_asset,
            "free": free,
            "locked": locked,
            "total": total,
        }

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

    @staticmethod
    def _normalize_cycle_target(value: object) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 1
        return max(1, min(1000, parsed))
