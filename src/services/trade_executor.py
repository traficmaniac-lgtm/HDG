from __future__ import annotations

import math
import re
import time
from decimal import Decimal, ROUND_FLOOR
from enum import Enum
from typing import Callable, Optional

import httpx

from src.core.models import Settings, SymbolProfile
from src.services.binance_rest import BinanceRestClient
from src.services.price_router import PriceRouter


class TradeState(Enum):
    STATE_IDLE = "IDLE"
    STATE_ENTRY_WORKING = "ENTRY_WORKING"
    STATE_POS_OPEN = "POS_OPEN"
    STATE_EXIT_TP_WORKING = "EXIT_TP_WORKING"
    STATE_EXIT_SL_WORKING = "EXIT_SL_WORKING"
    STATE_RECOVERY = "RECOVERY"
    STATE_FLAT = "FLAT"
    STATE_NEXT_CYCLE = "NEXT_CYCLE"
    STATE_ERROR = "ERROR"


class TradeExecutor:
    TAG = "PRE_V0_8_0"
    PRICE_MOVE_TICK_THRESHOLD = 1

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
        self._entry_attempt_started_ts: Optional[float] = None
        self._entry_reprice_last_ts = 0.0
        self._entry_consecutive_fresh_reads = 0
        self._entry_last_seen_bid: Optional[float] = None
        self._entry_last_ws_bad_ts: Optional[float] = None
        self._entry_last_ref_bid: Optional[float] = None
        self._entry_last_ref_ts_ms: Optional[int] = None
        self._entry_order_price: Optional[float] = None
        self._sell_wait_started_ts: Optional[float] = None
        self._sell_retry_count = 0
        self.sell_active_order_id: Optional[int] = None
        self.sell_cancel_pending = False
        self.sell_place_inflight = False
        self.sell_last_attempt_ts = 0.0
        self.sell_backoff_ms = 500
        self._pending_sell_retry_reason: Optional[str] = None
        self.sell_retry_pending = False
        self._last_sell_ref_price: Optional[float] = None
        self._last_sell_ref_side: Optional[str] = None
        self._aggregate_sold_qty = 0.0
        self._aggregate_sold_notional = 0.0
        self.position_qty_base: Optional[float] = None
        self.exit_intent: Optional[str] = None
        self.exit_intent_set_ts: Optional[float] = None
        self._last_place_error_code: Optional[int] = None
        self.state = TradeState.STATE_IDLE
        self._cycle_id_counter = 0
        self._current_cycle_id: Optional[int] = None
        self._processed_fill_keys: set[tuple[int, str, float, float]] = set()
        self.wins = 0
        self.losses = 0
        self.cycles_target = self._normalize_cycle_target(
            getattr(settings, "cycle_count", 1)
        )
        self.cycles_done = 0
        self.run_active = False
        self._next_cycle_ready_ts: Optional[float] = None
        self._cycle_cooldown_s = 0.3
        self._exits_frozen = False
        self.entry_active_order_id: Optional[int] = None
        self.entry_active_price: Optional[float] = None
        self.entry_cancel_pending = False
        self._entry_price_wait_started_ts: Optional[float] = None
        self._exit_price_wait_started_ts: Optional[float] = None
        self._exit_started_ts: Optional[float] = None
        self._tp_exit_phase: Optional[str] = None
        self._recovery_residual_qty = 0.0
        self._recovery_active = False
        self._recovery_last_check_ts: Optional[float] = None

    def get_state_label(self) -> str:
        return self.state.value

    def _transition_state(self, next_state: TradeState) -> None:
        if self.state == next_state:
            return
        previous = self.state
        self.state = next_state
        self._logger(f"[STATE] {previous.value} → {next_state.value}")

    def _exit_state_for_intent(self, intent: Optional[str]) -> TradeState:
        normalized = self._normalize_exit_intent(intent)
        if normalized == "SL":
            return TradeState.STATE_EXIT_SL_WORKING
        return TradeState.STATE_EXIT_TP_WORKING

    def _entry_order_active(self) -> bool:
        order = self._get_active_entry_order()
        return order is not None and not self._entry_order_is_final(order)

    def _transition_to_position_state(self) -> None:
        if self.position is None:
            self._transition_state(TradeState.STATE_IDLE)
            return
        if self._recovery_active:
            self._transition_state(TradeState.STATE_RECOVERY)
            return
        if self._entry_order_active():
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
        else:
            self._transition_state(TradeState.STATE_POS_OPEN)

    def _in_exit_workflow(self) -> bool:
        return self.state in {
            TradeState.STATE_EXIT_TP_WORKING,
            TradeState.STATE_EXIT_SL_WORKING,
            TradeState.STATE_RECOVERY,
        }

    def _cycle_id_label(self) -> str:
        return "—" if self._current_cycle_id is None else str(self._current_cycle_id)

    def _get_cycle_id_for_order(self, order: Optional[dict]) -> Optional[int]:
        if order is None:
            return self._current_cycle_id
        cycle_id = order.get("cycle_id")
        if isinstance(cycle_id, int):
            return cycle_id
        return self._current_cycle_id

    def _log_cycle_start(self, cycle_id: int) -> None:
        self._logger(f"[CYCLE_START] id={cycle_id}")

    def _log_cycle_fill(self, cycle_id: Optional[int], side: str, delta: float, cum: float) -> None:
        if cycle_id is None:
            cycle_id = self._current_cycle_id
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        self._logger(
            f"[CYCLE_FILL] id={cycle_label} side={side} delta={delta:.8f} cum={cum:.8f}"
        )

    def _log_cycle_close(
        self,
        cycle_id: Optional[int],
        pnl_quote: Optional[float],
        pnl_bps: Optional[float],
        reason: str,
    ) -> None:
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        pnl_quote_label = "—" if pnl_quote is None else f"{pnl_quote:.8f}"
        pnl_bps_label = "—" if pnl_bps is None else f"{pnl_bps:.2f}"
        self._logger(
            "[CYCLE_CLOSE] "
            f"id={cycle_label} pnl_quote={pnl_quote_label} pnl_bps={pnl_bps_label} "
            f"reason={reason}"
        )

    def _log_cycle_cleanup(self, cycle_id: Optional[int]) -> None:
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        self._logger(f"[CYCLE_CLEANUP] id={cycle_label} ok")

    def _fill_event_key(
        self,
        order_id: Optional[int],
        side: str,
        cum_qty: float,
        avg_price: float,
    ) -> Optional[tuple[int, str, float, float]]:
        if order_id is None:
            return None
        return (int(order_id), side, float(cum_qty), float(avg_price))

    def _should_process_fill(
        self,
        order_id: Optional[int],
        side: str,
        cum_qty: float,
        avg_price: float,
        cycle_id: Optional[int],
    ) -> bool:
        key = self._fill_event_key(order_id, side, cum_qty, avg_price)
        if key is None:
            return True
        if key in self._processed_fill_keys:
            cycle_label = "—" if cycle_id is None else str(cycle_id)
            self._logger(
                "[FILL_SKIP_DUPLICATE] "
                f"id={cycle_label} order_id={order_id} side={side} "
                f"cum={cum_qty:.8f} avg={avg_price:.8f}"
            )
            return False
        self._processed_fill_keys.add(key)
        return True

    def _entry_plan_qty(self) -> float:
        entry_order = self._get_active_entry_order()
        if entry_order is not None:
            return float(entry_order.get("qty") or 0.0)
        if self.position is not None:
            return float(self.position.get("initial_qty") or 0.0)
        return 0.0

    def _entry_open_qty(self) -> float:
        entry_order = self._get_active_entry_order()
        if entry_order is None:
            return 0.0
        qty = float(entry_order.get("qty") or 0.0)
        cum = float(entry_order.get("cum_qty") or 0.0)
        return max(qty - cum, 0.0)

    def _tp_order_qty(self) -> float:
        sell_order = self._get_active_sell_order()
        if not sell_order:
            return 0.0
        reason = str(sell_order.get("reason") or "").upper()
        if reason == "TP":
            return float(sell_order.get("qty") or 0.0)
        return 0.0

    def _log_trade_snapshot(self, reason: str, sl_mode: Optional[str] = None) -> None:
        pos_qty = self._resolve_remaining_qty_raw()
        executed_qty = float(self.position_qty_base or 0.0)
        entry_open_qty = self._entry_open_qty()
        plan_qty = self._entry_plan_qty()
        balance = self._get_base_balance_snapshot()
        base_free = balance["free"] if balance else 0.0
        base_locked = balance["locked"] if balance else 0.0
        tp_order_qty = self._tp_order_qty()
        sl_label = sl_mode or (self.exit_intent or "—")
        self._logger(
            "[STATUS] "
            f"plan_qty={plan_qty:.8f} executed_qty_total={executed_qty:.8f} "
            f"pos_qty={pos_qty:.8f} entry_open_qty={entry_open_qty:.8f} "
            f"base_free={base_free:.8f} base_locked={base_locked:.8f} "
            f"tp_order_qty={tp_order_qty:.8f} sl_mode={sl_label} reason={reason}"
        )

    def _settlement_balance_retry(
        self, required_qty: float, base_free: float, base_locked: float
    ) -> tuple[float, float, int]:
        attempts = 0
        for delay_s in (0.2, 0.4, 0.8):
            if base_free + base_locked >= required_qty:
                break
            time.sleep(delay_s)
            balance = self._get_base_balance_snapshot()
            attempts += 1
            if balance is None:
                continue
            base_free = balance["free"]
            base_locked = balance["locked"]
        return base_free, base_locked, attempts

    def _set_recovery(self, residual_qty: float, reason: str) -> None:
        self._recovery_residual_qty = max(residual_qty, 0.0)
        self._recovery_active = self._recovery_residual_qty > 0
        self._recovery_last_check_ts = time.monotonic()
        if self._recovery_active:
            self.last_action = "recovery"
            self._transition_state(TradeState.STATE_RECOVERY)
            self._logger(
                f"[RECOVERY_SET] residual={self._recovery_residual_qty:.8f} reason={reason}"
            )
            self._log_trade_snapshot(reason=f"recovery:{reason}")

    def _clear_recovery(self, reason: str) -> None:
        if self._recovery_active:
            self._logger(f"[RECOVERY_CLEAR] reason={reason}")
        self._recovery_residual_qty = 0.0
        self._recovery_active = False
        self._recovery_last_check_ts = None

    def _attempt_recovery_sell(self) -> None:
        if not self._recovery_active:
            return
        if self.sell_place_inflight or self.sell_cancel_pending or self._has_active_order("SELL"):
            return
        remaining_qty = self._resolve_remaining_qty_raw()
        if remaining_qty <= self._get_step_size_tolerance():
            self._clear_recovery(reason="position_flat")
            return
        balance = self._get_base_balance_snapshot()
        if balance is None:
            return
        base_free = balance["free"]
        base_locked = balance["locked"]
        total_available = base_free + base_locked
        safety_epsilon = self._get_step_size_tolerance()
        available_qty = max(0.0, total_available - safety_epsilon)
        if self._profile.step_size:
            available_qty = self._round_down(available_qty, self._profile.step_size)
        if available_qty <= 0:
            return
        qty_to_sell = min(available_qty, remaining_qty)
        residual = max(remaining_qty - qty_to_sell, 0.0)
        self._set_recovery(residual, reason="RECOVERY_TICK")
        self._logger(
            "[RECOVERY_RETRY] "
            f"available={available_qty:.8f} sell_qty={qty_to_sell:.8f} "
            f"residual={residual:.8f} free={base_free:.8f} locked={base_locked:.8f}"
        )
        self._place_sell_order(
            reason="RECOVERY",
            exit_intent=self.exit_intent,
            qty_override=qty_to_sell,
        )

    def _record_cycle_pnl(self, pnl: Optional[float]) -> None:
        self.pnl_cycle = pnl
        if pnl is not None:
            self.pnl_session += pnl

    def _cleanup_cycle_state(self, cycle_id: Optional[int]) -> None:
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        for order in list(self.active_test_orders):
            order_id = order.get("orderId")
            if not order_id:
                continue
            status = str(order.get("status") or "").upper()
            if status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                continue
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=order.get("tag", self.TAG),
            )
        self.active_test_orders = []
        self.orders_count = 0
        self.sell_active_order_id = None
        self.sell_cancel_pending = False
        self.sell_place_inflight = False
        self.sell_retry_pending = False
        self._pending_sell_retry_reason = None
        self._reset_buy_retry()
        self._reset_sell_retry()
        self._clear_entry_attempt()
        self._entry_reprice_last_ts = 0.0
        self._entry_consecutive_fresh_reads = 0
        self._entry_last_seen_bid = None
        self._entry_last_ws_bad_ts = None
        self._processed_fill_keys = set()
        self.entry_active_order_id = None
        self.entry_active_price = None
        self.entry_cancel_pending = False
        self._current_cycle_id = None
        self._log_cycle_cleanup(cycle_id)

    def set_margin_capabilities(
        self, margin_api_access: bool, borrow_allowed_by_api: Optional[bool]
    ) -> None:
        self._borrow_allowed_by_api = borrow_allowed_by_api

    def place_test_orders_margin(self) -> int:
        if self._inflight_trade_action:
            self.last_action = "inflight"
            return 0
        self._inflight_trade_action = True
        cycle_pending = False
        buy_placed = False
        if self._current_cycle_id is None:
            self._current_cycle_id = self._cycle_id_counter + 1
            cycle_pending = True
        try:
            if self.state != TradeState.STATE_IDLE:
                self._logger(
                    f"[TRADE] START ignored: state={self.state.value}"
                )
                self.last_action = "state_blocked"
                return 0
            if not self._purge_open_orders_before_cycle():
                self._logger("[PRECHECK] open_orders_not_cleared -> SAFE_STOP")
                self.abort_cycle_with_reason(reason="OPEN_ORDERS_PRESENT", critical=True)
                return 0
            if self._has_active_order("SELL") or self.sell_active_order_id is not None:
                self._logger("[ERROR] active SELL detected during PLACE_BUY -> SAFE_STOP")
                self.abort_cycle_with_reason(reason="ACTIVE_SELL_ON_BUY", critical=True)
                return 0
            if self._has_active_order("BUY") or self.position is not None:
                self._logger("[TRADE] place_test_orders | BUY already active")
                self.last_action = "buy_active"
                return 0
            price_state, health_state = self._router.build_price_state()
            mid = price_state.mid
            bid = price_state.bid
            ask = price_state.ask
            active_src = price_state.source
            ws_bad = active_src == "WS" and self._entry_ws_bad(health_state.ws_connected)
            entry_context = self._entry_log_context(
                active_src,
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
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

            buy_price = self._calculate_entry_price(bid)
            if buy_price is None:
                self._logger("[TRADE] place_test_orders | invalid entry price")
                return 0
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

            cycle_label = self._cycle_id_label()
            bid_label = "?" if bid is None else f"{bid:.8f}"
            self._logger(
                f"[PLACE_BUY] cycle_id={cycle_label} price=best_bid best_bid={bid_label} "
                f"src={price_state.source}"
            )
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
            self.last_action = "placing_buy"
            self._start_entry_attempt()
            self._entry_reprice_last_ts = time.monotonic()
            self._log_entry_place(buy_price, bid, ask, entry_context)
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
                self._clear_entry_attempt()
                self._transition_state(TradeState.STATE_ERROR)
                self._handle_cycle_completion(reason="ERROR")
                self._transition_state(TradeState.STATE_FLAT)
                self._transition_state(TradeState.STATE_IDLE)
                self.orders_count = 0
                return 0
            if cycle_pending and self._current_cycle_id is not None:
                self._cycle_id_counter = self._current_cycle_id
                self._log_cycle_start(self._current_cycle_id)
            buy_placed = True
            self._logger(
                f"[ORDER] BUY placed cycle_id={cycle_label} qty={qty:.8f} "
                f"price={buy_price:.8f} id={buy_order.get('orderId')}"
            )
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
            self._reset_buy_retry()
            self._start_buy_wait()
            now_ms = int(time.time() * 1000)
            self.active_test_orders = [
                {
                    "orderId": buy_order.get("orderId"),
                    "side": "BUY",
                    "price": buy_price,
                    "bid_at_place": bid,
                    "qty": qty,
                    "cum_qty": 0.0,
                    "avg_fill_price": 0.0,
                    "last_fill_price": None,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": buy_order.get("clientOrderId", buy_client_id),
                    "cycle_id": self._current_cycle_id,
                }
            ]
            self.entry_active_order_id = buy_order.get("orderId")
            self.entry_active_price = buy_price
            self.entry_cancel_pending = False
            self._entry_last_ref_bid = bid
            self._entry_last_ref_ts_ms = now_ms
            self._entry_order_price = buy_price
            self.orders_count = len(self.active_test_orders)
            self.pnl_cycle = None
            self.last_exit_reason = None
            self._last_buy_price = buy_price
            self._reset_exit_intent()
            return self.orders_count
        finally:
            if cycle_pending and not buy_placed:
                self._current_cycle_id = None
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
        self._clear_entry_attempt()
        self._logger(f"[STOP] cancel_wait={cancel_wait:.1f}s")
        if self.position is not None:
            self._transition_to_position_state()
            self._logger("[STOP] auto_exit closing position")
            self._flatten_position("STOP")
            self._logger(f"[CYCLE_STOPPED_BY_USER] reason={reason}")
            return
        self._transition_state(TradeState.STATE_FLAT)
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
        if self.state not in {TradeState.STATE_IDLE, TradeState.STATE_NEXT_CYCLE}:
            return 0
        self._next_cycle_ready_ts = None
        placed = self._attempt_cycle_start()
        if not placed:
            self._next_cycle_ready_ts = time.monotonic() + self._cycle_cooldown_s
        return placed

    def _attempt_cycle_start(self) -> int:
        if self.state not in {TradeState.STATE_IDLE, TradeState.STATE_NEXT_CYCLE}:
            return 0
        if not self._purge_open_orders_before_cycle():
            self._logger("[PRECHECK] open_orders_not_cleared -> SAFE_STOP")
            self._transition_state(TradeState.STATE_ERROR)
            self.abort_cycle_with_reason(reason="OPEN_ORDERS_PRESENT", critical=True)
            return 0
        cycle_index = self.cycles_done + 1
        pending_cycle_id = self._cycle_id_counter + 1
        self._current_cycle_id = pending_cycle_id
        placed = self.place_test_orders_margin()
        if placed:
            self._cycle_id_counter = pending_cycle_id
            self._log_cycle_start(pending_cycle_id)
            self._logger(
                f"[CYCLE_START] idx={cycle_index} target={self.cycles_target}"
            )
        else:
            self._current_cycle_id = None
        return placed

    def _handle_cycle_completion(self, reason: str, critical: bool = False) -> None:
        self.cycles_done += 1
        pnl_label = "—" if self.pnl_cycle is None else f"{self.pnl_cycle:.8f}"
        self._logger(
            f"[CYCLE_DONE] idx={self.cycles_done} pnl={pnl_label} reason={reason}"
        )
        if not self.run_active:
            return
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
        if self.state not in {
            TradeState.STATE_POS_OPEN,
            TradeState.STATE_ENTRY_WORKING,
            TradeState.STATE_RECOVERY,
        }:
            self._logger(
                f"[TRADE] close_position | blocked state={self.state.value}"
            )
            self.last_action = "state_blocked"
            return 0
        if self.exit_intent is None:
            self._set_exit_intent("MANUAL_CLOSE", mid=None)
        return self._place_sell_order(reason="manual", exit_intent=self.exit_intent)

    def evaluate_exit_conditions(self) -> None:
        if self.position is None:
            return
        if self.state not in {
            TradeState.STATE_POS_OPEN,
            TradeState.STATE_ENTRY_WORKING,
            TradeState.STATE_RECOVERY,
        }:
            return
        if not self._settings.auto_exit_enabled:
            return
        if self._recovery_active:
            self._attempt_recovery_sell()
        price_state, health_state = self._router.build_price_state()
        mid = price_state.mid
        bid = price_state.bid
        ask = price_state.ask
        exit_max_age_ms = int(getattr(self._settings, "exit_max_age_ms", 2500))
        mid_age_ms = price_state.mid_age_ms
        ws_lost = price_state.source == "WS" and not health_state.ws_connected
        stale = (
            price_state.source == "NONE"
            or mid_age_ms is None
            or mid_age_ms > exit_max_age_ms
        )
        if price_state.data_blind or ws_lost or stale:
            self._log_trade_snapshot(reason="panic_exit", sl_mode="PANIC")
            self._panic_exit(
                reason="DATA_BLIND" if price_state.data_blind else "STALE_DATA",
                bid=bid,
                ask=ask,
            )
            return
        exit_source = price_state.source
        exit_age_ms = mid_age_ms
        if not self._profile.tick_size:
            return
        buy_price = self.get_buy_price()
        if buy_price is None:
            return
        tick_size = self._profile.tick_size
        take_profit_ticks = int(self._settings.take_profit_ticks)
        stop_loss_ticks = int(self._settings.stop_loss_ticks)
        position_side = "LONG"
        if self.position is not None:
            position_side = str(self.position.get("side", "LONG") or "LONG").upper()
        if position_side == "SHORT":
            tp_price = (
                buy_price - take_profit_ticks * tick_size
                if take_profit_ticks > 0
                else None
            )
            sl_price = (
                buy_price + stop_loss_ticks * tick_size
                if stop_loss_ticks > 0
                else None
            )
        else:
            tp_price = (
                buy_price + take_profit_ticks * tick_size
                if take_profit_ticks > 0
                else None
            )
            sl_price = (
                buy_price - stop_loss_ticks * tick_size if stop_loss_ticks > 0 else None
            )
        if position_side == "SHORT":
            tp_ready = (
                tp_price is not None and bid is not None and bid <= tp_price
            )
            sl_ready = (
                sl_price is not None and ask is not None and ask >= sl_price
            )
        else:
            tp_ready = (
                tp_price is not None and ask is not None and ask >= tp_price
            )
            sl_ready = (
                sl_price is not None and bid is not None and bid <= sl_price
            )
        self._log_trade_snapshot(reason="exit_eval", sl_mode="SL" if sl_ready else "—")
        if not self._should_dedup_log("exit_eval", 0.5):
            bid_label = "—" if bid is None else f"{bid:.8f}"
            ask_label = "—" if ask is None else f"{ask:.8f}"
            mid_label = "—" if mid is None else f"{mid:.8f}"
            entry_label = "—" if buy_price is None else f"{buy_price:.8f}"
            tp_label = "—" if tp_price is None else f"{tp_price:.8f}"
            sl_label = "—" if sl_price is None else f"{sl_price:.8f}"
            self._logger(
                "[EXIT_CHECK] "
                f"side={position_side} bid={bid_label} ask={ask_label} mid={mid_label} "
                f"entry={entry_label} tp_price={tp_label} sl_price={sl_label} "
                f"TP_ready={tp_ready} SL_ready={sl_ready}"
            )
        if sl_ready:
            self._set_exit_intent("SL", mid=mid, ticks=stop_loss_ticks, force=True)
            self._override_to_sl(bid=bid, ask=ask)
            return
        if self.exit_intent is None and tp_ready:
            self._set_exit_intent("TP", mid=mid, ticks=take_profit_ticks)
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
            ws_age_label = health_state.ws_age_ms if health_state.ws_age_ms is not None else "?"
            http_age_label = (
                health_state.http_age_ms if health_state.http_age_ms is not None else "?"
            )
            self._logger(
                "[EXIT_ALLOWED] "
                f"src={exit_source} age={age_label} "
                f"ws_age={ws_age_label} http_age={http_age_label}"
            )
        self._place_sell_order(
            reason=self.exit_intent,
            mid_override=mid,
            exit_intent=self.exit_intent,
        )

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

    @staticmethod
    def _clamp_int(value: int, min_value: int, max_value: int) -> int:
        return max(min_value, min(max_value, value))

    def _resolve_exit_timeout_ms(self) -> int:
        configured = int(getattr(self._settings, "max_wait_sell_ms", 8000) or 0)
        if configured <= 0:
            configured = 8000
        return self._clamp_int(configured, 6000, 9000)

    def _tp_hybrid_params(self) -> tuple[int, int, int]:
        maker_grace_ms = int(getattr(self._settings, "tp_maker_grace_ms", 1200) or 1200)
        force_cross_ms = int(getattr(self._settings, "tp_force_cross_ms", 2500) or 2500)
        cross_ticks = int(getattr(self._settings, "tp_cross_ticks", 0) or 0)
        if maker_grace_ms < 0:
            maker_grace_ms = 0
        if force_cross_ms < maker_grace_ms:
            force_cross_ms = maker_grace_ms
        if cross_ticks < 0:
            cross_ticks = 0
        if cross_ticks > 1:
            cross_ticks = 1
        return maker_grace_ms, force_cross_ms, cross_ticks

    def _exit_elapsed_ms(self) -> Optional[int]:
        if self._exit_started_ts is None:
            return None
        return int((time.monotonic() - self._exit_started_ts) * 1000.0)

    def _resolve_exit_ref_side(self, intent: str) -> str:
        if intent == "TP" and self._tp_exit_phase == "CROSS":
            return "bid"
        _, ref_side = TradeExecutor._resolve_exit_policy(intent)
        return ref_side

    def _compute_tp_cross_price(
        self,
        bid: Optional[float],
        tick_size: Optional[float],
        cross_ticks: int,
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if bid is None or tick_size is None or tick_size <= 0:
            return None, None, None, "ref_missing"
        raw_price = bid - (cross_ticks * tick_size)
        rounded = TradeExecutor._round_to_step(raw_price, tick_size)
        if rounded < tick_size:
            rounded = tick_size
        delta_ticks = (rounded - bid) / tick_size
        return rounded, bid, delta_ticks, "TP_CROSS"

    def _compute_exit_sell_price_dynamic(
        self,
        intent: str,
        bid: Optional[float],
        ask: Optional[float],
        tick_size: Optional[float],
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if intent == "TP" and self._tp_exit_phase == "CROSS":
            _, _, cross_ticks = self._tp_hybrid_params()
            return self._compute_tp_cross_price(bid, tick_size, cross_ticks)
        return TradeExecutor._compute_exit_sell_price(intent, bid, ask, tick_size)

    @staticmethod
    def _normalize_exit_intent(intent: Optional[str]) -> str:
        upper = str(intent or "").upper()
        if upper in {"TP", "SL"}:
            return upper
        return "MANUAL"

    @staticmethod
    def _resolve_exit_policy(intent: str) -> tuple[str, str]:
        if intent == "TP":
            return "TP_MAKER", "ask"
        if intent == "SL":
            return "SL_CROSS", "bid"
        return "MANUAL", "ask"

    @staticmethod
    def _compute_exit_sell_price(
        intent: str,
        bid: Optional[float],
        ask: Optional[float],
        tick_size: Optional[float],
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if tick_size is None or tick_size <= 0:
            return None, None, None, "tick_size_missing"
        policy, ref_side = TradeExecutor._resolve_exit_policy(intent)
        ref_price = ask if ref_side == "ask" else bid
        if ref_price is None:
            return None, None, None, "ref_missing"
        raw_price = ref_price
        rounded = TradeExecutor._round_to_step(raw_price, tick_size)
        if rounded < tick_size:
            rounded = tick_size
        delta_ticks = (rounded - ref_price) / tick_size
        return rounded, ref_price, delta_ticks, policy

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

    def _cancel_entry_before_exit(self, timeout_s: float = 1.5) -> bool:
        buy_order = self._get_active_entry_order()
        if not buy_order:
            return True
        status_label = str(buy_order.get("status") or "").upper()
        if self._entry_order_is_final(buy_order):
            return True
        order_id = buy_order.get("orderId")
        self._logger(
            "[CANCEL_ENTRY_BEFORE_EXIT] "
            f"id={order_id} status={status_label} timeout_s={timeout_s:.1f}"
        )
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        if order_id:
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=buy_order.get("tag", self.TAG),
            )
        start = time.monotonic()
        while time.monotonic() - start < timeout_s:
            time.sleep(0.15)
            if not order_id:
                break
            live_order = self._get_margin_order_snapshot(order_id)
            if not live_order:
                self._handle_entry_cancel_confirmed(buy_order, order_id, "CANCELED")
                return True
            status = str(live_order.get("status") or status_label or "").upper()
            executed_qty = float(
                live_order.get("executedQty", 0.0)
                or live_order.get("executedQuantity", 0.0)
                or 0.0
            )
            cum_quote = float(live_order.get("cummulativeQuoteQty", 0.0) or 0.0)
            avg_price = 0.0
            if executed_qty > 0 and cum_quote > 0:
                avg_price = cum_quote / executed_qty
            elif live_order:
                avg_price = float(
                    live_order.get("avgPrice", 0.0)
                    or live_order.get("price", 0.0)
                    or 0.0
                )
            buy_order["status"] = status
            if status == "FILLED" and executed_qty > 0:
                buy_order["qty"] = executed_qty
                buy_order["price"] = avg_price if avg_price > 0 else buy_order.get("price")
                self._apply_order_filled(buy_order)
                return True
            if status == "PARTIALLY_FILLED" and executed_qty > 0:
                self._apply_entry_partial_fill(order_id, executed_qty, avg_price or None)
            if status in {"CANCELED", "REJECTED", "EXPIRED"}:
                self._handle_entry_cancel_confirmed(buy_order, order_id, status)
                return True
            if status not in {"NEW", "PARTIALLY_FILLED"}:
                self._handle_entry_cancel_confirmed(buy_order, order_id, status or "CANCELED")
                return True
        self._logger(f"[CANCEL_ENTRY_TIMEOUT] id={order_id}")
        return False

    def _flatten_position(self, reason: str) -> bool:
        remaining_qty_raw = self._resolve_remaining_qty_raw()
        if self._profile.step_size:
            qty = self._round_down(remaining_qty_raw, self._profile.step_size)
        else:
            qty = remaining_qty_raw
        self._logger(
            f"[STOP_FLATTEN] reason={reason} remaining={remaining_qty_raw:.8f} qty={qty:.8f}"
        )
        self.cancel_test_orders_margin(reason="flatten")
        if qty <= 0:
            self._logger("[FLATTEN_OK] reason=already_flat")
            return True
        price_state, _ = self._router.build_price_state()
        tick_size = self._profile.tick_size
        bid = price_state.bid
        cross_price = None
        if bid is not None and tick_size:
            cross_price = self._round_to_step(bid, tick_size)
        for attempt in range(1, 3):
            if cross_price is not None:
                placed = self._place_sell_order(
                    reason="EMERGENCY",
                    price_override=cross_price,
                    exit_intent="MANUAL",
                    ref_bid=bid,
                    ref_ask=price_state.ask,
                    tick_size=tick_size,
                )
                if placed:
                    self._logger(
                        f"[FLATTEN_OK] action=limit_cross attempt={attempt}"
                    )
                    return True
            time.sleep(0.2)
        if qty > 0:
            placed = self._place_emergency_market_sell(qty, force=True)
            if placed:
                self._logger("[FLATTEN_OK] action=market")
                return True
        self._logger("[FLATTEN_FAIL] action=market_unavailable")
        return False


    def get_buy_price(self) -> Optional[float]:
        if self.position is not None:
            return self.position.get("buy_price")
        return self._last_buy_price

    def _place_sell_order(
        self,
        reason: str = "manual",
        mid_override: Optional[float] = None,
        price_override: Optional[float] = None,
        reset_retry: bool = True,
        exit_intent: Optional[str] = None,
        qty_override: Optional[float] = None,
        ref_bid: Optional[float] = None,
        ref_ask: Optional[float] = None,
        tick_size: Optional[float] = None,
    ) -> int:
        if self.sell_place_inflight:
            self.last_action = "sell_place_inflight"
            return 0
        if self.sell_cancel_pending:
            self.last_action = "sell_cancel_pending"
            return 0
        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            self._logger("[SELL_SKIP_DUPLICATE] reason=active_sell")
            self._logger("[TRADE] close_position | SELL already active")
            self.last_action = "sell_active"
            return 0
        if self._has_active_order("BUY"):
            if not self._cancel_entry_before_exit():
                self._logger("[CANCEL_ENTRY_FAILED] action=flatten")
                self._flatten_position("CANCEL_ENTRY_FAILED")
                return 0
            if self._has_active_order("BUY"):
                self.last_action = "entry_cancel_pending"
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
            bid = ref_bid if ref_bid is not None else price_state.bid
            ask = ref_ask if ref_ask is not None else price_state.ask
            intent = self._normalize_exit_intent(exit_intent or self.exit_intent or reason)
            if intent == "TP" and self._tp_exit_phase is None:
                self._tp_exit_phase = "MAKER"
            if self._exit_started_ts is None and intent in {"TP", "SL", "MANUAL"}:
                self._exit_started_ts = time.monotonic()
            tick_size = tick_size or self._profile.tick_size
            exit_order_type = self._normalize_order_type(
                self._settings.exit_order_type
            )
            reason_upper = reason.upper()
            if exit_order_type == "MARKET" and reason_upper != "EMERGENCY":
                self._logger("[TRADE] SELL rejected: MARKET disabled")
                self.last_action = "market_sell_rejected"
                return 0
            if exit_order_type == "LIMIT" and ask is None and bid is None and price_override is None:
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
            qty = qty_override if qty_override is not None else remaining_qty_raw
            if self._profile.step_size:
                qty = self._round_down(qty, self._profile.step_size)
            if qty <= 0 or remaining_qty_raw <= self._get_step_size_tolerance():
                self._logger("[SELL_ALREADY_CLOSED_BY_PARTIAL]")
                self._finalize_partial_close(reason="PARTIAL")
                return 0
            self._log_trade_snapshot(reason=f"sell_place:{intent}")
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
                        self._log_trade_snapshot(reason="sell_blocked_locked")
                    self.last_action = "sell_blocked_locked"
                    if not self._in_exit_workflow():
                        self._transition_state(self._exit_state_for_intent(intent))
                        self._start_sell_wait()
                    return 0
                total_available = base_free + base_locked
                if total_available < qty_to_sell:
                    base_free, base_locked, attempts = self._settlement_balance_retry(
                        required_qty=qty_to_sell,
                        base_free=base_free,
                        base_locked=base_locked,
                    )
                    total_available = base_free + base_locked
                    if total_available < qty_to_sell:
                        safety_epsilon = self._get_step_size_tolerance()
                        available_qty = max(0.0, total_available - safety_epsilon)
                        if self._profile.step_size:
                            available_qty = self._round_down(
                                available_qty, self._profile.step_size
                            )
                        residual_qty = max(qty_to_sell - available_qty, 0.0)
                        self._set_recovery(residual_qty, reason="SELL_BLOCKED_NO_BALANCE")
                        if available_qty <= 0:
                            if not self._should_dedup_log(
                                "sell_blocked_no_balance", 2.0
                            ):
                                self._logger(
                                    "[SELL_BLOCKED_NO_BALANCE] "
                                    f"free={base_free:.8f} locked={base_locked:.8f} "
                                    f"need={qty_to_sell:.8f} attempts={attempts}"
                                )
                            self.last_action = "sell_blocked_no_balance"
                            return 0
                        self._logger(
                            "[SELL_RECOVERY_PARTIAL] "
                            f"available={available_qty:.8f} residual={residual_qty:.8f} "
                            f"free={base_free:.8f} locked={base_locked:.8f}"
                        )
                        qty = available_qty
                        qty_to_sell = available_qty

            sell_price = None
            sell_ref_price = None
            delta_ticks_to_ref = None
            exit_policy, sell_ref_label = self._resolve_exit_policy(intent)
            if intent == "TP" and self._tp_exit_phase == "CROSS":
                exit_policy = "TP_CROSS"
                sell_ref_label = "bid"
            if exit_order_type == "LIMIT":
                if price_override is None:
                    (
                        sell_price,
                        sell_ref_price,
                        delta_ticks_to_ref,
                        exit_policy,
                    ) = self._compute_exit_sell_price_dynamic(
                        intent=intent,
                        bid=bid,
                        ask=ask,
                        tick_size=tick_size or self._profile.tick_size,
                    )
                else:
                    sell_price = price_override
                    sell_ref_price = ask if sell_ref_label == "ask" else bid
                    if (
                        sell_ref_price is not None
                        and tick_size
                        and tick_size > 0
                        and sell_price is not None
                    ):
                        delta_ticks_to_ref = (sell_price - sell_ref_price) / tick_size
                if sell_price is None:
                    now = time.monotonic()
                    if now - self._last_no_price_log_ts >= 2.0:
                        self._last_no_price_log_ts = now
                        self._logger(
                            "[TRADE] blocked: no_price "
                            f"(ws_age={health_state.ws_age_ms} http_age={health_state.http_age_ms})"
                        )
                    self.last_action = "NO_PRICE"
                    return 0

            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            sell_client_id = self._build_client_order_id("SELL", timestamp + 1)
            side_effect_type = (
                "AUTO_REPAY"
                if self._settings.allow_borrow and self._borrow_allowed_by_api is not False
                else None
            )
            order_type = exit_order_type

            cycle_label = self._cycle_id_label()
            ask_label = "?" if ask is None else f"{ask:.8f}"
            ws_bad = price_state.source == "WS" and self._entry_ws_bad(health_state.ws_connected)
            ws_age_label = health_state.ws_age_ms if health_state.ws_age_ms is not None else "?"
            http_age_label = health_state.http_age_ms if health_state.http_age_ms is not None else "?"
            self._logger(
                f"[PLACE_SELL] cycle_id={cycle_label} price=best_ask best_ask={ask_label} "
                f"active_source={price_state.source} ws_age={ws_age_label} "
                f"http_age={http_age_label} ws_bad={ws_bad}"
            )
            self._transition_state(self._exit_state_for_intent(intent))
            self.last_action = "placing_sell_tp" if reason_upper == "TP" else "placing_sell"
            price_label = "MARKET" if sell_price is None else f"{sell_price:.8f}"
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} price={price_label} qty={qty:.8f} "
                f"policy={exit_policy}"
            )
            bid_label = "—" if bid is None else f"{bid:.8f}"
            ask_label = "—" if ask is None else f"{ask:.8f}"
            mid_label = "—" if mid is None else f"{mid:.8f}"
            delta_label = (
                "—" if delta_ticks_to_ref is None else f"{delta_ticks_to_ref:.2f}"
            )
            elapsed_ms = self._exit_elapsed_ms()
            elapsed_label = "—" if elapsed_ms is None else str(elapsed_ms)
            phase_label = "—" if self._tp_exit_phase is None else self._tp_exit_phase
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} exit_intent={intent} exit_policy={exit_policy} "
                f"sell_ref={sell_ref_label} bid={bid_label} ask={ask_label} "
                f"mid={mid_label} sell_price={price_label} "
                f"delta_ticks_to_ref={delta_label} qty={qty:.8f}"
                f" elapsed_ms={elapsed_label} phase={phase_label}"
            )
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
                self._logger("[ORDER] SELL rejected -> flatten")
                self.last_action = "place_failed"
                self._transition_state(TradeState.STATE_ERROR)
                self._flatten_position("SELL_REJECTED")
                return 0
            price_label = (
                f"{mid:.8f}" if sell_price is None else f"{sell_price:.8f}"
            )
            if reason_upper in {"TP", "SL"}:
                self._logger(
                    "[ORDER] SELL placed "
                    f"cycle_id={cycle_label} reason={reason_upper} type={order_type} "
                    f"price={price_label} qty={qty:.8f} id={sell_order.get('orderId')}"
                )
            else:
                self._logger(
                    f"[ORDER] SELL placed cycle_id={cycle_label} qty={qty:.8f} "
                    f"price={price_label} id={sell_order.get('orderId')}"
                )
            self._transition_state(self._exit_state_for_intent(intent))
            if reset_retry:
                self._reset_sell_retry()
            self._start_sell_wait()
            now_ms = int(time.time() * 1000)
            self.sell_active_order_id = sell_order.get("orderId")
            self.sell_cancel_pending = False
            self._last_sell_ref_price = sell_ref_price
            self._last_sell_ref_side = sell_ref_label
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
                    "cycle_id": self._current_cycle_id,
                }
            )
            self.orders_count = len(self.active_test_orders)
            return 1
        finally:
            self._inflight_trade_action = False
            self.sell_place_inflight = False

    def _place_emergency_market_sell(self, qty: float, force: bool = False) -> int:
        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            if not force:
                self._logger("[SELL_SKIP_DUPLICATE] reason=active_sell")
                self.last_action = "sell_active"
                return 0
            self._logger("[SELL_FORCE_MARKET_OVERRIDE] reason=active_sell")
        if self.position is None:
            self.last_action = "no_position"
            return 0
        self.sell_place_inflight = True
        self.sell_last_attempt_ts = time.monotonic()
        try:
            self._transition_state(self._exit_state_for_intent(self.exit_intent))
            self.last_action = "placing_sell"
            if self.position.get("partial"):
                self._logger(f"[SELL_FOR_PARTIAL] qty={qty:.8f}")
            price_state, health_state = self._router.build_price_state()
            cycle_label = self._cycle_id_label()
            ask_label = "?" if price_state.ask is None else f"{price_state.ask:.8f}"
            ws_bad = price_state.source == "WS" and self._entry_ws_bad(health_state.ws_connected)
            ws_age_label = health_state.ws_age_ms if health_state.ws_age_ms is not None else "?"
            http_age_label = health_state.http_age_ms if health_state.http_age_ms is not None else "?"
            self._logger(
                f"[PLACE_SELL] cycle_id={cycle_label} price=best_ask best_ask={ask_label} "
                f"active_source={price_state.source} ws_age={ws_age_label} "
                f"http_age={http_age_label} ws_bad={ws_bad}"
            )
            intent = self._normalize_exit_intent(self.exit_intent or "MANUAL")
            exit_policy, sell_ref_label = self._resolve_exit_policy(intent)
            bid_label = "—" if price_state.bid is None else f"{price_state.bid:.8f}"
            ask_label = "—" if price_state.ask is None else f"{price_state.ask:.8f}"
            mid_label = "—" if price_state.mid is None else f"{price_state.mid:.8f}"
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} price=MARKET qty={qty:.8f} "
                f"policy={exit_policy}"
            )
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} exit_intent={intent} exit_policy={exit_policy} "
                f"sell_ref={sell_ref_label} bid={bid_label} ask={ask_label} "
                f"mid={mid_label} sell_price=MARKET "
                "delta_ticks_to_ref=— "
                f"qty={qty:.8f}"
            )
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
                self._transition_to_position_state()
                return 0
            self._logger(
                "[ORDER] SELL placed "
                f"cycle_id={cycle_label} reason=EMERGENCY type=MARKET "
                f"qty={qty:.8f} id={sell_order.get('orderId')}"
            )
            self._transition_state(self._exit_state_for_intent(self.exit_intent))
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
                    "cycle_id": self._current_cycle_id,
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
        self.sell_retry_pending = False

    def _start_sell_wait(self) -> None:
        self._sell_wait_started_ts = time.monotonic()

    def _reset_buy_retry(self) -> None:
        self._buy_wait_started_ts = None
        self._buy_retry_count = 0
        self._clear_price_wait("ENTRY")
        self._entry_consecutive_fresh_reads = 0
        self._entry_last_seen_bid = None
        self._entry_last_ws_bad_ts = None

    def _start_buy_wait(self) -> None:
        self._buy_wait_started_ts = time.monotonic()

    def _start_entry_attempt(self) -> None:
        if self._entry_attempt_started_ts is None:
            self._entry_attempt_started_ts = time.monotonic()
        self._entry_consecutive_fresh_reads = 0
        self._entry_last_seen_bid = None
        self._entry_last_ws_bad_ts = None
        self._entry_last_ref_bid = None
        self._entry_last_ref_ts_ms = None
        self._entry_order_price = None

    def _clear_entry_attempt(self) -> None:
        self._entry_attempt_started_ts = None
        self._entry_last_ref_bid = None
        self._entry_last_ref_ts_ms = None
        self._entry_order_price = None

    def _entry_attempt_elapsed_ms(self) -> Optional[int]:
        if self._entry_attempt_started_ts is None:
            return None
        return int((time.monotonic() - self._entry_attempt_started_ts) * 1000.0)

    def _entry_ws_bad(self, ws_connected: bool) -> bool:
        ws_no_first_tick, ws_stale_ticks = self._router.get_ws_issue_counts()
        return (not ws_connected) or ws_no_first_tick >= 2 or ws_stale_ticks >= 2

    @staticmethod
    def _ticks_between(
        old_price: Optional[float],
        new_price: Optional[float],
        tick_size: Optional[float],
    ) -> int:
        if old_price is None or new_price is None or not tick_size:
            return 0
        tick = Decimal(str(tick_size))
        if tick <= 0:
            return 0
        diff = Decimal(str(new_price)) - Decimal(str(old_price))
        ticks = (abs(diff) / tick).to_integral_value(rounding=ROUND_FLOOR)
        return int(ticks)

    def _entry_ref_movement(
        self,
        ref_bid_current: float,
        tick_size: float,
        now_ms: int,
    ) -> tuple[int, bool]:
        last_ref = self._entry_last_ref_bid
        if last_ref is None:
            self._entry_last_ref_bid = ref_bid_current
            self._entry_last_ref_ts_ms = now_ms
            return 0, False
        moved_ticks = self._ticks_between(last_ref, ref_bid_current, tick_size)
        should_reprice = moved_ticks >= 1
        if should_reprice:
            self._entry_last_ref_bid = ref_bid_current
            self._entry_last_ref_ts_ms = now_ms
        return moved_ticks, should_reprice

    @staticmethod
    def _entry_log_context(
        active_src: str,
        ws_bad: bool,
        ws_age_ms: Optional[int],
        http_age_ms: Optional[int],
    ) -> str:
        ws_age_label = ws_age_ms if ws_age_ms is not None else "?"
        http_age_label = http_age_ms if http_age_ms is not None else "?"
        return (
            f"active_source={active_src} ws_bad={ws_bad} "
            f"http_age_ms={http_age_label} ws_age_ms={ws_age_label}"
        )

    def _log_entry_degraded(
        self,
        ws_connected: bool,
        entry_context: str,
    ) -> None:
        window_s = max(self._settings.price_wait_log_every_ms, 1) / 1000.0
        if self._should_dedup_log("entry_degraded:http", window_s):
            return
        ws_no_first_tick, ws_stale_ticks = self._router.get_ws_issue_counts()
        self._logger(
            "[ENTRY_DEGRADED_TO_HTTP] "
            f"reason=ws_bad ws_connected={ws_connected} "
            f"no_first_tick={ws_no_first_tick} stale_ticks={ws_stale_ticks} "
            f"{entry_context}"
        )

    def _resolve_entry_snapshot(
        self,
    ) -> tuple[
        bool,
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[int],
        str,
        str,
        bool,
        Optional[int],
        Optional[int],
    ]:
        price_state, health_state = self._router.build_price_state()
        active_src = price_state.source
        ws_bad = False
        if active_src == "WS":
            ws_bad = self._entry_ws_bad(health_state.ws_connected)
            if ws_bad:
                entry_context = self._entry_log_context(
                    active_src,
                    ws_bad,
                    health_state.ws_age_ms,
                    health_state.http_age_ms,
                )
                self._log_entry_degraded(health_state.ws_connected, entry_context)
        bid = price_state.bid
        ask = price_state.ask
        mid_val = price_state.mid
        age_ms = price_state.mid_age_ms
        source = price_state.source
        if price_state.data_blind:
            return (
                False,
                bid,
                ask,
                mid_val,
                age_ms,
                source,
                "data_blind",
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
        if mid_val is None:
            return (
                False,
                bid,
                ask,
                mid_val,
                age_ms,
                source,
                "no_mid",
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
        if age_ms is None:
            return (
                False,
                bid,
                ask,
                mid_val,
                age_ms,
                source,
                "no_age",
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
        entry_max_age_ms = int(getattr(self._settings, "entry_max_age_ms", 800))
        if age_ms > entry_max_age_ms:
            return (
                False,
                bid,
                ask,
                mid_val,
                age_ms,
                source,
                "stale",
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
        if bid is None or ask is None:
            return (
                False,
                bid,
                ask,
                mid_val,
                age_ms,
                source,
                "no_quote",
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
        return (
            True,
            bid,
            ask,
            mid_val,
            age_ms,
            source,
            "fresh",
            ws_bad,
            health_state.ws_age_ms,
            health_state.http_age_ms,
        )

    def _calculate_entry_price(self, bid: float) -> Optional[float]:
        if not self._profile.tick_size:
            return None
        tick_size = self._profile.tick_size
        return self._round_to_step(bid, tick_size)

    def _log_entry_place(
        self,
        price: float,
        bid: Optional[float],
        ask: Optional[float],
        entry_context: str,
    ) -> None:
        bid_label = "?" if bid is None else f"{bid:.8f}"
        ask_label = "?" if ask is None else f"{ask:.8f}"
        cycle_label = self._cycle_id_label()
        self._logger(
            f"[ENTRY_PLACE] cycle_id={cycle_label} price={price:.8f} "
            f"bid={bid_label} ask={ask_label} {entry_context}"
        )

    def _handle_entry_follow_top(self, open_orders: dict[int, dict]) -> None:
        if self.state != TradeState.STATE_ENTRY_WORKING:
            return
        buy_order = self._get_active_entry_order()
        if not buy_order or buy_order.get("status") == "FILLED":
            return
        if self.entry_cancel_pending:
            self._maybe_confirm_entry_cancel(buy_order, open_orders)
            if self.entry_cancel_pending:
                return
            status_label = str(buy_order.get("status") or "").upper()
            if status_label in {"FILLED", "PARTIALLY_FILLED"}:
                return
        if self._buy_wait_started_ts is None:
            self._start_buy_wait()
        (
            ok,
            bid,
            ask,
            mid,
            age_ms,
            source,
            status,
            ws_bad,
            ws_age_ms,
            http_age_ms,
        ) = self._resolve_entry_snapshot()
        entry_context = self._entry_log_context(
            source,
            ws_bad,
            ws_age_ms,
            http_age_ms,
        )
        now = time.monotonic()
        if ws_bad and source == "WS":
            self._entry_last_ws_bad_ts = now
        if not ok or bid is None or mid is None:
            self._entry_consecutive_fresh_reads = 0
            if ws_bad and source == "WS":
                if not self._should_dedup_log("entry_hold_wsbad", 2.0):
                    ws_no_first_tick, ws_stale_ticks = self._router.get_ws_issue_counts()
                    self._logger(
                        "[ENTRY_HOLD_WSBAD] "
                        f"no_first_tick={ws_no_first_tick} stale_ticks={ws_stale_ticks} "
                        f"{entry_context}"
                    )
                if not self._should_dedup_log("entry_skip_wsbad", 2.0):
                    age_label = age_ms if age_ms is not None else "?"
                    self._logger(
                        f"[ENTRY_REPRICE_SKIP] reason=ws_bad age_ms={age_label} "
                        f"{entry_context}"
                    )
            else:
                if not self._should_dedup_log("entry_hold_stale", 2.0):
                    age_label = age_ms if age_ms is not None else "?"
                    self._logger(
                        f"[ENTRY_HOLD_STALE] age_ms={age_label} source={source} "
                        f"{entry_context}"
                    )
                if not self._should_dedup_log("entry_skip_stale", 2.0):
                    age_label = age_ms if age_ms is not None else "?"
                    self._logger(
                        f"[ENTRY_REPRICE_SKIP] reason=stale age_ms={age_label} "
                        f"{entry_context}"
                    )
            if not self._should_dedup_log("entry_state", 0.4):
                cycle_label = self._cycle_id_label()
                order_id = buy_order.get("orderId")
                order_price = float(buy_order.get("price") or 0.0)
                order_price_label = "?" if order_price <= 0 else f"{order_price:.8f}"
                bid_label = "?" if bid is None else f"{bid:.8f}"
                ref_label = (
                    "?" if self._entry_last_ref_bid is None else f"{self._entry_last_ref_bid:.8f}"
                )
                self._logger(
                    "[ENTRY_STATE] "
                    f"cycle_id={cycle_label} order_id={order_id} order_price={order_price_label} "
                    f"best_bid={bid_label} last_ref_bid={ref_label} "
                    "moved_ticks=0 can_reprice=False "
                    f"reason={status} {entry_context}"
                )
            return
        self._clear_price_wait("ENTRY")
        elapsed_ms = self._entry_attempt_elapsed_ms()
        if elapsed_ms is not None and not self._should_dedup_log("entry_wait", 2.0):
            self._logger(f"[ENTRY_WAIT] ms={elapsed_ms} {entry_context}")
        if not self._profile.tick_size or not self._profile.step_size:
            return
        current_price = float(buy_order.get("price") or 0.0)
        if current_price <= 0:
            return
        tick_size = self._profile.tick_size
        now_ms = int(time.time() * 1000)
        last_ref_bid = self._entry_last_ref_bid
        if last_ref_bid is None:
            last_ref_bid = bid
        moved_ticks = 0
        ref_moved = False
        if bid is not None:
            moved_ticks = self._ticks_between(last_ref_bid, bid, tick_size)
            ref_moved = moved_ticks >= 1
        if self._entry_order_price is None:
            self._entry_order_price = current_price
        cooldown_ms = max(
            0, int(getattr(self._settings, "entry_reprice_cooldown_ms", 1200))
        )
        min_reprice_interval_ms = 250
        require_stable = bool(
            getattr(self._settings, "entry_reprice_require_stable_source", True)
        )
        stable_grace_ms = max(
            0, int(getattr(self._settings, "entry_reprice_stable_source_grace_ms", 3000))
        )
        min_fresh_reads = max(
            1,
            int(
                getattr(
                    self._settings, "entry_reprice_min_consecutive_fresh_reads", 2
                )
            ),
        )
        stable_ok = True
        if require_stable and self._entry_last_ws_bad_ts is not None:
            stable_ok = (now - self._entry_last_ws_bad_ts) * 1000.0 >= stable_grace_ms
        if ws_bad and source == "WS":
            self._entry_consecutive_fresh_reads = 0
            reprice_reason = "ws_bad"
            can_reprice = False
        else:
            self._entry_consecutive_fresh_reads += 1
            reprice_reason = "ready"
            can_reprice = True
            if not stable_ok:
                reprice_reason = "not_stable"
                can_reprice = False
            elif self._entry_consecutive_fresh_reads < min_fresh_reads:
                reprice_reason = "not_enough_fresh"
                can_reprice = False
            elif not ref_moved:
                reprice_reason = "ref_not_moved"
                can_reprice = False
        new_price = None
        if bid is not None:
            new_price = self._round_to_step(bid, tick_size)
        if can_reprice:
            reprice_reason = "ref_moved"
            if new_price is None:
                reprice_reason = "price_invalid"
                can_reprice = False
            elif new_price == current_price:
                reprice_reason = "already_at_best_bid"
                can_reprice = False
            elif (now - self._entry_reprice_last_ts) * 1000.0 < min_reprice_interval_ms:
                reprice_reason = "min_interval"
                can_reprice = False
            elif (now - self._entry_reprice_last_ts) * 1000.0 < cooldown_ms:
                reprice_reason = "cooldown"
                can_reprice = False
        order_id = buy_order.get("orderId")
        order_price_label = f"{current_price:.8f}"
        bid_label = "?" if bid is None else f"{bid:.8f}"
        ref_label = "?" if last_ref_bid is None else f"{last_ref_bid:.8f}"
        reason_label = reprice_reason
        can_reprice_label = "True" if can_reprice else "False"
        cycle_label = self._cycle_id_label()
        if not self._should_dedup_log("entry_state", 0.4):
            self._logger(
                "[ENTRY_STATE] "
                f"cycle_id={cycle_label} order_id={order_id} order_price={order_price_label} "
                f"best_bid={bid_label} last_ref_bid={ref_label} "
                f"moved_ticks={moved_ticks} can_reprice={can_reprice_label} "
                f"reason={reason_label} {entry_context}"
            )
        if not can_reprice:
            if not self._should_dedup_log("entry_reprice_skip", 0.4):
                new_price_label = "?" if new_price is None else f"{new_price:.8f}"
                self._logger(
                    "[ENTRY_REPRICE] action=SKIP "
                    f"reason={reason_label} old_price={order_price_label} "
                    f"new_price={new_price_label} {entry_context}"
                )
            if bid is not None and (self._entry_last_ref_bid is None or ref_moved):
                self._entry_last_ref_bid = bid
                self._entry_last_ref_ts_ms = now_ms
            return
        if bid is not None and (self._entry_last_ref_bid is None or ref_moved):
            self._entry_last_ref_bid = bid
            self._entry_last_ref_ts_ms = now_ms
        if not self._should_dedup_log("entry_reprice_do", 0.4):
            new_price_label = "?" if new_price is None else f"{new_price:.8f}"
            self._logger(
                "[ENTRY_REPRICE] action=DO "
                f"reason={reason_label} old_price={order_price_label} "
                f"new_price={new_price_label} {entry_context}"
            )
        if order_id:
            status_label = str(buy_order.get("status") or "").upper()
            if status_label not in {"FILLED", "PARTIALLY_FILLED"}:
                is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
                cancel_result = False
                final_status = "UNKNOWN"
                if order_id in open_orders:
                    cancel_result = self._cancel_margin_order(
                        symbol=self._settings.symbol,
                        order_id=order_id,
                        is_isolated=is_isolated,
                        tag=buy_order.get("tag", self.TAG),
                    )
                live_order = self._get_margin_order_snapshot(order_id)
                if live_order:
                    final_status = str(live_order.get("status") or "").upper()
                self._logger(
                    "[ENTRY_REPRICE_CANCEL] "
                    f"old_id={order_id} new_price={new_price:.8f} "
                    f"cancel_result={cancel_result} final_status_old={final_status}"
                )
                if final_status in {"FILLED", "PARTIALLY_FILLED"}:
                    self._logger(
                        "[ENTRY_REPRICE_ABORT] "
                        f"old_id={order_id} status={final_status}"
                    )
                    if live_order:
                        executed_qty = float(
                            live_order.get("executedQty", 0.0)
                            or live_order.get("executedQuantity", 0.0)
                            or 0.0
                        )
                        cum_quote = float(
                            live_order.get("cummulativeQuoteQty", 0.0) or 0.0
                        )
                        avg_price = 0.0
                        if executed_qty > 0 and cum_quote > 0:
                            avg_price = cum_quote / executed_qty
                        if final_status == "FILLED" and executed_qty > 0:
                            buy_order["qty"] = executed_qty
                            buy_order["price"] = avg_price or buy_order.get("price")
                            buy_order["status"] = "FILLED"
                            self._apply_order_filled(buy_order)
                        elif final_status == "PARTIALLY_FILLED" and executed_qty > 0:
                            self._apply_entry_partial_fill(
                                order_id, executed_qty, avg_price or None
                            )
                    self.entry_cancel_pending = False
                    return
                if final_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                    self.entry_cancel_pending = False
                elif not cancel_result:
                    return
                else:
                    self.entry_cancel_pending = True
                    return
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        order_quote = float(self._settings.order_quote)
        qty = self._round_down(order_quote / new_price, self._profile.step_size)
        if self._profile.min_qty and qty < self._profile.min_qty:
            return
        if self._profile.min_notional is not None and qty * new_price < self._profile.min_notional:
            return
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        timestamp = int(time.time() * 1000)
        buy_client_id = self._build_client_order_id("BUY", timestamp)
        side_effect_type = self._normalize_side_effect_type(
            self._settings.side_effect_type
        )
        self._log_entry_place(new_price, bid, ask, entry_context)
        buy_order = self._place_margin_order(
            symbol=self._settings.symbol,
            side="BUY",
            quantity=qty,
            price=new_price,
            is_isolated=is_isolated,
            client_order_id=buy_client_id,
            side_effect=side_effect_type,
            order_type="LIMIT",
        )
        if not buy_order:
            return
        cycle_label = self._cycle_id_label()
        self._logger(
            f"[ORDER] BUY placed cycle_id={cycle_label} qty={qty:.8f} "
            f"price={new_price:.8f} id={buy_order.get('orderId')}"
        )
        self._entry_reprice_last_ts = now
        self._entry_order_price = new_price
        now_ms = int(time.time() * 1000)
        self.active_test_orders = [
            {
                "orderId": buy_order.get("orderId"),
                "side": "BUY",
                "price": new_price,
                "bid_at_place": bid,
                "qty": qty,
                "cum_qty": 0.0,
                "avg_fill_price": 0.0,
                "last_fill_price": None,
                "created_ts": now_ms,
                "updated_ts": now_ms,
                "status": "NEW",
                "tag": self.TAG,
                "clientOrderId": buy_order.get("clientOrderId", buy_client_id),
                "cycle_id": self._current_cycle_id,
            }
        ]
        self.entry_active_order_id = buy_order.get("orderId")
        self.entry_active_price = new_price
        self.entry_cancel_pending = False
        self._entry_last_ref_bid = bid
        self._entry_last_ref_ts_ms = now_ms
        self.orders_count = len(self.active_test_orders)

    def _maybe_handle_sell_reprice(self, open_orders: dict[int, dict]) -> None:
        if not self._in_exit_workflow():
            return
        sell_order = self._find_order("SELL")
        if sell_order and self.sell_active_order_id is None:
            self.sell_active_order_id = sell_order.get("orderId")
        if not sell_order:
            if self.sell_retry_pending or self._pending_sell_retry_reason:
                self._maybe_place_sell_retry()
                return
            if self.sell_cancel_pending:
                self._handle_sell_cancel_confirmed(None, self.sell_active_order_id)
            return
        if sell_order.get("status") == "FILLED":
            return
        if self.sell_cancel_pending:
            self._maybe_confirm_sell_cancel(sell_order, open_orders)
            return
        order_id = sell_order.get("orderId")
        intent = self._normalize_exit_intent(self.exit_intent or sell_order.get("reason"))
        price_state, _ = self._router.build_price_state()
        if price_state.data_blind:
            self._logger(
                "[REPRICE_SKIP reason=data_blind] "
                f"exit_intent={intent} ref_old=— ref_new=— tick_delta=—"
            )
            return
        ref_label = self._resolve_exit_ref_side(intent)
        ref_now = price_state.bid if ref_label == "bid" else price_state.ask
        ref_old = (
            self._last_sell_ref_price
            if self._last_sell_ref_side == ref_label
            else None
        )
        if ref_now is None or ref_old is None or not self._profile.tick_size:
            ref_old_label = "—" if ref_old is None else f"{ref_old:.8f}"
            ref_new_label = "—" if ref_now is None else f"{ref_now:.8f}"
            self._logger(
                "[REPRICE_SKIP reason=ref_missing] "
                f"exit_intent={intent} ref_old={ref_old_label} "
                f"ref_new={ref_new_label} tick_delta=—"
            )
            return
        tick_delta = abs((ref_now - ref_old) / self._profile.tick_size)
        if tick_delta < self.PRICE_MOVE_TICK_THRESHOLD:
            self._logger(
                "[REPRICE_SKIP reason=ref_not_moved] "
                f"exit_intent={intent} ref_old={ref_old:.8f} "
                f"ref_new={ref_now:.8f} tick_delta={tick_delta:.2f}"
            )
            return
        self._logger(
            "[REPRICE_DO reason=ref_moved] "
            f"exit_intent={intent} ref_old={ref_old:.8f} "
            f"ref_new={ref_now:.8f} tick_delta={tick_delta:.2f}"
        )
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
                cycle_id = self._get_cycle_id_for_order(sell_order)
                prev_cum = float(sell_order.get("cum_qty") or 0.0)
                prev_avg = float(sell_order.get("avg_fill_price") or 0.0)
                if status:
                    sell_order["status"] = status
                    sell_order["sell_status"] = status
                delta = 0.0
                if executed_qty > 0:
                    delta = self._update_sell_execution(sell_order, executed_qty)
                if avg_price > 0:
                    prev_notional = prev_avg * prev_cum
                    new_notional = avg_price * executed_qty
                    notional_delta = max(new_notional - prev_notional, 0.0)
                    if notional_delta > 0:
                        self._aggregate_sold_notional += notional_delta
                    sell_order["avg_fill_price"] = avg_price
                    sell_order["last_fill_price"] = avg_price
                if delta > 0:
                    self._log_cycle_fill(cycle_id, "SELL", delta, executed_qty)
                if status == "FILLED":
                    sell_order["qty"] = (
                        executed_qty if executed_qty > 0 else sell_order.get("qty")
                    )
                    if avg_price > 0:
                        sell_order["price"] = avg_price
                    self._apply_order_filled(sell_order)
                    self.active_test_orders = [
                        entry
                        for entry in self.active_test_orders
                        if entry.get("orderId") != order_id
                    ]
                    self.orders_count = len(self.active_test_orders)
                    return
                if status in {"CANCELED", "REJECTED", "EXPIRED"}:
                    self._handle_sell_cancel_confirmed(sell_order, order_id)
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
                    self._handle_sell_cancel_confirmed(sell_order, order_id)
                else:
                    self.sell_cancel_pending = True
                    if not self._should_dedup_log(f"sell_cancel_wait:{order_id}", 2.0):
                        self._logger(f"[SELL_CANCEL_WAIT] id={order_id}")
                return
            return
        self._handle_sell_cancel_confirmed(sell_order, order_id)

    def _maybe_handle_tp_hybrid(self, open_orders: dict[int, dict]) -> bool:
        if not self._in_exit_workflow():
            return False
        sell_order = self._find_order("SELL")
        if not sell_order or sell_order.get("status") == "FILLED":
            return False
        intent = self._normalize_exit_intent(self.exit_intent or sell_order.get("reason"))
        if intent != "TP":
            return False
        if self.sell_cancel_pending:
            return False
        if self._exit_started_ts is None:
            self._exit_started_ts = time.monotonic()
        elapsed_ms = self._exit_elapsed_ms()
        maker_grace_ms, force_cross_ms, _ = self._tp_hybrid_params()
        price_state, _ = self._router.build_price_state()
        bid = price_state.bid
        ask = price_state.ask
        tick_size = self._profile.tick_size
        buy_price = self.get_buy_price()
        position_side = "LONG"
        if self.position is not None:
            position_side = str(self.position.get("side", "LONG") or "LONG").upper()
        tp_price = None
        if buy_price is not None and tick_size and self._settings.take_profit_ticks:
            ticks = int(self._settings.take_profit_ticks)
            if position_side == "SHORT":
                tp_price = buy_price - ticks * tick_size
            else:
                tp_price = buy_price + ticks * tick_size
        price_retraced = False
        if tp_price is not None:
            if position_side == "SHORT":
                if bid is not None and bid > tp_price:
                    price_retraced = True
            else:
                if ask is not None and ask < tp_price:
                    price_retraced = True
        if elapsed_ms is not None and elapsed_ms >= force_cross_ms:
            order_id = sell_order.get("orderId")
            self._logger(
                f"[TP_FORCE_MARKET] elapsed_ms={elapsed_ms} action=market_close"
            )
            if order_id:
                self._cancel_margin_order(
                    symbol=self._settings.symbol,
                    order_id=order_id,
                    is_isolated="TRUE" if self._settings.margin_isolated else "FALSE",
                    tag=sell_order.get("tag", self.TAG),
                )
            remaining_qty_raw = self._resolve_remaining_qty_raw()
            if self._profile.step_size:
                qty = self._round_down(remaining_qty_raw, self._profile.step_size)
            else:
                qty = remaining_qty_raw
            if qty > 0:
                self._place_emergency_market_sell(qty, force=True)
                return True
            return False
        if self._tp_exit_phase != "MAKER":
            return False
        should_cross = False
        reason = None
        if elapsed_ms is not None and elapsed_ms >= maker_grace_ms:
            should_cross = True
            reason = "grace_elapsed"
        elif price_retraced:
            should_cross = True
            reason = "price_retraced"
        if not should_cross:
            return False
        self._tp_exit_phase = "CROSS"
        order_id = sell_order.get("orderId")
        elapsed_label = "?" if elapsed_ms is None else str(elapsed_ms)
        self._logger(
            "[TP_MAKER_EXPIRE] "
            f"reason={reason} elapsed_ms={elapsed_label} action=cancel_to_cross"
        )
        if order_id and order_id in open_orders:
            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            cancelled = self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=sell_order.get("tag", self.TAG),
            )
            if cancelled:
                self.sell_cancel_pending = True
        else:
            self._handle_sell_cancel_confirmed(sell_order, order_id)
        return True

    def _maybe_handle_sell_watchdog(self, open_orders: dict[int, dict]) -> bool:
        if not self._in_exit_workflow():
            return False
        max_wait_ms = self._resolve_exit_timeout_ms()
        if max_wait_ms <= 0:
            return False
        if self._sell_wait_started_ts is None:
            self._start_sell_wait()
            return False
        elapsed_ms = int((time.monotonic() - self._sell_wait_started_ts) * 1000.0)
        if elapsed_ms <= max_wait_ms:
            return False
        self._logger(
            f"[SELL_STUCK_TIMEOUT] waited_ms={elapsed_ms} "
            f"limit_ms={max_wait_ms} action=force_close_market"
        )
        self._logger("[EXIT_EMERGENCY] reason=timeout")
        for order in list(self.active_test_orders):
            if order.get("side") != "SELL":
                continue
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order.get("orderId"),
                is_isolated="TRUE" if self._settings.margin_isolated else "FALSE",
                tag=order.get("tag", self.TAG),
            )
        remaining_qty_raw = self._resolve_remaining_qty_raw()
        if self._profile.step_size:
            qty = self._round_down(remaining_qty_raw, self._profile.step_size)
        else:
            qty = remaining_qty_raw
        if qty > 0:
            self._place_emergency_market_sell(qty, force=True)
            return True
        self._finalize_sell_timeout()
        return True

    def _finalize_sell_timeout(self) -> None:
        self._sell_wait_started_ts = None
        self._sell_retry_count = 0
        self.sell_active_order_id = None
        self.sell_cancel_pending = False
        self.sell_retry_pending = False
        self._pending_sell_retry_reason = None
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("side") != "SELL"
        ]
        self.orders_count = len(self.active_test_orders)
        self.last_exit_reason = "SELL_TIMEOUT"
        self.position = None
        self._reset_position_tracking()
        self._transition_state(TradeState.STATE_FLAT)
        self._transition_state(TradeState.STATE_IDLE)
        self._handle_cycle_completion(reason="SELL_TIMEOUT")

    def _schedule_sell_retry(self, reason: str) -> None:
        remaining_qty_raw = self._resolve_remaining_qty_raw()
        remaining_qty = self._round_down(
            remaining_qty_raw, self._profile.step_size
        )
        self._logger(
            "[SELL_RETRY_PENDING] "
            f"intent={reason} remaining={remaining_qty:.8f}"
        )
        self.sell_retry_pending = True
        self._pending_sell_retry_reason = reason
        if not self._in_exit_workflow():
            self._transition_state(self._exit_state_for_intent(reason))

    def _maybe_place_sell_retry(self) -> bool:
        if not self.sell_retry_pending and not self._pending_sell_retry_reason:
            return False
        if self.sell_place_inflight or self.sell_cancel_pending:
            return False
        if self._has_active_order("SELL"):
            self._logger("[SELL_SKIP_DUPLICATE] reason=active_sell")
            return False
        if not self._in_exit_workflow():
            return False
        intent = self._normalize_exit_intent(
            self._pending_sell_retry_reason or self.exit_intent or "MANUAL"
        )
        snapshot = self._router.get_mid_snapshot(
            int(getattr(self._settings, "exit_max_age_ms", 2500))
        )
        ok, mid, age_ms, source, status = snapshot
        if not ok:
            self._start_price_wait("EXIT")
            self._log_price_wait("EXIT", age_ms, source)
            if not self._in_exit_workflow():
                self._transition_state(self._exit_state_for_intent(intent))
                self._start_sell_wait()
            return False
        self._clear_price_wait("EXIT")
        price_state, _ = self._router.build_price_state()
        bid = price_state.bid
        ask = price_state.ask
        exit_order_type = self._normalize_order_type(self._settings.exit_order_type)
        if exit_order_type == "LIMIT" and ((ask is None and bid is None) or not self._profile.tick_size):
            self._start_price_wait("EXIT")
            self._log_price_wait("EXIT", age_ms, source)
            if not self._in_exit_workflow():
                self._transition_state(self._exit_state_for_intent(intent))
                self._start_sell_wait()
            return False
        new_price = None
        if exit_order_type == "LIMIT":
            (
                new_price,
                _,
                _,
                _,
            ) = self._compute_exit_sell_price_dynamic(
                intent=intent,
                bid=bid,
                ask=ask,
                tick_size=self._profile.tick_size,
            )
        price_label = "MARKET" if new_price is None else f"{new_price:.8f}"
        mid_label = "—" if mid is None else f"{mid:.8f}"
        bid_label = "—" if bid is None else f"{bid:.8f}"
        ask_label = "—" if ask is None else f"{ask:.8f}"
        self._logger(
            "[SELL_REPRICE] "
            f"mid={mid_label} bid={bid_label} ask={ask_label} new_price={price_label}"
        )
        max_retries = int(self._settings.max_sell_retries)
        if self._sell_retry_count >= max_retries:
            self._logger("[SELL_RETRY_LIMIT] max retries reached")
            return True
        self._sell_retry_count += 1
        retry_index = self._sell_retry_count
        self._logger(
            f"[SELL_RETRY {retry_index}/{max_retries} price={price_label}]"
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
        self.sell_retry_pending = False
        reason = str(self._pending_sell_retry_reason or self.exit_intent or "MANUAL").upper()
        self._pending_sell_retry_reason = None
        placed = self._place_sell_order(
            reason=reason,
            reset_retry=False,
            mid_override=mid,
            price_override=new_price,
            exit_intent=intent,
            ref_bid=bid,
            ref_ask=ask,
            tick_size=self._profile.tick_size,
        )
        if placed:
            return True
        self.sell_retry_pending = True
        self._pending_sell_retry_reason = reason
        if not self._in_exit_workflow():
            self._transition_state(self._exit_state_for_intent(intent))
            self._start_sell_wait()
        return False

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
        if self.entry_active_order_id is None:
            buy_order = self._find_order("BUY")
            if buy_order:
                self.entry_active_order_id = buy_order.get("orderId")
                self.entry_active_price = buy_order.get("price")
        self._handle_entry_follow_top(open_map)
        if self._maybe_handle_tp_hybrid(open_map):
            return
        if self._maybe_handle_sell_watchdog(open_map):
            return
        self._maybe_handle_sell_reprice(open_map)
        self._maybe_place_sell_retry()
        self._attempt_recovery_sell()

    def handle_order_filled(
        self, order_id: int, side: str, price: float, qty: float, ts_ms: int
    ) -> None:
        order = next(
            (entry for entry in self.active_test_orders if entry.get("orderId") == order_id),
            None,
        )
        if order is None:
            return
        side_upper = str(side).upper()
        cycle_id = self._get_cycle_id_for_order(order)
        if not self._should_process_fill(order_id, side_upper, qty, price, cycle_id):
            return
        if order.get("status") == "FILLED":
            return
        order["status"] = "FILLED"
        if order.get("side") == "SELL":
            order["sell_status"] = "FILLED"
        order["updated_ts"] = ts_ms
        order["price"] = price
        order["qty"] = qty
        self._apply_order_filled(order, dedup_checked=True)
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
        cycle_id = self._get_cycle_id_for_order(order)
        if not self._should_process_fill(order_id, side_upper, cum_qty, avg_price, cycle_id):
            return
        if side_upper == "BUY":
            prev_cum = float(order.get("cum_qty") or 0.0)
            order["cum_qty"] = cum_qty
            if avg_price > 0:
                order["avg_fill_price"] = avg_price
                order["last_fill_price"] = avg_price
            order["updated_ts"] = ts_ms
            delta = max(cum_qty - prev_cum, 0.0)
            if delta > 0:
                self._log_cycle_fill(cycle_id, "BUY", delta, cum_qty)
                fill_price = avg_price if avg_price > 0 else order.get("price")
                if self.position is None:
                    self.position = {
                        "buy_price": fill_price,
                        "qty": cum_qty,
                        "opened_ts": ts_ms,
                        "partial": True,
                        "initial_qty": cum_qty,
                    }
                    self._reset_exit_intent()
                else:
                    if fill_price is not None:
                        self.position["buy_price"] = fill_price
                    self.position["partial"] = True
                    if not self.position.get("opened_ts"):
                        self.position["opened_ts"] = ts_ms
                if self.position_qty_base is None:
                    self._set_position_qty_base(cum_qty)
                else:
                    base_qty = float(cum_qty)
                    if self._profile.step_size:
                        base_qty = self._round_down(base_qty, self._profile.step_size)
                    self.position_qty_base = base_qty
                    if self.position is not None:
                        self.position["initial_qty"] = base_qty
                if self.position is not None:
                    remaining_qty = self._resolve_remaining_qty_raw()
                    self.position["qty"] = remaining_qty
                self._transition_to_position_state()
                action, sell_qty = self._sync_sell_for_partial(delta)
                price_label = "—" if avg_price <= 0 else f"{avg_price:.8f}"
                self._logger(
                    "[BUY_FILL] "
                    f"status=PARTIALLY_FILLED executed_qty={cum_qty:.8f} "
                    f"delta={delta:.8f} avg_price={price_label} "
                    f"-> action={action} qty={sell_qty:.8f}"
                )
                self._log_trade_snapshot(reason="entry_partial")
            pos_qty = self.position.get("qty") if self.position is not None else 0.0
            avg_value = avg_price
            if avg_value <= 0 and self.position is not None:
                avg_value = float(self.position.get("buy_price") or 0.0)
            cycle_label = "—" if cycle_id is None else str(cycle_id)
            self._logger(
                "[BUY_PARTIAL] "
                f"cycle_id={cycle_label} delta={delta:.8f} "
                f"pos_qty={pos_qty:.8f} avg={avg_value:.8f}"
            )
            return
        if side_upper == "SELL":
            prev_cum = float(order.get("cum_qty") or 0.0)
            prev_avg = float(order.get("avg_fill_price") or 0.0)
            delta = self._update_sell_execution(order, cum_qty)
            if avg_price > 0:
                prev_notional = prev_avg * prev_cum
                new_notional = avg_price * cum_qty
                notional_delta = max(new_notional - prev_notional, 0.0)
                if notional_delta > 0:
                    self._aggregate_sold_notional += notional_delta
                order["avg_fill_price"] = avg_price
                order["last_fill_price"] = avg_price
            order["updated_ts"] = ts_ms
            if self.position is None:
                return
            remaining_qty = self._resolve_remaining_qty_raw()
            self.position["qty"] = remaining_qty
            if delta > 0:
                self._log_cycle_fill(cycle_id, "SELL", delta, cum_qty)
                cycle_label = "—" if cycle_id is None else str(cycle_id)
                self._logger(
                    "[SELL_PARTIAL] "
                    f"cycle_id={cycle_label} delta={delta:.8f} "
                    f"remaining={remaining_qty:.8f}"
                )
                self._sync_sell_for_partial(delta, allow_replace=False)
            if remaining_qty <= self._get_step_size_tolerance():
                self._finalize_partial_close(reason="PARTIAL")
                return

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
        cycle_id = self._get_cycle_id_for_order(order)
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        self._logger(
            f"[ORDER] {side} {status} cycle_id={cycle_label} id={order_id}"
        )
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        if side == "BUY":
            self._reset_buy_retry()
            self._clear_entry_attempt()
            if self.entry_active_order_id == order_id:
                self.entry_active_order_id = None
                self.entry_active_price = None
                self.entry_cancel_pending = False
            if self.position is None:
                self._transition_state(TradeState.STATE_IDLE)
            else:
                self._transition_to_position_state()
        elif side == "SELL":
            self._reset_sell_retry()
            self.sell_active_order_id = None
            self.sell_cancel_pending = False
            if self.position is not None:
                self._transition_to_position_state()
            else:
                self._transition_state(TradeState.STATE_IDLE)

    def _apply_order_filled(self, order: dict, dedup_checked: bool = False) -> None:
        side = order.get("side")
        cycle_id = self._get_cycle_id_for_order(order)
        order_id = order.get("orderId")
        qty = float(order.get("qty") or 0.0)
        price = float(order.get("price") or 0.0)
        side_label = str(side or "UNKNOWN").upper()
        if not dedup_checked:
            if not self._should_process_fill(order_id, side_label, qty, price, cycle_id):
                return
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        if side == "BUY":
            prev_cum = float(order.get("cum_qty") or 0.0)
            order["cum_qty"] = qty
            self._logger(
                "[ORDER] BUY filled "
                f"cycle_id={cycle_label} qty={qty:.8f} "
                f"price={price:.8f}"
            )
            delta = max(qty - prev_cum, 0.0)
            if delta > 0:
                self._log_cycle_fill(cycle_id, "BUY", delta, qty)
            self._reset_buy_retry()
            self._clear_entry_attempt()
            if self.entry_active_order_id == order_id:
                self.entry_active_order_id = None
                self.entry_active_price = None
                self.entry_cancel_pending = False
            if self.position is None:
                self.position = {
                    "buy_price": order.get("price"),
                    "qty": qty,
                    "opened_ts": order.get("updated_ts"),
                    "partial": False,
                    "initial_qty": qty,
                }
                self._set_position_qty_base(qty)
                self._reset_exit_intent()
            self._transition_to_position_state()
            action, sell_qty = self._sync_sell_for_partial(delta)
            price_label = "—" if price <= 0 else f"{price:.8f}"
            self._logger(
                "[BUY_FILL] "
                f"status=FILLED executed_qty={qty:.8f} delta={delta:.8f} "
                f"avg_price={price_label} -> action={action} qty={sell_qty:.8f}"
            )
            self._log_trade_snapshot(reason="entry_filled")
        elif side == "SELL":
            prev_cum = float(order.get("cum_qty") or 0.0)
            if qty > 0:
                self._update_sell_execution(order, qty)
                if price > 0:
                    prev_avg = float(order.get("avg_fill_price") or 0.0)
                    prev_notional = prev_avg * prev_cum
                    new_notional = price * qty
                    notional_delta = max(new_notional - prev_notional, 0.0)
                    if notional_delta > 0:
                        self._aggregate_sold_notional += notional_delta
                    order["avg_fill_price"] = price
                    order["last_fill_price"] = price
            self._reset_sell_retry()
            self.sell_active_order_id = None
            self.sell_cancel_pending = False
            reason = str(order.get("reason") or "MANUAL").upper()
            fill_price = order.get("price")
            buy_price = self.position.get("buy_price") if self.position else self._last_buy_price
            if reason in {"TP", "SL"}:
                fill_price_label = "—" if fill_price is None else f"{fill_price:.8f}"
                qty_label = "—" if qty <= 0 else f"{qty:.8f}"
                self._logger(
                    "[ORDER] SELL filled "
                    f"cycle_id={cycle_label} reason={reason} "
                    f"fill_price={fill_price_label} qty={qty_label}"
                )
            else:
                fill_price_label = "—" if fill_price is None else f"{fill_price:.8f}"
                qty_label = "—" if qty <= 0 else f"{qty:.8f}"
                self._logger(
                    "[ORDER] SELL filled "
                    f"cycle_id={cycle_label} qty={qty_label} "
                    f"price={fill_price_label}"
                )
            delta = max(qty - prev_cum, 0.0)
            if delta > 0:
                self._log_cycle_fill(cycle_id, "SELL", delta, qty)
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
            self._transition_state(TradeState.STATE_FLAT)
            self._finalize_cycle_close(reason=reason, pnl=pnl, qty=qty, buy_price=buy_price, cycle_id=cycle_id)

    def _apply_entry_partial_fill(
        self,
        order_id: Optional[int],
        qty: float,
        price: Optional[float],
    ) -> None:
        if qty <= 0:
            return
        cycle_id = self._get_cycle_id_for_order(self._find_order("BUY"))
        avg_price = price if price is not None else 0.0
        if not self._should_process_fill(order_id, "BUY", qty, avg_price, cycle_id):
            return
        price_label = "?" if price is None else f"{price:.8f}"
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        self._logger(
            "[ORDER] BUY partial timeout "
            f"cycle_id={cycle_label} qty={qty:.8f} price={price_label}"
        )
        self._log_cycle_fill(cycle_id, "BUY", qty, qty)
        self._reset_buy_retry()
        self._clear_entry_attempt()
        if self.position is None:
            self.position = {
                "buy_price": price,
                "qty": qty,
                "opened_ts": int(time.time() * 1000),
                "partial": True,
                "initial_qty": qty,
            }
            self._set_position_qty_base(qty)
            self._reset_exit_intent()
        else:
            if price is not None:
                self.position["buy_price"] = price
            self.position["partial"] = True
            if self.position_qty_base is None:
                self._set_position_qty_base(qty)
            else:
                base_qty = float(qty)
                if self._profile.step_size:
                    base_qty = self._round_down(base_qty, self._profile.step_size)
                self.position_qty_base = base_qty
                self.position["initial_qty"] = base_qty
            remaining_qty = self._resolve_remaining_qty_raw()
            self.position["qty"] = remaining_qty
        action, sell_qty = self._sync_sell_for_partial(qty)
        price_label = "—" if price is None else f"{price:.8f}"
        self._logger(
            "[BUY_FILL] "
            f"status=PARTIALLY_FILLED executed_qty={qty:.8f} delta={qty:.8f} "
            f"avg_price={price_label} -> action={action} qty={sell_qty:.8f}"
        )
        self._transition_to_position_state()
        self._log_trade_snapshot(reason="entry_partial_timeout")

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
        self._record_cycle_pnl(pnl)
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
        self._aggregate_sold_notional = 0.0
        self._reset_exit_intent()
        self._clear_recovery(reason="reset")

    def _reset_exit_intent(self) -> None:
        self.exit_intent = None
        self.exit_intent_set_ts = None
        self._last_sell_ref_price = None
        self._last_sell_ref_side = None
        self._exit_started_ts = None
        self._tp_exit_phase = None

    def _set_exit_intent(
        self,
        intent: str,
        mid: Optional[float],
        ticks: Optional[int] = None,
        force: bool = False,
    ) -> None:
        if self.exit_intent is not None and not force:
            return
        self.exit_intent = intent
        self.exit_intent_set_ts = time.monotonic()
        self._exit_started_ts = None
        self._tp_exit_phase = "MAKER" if intent == "TP" else None
        self._logger(f"[EXIT_INTENT] {intent}")
        ticks_label = "?" if ticks is None else str(ticks)
        mid_label = "—" if mid is None else f"{mid:.8f}"
        self._logger(
            f"[EXIT_INTENT_SET] intent={intent} ticks={ticks_label} mid={mid_label}"
        )

    def _override_to_sl(self, bid: Optional[float], ask: Optional[float]) -> None:
        if self.sell_active_order_id or self._has_active_order("SELL"):
            sell_order = self._get_active_sell_order()
            if sell_order:
                order_id = sell_order.get("orderId")
                is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
                if order_id:
                    self._logger(f"[SL_OVERRIDE] cancel_sell id={order_id}")
                    self._cancel_margin_order(
                        symbol=self._settings.symbol,
                        order_id=order_id,
                        is_isolated=is_isolated,
                        tag=sell_order.get("tag", self.TAG),
                    )
                self.sell_active_order_id = None
                self.sell_cancel_pending = False
        price_override = bid if bid is not None else ask
        self._place_sell_order(
            reason="SL",
            price_override=price_override,
            exit_intent="SL",
            ref_bid=bid,
            ref_ask=ask,
        )

    def _panic_exit(self, reason: str, bid: Optional[float], ask: Optional[float]) -> None:
        self._logger(f"[PANIC_EXIT] reason={reason}")
        self._cancel_entry_before_exit(timeout_s=1.0)
        price_override = bid if bid is not None else ask
        self._set_exit_intent("SL", mid=None, force=True)
        placed = self._place_sell_order(
            reason="PANIC",
            price_override=price_override,
            exit_intent="SL",
            ref_bid=bid,
            ref_ask=ask,
        )
        if not placed and self.position is not None:
            qty = self._round_down(
                self._resolve_remaining_qty_raw(), self._profile.step_size
            )
            if qty > 0:
                self._place_emergency_market_sell(qty, force=True)

    def _update_sell_execution(self, order: dict, cum_qty: float) -> float:
        prev_cum = float(order.get("cum_qty") or 0.0)
        delta = max(cum_qty - prev_cum, 0.0)
        if delta > 0:
            self._aggregate_sold_qty += delta
        order["cum_qty"] = cum_qty
        order["sell_executed_qty"] = cum_qty
        if self.position is not None:
            remaining_qty = self._resolve_remaining_qty_raw()
            self.position["qty"] = remaining_qty
        return delta

    def _sync_sell_for_partial(self, delta_qty: float, allow_replace: bool = True) -> tuple[str, float]:
        if delta_qty <= 0:
            return "SKIP_EMPTY", 0.0
        remaining_qty_raw = self._resolve_remaining_qty_raw()
        qty_to_sell = remaining_qty_raw
        if self._profile.step_size:
            qty_to_sell = self._round_down(remaining_qty_raw, self._profile.step_size)
        if qty_to_sell <= 0 or remaining_qty_raw <= self._get_step_size_tolerance():
            return "SKIP_EMPTY", qty_to_sell
        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            existing_qty = float(active_sell_order.get("qty") or 0.0)
            if math.isclose(
                existing_qty,
                qty_to_sell,
                rel_tol=0.0,
                abs_tol=self._get_step_size_tolerance(),
            ):
                self._logger(
                    f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill action=SKIP_SAME_QTY"
                )
                return "SKIP_SAME_QTY", qty_to_sell
            if not allow_replace:
                self._logger(
                    f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill action=SKIP_ACTIVE_SELL"
                )
                return "SKIP_ACTIVE_SELL", qty_to_sell
            if self.sell_cancel_pending:
                self._logger(
                    f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill action=SKIP_CANCEL_PENDING"
                )
                return "SKIP_CANCEL_PENDING", qty_to_sell
            order_id = active_sell_order.get("orderId")
            if order_id is None:
                self._logger(
                    f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill action=SKIP_NO_ID"
                )
                return "SKIP_NO_ID", qty_to_sell
            if not self._should_dedup_log(f"sell_replace_request:{order_id}", 1.0):
                self._logger(
                    f"[SELL_REPLACE_REQUEST] id={order_id} old_qty={existing_qty:.8f} "
                    f"new_qty={qty_to_sell:.8f}"
                )
            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            cancelled, error_code = self._cancel_margin_order_with_code(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=active_sell_order.get("tag", self.TAG),
            )
            if cancelled or error_code == -2011:
                self.sell_cancel_pending = False
                self.sell_active_order_id = None
                self.active_test_orders = [
                    entry
                    for entry in self.active_test_orders
                    if entry.get("orderId") != order_id
                ]
                self.orders_count = len(self.active_test_orders)
                self._logger(
                    f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill action=REPLACE"
                )
                self._place_sell_order(reason="partial_fill", exit_intent=self.exit_intent)
                return "REPLACE", qty_to_sell
            self.sell_cancel_pending = True
            self._logger(
                f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill action=WAIT_CANCEL"
            )
            return "WAIT_CANCEL", qty_to_sell
        self._logger(
            f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill action=PLACE"
        )
        self._place_sell_order(reason="partial_fill", exit_intent=self.exit_intent)
        return "PLACE", qty_to_sell

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
                if not self._in_exit_workflow():
                    self._transition_state(self._exit_state_for_intent(reason))
                    self._start_sell_wait()
                return 0
            total_available = base_free + base_locked
            if total_available < qty_needed:
                safety_epsilon = self._get_step_size_tolerance()
                available_qty = max(0.0, total_available - safety_epsilon)
                if self._profile.step_size:
                    available_qty = self._round_down(
                        available_qty, self._profile.step_size
                    )
                residual = max(qty_needed - available_qty, 0.0)
                self._set_recovery(residual, reason="SELL_PLACE_2010")
                if available_qty > 0:
                    return self._place_sell_order(
                        reason="RECOVERY",
                        exit_intent=self.exit_intent,
                        qty_override=available_qty,
                    )
                self._logger(
                    "[SELL_PLACE_2010] "
                    f"free={base_free:.8f} locked={base_locked:.8f} "
                    f"need={qty_needed:.8f} action=recovery_wait"
                )
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
            self._transition_to_position_state()
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
        return self._place_sell_order(
            reason=reason, reset_retry=False, exit_intent=self.exit_intent
        )

    def _finalize_cycle_close(
        self,
        reason: str,
        pnl: Optional[float],
        qty: Optional[float],
        buy_price: Optional[float],
        cycle_id: Optional[int],
    ) -> None:
        if pnl is not None:
            if pnl > 0:
                self.wins += 1
            elif pnl < 0:
                self.losses += 1
        pnl_bps = None
        if pnl is not None and buy_price and qty:
            notion = buy_price * qty
            if notion > 0:
                pnl_bps = (pnl / notion) * 10000
        self._log_cycle_close(cycle_id, pnl, pnl_bps, reason)
        self._handle_cycle_completion(reason=reason)
        self._transition_state(TradeState.STATE_FLAT)
        if self.run_active and self.cycles_done < self.cycles_target:
            self._transition_state(TradeState.STATE_NEXT_CYCLE)
        else:
            self._transition_state(TradeState.STATE_IDLE)
        self._cleanup_cycle_state(cycle_id)

    def _finalize_partial_close(self, reason: str) -> None:
        self.last_exit_reason = reason
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        for order in list(self.active_test_orders):
            order_id = order.get("orderId")
            if not order_id:
                continue
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=order.get("tag", self.TAG),
            )
        cycle_id = self._get_cycle_id_for_order(self._find_order("SELL"))
        if cycle_id is None:
            cycle_id = self._current_cycle_id
        qty = self.position_qty_base
        buy_price = self.position.get("buy_price") if self.position else self._last_buy_price
        pnl = None
        if qty and buy_price and self._aggregate_sold_notional > 0:
            pnl = self._aggregate_sold_notional - (buy_price * qty)
        self._record_cycle_pnl(pnl)
        self.position = None
        self._reset_position_tracking()
        self._transition_state(TradeState.STATE_FLAT)
        self._finalize_cycle_close(
            reason=reason,
            pnl=pnl,
            qty=qty,
            buy_price=buy_price,
            cycle_id=cycle_id,
        )

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

    def _get_active_entry_order(self) -> Optional[dict]:
        if self.entry_active_order_id is None:
            return self._find_order("BUY")
        return next(
            (
                entry
                for entry in self.active_test_orders
                if entry.get("orderId") == self.entry_active_order_id
            ),
            None,
        )

    @staticmethod
    def _entry_order_is_final(order: dict) -> bool:
        status = str(order.get("status") or "").upper()
        return status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}

    def _handle_entry_cancel_confirmed(
        self, buy_order: dict, order_id: Optional[int], status: str
    ) -> None:
        self.entry_cancel_pending = False
        if order_id is None:
            return
        buy_order["status"] = status
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        if self.entry_active_order_id == order_id:
            self.entry_active_order_id = None
            self.entry_active_price = None
        if status in {"CANCELED", "REJECTED", "EXPIRED"}:
            self._reset_buy_retry()
            self._clear_entry_attempt()
        self._logger(f"[ENTRY_CANCEL_CONFIRMED] id={order_id} status={status}")
        if self.position is not None:
            self._transition_to_position_state()

    def _maybe_confirm_entry_cancel(
        self, buy_order: dict, open_orders: dict[int, dict]
    ) -> None:
        order_id = buy_order.get("orderId")
        if order_id is None:
            self.entry_cancel_pending = False
            return
        live_order = self._get_margin_order_snapshot(order_id)
        status = str(buy_order.get("status") or "").upper()
        if live_order:
            status = str(live_order.get("status") or status or "").upper()
        if status in {"FILLED", "PARTIALLY_FILLED"}:
            executed_qty = 0.0
            cum_quote = 0.0
            if live_order:
                executed_qty = float(
                    live_order.get("executedQty", 0.0)
                    or live_order.get("executedQuantity", 0.0)
                    or 0.0
                )
                cum_quote = float(live_order.get("cummulativeQuoteQty", 0.0) or 0.0)
            avg_price = 0.0
            if executed_qty > 0 and cum_quote > 0:
                avg_price = cum_quote / executed_qty
            elif live_order:
                avg_price = float(
                    live_order.get("avgPrice", 0.0)
                    or live_order.get("price", 0.0)
                    or 0.0
                )
            self.entry_cancel_pending = False
            self._logger(
                "[ENTRY_CANCEL_ABORT] "
                f"id={order_id} status={status} executed={executed_qty:.8f}"
            )
            if status == "FILLED" and executed_qty > 0:
                buy_order["qty"] = executed_qty
                buy_order["price"] = avg_price if avg_price > 0 else buy_order.get("price")
                buy_order["status"] = "FILLED"
                self._apply_order_filled(buy_order)
            elif status == "PARTIALLY_FILLED" and executed_qty > 0:
                self._apply_entry_partial_fill(order_id, executed_qty, avg_price or None)
            return
        if status in {"CANCELED", "REJECTED", "EXPIRED"} or order_id not in open_orders:
            final_status = status if status else "CANCELED"
            self._handle_entry_cancel_confirmed(buy_order, order_id, final_status)
            return
        if not self._should_dedup_log(f"entry_cancel_wait:{order_id}", 2.0):
            self._logger(f"[ENTRY_CANCEL_WAIT] id={order_id}")

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
            self._handle_sell_cancel_confirmed(sell_order, order_id)
            return
        if order_id not in open_orders:
            self._handle_sell_cancel_confirmed(sell_order, order_id)
            return
        if not self._should_dedup_log(f"sell_cancel_wait:{order_id}", 2.0):
            self._logger(f"[SELL_CANCEL_WAIT] id={order_id}")

    def _finalize_sell_cancel(self, order_id: Optional[int]) -> None:
        if order_id is not None:
            self._logger(f"[SELL_CANCEL_CONFIRMED] id={order_id}")
        self.sell_cancel_pending = False
        self.sell_active_order_id = None
        self.sell_retry_pending = False
        if order_id is not None:
            self.active_test_orders = [
                entry
                for entry in self.active_test_orders
                if entry.get("orderId") != order_id
            ]
            self.orders_count = len(self.active_test_orders)
        self._sell_wait_started_ts = None

    def _handle_sell_cancel_confirmed(
        self, sell_order: Optional[dict], order_id: Optional[int]
    ) -> None:
        if order_id is None:
            self._finalize_sell_cancel(order_id)
            return
        if sell_order is None:
            sell_order = next(
                (
                    entry
                    for entry in self.active_test_orders
                    if entry.get("orderId") == order_id
                ),
                None,
            )
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
            if sell_order is not None:
                if status:
                    sell_order["status"] = status
                    sell_order["sell_status"] = status
                cycle_id = self._get_cycle_id_for_order(sell_order)
                prev_cum = float(sell_order.get("cum_qty") or 0.0)
                prev_avg = float(sell_order.get("avg_fill_price") or 0.0)
                delta = 0.0
                if executed_qty > 0:
                    delta = self._update_sell_execution(sell_order, executed_qty)
                if avg_price > 0:
                    prev_notional = prev_avg * prev_cum
                    new_notional = avg_price * executed_qty
                    notional_delta = max(new_notional - prev_notional, 0.0)
                    if notional_delta > 0:
                        self._aggregate_sold_notional += notional_delta
                    sell_order["avg_fill_price"] = avg_price
                    sell_order["last_fill_price"] = avg_price
                if delta > 0:
                    self._log_cycle_fill(cycle_id, "SELL", delta, executed_qty)
                if status == "FILLED":
                    sell_order["qty"] = (
                        executed_qty if executed_qty > 0 else sell_order.get("qty")
                    )
                    if avg_price > 0:
                        sell_order["price"] = avg_price
                    self._apply_order_filled(sell_order)
                    self.active_test_orders = [
                        entry
                        for entry in self.active_test_orders
                        if entry.get("orderId") != order_id
                    ]
                    self.orders_count = len(self.active_test_orders)
                    return
        if sell_order is not None:
            reason = str(sell_order.get("reason") or "MANUAL").upper()
        else:
            reason = str(self.exit_intent or "MANUAL").upper()
        self._finalize_sell_cancel(order_id)
        self._schedule_sell_retry(reason)

    def _retry_sell_after_cancel(self, sell_order: dict) -> None:
        max_retries = int(self._settings.max_sell_retries)
        if self._sell_retry_count >= max_retries:
            self._logger("[SELL_RETRY_LIMIT] max retries reached after cancel")
            return
        reason = str(sell_order.get("reason") or "MANUAL").upper()
        self._schedule_sell_retry(reason)
        self._maybe_place_sell_retry()

    def abort_cycle(self) -> int:
        return self.abort_cycle_with_reason(reason="ABORT")

    def abort_cycle_with_reason(self, reason: str, critical: bool = False) -> int:
        cycle_id = self._current_cycle_id
        if self.position is not None and self._resolve_remaining_qty_raw() > 0:
            self.run_active = False
            self._next_cycle_ready_ts = None
            self._logger(f"[STOP] abort_reason={reason} critical={critical}")
            self._flatten_position(f"ABORT:{reason}")
            return 0
        cancelled = self.cancel_test_orders_margin(reason="aborted")
        self._clear_entry_attempt()
        self._transition_state(TradeState.STATE_FLAT)
        self._transition_state(TradeState.STATE_IDLE)
        self.last_action = "aborted"
        self._handle_cycle_completion(reason=reason, critical=critical)
        self._cleanup_cycle_state(cycle_id)
        return cancelled

    def _purge_open_orders_before_cycle(self) -> bool:
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("openOrders", exc, {"symbol": self._settings.symbol})
            return False
        except Exception as exc:
            self._logger(f"[TRADE] openOrders error: {exc} tag={self.TAG}")
            return False
        if not open_orders:
            return True
        self._logger(
            f"[PRECHECK] open_orders_detected count={len(open_orders)} action=cancel_all"
        )
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        for order in open_orders:
            order_id = order.get("orderId")
            if not order_id:
                continue
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=self.TAG,
            )
        try:
            remaining = self._rest.get_margin_open_orders(self._settings.symbol)
        except Exception:
            return False
        if remaining:
            self._logger(
                f"[PRECHECK] open_orders_remaining count={len(remaining)}"
            )
            return False
        return True

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
        self.entry_active_order_id = None
        self.entry_active_price = None
        self.entry_cancel_pending = False
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
