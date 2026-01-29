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
        self._sell_wait_started_ts: Optional[float] = None
        self._sell_retry_count = 0
        self._sell_ttl_retry_count = 0
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
            if self._has_active_order("BUY") or self.position is not None:
                self._logger("[TRADE] place_test_orders | BUY already active")
                self.last_action = "buy_active"
                return 0
            price_state, health_state = self._router.build_price_state()
            mid = price_state.mid
            bid = price_state.bid
            ask = price_state.ask
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
            self._transition_state(TradeState.STATE_PLACE_BUY)
            self.last_action = "placing_buy"
            self._start_entry_attempt()
            self._entry_reprice_last_ts = time.monotonic()
            self._log_entry_place(buy_price, bid, ask)
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
                self._transition_state(TradeState.STATE_CLOSED)
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
            self._transition_state(TradeState.STATE_WAIT_BUY)
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
        if self.position is not None and self._settings.auto_exit_enabled:
            self._transition_state(TradeState.STATE_POSITION_OPEN)
            self._logger("[STOP] auto_exit closing position")
            qty = self._round_down(
                self._resolve_remaining_qty_raw(), self._profile.step_size
            )
            self._logger(f"[STOP] flatten_start qty={qty:.8f}")
            if self.exit_intent is None:
                self._set_exit_intent("MANUAL_CLOSE", mid=None)
            self._place_sell_order(reason="STOP", exit_intent=self.exit_intent)
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
        if self.state != TradeState.STATE_POSITION_OPEN:
            self._logger(
                f"[TRADE] close_position | blocked state={self.state.value}"
            )
            self.last_action = "state_blocked"
            return 0
        if self.exit_intent is None:
            self._set_exit_intent("MANUAL_CLOSE", mid=None)
        return self._place_sell_order(reason="manual", exit_intent=self.exit_intent)

    def evaluate_exit_conditions(self) -> None:
        if self.state != TradeState.STATE_POSITION_OPEN:
            return
        if not self._settings.auto_exit_enabled:
            return
        price_state, health_state = self._router.build_price_state()
        mid = price_state.mid
        mid_fresh_ms = int(self._settings.mid_fresh_ms)
        mid_age_ms = price_state.mid_age_ms
        if price_state.data_blind or mid_age_ms is None or mid_age_ms > mid_fresh_ms:
            if not self._exits_frozen:
                ws_age_label = health_state.ws_age_ms if health_state.ws_age_ms is not None else "?"
                http_age_label = (
                    health_state.http_age_ms if health_state.http_age_ms is not None else "?"
                )
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
        sl_offset_ticks: int,
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if tick_size is None or tick_size <= 0:
            return None, None, None, "tick_size_missing"
        policy, ref_side = TradeExecutor._resolve_exit_policy(intent)
        ref_price = ask if ref_side == "ask" else bid
        if ref_price is None:
            return None, None, None, "ref_missing"
        if intent == "SL":
            raw_price = ref_price - (sl_offset_ticks * tick_size)
        else:
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
        ref_bid: Optional[float] = None,
        ref_ask: Optional[float] = None,
        tick_size: Optional[float] = None,
        sl_offset_ticks: Optional[int] = None,
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
            tick_size = tick_size or self._profile.tick_size
            sl_ticks = (
                max(0, int(sl_offset_ticks))
                if sl_offset_ticks is not None
                else max(0, int(getattr(self._settings, "sl_offset_ticks", 0)))
            )
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
            sell_ref_price = None
            delta_ticks_to_ref = None
            exit_policy, sell_ref_label = self._resolve_exit_policy(intent)
            if exit_order_type == "LIMIT":
                if price_override is None:
                    (
                        sell_price,
                        sell_ref_price,
                        delta_ticks_to_ref,
                        exit_policy,
                    ) = self._compute_exit_sell_price(
                        intent=intent,
                        bid=bid,
                        ask=ask,
                        tick_size=tick_size or self._profile.tick_size,
                        sl_offset_ticks=sl_ticks,
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
            self._logger(
                f"[PLACE_SELL] cycle_id={cycle_label} price=best_ask best_ask={ask_label} "
                f"src={price_state.source}"
            )
            self._transition_state(TradeState.STATE_PLACE_SELL)
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
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} exit_intent={intent} exit_policy={exit_policy} "
                f"sell_ref={sell_ref_label} bid={bid_label} ask={ask_label} "
                f"mid={mid_label} sl_offset_ticks={sl_ticks} sell_price={price_label} "
                f"delta_ticks_to_ref={delta_label} qty={qty:.8f}"
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
                    f"cycle_id={cycle_label} reason={reason_upper} type={order_type} "
                    f"price={price_label} qty={qty:.8f} id={sell_order.get('orderId')}"
                )
            else:
                self._logger(
                    f"[ORDER] SELL placed cycle_id={cycle_label} qty={qty:.8f} "
                    f"price={price_label} id={sell_order.get('orderId')}"
                )
            self._transition_state(TradeState.STATE_WAIT_SELL)
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

    def _place_emergency_market_sell(self, qty: float) -> int:
        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            self._logger("[SELL_SKIP_DUPLICATE] reason=active_sell")
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
            price_state, _ = self._router.build_price_state()
            cycle_label = self._cycle_id_label()
            ask_label = "?" if price_state.ask is None else f"{price_state.ask:.8f}"
            self._logger(
                f"[PLACE_SELL] cycle_id={cycle_label} price=best_ask best_ask={ask_label} "
                f"src={price_state.source}"
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
                f"mid={mid_label} sl_offset_ticks=— sell_price=MARKET "
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
                self._transition_state(TradeState.STATE_POSITION_OPEN)
                return 0
            self._logger(
                "[ORDER] SELL placed "
                f"cycle_id={cycle_label} reason=EMERGENCY type=MARKET "
                f"qty={qty:.8f} id={sell_order.get('orderId')}"
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
        self._sell_ttl_retry_count = 0
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

    def _clear_entry_attempt(self) -> None:
        self._entry_attempt_started_ts = None

    def _entry_attempt_elapsed_ms(self) -> Optional[int]:
        if self._entry_attempt_started_ts is None:
            return None
        return int((time.monotonic() - self._entry_attempt_started_ts) * 1000.0)

    def _entry_ws_bad(self, ws_connected: bool) -> bool:
        ws_no_first_tick, ws_stale_ticks = self._router.get_ws_issue_counts()
        return (not ws_connected) or ws_no_first_tick >= 2 or ws_stale_ticks >= 2

    def _log_entry_degraded(self, ws_connected: bool) -> None:
        window_s = max(self._settings.price_wait_log_every_ms, 1) / 1000.0
        if self._should_dedup_log("entry_degraded:http", window_s):
            return
        ws_no_first_tick, ws_stale_ticks = self._router.get_ws_issue_counts()
        self._logger(
            "[ENTRY_DEGRADED_TO_HTTP] "
            f"reason=ws_bad ws_connected={ws_connected} "
            f"no_first_tick={ws_no_first_tick} stale_ticks={ws_stale_ticks}"
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
    ]:
        price_state, health_state = self._router.build_price_state()
        ws_bad = self._entry_ws_bad(health_state.ws_connected)
        if ws_bad:
            self._log_entry_degraded(health_state.ws_connected)
        bid = price_state.bid
        ask = price_state.ask
        mid_val = price_state.mid
        age_ms = price_state.mid_age_ms
        source = price_state.source
        if price_state.data_blind:
            return False, bid, ask, mid_val, age_ms, source, "data_blind", ws_bad
        if mid_val is None:
            return False, bid, ask, mid_val, age_ms, source, "no_mid", ws_bad
        if age_ms is None:
            return False, bid, ask, mid_val, age_ms, source, "no_age", ws_bad
        if age_ms > int(self._settings.mid_fresh_ms):
            return False, bid, ask, mid_val, age_ms, source, "stale", ws_bad
        if bid is None or ask is None:
            return False, bid, ask, mid_val, age_ms, source, "no_quote", ws_bad
        return True, bid, ask, mid_val, age_ms, source, "fresh", ws_bad

    def _calculate_entry_price(self, bid: float) -> Optional[float]:
        if not self._profile.tick_size:
            return None
        tick_size = self._profile.tick_size
        return self._round_to_step(bid, tick_size)

    def _log_entry_place(
        self, price: float, bid: Optional[float], ask: Optional[float]
    ) -> None:
        bid_label = "?" if bid is None else f"{bid:.8f}"
        ask_label = "?" if ask is None else f"{ask:.8f}"
        cycle_label = self._cycle_id_label()
        self._logger(
            f"[ENTRY_PLACE] cycle_id={cycle_label} price={price:.8f} "
            f"bid={bid_label} ask={ask_label}"
        )

    def _handle_entry_follow_top(self, open_orders: dict[int, dict]) -> None:
        if self.state != TradeState.STATE_WAIT_BUY:
            return
        buy_order = self._find_order("BUY")
        if not buy_order or buy_order.get("status") == "FILLED":
            return
        if self._buy_wait_started_ts is None:
            self._start_buy_wait()
        ok, bid, ask, mid, age_ms, source, status, ws_bad = self._resolve_entry_snapshot()
        now = time.monotonic()
        if ws_bad:
            self._entry_last_ws_bad_ts = now
        if not ok or bid is None or mid is None:
            self._entry_consecutive_fresh_reads = 0
            if ws_bad:
                if not self._should_dedup_log("entry_hold_wsbad", 2.0):
                    ws_no_first_tick, ws_stale_ticks = self._router.get_ws_issue_counts()
                    self._logger(
                        "[ENTRY_HOLD_WSBAD] "
                        f"no_first_tick={ws_no_first_tick} stale_ticks={ws_stale_ticks}"
                    )
                if not self._should_dedup_log("entry_skip_wsbad", 2.0):
                    age_label = age_ms if age_ms is not None else "?"
                    self._logger(
                        f"[ENTRY_REPRICE_SKIP] reason=ws_bad age_ms={age_label}"
                    )
            else:
                if not self._should_dedup_log("entry_hold_stale", 2.0):
                    age_label = age_ms if age_ms is not None else "?"
                    self._logger(
                        f"[ENTRY_HOLD_STALE] age_ms={age_label} source={source}"
                    )
                if not self._should_dedup_log("entry_skip_stale", 2.0):
                    age_label = age_ms if age_ms is not None else "?"
                    self._logger(
                        f"[ENTRY_REPRICE_SKIP] reason=stale age_ms={age_label}"
                    )
            return
        self._clear_price_wait("ENTRY")
        elapsed_ms = self._entry_attempt_elapsed_ms()
        if elapsed_ms is not None and not self._should_dedup_log("entry_wait", 2.0):
            self._logger(f"[ENTRY_WAIT] ms={elapsed_ms}")
        if not self._profile.tick_size or not self._profile.step_size:
            return
        current_price = float(buy_order.get("price") or 0.0)
        if current_price <= 0:
            return
        min_ticks = 1
        tick_size = self._profile.tick_size
        bid_at_place = buy_order.get("bid_at_place")
        if bid_at_place is None:
            bid_at_place = bid
        bid_move_ticks = abs(bid - bid_at_place) / tick_size
        spread_ticks = 0.0
        if ask is not None:
            spread_ticks = (ask - bid) / tick_size
        spread_reprice_threshold = max(
            1, int(getattr(self._settings, "spread_reprice_threshold", 2))
        )
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
        if ws_bad:
            self._entry_consecutive_fresh_reads = 0
            if not self._should_dedup_log("entry_skip_wsbad_live", 2.0):
                age_label = age_ms if age_ms is not None else "?"
                self._logger(f"[ENTRY_REPRICE_SKIP] reason=ws_bad age_ms={age_label}")
            return
        self._entry_consecutive_fresh_reads += 1
        if not stable_ok:
            if not self._should_dedup_log("entry_skip_not_stable", 2.0):
                age_label = age_ms if age_ms is not None else "?"
                self._logger(f"[ENTRY_REPRICE_SKIP] reason=not_stable age_ms={age_label}")
            return
        if self._entry_consecutive_fresh_reads < min_fresh_reads:
            if not self._should_dedup_log("entry_skip_fresh_reads", 2.0):
                age_label = age_ms if age_ms is not None else "?"
                self._logger(
                    f"[ENTRY_REPRICE_SKIP] reason=stale age_ms={age_label}"
                )
            self._entry_last_seen_bid = bid
            return
        reprice_reason = None
        if bid_move_ticks >= min_ticks:
            reprice_reason = "bid_move"
        elif spread_ticks >= spread_reprice_threshold:
            reprice_reason = "spread_expand"
        if reprice_reason is None:
            if not self._should_dedup_log("entry_skip_no_move", 2.0):
                age_label = age_ms if age_ms is not None else "?"
                self._logger(
                    f"[ENTRY_REPRICE_SKIP] reason=no_move age_ms={age_label}"
                )
            self._entry_last_seen_bid = bid
            return
        if (now - self._entry_reprice_last_ts) * 1000.0 < min_reprice_interval_ms:
            if not self._should_dedup_log("entry_skip_min_interval", 2.0):
                age_label = age_ms if age_ms is not None else "?"
                self._logger(
                    f"[ENTRY_REPRICE_SKIP] reason=min_interval age_ms={age_label}"
                )
            self._entry_last_seen_bid = bid
            return
        if (now - self._entry_reprice_last_ts) * 1000.0 < cooldown_ms:
            if not self._should_dedup_log("entry_skip_cooldown", 2.0):
                age_label = age_ms if age_ms is not None else "?"
                self._logger(
                    f"[ENTRY_REPRICE_SKIP] reason=cooldown age_ms={age_label}"
                )
            self._entry_last_seen_bid = bid
            return
        new_price = self._round_to_step(bid, tick_size)
        if new_price is None:
            self._entry_last_seen_bid = bid
            return
        if new_price == current_price:
            self._entry_last_seen_bid = bid
            return
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
        order_quote = float(self._settings.order_quote)
        qty = self._round_down(order_quote / new_price, self._profile.step_size)
        if self._profile.min_qty and qty < self._profile.min_qty:
            return
        if self._profile.min_notional is not None and qty * new_price < self._profile.min_notional:
            return
        spread_label = f"{spread_ticks:.2f}"
        bid_old_label = "?" if bid_at_place is None else f"{bid_at_place:.8f}"
        self._logger(
            "[ENTRY_REPRICE] "
            f"reason={reprice_reason} bid_old={bid_old_label} bid_new={bid:.8f} "
            f"spread_ticks={spread_label} new_price={new_price:.8f}"
        )
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        timestamp = int(time.time() * 1000)
        buy_client_id = self._build_client_order_id("BUY", timestamp)
        side_effect_type = self._normalize_side_effect_type(
            self._settings.side_effect_type
        )
        self._log_entry_place(new_price, bid, ask)
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
        self._entry_last_seen_bid = bid
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
        if self._sell_wait_started_ts is None:
            self._start_sell_wait()
            return
        elapsed_ms = (time.monotonic() - self._sell_wait_started_ts) * 1000.0
        if elapsed_ms < ttl_ms:
            return
        order_id = sell_order.get("orderId")
        intent = self._normalize_exit_intent(self.exit_intent or sell_order.get("reason"))
        self._sell_ttl_retry_count += 1
        self._logger(
            "[SELL_TTL_EXPIRED] "
            f"id={order_id} exit_intent={intent} "
            f"ttl_retry_count={self._sell_ttl_retry_count}"
        )
        max_sl_retries = int(getattr(self._settings, "max_sl_ttl_retries", 0) or 0)
        max_exit_total_ms = int(getattr(self._settings, "max_exit_total_ms", 0) or 0)
        exit_elapsed_ms = None
        if self.exit_intent_set_ts is not None:
            exit_elapsed_ms = int((time.monotonic() - self.exit_intent_set_ts) * 1000.0)
        if (
            max_exit_total_ms > 0
            and exit_elapsed_ms is not None
            and exit_elapsed_ms >= max_exit_total_ms
        ):
            timeout_label = "?" if exit_elapsed_ms is None else str(exit_elapsed_ms)
            self._logger(
                "[SELL_EMERGENCY_TRIGGER] "
                f"reason=exit_timeout ttl_retry_count={self._sell_ttl_retry_count} "
                f"exit_elapsed_ms={timeout_label}"
            )
            self._force_close_after_ttl(reason="EXIT_TIMEOUT")
            return
        if intent == "SL" and (
            max_sl_retries > 0 and self._sell_ttl_retry_count >= max_sl_retries
        ):
            timeout_label = "?" if exit_elapsed_ms is None else str(exit_elapsed_ms)
            self._logger(
                "[SELL_EMERGENCY_TRIGGER] "
                f"reason=sl_ttl_escalation ttl_retry_count={self._sell_ttl_retry_count} "
                f"exit_elapsed_ms={timeout_label}"
            )
            self._force_close_after_ttl(reason="SL_TTL")
            return
        max_retries = int(self._settings.max_sell_retries)
        if self._sell_retry_count >= max_retries:
            self._force_close_after_ttl()
            return
        price_state, _ = self._router.build_price_state()
        if price_state.data_blind:
            self._logger(
                "[REPRICE_SKIP reason=data_blind] "
                f"exit_intent={intent} ref_old=— ref_new=— tick_delta=—"
            )
            self._sell_wait_started_ts = time.monotonic()
            return
        _, ref_label = self._resolve_exit_policy(intent)
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
            self._sell_wait_started_ts = time.monotonic()
            return
        tick_delta = abs((ref_now - ref_old) / self._profile.tick_size)
        if tick_delta < self.PRICE_MOVE_TICK_THRESHOLD:
            self._logger(
                "[REPRICE_SKIP reason=ref_not_moved] "
                f"exit_intent={intent} ref_old={ref_old:.8f} "
                f"ref_new={ref_now:.8f} tick_delta={tick_delta:.2f}"
            )
            self._sell_wait_started_ts = time.monotonic()
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

    def _maybe_handle_sell_watchdog(self, open_orders: dict[int, dict]) -> bool:
        if self.state != TradeState.STATE_WAIT_SELL:
            return False
        max_wait_ms = int(getattr(self._settings, "max_wait_sell_ms", 0) or 0)
        if max_wait_ms <= 0:
            return False
        if self._sell_wait_started_ts is None:
            self._start_sell_wait()
            return False
        elapsed_ms = int((time.monotonic() - self._sell_wait_started_ts) * 1000.0)
        if elapsed_ms <= max_wait_ms:
            return False
        self._logger(f"[SELL_STUCK_TIMEOUT] waited_ms={elapsed_ms} action=force_close_market")
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
        qty = self._round_down(
            self._resolve_remaining_qty_raw(), self._profile.step_size
        )
        if qty > 0:
            self._place_emergency_market_sell(qty)
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
        self._transition_state(TradeState.STATE_CLOSED)
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
        if self.state != TradeState.STATE_PLACE_SELL:
            self._transition_state(TradeState.STATE_PLACE_SELL)

    def _maybe_place_sell_retry(self) -> bool:
        if not self.sell_retry_pending and not self._pending_sell_retry_reason:
            return False
        if self.sell_place_inflight or self.sell_cancel_pending:
            return False
        if self._has_active_order("SELL"):
            self._logger("[SELL_SKIP_DUPLICATE] reason=active_sell")
            return False
        if self.state not in {
            TradeState.STATE_WAIT_SELL,
            TradeState.STATE_PLACE_SELL,
        }:
            return False
        snapshot = self._router.get_mid_snapshot(int(self._settings.mid_fresh_ms))
        ok, mid, age_ms, source, status = snapshot
        if not ok:
            self._start_price_wait("EXIT")
            self._log_price_wait("EXIT", age_ms, source)
            if self.state != TradeState.STATE_WAIT_SELL:
                self._transition_state(TradeState.STATE_WAIT_SELL)
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
            if self.state != TradeState.STATE_WAIT_SELL:
                self._transition_state(TradeState.STATE_WAIT_SELL)
                self._start_sell_wait()
            return False
        new_price = None
        intent = self._normalize_exit_intent(
            self._pending_sell_retry_reason or self.exit_intent or "MANUAL"
        )
        if exit_order_type == "LIMIT":
            (
                new_price,
                _,
                _,
                _,
            ) = self._compute_exit_sell_price(
                intent=intent,
                bid=bid,
                ask=ask,
                tick_size=self._profile.tick_size,
                sl_offset_ticks=max(0, int(getattr(self._settings, "sl_offset_ticks", 0))),
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
            self._force_close_after_ttl()
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
            sl_offset_ticks=max(0, int(getattr(self._settings, "sl_offset_ticks", 0))),
        )
        if placed:
            return True
        self.sell_retry_pending = True
        self._pending_sell_retry_reason = reason
        if self.state != TradeState.STATE_WAIT_SELL:
            self._transition_state(TradeState.STATE_WAIT_SELL)
            self._start_sell_wait()
        return False

    def _force_close_after_ttl(self, reason: str = "TTL_EXCEEDED") -> None:
        self._logger(f"[SELL_FORCE_CLOSE reason={reason}]")
        emergency_reason = "timeout" if reason == "EXIT_TIMEOUT" else "ttl"
        self._logger(f"[EXIT_EMERGENCY] reason={emergency_reason}")
        self._sell_wait_started_ts = None
        self._sell_retry_count = 0
        self.sell_active_order_id = None
        self.sell_cancel_pending = False
        self.sell_retry_pending = False
        self._pending_sell_retry_reason = None
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
        qty = self._round_down(
            self._resolve_remaining_qty_raw(), self._profile.step_size
        )
        if qty <= 0:
            self.last_exit_reason = "TTL_EXCEEDED"
            self._transition_state(TradeState.STATE_CLOSED)
            self._transition_state(TradeState.STATE_IDLE)
            self._handle_cycle_completion(reason="TTL_EXCEEDED")
            return
        self._place_emergency_market_sell(qty)

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
        self._handle_entry_follow_top(open_map)
        if self._maybe_handle_sell_watchdog(open_map):
            return
        self._maybe_handle_sell_ttl(open_map)
        self._maybe_place_sell_retry()

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
                self._transition_state(TradeState.STATE_POSITION_OPEN)
                self._sync_sell_for_partial(delta)
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
                self._sync_sell_for_partial(delta)
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
            self._transition_state(TradeState.STATE_POSITION_OPEN)
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
            self._transition_state(TradeState.STATE_CLOSED)
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
        self._sync_sell_for_partial(qty)
        self._transition_state(TradeState.STATE_POSITION_OPEN)

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

    def _reset_exit_intent(self) -> None:
        self.exit_intent = None
        self.exit_intent_set_ts = None
        self._last_sell_ref_price = None
        self._last_sell_ref_side = None
        self._sell_ttl_retry_count = 0

    def _set_exit_intent(
        self, intent: str, mid: Optional[float], ticks: Optional[int] = None
    ) -> None:
        if self.exit_intent is not None:
            return
        self.exit_intent = intent
        self.exit_intent_set_ts = time.monotonic()
        self._logger(f"[EXIT_INTENT] {intent}")
        ticks_label = "?" if ticks is None else str(ticks)
        mid_label = "—" if mid is None else f"{mid:.8f}"
        self._logger(
            f"[EXIT_INTENT_SET] intent={intent} ticks={ticks_label} mid={mid_label}"
        )

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

    def _sync_sell_for_partial(self, delta_qty: float) -> None:
        if delta_qty <= 0:
            return
        remaining_qty_raw = self._resolve_remaining_qty_raw()
        qty_to_sell = remaining_qty_raw
        if self._profile.step_size:
            qty_to_sell = self._round_down(remaining_qty_raw, self._profile.step_size)
        if qty_to_sell <= 0 or remaining_qty_raw <= self._get_step_size_tolerance():
            return
        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            active_sell_order["qty"] = qty_to_sell
            self._logger(
                f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill"
            )
            return
        self._logger(f"[SELL_SYNC] qty={qty_to_sell:.8f} reason=partial_fill")
        self._place_sell_order(reason="partial_fill", exit_intent=self.exit_intent)

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
        self._transition_state(TradeState.STATE_CLOSED)
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
            self._force_close_after_ttl()
            return
        reason = str(sell_order.get("reason") or "MANUAL").upper()
        self._schedule_sell_retry(reason)
        self._maybe_place_sell_retry()

    def abort_cycle(self) -> int:
        return self.abort_cycle_with_reason(reason="ABORT")

    def abort_cycle_with_reason(self, reason: str, critical: bool = False) -> int:
        cycle_id = self._current_cycle_id
        cancelled = self.cancel_test_orders_margin(reason="aborted")
        self._clear_entry_attempt()
        self._transition_state(TradeState.STATE_CLOSED)
        self._transition_state(TradeState.STATE_IDLE)
        self.last_action = "aborted"
        self._handle_cycle_completion(reason=reason, critical=critical)
        self._cleanup_cycle_state(cycle_id)
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
