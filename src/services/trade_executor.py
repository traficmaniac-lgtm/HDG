from __future__ import annotations

import math
import re
import secrets
import time
from collections import deque
from decimal import Decimal, ROUND_FLOOR
from enum import Enum
from threading import Lock
from typing import Callable, Optional

import httpx

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.core.order_registry import OrderMeta, OrderRegistry, OrderRole
from src.core.trade_ledger import CycleStatus, TradeLedger
from src.services.binance_rest import BinanceRestClient
from src.services.price_router import PriceRouter


class TradeState(Enum):
    STATE_IDLE = "IDLE"
    STATE_STOPPING = "STOPPING"
    STATE_STOPPED = "STOPPED"
    STATE_ENTRY_WORKING = "ENTRY_WORKING"
    STATE_POS_OPEN = "POS_OPEN"
    STATE_EXIT_TP_WORKING = "EXIT_TP_WORKING"
    STATE_EXIT_SL_WORKING = "EXIT_SL_WORKING"
    STATE_RECOVER_STUCK = "RECOVER_STUCK"
    STATE_RECOVERY = "RECOVERY"
    STATE_RECONCILE = "RECONCILE"
    STATE_FLAT = "FLAT"
    STATE_SAFE_STOP = "SAFE_STOP"
    STATE_ERROR = "ERROR"


class PipelineState(Enum):
    IDLE = "IDLE"
    ARM_ENTRY = "ARM_ENTRY"
    WAIT_ENTRY_FILL = "WAIT_ENTRY_FILL"
    ARM_EXIT = "ARM_EXIT"
    WAIT_EXIT = "WAIT_EXIT"
    FINALIZE_DEAL = "FINALIZE_DEAL"
    NEXT_DEAL = "NEXT_DEAL"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class TradeExecutor:
    TAG = "PRE_V0_8_0"
    PRICE_MOVE_TICK_THRESHOLD = 1
    MAX_FILL_KEYS = 50_000
    MAX_SEEN_FILL_KEYS = 10_000
    SEEN_FILL_TTL_S = 15 * 60
    ORPHAN_FILL_TTL_S = 5.0
    ENTRY_WAIT_DEADLINE_MS = 12000
    ENTRY_CANCEL_TIMEOUT_MS = 5000
    EXIT_MAKER_WAIT_DEADLINE_MS = 3500
    EXIT_CROSS_WAIT_DEADLINE_MS = 2500
    CANCEL_WAIT_DEADLINE_MS = 2000
    HARD_STALL_MS = 20000
    EXIT_CROSS_MAX_ATTEMPTS = 3
    EXIT_STATUS_MISSING_MS = 3500
    EXIT_SL_EMERGENCY_TICKS = 0
    MAX_TP_ORDERS = 5

    def __init__(
        self,
        rest: BinanceRestClient,
        router: PriceRouter,
        settings: Settings,
        profile: SymbolProfile,
        logger: Callable[..., None],
    ) -> None:
        self._rest = rest
        self._router = router
        self._settings = settings
        self._profile = profile
        self._logger = logger
        self.active_test_orders: list[dict] = []
        self._client_tag = self._sanitize_client_order_id(
            f"HDG-{self._settings.symbol}-"
        )
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
        self.ledger = TradeLedger(logger=self._logger)
        self._order_registry = OrderRegistry()
        self._active_tp_keys: set[tuple[object, ...]] = set()
        self._last_ledger_unreal_ts = 0.0
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
        self._entry_ref_not_moved_log_ts = 0.0
        self._entry_ref_not_moved_payload: Optional[tuple[object, ...]] = None
        self._sell_wait_started_ts: Optional[float] = None
        self._sell_retry_count = 0
        self._exit_reprice_last_ts = 0.0
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
        self._last_place_error_msg: Optional[str] = None
        self.state = TradeState.STATE_IDLE
        self._pipeline_state = PipelineState.IDLE
        self._cycle_id_counter = 0
        self._current_cycle_id: Optional[int] = None
        self._processed_fill_keys: set[tuple[object, ...]] = set()
        self._processed_fill_queue: deque[tuple[object, ...]] = deque()
        self._seen_fill_keys: set[tuple[object, ...]] = set()
        self._seen_fill_queue: deque[tuple[tuple[object, ...], float]] = deque()
        self._orphan_fills: deque[dict[str, object]] = deque()
        self._orphan_fill_keys: set[tuple[object, ...]] = set()
        self._last_applied_cum_qty: dict[int, Decimal] = {}
        self.wins = 0
        self.losses = 0
        self.cycles_target = self._normalize_cycle_target(
            getattr(settings, "cycle_count", 1)
        )
        self.cycles_done = 0
        self.run_active = False
        self.stop_requested = False
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
        self._tp_armed = False
        self._recovery_residual_qty = 0.0
        self._recovery_active = False
        self._recovery_last_check_ts: Optional[float] = None
        self._executed_qty_total = 0.0
        self._entry_avg_price: Optional[float] = None
        self._closed_qty_total = 0.0
        self._remaining_qty = 0.0
        self._last_pos_snapshot: Optional[tuple[float, float, float, Optional[float]]] = None
        self._tp_maker_started_ts: Optional[float] = None
        self._sell_last_seen_ts: Optional[float] = None
        self._entry_inflight_deadline_ts: Optional[float] = None
        self._sell_inflight_deadline_ts: Optional[float] = None
        self._cycle_started_ts: Optional[float] = None
        now = time.monotonic()
        self._state_entered_ts = now
        self._last_progress_ts = now
        self._last_progress_reason = "init"
        self._reconcile_inflight = False
        self._reconcile_attempts = 0
        self._reconcile_recovery_attempts = 0
        self._reconcile_invalid_totals = False
        self._reconcile_refresh_requested = False
        self._safe_stop_issued = False
        self._last_entry_state_payload: Optional[tuple[object, ...]] = None
        self._last_entry_degraded_ts = 0.0
        self._last_entry_degraded_counts: Optional[tuple[int, int]] = None
        self._last_buy_partial_cum: dict[int, float] = {}
        self._cycle_start_ts_ms: Optional[int] = None
        self._cycle_order_ids: set[int] = set()
        self._last_progress_bid: Optional[float] = None
        self._last_progress_ask: Optional[float] = None
        self._entry_missing_since_ts: Optional[float] = None
        self._exit_missing_since_ts: Optional[float] = None
        self._data_blind_since_ts: Optional[float] = None
        self._tp_missing_since_ts: Optional[float] = None
        self._wait_state_kind: Optional[str] = None
        self._wait_state_enter_ts_ms: Optional[int] = None
        self._wait_state_deadline_ms: Optional[int] = None
        self._entry_wait_kind: Optional[str] = None
        self._entry_wait_enter_ts_ms: Optional[int] = None
        self._entry_wait_deadline_ms: Optional[int] = None
        self._entry_deadline_ts: Optional[float] = None
        self._exit_wait_kind: Optional[str] = None
        self._exit_wait_enter_ts_ms: Optional[int] = None
        self._exit_wait_deadline_ms: Optional[int] = None
        self._exit_deadline_ts: Optional[float] = None
        self._cancel_wait_kind: Optional[str] = None
        self._cancel_wait_enter_ts_ms: Optional[int] = None
        self._cancel_wait_deadline_ms: Optional[int] = None
        self._cancel_deadline_ts: Optional[float] = None
        self._entry_replace_after_cancel = False
        self._entry_cancel_requested_ts: Optional[float] = None
        self._last_effective_source: Optional[str] = None
        self._last_open_orders_signature: Optional[tuple] = None
        self._last_open_orders: Optional[list[dict]] = None
        self._recover_cancel_attempts = {"entry": 0, "exit": 0}
        self._last_recover_reason: Optional[str] = None
        self._last_recover_from: Optional[TradeState] = None
        self._exit_cross_attempts = 0
        self._direction = "LONG"
        self._order_roles: dict[int, dict] = {}
        self._pending_replace_clients: dict[OrderRole, str] = {}
        self._last_entry_wait_payload: Optional[tuple[object, ...]] = None
        self._dust_accumulate = False
        self._dust_carry_qty = 0.0
        self._start_inflight = False
        self._start_inflight_lock = Lock()
        self._trade_hold = False
        self._entry_target_qty: Optional[float] = None
        self._continuous_run = True
        self._adaptive_mode = True
        self._sl_close_retries = 0

    @staticmethod
    def _normalize_direction(direction: Optional[str]) -> str:
        upper = str(direction or "LONG").upper()
        if upper == "SHORT":
            return "SHORT"
        return "LONG"

    def _set_direction(self, direction: Optional[str]) -> None:
        self._direction = self._normalize_direction(direction)

    def _direction_tag(self) -> str:
        return f"dir={self._direction}"

    def get_direction(self) -> Optional[str]:
        direction = str(self._direction or "").upper()
        if direction in {"LONG", "SHORT"}:
            return direction
        return None

    def get_entry_avg_price(self) -> Optional[float]:
        entry_avg = self._entry_avg_price
        if entry_avg is None or entry_avg <= 0:
            return None
        return entry_avg

    def get_executed_qty_total(self) -> float:
        return float(self._executed_qty_total or self.position_qty_base or 0.0)

    def _is_continuous_run(self) -> bool:
        return self._continuous_run

    def _resolve_entry_target_qty(self, entry_price: Optional[float]) -> float:
        target_usd = float(self._settings.order_quote)
        if not entry_price or entry_price <= 0:
            return 0.0
        target_qty = target_usd / entry_price
        if self._profile.step_size:
            target_qty = self._round_down(target_qty, self._profile.step_size)
        return max(target_qty, 0.0)

    def _remaining_to_target(self, entry_price: Optional[float]) -> float:
        target_qty = self._resolve_entry_target_qty(entry_price)
        self._entry_target_qty = target_qty
        remaining = max(target_qty - float(self._executed_qty_total or 0.0), 0.0)
        if self._profile.step_size:
            remaining = self._round_down(remaining, self._profile.step_size)
        return max(remaining, 0.0)

    def _log_entry_target(
        self,
        entry_price: float,
        remaining: float,
    ) -> None:
        target_usd = float(self._settings.order_quote)
        target_qty = self._entry_target_qty or self._resolve_entry_target_qty(entry_price)
        executed = float(self._executed_qty_total or 0.0)
        self._logger(
            "[ENTRY_TARGET] "
            f"dir={self._direction} target_usd={target_usd:.8f} "
            f"target_qty={target_qty:.8f} exec={executed:.8f} rem={remaining:.8f} "
            f"price={entry_price:.8f}"
        )

    def _log_entry_fill(self, cycle_id: Optional[int], qty: float, price: float) -> None:
        deal = self.ledger.deals_by_cycle_id.get(cycle_id or -1)
        deal_label = deal.deal_id if deal else cycle_id
        self._logger(
            f"[ENTRY_FILL_APPLY] deal={deal_label} qty={qty:.8f} price={price:.8f}"
        )

    def _log_tp_place(
        self,
        cycle_id: Optional[int],
        side: str,
        qty: float,
        price: float,
        tp_key: tuple[object, ...],
    ) -> None:
        deal_label = self._resolve_deal_id(cycle_id)
        self._logger(
            "[TP_PLACE] "
            f"deal={deal_label} side={side} qty={qty:.8f} price={price:.8f} tp_key={tp_key}"
        )

    def _log_tp_fill(self, cycle_id: Optional[int], qty: float, price: float) -> None:
        deal = self.ledger.deals_by_cycle_id.get(cycle_id or -1)
        deal_label = deal.deal_id if deal else cycle_id
        self._logger(
            f"[TP_FILL_APPLY] deal={deal_label} qty={qty:.8f} price={price:.8f}"
        )

    def _log_tp_skip_dup(self, cycle_id: Optional[int], tp_key: tuple[object, ...]) -> None:
        deal_label = self._resolve_deal_id(cycle_id)
        self._logger(f"[TP_SKIP_DUP] deal={deal_label} tp_key={tp_key}")

    def _log_tp_key_free(
        self,
        cycle_id: Optional[int],
        tp_key: tuple[object, ...],
        status: str,
    ) -> None:
        deal_label = self._resolve_deal_id(cycle_id)
        self._logger(
            f"[TP_KEY_FREE] deal={deal_label} tp_key={tp_key} status={status}"
        )

    def _resolve_deal_id(self, cycle_id: Optional[int]) -> int:
        deal = self.ledger.deals_by_cycle_id.get(cycle_id or -1)
        if deal:
            return deal.deal_id
        return cycle_id or 0

    def _round_tp_qty(self, qty: float) -> float:
        step = self._profile.step_size
        if step and step > 0:
            return self._round_to_step(qty, step)
        return qty

    def _round_tp_price(self, price: float) -> float:
        tick = self._profile.tick_size
        if tick and tick > 0:
            return self._round_to_step(price, tick)
        return price

    def _build_tp_key(
        self,
        deal_id: int,
        direction: str,
        side: str,
        qty: float,
        price: float,
    ) -> tuple[object, ...]:
        return (
            deal_id,
            direction,
            side,
            self._round_tp_qty(qty),
            self._round_tp_price(price),
        )

    def _tp_key_from_open_order(
        self,
        order: dict,
        meta: Optional[OrderMeta],
    ) -> Optional[tuple[object, ...]]:
        if meta and meta.tp_key:
            return meta.tp_key
        side = str(order.get("side") or "").upper()
        if not side:
            return None
        raw_qty = float(order.get("origQty") or order.get("quantity") or 0.0)
        raw_price = float(order.get("price") or 0.0)
        if raw_qty <= 0 or raw_price <= 0:
            return None
        direction = meta.direction if meta else None
        cycle_id = meta.cycle_id if meta else None
        if direction is None or cycle_id is None:
            client_order_id = str(order.get("clientOrderId") or "")
            parsed = self._parse_client_order_id(client_order_id)
            if parsed:
                direction = str(parsed["direction"])
                cycle_id = int(parsed["cycle_id"])
        if direction is None:
            direction = self._direction
        deal_id = self._resolve_deal_id(cycle_id)
        return self._build_tp_key(deal_id, direction, side, raw_qty, raw_price)

    def _maybe_release_tp_key(self, meta: OrderMeta, status: str) -> None:
        status_label = str(status).upper()
        if status_label not in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
            return
        if not meta.tp_key:
            return
        if meta.tp_key in self._active_tp_keys:
            self._active_tp_keys.remove(meta.tp_key)
            self._log_tp_key_free(meta.cycle_id, meta.tp_key, status_label)

    def _update_order_status(
        self,
        client_id: str,
        status: str,
        filled_qty_delta: float = 0.0,
    ) -> None:
        meta = self._order_registry.get_meta_by_client_id(client_id)
        self._order_registry.update_status(
            client_id, status, filled_qty_delta=filled_qty_delta
        )
        if meta:
            self._maybe_release_tp_key(meta, status)

    def _log_sl_trigger(
        self,
        cycle_id: Optional[int],
        remaining_qty: float,
        sl_price: float,
        market_price: Optional[float],
    ) -> None:
        deal = self.ledger.deals_by_cycle_id.get(cycle_id or -1)
        deal_label = deal.deal_id if deal else cycle_id
        market_label = "—" if market_price is None else f"{market_price:.8f}"
        self._logger(
            f"[SL_TRIGGER] deal={deal_label} price={sl_price:.8f} "
            f"mkt={market_label} rem={remaining_qty:.8f}"
        )

    def _log_sl_cancel(self, canceled_orders: int) -> None:
        self._logger(f"[SL_CANCEL] canceled_orders={canceled_orders}")

    def _log_sl_cross(self, side: str, qty: float, attempt: Optional[int] = None) -> None:
        attempt_label = "" if attempt is None else f" attempt={attempt}"
        self._logger(f"[SL_CROSS] side={side} qty={qty:.8f}{attempt_label}")

    def _log_sl_sync(self, exchange_qty: float) -> None:
        self._logger(f"[SL_SYNC] exchange_qty={exchange_qty:.8f}")

    def _sl_exchange_qty(self) -> float:
        balance = self._get_base_balance_snapshot()
        return self._resolve_exchange_qty(balance)

    def _cancel_orders_for_sl(self) -> int:
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        order_ids: dict[int, str] = {}
        for order in self.active_test_orders:
            order_id = order.get("orderId")
            if not order_id:
                continue
            order_ids[int(order_id)] = order.get("tag", self.TAG)
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except Exception as exc:
            self._logger(f"[SL_CANCEL] open_orders_error={exc}")
            open_orders = []
        for order in open_orders:
            order_id = order.get("orderId")
            if not order_id:
                continue
            order_ids.setdefault(int(order_id), self.TAG)
        canceled = 0
        for order_id, tag in order_ids.items():
            final_status = self._safe_cancel_and_sync(
                order_id=order_id,
                is_isolated=is_isolated,
                tag=tag,
                attempt_cancel=True,
            )
            if final_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                canceled += 1
            for entry in self.active_test_orders:
                if entry.get("orderId") == order_id:
                    entry["status"] = final_status
                    if entry.get("side") == self._exit_side():
                        entry["sell_status"] = final_status
        self.active_test_orders = [
            entry
            for entry in self.active_test_orders
            if entry.get("status") not in {"CANCELED", "REJECTED", "EXPIRED"}
        ]
        self.orders_count = len(self.active_test_orders)
        self.entry_active_order_id = None
        self.entry_active_price = None
        self.entry_cancel_pending = False
        self._entry_replace_after_cancel = False
        self.sell_active_order_id = None
        self.sell_cancel_pending = False
        self.sell_place_inflight = False
        return canceled

    def _handle_sl_trigger(
        self,
        remaining_qty: float,
        sl_price: float,
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> None:
        exit_side = self._exit_side()
        market_price = best_bid if exit_side == "SELL" else best_ask
        self._log_sl_trigger(self._current_cycle_id, remaining_qty, sl_price, market_price)
        self._set_exit_intent(
            "SL",
            mid=None,
            ticks=int(self._settings.stop_loss_ticks),
            force=True,
        )
        self._sl_close_retries = 0
        canceled = self._cancel_orders_for_sl()
        self._log_sl_cancel(canceled)
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except Exception as exc:
            self._logger(f"[SL_SYNC] open_orders_error={exc}")
            open_orders = []
        self._last_open_orders = open_orders
        exchange_qty = self._sl_exchange_qty()
        self._log_sl_sync(exchange_qty)
        if exchange_qty <= self._epsilon_qty() and remaining_qty > self._epsilon_qty():
            self._reconcile_with_exchange()
            return
        qty = remaining_qty
        if self._profile.step_size:
            qty = self._round_down(qty, self._profile.step_size)
        if qty <= self._epsilon_qty():
            return
        self._log_sl_cross(exit_side, qty)
        self._place_sell_order(
            reason="SL_HARD",
            exit_intent="SL",
            qty_override=qty,
            ref_bid=best_bid,
            ref_ask=best_ask,
        )

    def _post_sl_reconcile(self, exit_side: str) -> bool:
        tolerance = self._get_step_size_tolerance()
        exchange_qty = self._sl_exchange_qty()
        self._log_sl_sync(exchange_qty)
        if exchange_qty <= tolerance:
            self._sl_close_retries = 0
            self._logger("[SL_DONE] exchange_qty=0")
            return True
        max_retries = 2
        while exchange_qty > tolerance and self._sl_close_retries < max_retries:
            self._sl_close_retries += 1
            qty = exchange_qty
            if self._profile.step_size:
                qty = self._round_down(qty, self._profile.step_size)
            if qty <= self._epsilon_qty():
                break
            self._log_sl_cross(exit_side, qty, attempt=self._sl_close_retries)
            placed = self._place_sell_order(
                reason="SL_HARD",
                exit_intent="SL",
                qty_override=qty,
            )
            if placed:
                self._transition_state(TradeState.STATE_EXIT_SL_WORKING)
                return False
            exchange_qty = self._sl_exchange_qty()
            self._log_sl_sync(exchange_qty)
        if exchange_qty > tolerance:
            self._logger(
                f"[SL_FAILED] exchange_qty={exchange_qty:.8f}",
                level="ERROR",
            )
            self.stop_run_by_user(reason="SL_FAILED")
            return False
        self._sl_close_retries = 0
        return True

    def _log_deal_close(
        self,
        cycle_id: Optional[int],
        pnl: Optional[float],
        result: Optional[str] = None,
    ) -> None:
        if cycle_id is None:
            return
        deal = self.ledger.deals_by_cycle_id.get(cycle_id)
        if not deal:
            return
        realized = pnl if pnl is not None else deal.realized_pnl
        result_label = result
        if result_label is None:
            result_label = "FLAT"
            if realized is not None:
                if realized > 0:
                    result_label = "WIN"
                elif realized < 0:
                    result_label = "LOSS"
        realized_label = "—" if realized is None else f"{realized:.8f}"
        self._logger(
            f"[DEAL_CLOSE] deal={deal.deal_id} result={result_label} realized={realized_label}"
        )

    def _active_tp_orders(self) -> list[dict]:
        active: list[dict] = []
        for order in self.active_test_orders:
            if str(order.get("role") or "").upper() != "EXIT_TP":
                continue
            if self._sell_order_is_final(order):
                continue
            active.append(order)
        return active

    def _count_active_tp_orders(self) -> int:
        return len(self._active_tp_orders())

    def _cancel_active_tp_orders(self) -> None:
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        for order in self._active_tp_orders():
            order_id = order.get("orderId")
            if not order_id:
                continue
            attempt_cancel = True
            final_status = self._safe_cancel_and_sync(
                order_id=order_id,
                is_isolated=is_isolated,
                tag=order.get("tag", self.TAG),
                attempt_cancel=attempt_cancel,
            )
            if final_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                order["status"] = final_status

    def _tp_price_from_book(
        self,
        entry_avg: Optional[float],
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> Optional[float]:
        tick_size = self._profile.tick_size
        if tick_size is None or tick_size <= 0:
            return None
        tp_trigger, _ = self._compute_tp_sl_prices(entry_avg)
        if tp_trigger is None:
            return None
        if self._position_side() == "SHORT":
            raw_price = tp_trigger if best_bid is None else min(best_bid, tp_trigger)
            return self._round_to_tick(raw_price, tick_size, mode="DOWN")
        raw_price = tp_trigger if best_ask is None else max(best_ask, tp_trigger)
        return self._round_to_tick(raw_price, tick_size, mode="UP")

    def _place_tp_for_fill(self, fill_qty: float, price_state: PriceState) -> None:
        if fill_qty <= 0:
            return
        if self._normalize_exit_intent(self.exit_intent or "") == "SL":
            return
        if self._executed_qty_total <= self._epsilon_qty():
            return
        if not self._can_trade_now(
            price_state,
            action="EXIT",
            max_age_ms=self._trade_gate_age_ms("EXIT"),
        ):
            return
        best_bid, best_ask = self._book_refs(price_state)
        entry_avg = self._entry_avg_price or self.get_buy_price()
        tp_price = self._tp_price_from_book(entry_avg, best_bid, best_ask)
        if tp_price is None:
            return
        remaining_qty = self._resolve_remaining_qty_raw()
        if remaining_qty <= self._get_step_size_tolerance():
            return
        if fill_qty > remaining_qty:
            fill_qty = remaining_qty
        if self._profile.step_size:
            fill_qty = self._round_down(fill_qty, self._profile.step_size)
        if fill_qty <= 0:
            return
        if self._count_active_tp_orders() >= self.MAX_TP_ORDERS:
            self._cancel_active_tp_orders()
            remaining_qty = self._resolve_remaining_qty_raw()
            if self._profile.step_size:
                fill_qty = self._round_down(remaining_qty, self._profile.step_size)
        if fill_qty <= 0:
            return
        self._set_exit_intent("TP", mid=price_state.mid, ticks=int(self._settings.take_profit_ticks))
        placed = self._place_sell_order(
            reason="TP_MAKER",
            price_override=tp_price,
            exit_intent="TP",
            qty_override=fill_qty,
            ref_bid=best_bid,
            ref_ask=best_ask,
        )
        if placed:
            self._transition_pipeline_state(PipelineState.WAIT_EXIT, reason="tp_placed")
        if placed and self._entry_order_active():
            self._transition_to_position_state()

    def get_exit_trigger_prices(self) -> tuple[Optional[float], Optional[float]]:
        entry_avg = self.get_entry_avg_price()
        if entry_avg is None:
            return None, None
        return self._compute_tp_sl_prices(entry_avg)

    def _entry_side(self) -> str:
        return "SELL" if self._direction == "SHORT" else "BUY"

    def _exit_side(self) -> str:
        return "BUY" if self._direction == "SHORT" else "SELL"

    def _entry_role_tag(self) -> str:
        return "ENTRY"

    def _exit_role_tag(self, role: str) -> str:
        if role == "EXIT_TP":
            return "XTP"
        if role == "EXIT_CROSS":
            return "XCR"
        return role

    def _set_start_inflight(self) -> bool:
        with self._start_inflight_lock:
            if self._start_inflight:
                return False
            self._start_inflight = True
            return True

    def _clear_start_inflight(self) -> None:
        with self._start_inflight_lock:
            self._start_inflight = False

    def _apply_dust_carry_to_entry_qty(self, base_qty: float) -> float:
        if not self._profile.step_size:
            return base_qty
        if self._direction != "LONG":
            return self._round_down(base_qty, self._profile.step_size)
        carry_qty = self._dust_carry_qty
        if carry_qty <= self._epsilon_qty():
            return self._round_down(base_qty, self._profile.step_size)
        combined = base_qty + carry_qty
        rounded = self._round_to_step(combined, self._profile.step_size)
        self._dust_carry_qty = 0.0
        self._logger(f"[DUST_CARRY_APPLIED] carry_used={carry_qty:.8f}")
        return rounded

    def _should_carry_dust(
        self,
        remaining_qty: float,
        notional: Optional[float],
        min_notional: Optional[float],
    ) -> bool:
        if remaining_qty <= self._epsilon_qty():
            return False
        if not self._profile.step_size or self._profile.step_size <= 0:
            return False
        if remaining_qty > (self._profile.step_size * 1.01):
            return False
        if min_notional is None or min_notional <= 0:
            return False
        if notional is None or notional >= min_notional:
            return False
        return True

    def _apply_dust_carry(self, remaining_qty: float, notional: float) -> None:
        self._dust_accumulate = False
        self._dust_carry_qty += remaining_qty
        self._remaining_qty = 0.0
        self._logger(
            "[DUST_CARRY] "
            f"carry_qty={self._dust_carry_qty:.8f} added={remaining_qty:.8f} "
            f"notional={notional:.8f}"
        )
        self._finalize_partial_close(reason="DUST_CARRY")

    def _position_side(self) -> str:
        if self.position is not None:
            return str(self.position.get("side", self._direction) or self._direction).upper()
        return self._direction

    def get_best_bid(self, price_state: PriceState) -> Optional[float]:
        if price_state.bid is not None:
            return price_state.bid
        return price_state.mid

    def get_best_ask(self, price_state: PriceState) -> Optional[float]:
        if price_state.ask is not None:
            return price_state.ask
        return price_state.mid

    def _entry_reference(self, bid: Optional[float], ask: Optional[float]) -> tuple[Optional[float], str]:
        if self._direction == "SHORT":
            return ask, "best_ask"
        return bid, "best_bid"

    def _entry_reference_label(self) -> str:
        return "best_ask" if self._direction == "SHORT" else "best_bid"

    def get_state_label(self) -> str:
        if self._pipeline_state is not None:
            return self._pipeline_state.value
        return self.state.value

    def get_entry_side(self) -> str:
        return self._entry_side()

    def get_exit_side(self) -> str:
        return self._exit_side()

    def resolve_exit_policy(self, intent: str) -> tuple[str, str]:
        return self._resolve_exit_policy_for_side(intent)

    def note_progress(self, reason: str = "") -> None:
        return

    def mark_progress(self, reason: str, reset_reconcile: bool = True) -> None:
        self._mark_progress(reason=reason, reset_reconcile=reset_reconcile)

    def _mark_progress(self, reason: str, reset_reconcile: bool = True) -> None:
        self._last_progress_ts = time.monotonic()
        self._last_progress_reason = reason
        if reset_reconcile:
            self._reconcile_attempts = 0

    def _state_age_ms(self, now: Optional[float] = None) -> int:
        now = time.monotonic() if now is None else now
        return int((now - self._state_entered_ts) * 1000.0)

    def _no_progress_ms(self, now: Optional[float] = None) -> int:
        now = time.monotonic() if now is None else now
        return int((now - self._last_progress_ts) * 1000.0)

    def _resolve_wait_deadline_ms(self, kind: str) -> int:
        if kind == "ENTRY_WAIT":
            default = self.ENTRY_WAIT_DEADLINE_MS
            return int(getattr(self._settings, "entry_wait_deadline_ms", default) or default)
        if kind == "ENTRY_CANCEL_WAIT":
            default = self.ENTRY_CANCEL_TIMEOUT_MS
            return int(
                getattr(self._settings, "entry_cancel_timeout_ms", default) or default
            )
        if kind == "EXIT_MAKER_WAIT":
            default = self.EXIT_MAKER_WAIT_DEADLINE_MS
            return int(getattr(self._settings, "exit_maker_wait_deadline_ms", default) or default)
        if kind == "EXIT_CROSS_WAIT":
            default = self.EXIT_CROSS_WAIT_DEADLINE_MS
            return int(getattr(self._settings, "exit_cross_wait_deadline_ms", default) or default)
        if kind == "EXIT_CANCEL_WAIT":
            default = self.CANCEL_WAIT_DEADLINE_MS
            return int(getattr(self._settings, "cancel_wait_deadline_ms", default) or default)
        return 0

    def _clear_entry_wait(self) -> None:
        self._entry_wait_kind = None
        self._entry_wait_enter_ts_ms = None
        self._entry_wait_deadline_ms = None
        self._entry_deadline_ts = None

    def _clear_wait_state(self) -> None:
        self._wait_state_kind = None
        self._wait_state_enter_ts_ms = None
        self._wait_state_deadline_ms = None

    def _clear_exit_wait(self) -> None:
        self._exit_wait_kind = None
        self._exit_wait_enter_ts_ms = None
        self._exit_wait_deadline_ms = None
        self._exit_deadline_ts = None

    def _clear_cancel_wait(self) -> None:
        self._cancel_wait_kind = None
        self._cancel_wait_enter_ts_ms = None
        self._cancel_wait_deadline_ms = None
        self._cancel_deadline_ts = None

    def _clear_entry_cancel_trackers(self) -> None:
        self.entry_cancel_pending = False
        self._entry_cancel_requested_ts = None
        self._entry_replace_after_cancel = False

    def _finalize_entry_ownership(self, reason: str) -> None:
        # Invariant: entry ownership must be cleared once entry is filled/canceled or we leave entry flow.
        self.entry_active_order_id = None
        self.entry_active_price = None
        self._clear_entry_cancel_trackers()
        self._entry_missing_since_ts = None
        if not self._should_dedup_log(f"entry_finalize:{reason}", 2.0):
            self._logger(f"[ENTRY_FINALIZE] reason={reason}")

    def _set_entry_wait_state(self, kind: Optional[str], force_reset: bool = False) -> None:
        if kind is None:
            self._clear_entry_wait()
            return
        if (
            force_reset
            or self._entry_wait_kind != kind
            or self._entry_wait_enter_ts_ms is None
        ):
            self._entry_wait_kind = kind
            self._entry_wait_enter_ts_ms = int(time.monotonic() * 1000)
            self._entry_wait_deadline_ms = self._resolve_wait_deadline_ms(kind)
            deadline_ms = self._entry_wait_deadline_ms or 0
            self._entry_deadline_ts = (
                time.monotonic() + (deadline_ms / 1000.0) if deadline_ms > 0 else None
            )

    def _set_exit_wait_state(self, kind: Optional[str], force_reset: bool = False) -> None:
        if kind is None:
            self._clear_exit_wait()
            return
        if (
            force_reset
            or self._exit_wait_kind != kind
            or self._exit_wait_enter_ts_ms is None
        ):
            self._exit_wait_kind = kind
            self._exit_wait_enter_ts_ms = int(time.monotonic() * 1000)
            self._exit_wait_deadline_ms = self._resolve_wait_deadline_ms(kind)
            deadline_ms = self._exit_wait_deadline_ms or 0
            self._exit_deadline_ts = (
                time.monotonic() + (deadline_ms / 1000.0) if deadline_ms > 0 else None
            )

    def _set_cancel_wait_state(self, kind: Optional[str], force_reset: bool = False) -> None:
        if kind is None:
            self._clear_cancel_wait()
            return
        if (
            force_reset
            or self._cancel_wait_kind != kind
            or self._cancel_wait_enter_ts_ms is None
        ):
            self._cancel_wait_kind = kind
            self._cancel_wait_enter_ts_ms = int(time.monotonic() * 1000)
            self._cancel_wait_deadline_ms = self._resolve_wait_deadline_ms(kind)
            deadline_ms = self._cancel_wait_deadline_ms or 0
            self._cancel_deadline_ts = (
                time.monotonic() + (deadline_ms / 1000.0) if deadline_ms > 0 else None
            )

    def _note_effective_source(self, source: Optional[str]) -> None:
        if source is None:
            return
        if self._last_effective_source is None:
            self._last_effective_source = source
            return
        if source != self._last_effective_source:
            if not self._should_dedup_log("source_switch", 2.0):
                self._logger(
                    f"[SOURCE_SWITCH] from={self._last_effective_source} to={source}"
                )
            self._last_effective_source = source

    def _register_order_role(self, order_id: Optional[int], role: str) -> None:
        if not isinstance(order_id, int):
            return
        self._order_roles[order_id] = {
            "role": role,
            "cycle_id": self._current_cycle_id,
            "dir": self._direction,
            "created_ts": int(time.time() * 1000),
        }

    def _normalize_order_role(self, role: str) -> OrderRole:
        role_upper = role.upper()
        if role_upper in {"ENTRY", "ENTRY_TOPUP"}:
            return OrderRole.ENTRY
        if role_upper == "EXIT_TP":
            return OrderRole.EXIT_TP
        if role_upper == "EXIT_CROSS":
            return OrderRole.EXIT_CROSS
        return OrderRole.CANCEL_ONLY

    def _register_order_meta(
        self,
        client_id: str,
        role: str,
        side: str,
        price: float,
        qty: float,
        tp_key: Optional[tuple[object, ...]] = None,
    ) -> None:
        cycle_id = self._current_cycle_id or 0
        now = time.time()
        meta = OrderMeta(
            client_id=client_id,
            order_id=None,
            symbol=self._settings.symbol,
            cycle_id=cycle_id,
            direction=self._direction,
            role=self._normalize_order_role(role),
            side=side,
            price=price,
            orig_qty=qty,
            tp_key=tp_key,
            created_ts=now,
            last_update_ts=now,
            status="NEW",
        )
        self._order_registry.register_new(meta)
        if not self._should_dedup_log(f"order_reg:{client_id}", 0.5):
            self._logger(
                "[REG_ADD] "
                f"client_id={client_id} role={meta.role.value} cycle={cycle_id} "
                f"dir={meta.direction} side={side} price={price:.8f} qty={qty:.8f}"
            )

    def _bind_order_meta(self, client_id: str, order_id: Optional[int]) -> None:
        if not isinstance(order_id, int):
            return
        self._order_registry.bind_order_id(client_id, order_id)
        self._update_order_status(client_id, "WORKING")
        if not self._should_dedup_log(f"order_bind:{client_id}:{order_id}", 0.5):
            self._logger(
                f"[ORDER_BIND] client_id={client_id} order_id={order_id}"
            )
        self._rebind_orphan_fills(order_id=order_id, client_order_id=client_id)

    def _log_fill_applied(
        self,
        meta: OrderMeta,
        qty: float,
        price: float,
    ) -> None:
        cycle_id = meta.cycle_id
        deal = self.ledger.deals_by_cycle_id.get(cycle_id)
        deal_label = deal.deal_id if deal else cycle_id
        self._logger(
            "[FILL_APPLIED] "
            f"deal={deal_label} role={meta.role.value} dir={meta.direction} "
            f"qty={qty:.8f} price={price:.8f}"
        )

    def _stash_orphan_fill(
        self,
        order_id: Optional[int],
        client_order_id: Optional[str],
        side: str,
        qty: float,
        price: float,
        kind: str,
        ts_ms: int,
    ) -> None:
        if not isinstance(order_id, int):
            return
        key = (order_id, kind, round(qty, 12), round(price, 8))
        if key in self._orphan_fill_keys:
            return
        self._orphan_fill_keys.add(key)
        self._orphan_fills.append(
            {
                "order_id": order_id,
                "client_order_id": client_order_id,
                "side": side,
                "qty": qty,
                "price": price,
                "kind": kind,
                "ts_ms": ts_ms,
                "created_ts": time.monotonic(),
            }
        )
        self._logger(
            "[FILL_ORPHAN] "
            f"order_id={order_id} qty={qty:.8f} price={price:.8f}"
        )

    def _prune_orphan_fills(self) -> None:
        now = time.monotonic()
        while self._orphan_fills and now - float(self._orphan_fills[0]["created_ts"]) > self.ORPHAN_FILL_TTL_S:
            orphan = self._orphan_fills.popleft()
            key = (
                orphan.get("order_id"),
                orphan.get("kind"),
                round(float(orphan.get("qty") or 0.0), 12),
                round(float(orphan.get("price") or 0.0), 8),
            )
            self._orphan_fill_keys.discard(key)

    def _rebind_orphan_fills(
        self,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> None:
        if not self._orphan_fills:
            return
        self._prune_orphan_fills()
        if not self._orphan_fills:
            return
        kept: deque[dict[str, object]] = deque()
        for orphan in self._orphan_fills:
            orphan_order_id = orphan.get("order_id")
            orphan_client_id = orphan.get("client_order_id")
            if order_id is not None and orphan_order_id != order_id and client_order_id is not None:
                if orphan_client_id != client_order_id:
                    kept.append(orphan)
                    continue
            meta = None
            if isinstance(orphan_order_id, int):
                meta = self._order_registry.get_meta_by_order_id(orphan_order_id)
            if meta is None and isinstance(orphan_client_id, str):
                meta = self._order_registry.get_meta_by_client_id(orphan_client_id)
            if meta is None:
                kept.append(orphan)
                continue
            self._logger(
                "[FILL_REBIND] "
                f"order_id={orphan_order_id} role={meta.role.value} cycle={meta.cycle_id}"
            )
            self._apply_orphan_fill(orphan)
            key = (
                orphan.get("order_id"),
                orphan.get("kind"),
                round(float(orphan.get("qty") or 0.0), 12),
                round(float(orphan.get("price") or 0.0), 8),
            )
            self._orphan_fill_keys.discard(key)
        self._orphan_fills = kept

    def _apply_orphan_fill(self, orphan: dict[str, object]) -> None:
        order_id = orphan.get("order_id")
        side = str(orphan.get("side") or "UNKNOWN")
        qty = float(orphan.get("qty") or 0.0)
        price = float(orphan.get("price") or 0.0)
        ts_ms = int(orphan.get("ts_ms") or int(time.time() * 1000))
        kind = str(orphan.get("kind") or "FILLED").upper()
        if not isinstance(order_id, int):
            return
        if kind == "PARTIAL":
            self.handle_order_partial(order_id, side, qty, price, ts_ms)
        else:
            self.handle_order_filled(order_id, side, price, qty, ts_ms)

    def _update_registry_status_by_order(
        self,
        order_id: Optional[int],
        status: str,
        filled_qty_delta: float = 0.0,
    ) -> None:
        if not isinstance(order_id, int):
            return
        meta = self._order_registry.get_meta_by_order_id(order_id)
        if meta is None:
            return
        self._update_order_status(
            meta.client_id, status, filled_qty_delta=filled_qty_delta
        )

    def _log_role_active_skip(
        self,
        cycle_id: int,
        role: str,
        client_id: Optional[str],
    ) -> None:
        key = f"role_active_skip:{cycle_id}:{role}"
        if self._should_dedup_log(key, 0.5):
            return
        client_label = client_id or "?"
        self._logger(
            f"[ROLE_ACTIVE_SKIP] cycle={cycle_id} role={role} client_id={client_label}"
        )

    def _get_active_meta_for_role(
        self,
        cycle_id: int,
        role: str,
    ) -> Optional[OrderMeta]:
        role_enum = self._normalize_order_role(role)
        return self._order_registry.get_active_for_role(cycle_id, role_enum)

    def _note_pending_replace(self, order_id: Optional[int]) -> None:
        if not isinstance(order_id, int):
            return
        meta = self._order_registry.get_meta_by_order_id(order_id)
        if meta is None:
            return
        self._pending_replace_clients[meta.role] = meta.client_id

    def _apply_pending_replace(
        self,
        role: str,
        new_client_id: str,
        new_order_id: Optional[int],
    ) -> None:
        role_enum = self._normalize_order_role(role)
        old_client_id = self._pending_replace_clients.pop(role_enum, None)
        if not old_client_id:
            return
        old_meta = self._order_registry.get_meta_by_client_id(old_client_id)
        self._order_registry.mark_replaced(
            old_client_id, new_client_id, new_order_id
        )
        if old_meta:
            self._maybe_release_tp_key(old_meta, "CANCELED")

    def _clear_order_role(self, order_id: Optional[int]) -> None:
        if not isinstance(order_id, int):
            return
        self._order_roles.pop(order_id, None)
        self._last_applied_cum_qty.pop(order_id, None)

    def _log_guard_active_order(
        self,
        role: str,
        action: str = "skip_place",
        reason: str = "active_exists",
    ) -> None:
        key = f"guard_active_order:{role}:{action}:{reason}"
        if self._should_dedup_log(key, 1.0):
            return
        self._logger(
            f"[GUARD_ACTIVE_ORDER] role={role} action={action} reason={reason}"
        )
        if role == "ENTRY":
            if not self._should_dedup_log("guard_active_entry", 1.0):
                self._logger("[GUARD_ACTIVE_ENTRY] skip_place")

    def _client_order_has_role(self, client_order_id: str, role: str) -> bool:
        tag = None
        if role == "ENTRY":
            tag = self._entry_role_tag()
        elif role in {"EXIT_TP", "EXIT_CROSS"}:
            tag = self._exit_role_tag(role)
        if not tag:
            return False
        return f"-{tag}-" in client_order_id

    def _parse_client_order_id(self, client_order_id: str) -> Optional[dict[str, object]]:
        pattern = (
            r"^HDG-(?P<symbol>[^-]+)-(?P<direction>LONG|SHORT)-C(?P<cycle>\d+)-(?P<role>[A-Z]+)"
        )
        match = re.match(pattern, client_order_id)
        if not match:
            return None
        role_tag = match.group("role")
        role = OrderRole.CANCEL_ONLY
        if role_tag == self._entry_role_tag():
            role = OrderRole.ENTRY
        elif role_tag == self._exit_role_tag("EXIT_TP"):
            role = OrderRole.EXIT_TP
        elif role_tag == self._exit_role_tag("EXIT_CROSS"):
            role = OrderRole.EXIT_CROSS
        return {
            "symbol": match.group("symbol"),
            "direction": match.group("direction"),
            "cycle_id": int(match.group("cycle")),
            "role": role,
        }

    def _reconcile_registry_mapping(self, open_orders: list[dict]) -> tuple[int, int, int]:
        active_status = {"NEW", "WORKING", "PARTIAL"}
        open_order_ids = {
            order.get("orderId") for order in open_orders if order.get("orderId")
        }
        stale_local = 0
        for meta in self._order_registry.by_client.values():
            if meta.status not in active_status:
                continue
            if not isinstance(meta.order_id, int):
                continue
            if meta.order_id not in open_order_ids:
                self._update_order_status(meta.client_id, "CANCELED")
                stale_local += 1
        restored = 0
        unresolved = 0
        for order in open_orders:
            order_id = order.get("orderId")
            if not isinstance(order_id, int):
                continue
            if self._order_registry.get_meta_by_order_id(order_id) is not None:
                continue
            client_order_id = str(order.get("clientOrderId") or "")
            if client_order_id in self._order_registry.by_client:
                self._bind_order_meta(client_order_id, order_id)
                restored += 1
                continue
            if not client_order_id.startswith(self._client_tag):
                continue
            parsed = self._parse_client_order_id(client_order_id)
            if parsed is None:
                unresolved += 1
                continue
            price = float(order.get("price") or 0.0)
            orig_qty = float(order.get("origQty") or order.get("quantity") or 0.0)
            side = str(order.get("side") or "")
            tp_key = None
            if parsed["role"] == OrderRole.EXIT_TP:
                tp_key = self._tp_key_from_open_order(
                    order,
                    None,
                )
            now = time.time()
            meta = OrderMeta(
                client_id=client_order_id,
                order_id=order_id,
                symbol=str(parsed["symbol"]),
                cycle_id=int(parsed["cycle_id"]),
                direction=str(parsed["direction"]),
                role=parsed["role"],
                side=side,
                price=price,
                orig_qty=orig_qty,
                tp_key=tp_key,
                created_ts=now,
                last_update_ts=now,
                status=str(order.get("status") or "NEW").upper(),
            )
            try:
                self._order_registry.register_new(meta)
            except ValueError:
                self._order_registry.bind_order_id(client_order_id, order_id)
            else:
                self._order_registry.bind_order_id(client_order_id, order_id)
            restored += 1
        return restored, unresolved, stale_local

    def _has_active_role(self, role: str) -> bool:
        role_upper = role.upper()
        cycle_id = self._current_cycle_id or 0
        active_meta = self._get_active_meta_for_role(cycle_id, role_upper)
        if active_meta is not None:
            return True
        for order in self.active_test_orders:
            order_role = str(order.get("role") or "").upper()
            if order_role != role_upper and not (
                role_upper == "ENTRY" and order_role == "ENTRY_TOPUP"
            ):
                continue
            if role_upper == "ENTRY":
                if not self._entry_order_is_final(order):
                    return True
            elif role_upper in {"EXIT_TP", "EXIT_CROSS"}:
                if not self._sell_order_is_final(order):
                    return True
            else:
                status = str(order.get("status") or "").upper()
                if status not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                    return True
        open_orders = self._last_open_orders or []
        if open_orders:
            open_map = {order.get("orderId"): order for order in open_orders}
            for order_id, role_entry in self._order_roles.items():
                if str(role_entry.get("role") or "").upper() != role_upper:
                    continue
                if order_id in open_map:
                    return True
            for order in open_orders:
                client_order_id = str(order.get("clientOrderId") or "")
                if self._client_order_has_role(client_order_id, role_upper):
                    return True
        return False

    def _is_exit_tp_open_order(self, order: dict, meta: Optional[OrderMeta]) -> bool:
        if meta and meta.role == OrderRole.EXIT_TP:
            return True
        order_id = order.get("orderId")
        if isinstance(order_id, int):
            role_entry = self._order_roles.get(order_id, {})
            if str(role_entry.get("role") or "").upper() == "EXIT_TP":
                return True
        client_order_id = str(order.get("clientOrderId") or "")
        return self._client_order_has_role(client_order_id, "EXIT_TP")

    def _dedup_open_tp_orders(self, open_orders: list[dict]) -> set[int]:
        canceled_order_ids: set[int] = set()
        if not open_orders:
            return canceled_order_ids
        if not self._profile.step_size or not self._profile.tick_size:
            return canceled_order_ids
        grouped: dict[tuple[object, ...], list[dict]] = {}
        for order in open_orders:
            order_id = order.get("orderId")
            meta = (
                self._order_registry.get_meta_by_order_id(order_id)
                if isinstance(order_id, int)
                else None
            )
            if not self._is_exit_tp_open_order(order, meta):
                continue
            tp_key = self._tp_key_from_open_order(order, meta)
            if not tp_key:
                continue
            group_key = (tp_key[0], tp_key[2], tp_key[3], tp_key[4])
            grouped.setdefault(group_key, []).append(order)
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        for group_key, orders in grouped.items():
            if len(orders) <= 1:
                continue
            keep_order = min(
                orders,
                key=lambda entry: (
                    entry.get("orderId") is None,
                    entry.get("orderId") or 0,
                ),
            )
            keep_id = keep_order.get("orderId")
            canceled = []
            for order in orders:
                order_id = order.get("orderId")
                if order_id == keep_id:
                    continue
                if not isinstance(order_id, int):
                    continue
                self._safe_cancel_and_sync(
                    order_id=order_id,
                    is_isolated=is_isolated,
                    tag="tp_dedup",
                    attempt_cancel=True,
                )
                canceled.append(order_id)
                canceled_order_ids.add(order_id)
            if canceled:
                deal_id = group_key[0]
                self._logger(
                    "[TP_DEDUP_CANCEL] "
                    f"deal={deal_id} keep_order={keep_id} "
                    f"canceled_n={len(canceled)} canceled={canceled}"
                )
        return canceled_order_ids

    def _sync_tp_keys_from_open_orders(
        self,
        open_orders: list[dict],
        skip_order_ids: Optional[set[int]] = None,
    ) -> None:
        if not open_orders:
            return
        skip_order_ids = skip_order_ids or set()
        for order in open_orders:
            order_id = order.get("orderId")
            if isinstance(order_id, int) and order_id in skip_order_ids:
                continue
            meta = (
                self._order_registry.get_meta_by_order_id(order_id)
                if isinstance(order_id, int)
                else None
            )
            if not self._is_exit_tp_open_order(order, meta):
                continue
            tp_key = self._tp_key_from_open_order(order, meta)
            if tp_key and tp_key not in self._active_tp_keys:
                self._active_tp_keys.add(tp_key)

    def _should_skip_recover_for_active_order(
        self,
        reason: str,
        from_state: TradeState,
    ) -> bool:
        guard_reasons = {
            "deadline",
            "progress_deadline_entry",
            "progress_deadline_exit",
            "watchdog_timeout",
        }
        if reason not in guard_reasons:
            return False
        price_state, _ = self._router.build_price_state()
        self._update_data_blind_state(price_state)
        self._note_effective_source(price_state.source)
        effective_source = self._last_effective_source or price_state.source
        if price_state.data_blind or effective_source == "NONE":
            return False
        roles_to_check: list[str] = []
        if from_state == TradeState.STATE_ENTRY_WORKING:
            roles_to_check = ["ENTRY"]
        elif from_state in {
            TradeState.STATE_EXIT_TP_WORKING,
            TradeState.STATE_EXIT_SL_WORKING,
        } or self._in_exit_workflow():
            if self._tp_exit_phase == "CROSS":
                roles_to_check = ["EXIT_CROSS"]
            else:
                roles_to_check = ["EXIT_TP", "EXIT_CROSS"]
        if not roles_to_check:
            return False
        for role in roles_to_check:
            if self._has_active_role(role):
                if not self._should_dedup_log(
                    f"recover_skip_active:{from_state.value}:{role}",
                    1.0,
                ):
                    self._logger(
                        "[RECOVER_SKIP_ACTIVE] "
                        f"state={from_state.value} role={role} reason={reason}"
                    )
                return True
        return False

    def _resolve_fill_role(
        self, order_id: Optional[int], side_upper: str
    ) -> tuple[str, str]:
        if isinstance(order_id, int):
            meta = self._order_registry.get_meta_by_order_id(order_id)
            if meta is not None:
                return meta.role.value, "registry"
        fallback_role = None
        fallback_reason = "unknown"
        if isinstance(order_id, int) and order_id == self.entry_active_order_id:
            fallback_role = "ENTRY"
            fallback_reason = "entry_active_id"
        elif isinstance(order_id, int) and order_id == self.sell_active_order_id:
            fallback_role = "EXIT_CROSS" if self._tp_exit_phase == "CROSS" else "EXIT_TP"
            fallback_reason = "exit_active_id"
        elif self.position is None and self.state == TradeState.STATE_ENTRY_WORKING:
            fallback_role = "ENTRY"
            fallback_reason = "state_entry_working"
        elif self.position is not None or self._in_exit_workflow():
            fallback_role = "EXIT_CROSS" if self._tp_exit_phase == "CROSS" else "EXIT_TP"
            fallback_reason = "state_exit_or_position"
        else:
            if self._direction == "SHORT" and side_upper == "BUY":
                fallback_role = "EXIT_TP"
            elif self._direction == "LONG" and side_upper == "SELL":
                fallback_role = "EXIT_TP"
            else:
                fallback_role = "ENTRY"
            fallback_reason = "side_dir_fallback"
        self._logger(
            "[UNKNOWN_ROLE] "
            f"order_id={order_id} classify={fallback_role} "
            f"reason={fallback_reason} state={self.state.value} "
            f"dir={self._direction} side={side_upper}"
        )
        return fallback_role, fallback_reason

    def _get_meta_for_fill(
        self,
        order_id: Optional[int],
        qty: float,
        price: float,
        client_order_id: Optional[str] = None,
        side: Optional[str] = None,
        kind: str = "FILLED",
        ts_ms: Optional[int] = None,
    ) -> Optional[OrderMeta]:
        if not isinstance(order_id, int):
            return None
        meta = self._order_registry.get_meta_by_order_id(order_id)
        if meta is None and client_order_id:
            meta = self._order_registry.get_meta_by_client_id(client_order_id)
        if meta is None:
            side_label = str(side or "UNKNOWN").upper()
            self._stash_orphan_fill(
                order_id=order_id,
                client_order_id=client_order_id,
                side=side_label,
                qty=qty,
                price=price,
                kind="PARTIAL" if kind.upper().startswith("PART") else "FILLED",
                ts_ms=ts_ms or int(time.time() * 1000),
            )
            return None
        if not self._should_dedup_log(f"fill_map:{order_id}", 0.5):
            self._logger(
                "[FILL_MAP] "
                f"order_id={order_id} -> cycle={meta.cycle_id} "
                f"role={meta.role.value} qty={qty:.8f} price={price:.8f}"
            )
        return meta

    def _trade_gate_age_ms(self, action: str) -> int:
        if action == "ENTRY":
            entry_age = int(getattr(self._settings, "entry_max_age_ms", 2500))
            return max(entry_age, 2500)
        if action == "EXIT":
            return int(getattr(self._settings, "exit_max_age_ms", 2500))
        entry_age = int(getattr(self._settings, "entry_max_age_ms", 2500))
        entry_age = max(entry_age, 2500)
        exit_age = int(getattr(self._settings, "exit_max_age_ms", 2500))
        return max(entry_age, exit_age)

    def _log_trade_blocked(
        self,
        action: str,
        reason: str,
        price_state: PriceState,
        max_age_ms: Optional[int],
    ) -> None:
        if self._should_dedup_log(f"trade_blocked:{action}:{reason}", 0.5):
            return
        age_label = price_state.mid_age_ms if price_state.mid_age_ms is not None else "?"
        cache_label = (
            price_state.cache_age_ms
            if price_state.cache_age_ms is not None
            else "?"
        )
        cache_source_label = "Y" if price_state.from_cache else "N"
        max_age_label = max_age_ms if max_age_ms is not None else "?"
        bid_label = "—" if price_state.bid is None else f"{price_state.bid:.8f}"
        ask_label = "—" if price_state.ask is None else f"{price_state.ask:.8f}"
        self._logger(
            "[TRADE_BLOCKED] "
            f"action={action} reason={reason} source={price_state.source} "
            f"data_blind={price_state.data_blind} bid={bid_label} ask={ask_label} "
            f"mid_age_ms={age_label} cache_age_ms={cache_label} "
            f"from_cache={cache_source_label} max_age_ms={max_age_label}"
        )

    def _can_trade_now(
        self,
        price_state: PriceState,
        action: str,
        max_age_ms: Optional[int],
    ) -> bool:
        # Invariant: never place/replace/cancel entry/exit orders while blind or stale.
        reason = None
        if self._trade_hold:
            reason = "hold"
        if price_state.data_blind:
            reason = "data_blind"
        elif price_state.source == "NONE":
            reason = "source_none"
        elif price_state.bid is None or price_state.ask is None:
            reason = "no_quote"
        elif max_age_ms is not None:
            if price_state.mid_age_ms is None:
                reason = "no_age"
            elif price_state.mid_age_ms > max_age_ms:
                reason = "stale"
            elif (
                price_state.from_cache
                and price_state.cache_age_ms is not None
                and price_state.cache_age_ms > max_age_ms
            ):
                reason = "cache_stale"
        if reason:
            self._log_trade_blocked(action, reason, price_state, max_age_ms)
            return False
        return True

    def _set_wait_state(
        self,
        kind: Optional[str],
        deadline_ms: Optional[int],
        force_reset: bool = False,
    ) -> None:
        if kind is None:
            self._clear_wait_state()
            return
        if (
            force_reset
            or self._wait_state_kind != kind
            or self._wait_state_enter_ts_ms is None
        ):
            self._wait_state_kind = kind
            self._wait_state_enter_ts_ms = int(time.monotonic() * 1000)
        self._wait_state_deadline_ms = deadline_ms

    def _resolve_wait_state_deadline_ms(self, kind: str) -> int:
        if kind == "ENTRY_WAIT":
            return self._resolve_wait_deadline_ms("ENTRY_WAIT")
        if kind == "EXIT_WAIT":
            if self._tp_exit_phase == "CROSS":
                return self._resolve_wait_deadline_ms("EXIT_CROSS_WAIT")
            return self._resolve_wait_deadline_ms("EXIT_MAKER_WAIT")
        if kind == "CANCEL_WAIT":
            if self.entry_cancel_pending:
                return self._resolve_wait_deadline_ms("ENTRY_CANCEL_WAIT")
            return self._resolve_wait_deadline_ms("EXIT_CANCEL_WAIT")
        return 0

    def _log_wait_reset(self, kind: str, reason: str) -> None:
        if self._should_dedup_log(f"wait_reset:{kind}:{reason}", 1.0):
            return
        self._logger(f"[WAIT_RESET] kind={kind} reason={reason}")

    def _reset_entry_wait(self, reason: str) -> None:
        self._set_entry_wait_state("ENTRY_WAIT", force_reset=True)
        self._set_wait_state(
            "ENTRY_WAIT",
            self._resolve_wait_state_deadline_ms("ENTRY_WAIT"),
            force_reset=True,
        )
        self._log_wait_reset("ENTRY_WAIT", reason)

    def _reset_exit_wait(self, reason: str) -> None:
        wait_kind = "EXIT_CROSS_WAIT" if self._tp_exit_phase == "CROSS" else "EXIT_MAKER_WAIT"
        self._set_exit_wait_state(wait_kind, force_reset=True)
        self._set_wait_state(
            "EXIT_WAIT",
            self._resolve_wait_state_deadline_ms("EXIT_WAIT"),
            force_reset=True,
        )
        self._log_wait_reset("EXIT_WAIT", reason)

    def _reset_cancel_wait(self, kind: str) -> None:
        self._set_cancel_wait_state(kind, force_reset=True)
        self._set_wait_state(
            "CANCEL_WAIT",
            self._resolve_wait_state_deadline_ms("CANCEL_WAIT"),
            force_reset=True,
        )

    def _check_wait_deadline(self, now: float) -> bool:
        cancel_kind = None
        if self.state == TradeState.STATE_ENTRY_WORKING and self.entry_cancel_pending:
            cancel_kind = "ENTRY_CANCEL_WAIT"
        elif self.sell_cancel_pending:
            cancel_kind = "EXIT_CANCEL_WAIT"
        self._set_cancel_wait_state(cancel_kind)
        if cancel_kind is None:
            self._clear_cancel_wait()

        entry_kind = "ENTRY_WAIT" if self.state == TradeState.STATE_ENTRY_WORKING else None
        self._set_entry_wait_state(entry_kind)
        if entry_kind is None:
            self._clear_entry_wait()

        exit_kind = None
        if self._in_exit_workflow():
            exit_kind = "EXIT_MAKER_WAIT" if self._tp_exit_phase == "MAKER" else "EXIT_CROSS_WAIT"
        self._set_exit_wait_state(exit_kind)
        if exit_kind is None:
            self._clear_exit_wait()

        wait_state = None
        if self.entry_cancel_pending or self.sell_cancel_pending:
            wait_state = "CANCEL_WAIT"
        elif self.state == TradeState.STATE_ENTRY_WORKING:
            wait_state = "ENTRY_WAIT"
        elif self._in_exit_workflow():
            wait_state = "EXIT_WAIT"
        deadline_ms = self._resolve_wait_state_deadline_ms(wait_state) if wait_state else None
        self._set_wait_state(wait_state, deadline_ms)
        if (
            wait_state == "ENTRY_WAIT"
            and self._dust_accumulate
            and self._entry_order_active()
        ):
            return False
        if wait_state and deadline_ms and self._wait_state_enter_ts_ms is not None:
            age_ms = int(now * 1000) - self._wait_state_enter_ts_ms
            if age_ms >= deadline_ms:
                self._logger(
                    f"[WAIT_DEADLINE] state={wait_state} age_ms={age_ms} deadline_ms={deadline_ms}"
                )
                return self._recover_stuck(
                    reason="deadline",
                    wait_kind=wait_state,
                    age_ms=age_ms,
                )
        return False

    def _check_progress_deadline(self, now: float) -> bool:
        no_progress_ms = self._no_progress_ms(now)
        if self.state == TradeState.STATE_ENTRY_WORKING:
            if self._dust_accumulate and self._entry_order_active():
                return False
            deadline_ms = self._resolve_wait_deadline_ms("ENTRY_WAIT")
            if deadline_ms and no_progress_ms >= deadline_ms:
                self._logger(
                    f"[PROGRESS_DEADLINE] state=ENTRY_WORKING no_progress_ms={no_progress_ms}"
                )
                return self._recover_stuck(
                    reason="progress_deadline_entry",
                    wait_kind="ENTRY_WAIT",
                    age_ms=no_progress_ms,
                )
        if self._in_exit_workflow():
            exit_kind = "EXIT_CROSS_WAIT" if self._tp_exit_phase == "CROSS" else "EXIT_MAKER_WAIT"
            deadline_ms = self._resolve_wait_deadline_ms(exit_kind)
            if deadline_ms and no_progress_ms >= deadline_ms:
                self._logger(
                    f"[PROGRESS_DEADLINE] state=EXIT_WORKING no_progress_ms={no_progress_ms}"
                )
                return self._recover_stuck(
                    reason="progress_deadline_exit",
                    wait_kind="EXIT_WAIT",
                    age_ms=no_progress_ms,
                )
        return False

    def _open_orders_signature(self, open_orders: list[dict]) -> tuple:
        signature = []
        for order in open_orders:
            signature.append(
                (
                    order.get("orderId"),
                    str(order.get("status") or "").upper(),
                    str(order.get("side") or "").upper(),
                    float(order.get("price") or 0.0),
                    float(order.get("origQty") or order.get("origQuantity") or 0.0),
                    float(order.get("executedQty") or order.get("executedQuantity") or 0.0),
                )
            )
        signature.sort()
        return tuple(signature)

    def _update_open_orders_signature(self, open_orders: list[dict]) -> bool:
        signature = self._open_orders_signature(open_orders)
        if signature != self._last_open_orders_signature:
            self._last_open_orders_signature = signature
            return True
        return False

    def _transition_state(self, next_state: TradeState, reason: str = "auto") -> None:
        if self.state == next_state:
            return
        previous = self.state
        self.state = next_state
        self._state_entered_ts = time.monotonic()
        if previous == TradeState.STATE_ENTRY_WORKING and next_state in {
            TradeState.STATE_POS_OPEN,
            TradeState.STATE_EXIT_TP_WORKING,
            TradeState.STATE_EXIT_SL_WORKING,
        }:
            self._clear_entry_wait()
            self._finalize_entry_ownership(reason=f"state:{next_state.value}")
        if previous in {
            TradeState.STATE_EXIT_TP_WORKING,
            TradeState.STATE_EXIT_SL_WORKING,
        } and next_state == TradeState.STATE_FLAT:
            self._clear_exit_wait()
        reset_reconcile = next_state not in {
            TradeState.STATE_RECONCILE,
            TradeState.STATE_SAFE_STOP,
        }
        self._mark_progress(reason=f"state:{previous.value}->{next_state.value}", reset_reconcile=reset_reconcile)
        self._logger(
            f"[STATE] from={previous.value} to={next_state.value} reason={reason}"
        )
        if next_state == TradeState.STATE_STOPPING:
            self._transition_pipeline_state(PipelineState.STOPPING, reason="state")
        elif next_state == TradeState.STATE_STOPPED:
            self._transition_pipeline_state(PipelineState.STOPPED, reason="state")
        elif next_state == TradeState.STATE_ERROR:
            self._transition_pipeline_state(PipelineState.ERROR, reason="state")
        if next_state == TradeState.STATE_ERROR:
            self.ledger.mark_error(notes="STATE_ERROR")

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

    def _transition_pipeline_state(
        self, next_state: PipelineState, reason: str = "auto"
    ) -> None:
        if self._pipeline_state == next_state:
            return
        previous = self._pipeline_state
        self._pipeline_state = next_state
        prev_label = previous.value if previous else "—"
        self._logger(
            f"[FSM] from={prev_label} to={next_state.value} reason={reason}"
        )

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
        self._cycle_started_ts = time.monotonic()
        self._logger(f"[CYCLE_START] id={cycle_id}")
        self._entry_target_qty = None
        if self.ledger._next_id <= cycle_id:
            self.ledger._next_id = cycle_id
        ledger_cycle_id = self.ledger.start_cycle(
            symbol=self._settings.symbol,
            direction=self._direction,
            entry_qty=0.0,
            entry_avg=0.0,
            notes="START",
        )
        self._current_cycle_id = ledger_cycle_id

    def _log_cycle_fill(self, cycle_id: Optional[int], side: str, delta: float, cum: float) -> None:
        if cycle_id is None:
            cycle_id = self._current_cycle_id
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        self._logger(
            f"[CYCLE_FILL] id={cycle_label} side={side} delta={delta:.8f} cum={cum:.8f}"
        )

    def _sync_cycle_qty_from_ledger(self, cycle_id: Optional[int]) -> Optional[object]:
        cycle = self.ledger.get_cycle(cycle_id)
        if cycle is None or cycle.status != CycleStatus.OPEN:
            return None
        self._executed_qty_total = cycle.executed_qty
        self._closed_qty_total = cycle.closed_qty
        self._remaining_qty = cycle.remaining_qty
        self._aggregate_sold_qty = cycle.closed_qty
        if cycle.entry_avg > 0:
            self._entry_avg_price = cycle.entry_avg
            self._last_buy_price = cycle.entry_avg
        return cycle

    def _entry_blocked_by_active_position(self) -> bool:
        cycle = self.ledger.active_cycle
        if (
            cycle
            and cycle.status == CycleStatus.OPEN
            and cycle.remaining_qty > self._epsilon_qty()
        ):
            if not self._should_dedup_log("entry_blocked_active_pos", 1.0):
                self._logger(
                    "[ENTRY_BLOCKED_ACTIVE_POS] "
                    f"cycle={cycle.cycle_id} rem={cycle.remaining_qty:.8f}"
                )
            return True
        return False

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

    def _mark_cycle_entry_start(self) -> None:
        if self._cycle_start_ts_ms is None:
            self._cycle_start_ts_ms = int(time.time() * 1000)

    def _begin_cycle_entry(self, now_ms: Optional[int] = None) -> None:
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        if self._current_cycle_id is None:
            self._cycle_id_counter += 1
            self._current_cycle_id = self._cycle_id_counter
        else:
            if self._current_cycle_id > self._cycle_id_counter:
                self._cycle_id_counter = self._current_cycle_id
        self._cycle_start_ts_ms = now_ms
        self._cycle_order_ids = set()

    def _cycle_client_prefix(self, cycle_id: Optional[int]) -> str:
        cycle_label = 0 if cycle_id is None else cycle_id
        return f"{self._client_tag}_C{cycle_label}"

    @staticmethod
    def _extract_trade_id(order: Optional[dict]) -> Optional[object]:
        if not order:
            return None
        for key in (
            "tradeId",
            "trade_id",
            "aggTradeId",
            "agg_trade_id",
            "fillId",
            "fill_id",
        ):
            if order.get(key) is not None:
                return order.get(key)
        return None

    @staticmethod
    def _extract_is_buyer_maker(order: Optional[dict]) -> Optional[bool]:
        if not order:
            return None
        if "isBuyerMaker" in order:
            return bool(order.get("isBuyerMaker"))
        if "is_buyer_maker" in order:
            return bool(order.get("is_buyer_maker"))
        return None

    @staticmethod
    def _fill_dedupe_bucket(ts_ms: Optional[int]) -> int:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        return int(ts_ms / 1000)

    def _fill_dedupe_key(
        self,
        order_id: Optional[int],
        side: Optional[str],
        qty: float,
        price: float,
        is_buyer_maker: Optional[bool],
        ts_ms: Optional[int],
        trade_id: Optional[object],
    ) -> Optional[tuple[object, ...]]:
        if trade_id is not None:
            return ("trade", trade_id)
        if order_id is None:
            return None
        side_label = str(side or "UNKNOWN").upper()
        qty_key = qty
        price_key = price
        if self._profile.step_size and self._profile.step_size > 0:
            qty_key = self._round_down(qty, self._profile.step_size)
        if self._profile.tick_size and self._profile.tick_size > 0:
            price_key = self._round_to_tick(price, self._profile.tick_size)
        return (
            int(order_id),
            side_label,
            float(qty_key),
            float(price_key),
            is_buyer_maker,
            self._fill_dedupe_bucket(ts_ms),
        )

    def _prune_seen_fills(self, now: Optional[float] = None) -> None:
        if now is None:
            now = time.monotonic()
        while self._seen_fill_queue and (
            now - self._seen_fill_queue[0][1] > self.SEEN_FILL_TTL_S
            or len(self._seen_fill_queue) > self.MAX_SEEN_FILL_KEYS
        ):
            key, _ = self._seen_fill_queue.popleft()
            self._seen_fill_keys.discard(key)

    def _log_fill_dedup(
        self,
        action: str,
        order_id: Optional[int],
        qty: float,
        price: float,
        trade_id: Optional[object],
        ts_ms: Optional[int],
    ) -> None:
        order_label = "—" if order_id is None else str(order_id)
        trade_label = "—" if trade_id is None else str(trade_id)
        ts_label = "—" if ts_ms is None else str(ts_ms)
        self._logger(
            f"[FILL_DEDUP_{action}] order_id={order_label} "
            f"qty={qty:.8f} price={price:.8f} trade_id={trade_label} ts={ts_label}"
        )

    def _should_apply_fill_dedup(
        self,
        order_id: Optional[int],
        side: Optional[str],
        qty: float,
        price: float,
        is_buyer_maker: Optional[bool],
        ts_ms: Optional[int],
        trade_id: Optional[object],
    ) -> bool:
        key = self._fill_dedupe_key(
            order_id, side, qty, price, is_buyer_maker, ts_ms, trade_id
        )
        self._prune_seen_fills()
        if key is not None and key in self._seen_fill_keys:
            self._log_fill_dedup("SKIP", order_id, qty, price, trade_id, ts_ms)
            return False
        if key is not None:
            self._seen_fill_keys.add(key)
            self._seen_fill_queue.append((key, time.monotonic()))
            self._prune_seen_fills()
        self._log_fill_dedup("APPLY", order_id, qty, price, trade_id, ts_ms)
        return True

    def _fill_event_key(
        self,
        order_id: Optional[int],
        side: str,
        cum_qty: float,
        trade_id: Optional[object] = None,
    ) -> Optional[tuple[object, ...]]:
        if trade_id is not None:
            return ("trade", trade_id)
        if order_id is None:
            return None
        return (int(order_id), side, float(cum_qty))

    def _resolve_fill_delta(self, order_id: Optional[int], cum_qty: float) -> float:
        if cum_qty <= 0:
            return 0.0
        if not isinstance(order_id, int):
            return max(cum_qty, 0.0)
        new_cum = Decimal(str(cum_qty))
        prev_cum = self._last_applied_cum_qty.get(order_id, Decimal("0"))
        if new_cum <= prev_cum:
            self._logger(
                "[FILL_SKIP_DUP_CUM] "
                f"order_id={order_id} cum={new_cum:.8f} prev={prev_cum:.8f}"
            )
            return 0.0
        delta = new_cum - prev_cum
        self._last_applied_cum_qty[order_id] = new_cum
        self._logger(
            "[FILL_APPLY_DELTA] "
            f"order_id={order_id} cum={new_cum:.8f} prev={prev_cum:.8f} "
            f"delta={delta:.8f}"
        )
        return float(delta)

    def _should_process_fill(
        self,
        order_id: Optional[int],
        side: str,
        cum_qty: float,
        avg_price: float,
        cycle_id: Optional[int],
        trade_id: Optional[object] = None,
    ) -> bool:
        key = self._fill_event_key(order_id, side, cum_qty, trade_id)
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
        self._processed_fill_queue.append(key)
        if len(self._processed_fill_queue) > self.MAX_FILL_KEYS:
            expired = self._processed_fill_queue.popleft()
            self._processed_fill_keys.discard(expired)
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
        if reason in {"TP", "TP_MAKER", "TP_FORCE", "TP_CROSS"}:
            return float(sell_order.get("qty") or 0.0)
        return 0.0

    def _log_trade_snapshot(self, reason: str, sl_mode: Optional[str] = None) -> None:
        pos_qty = self._resolve_remaining_qty_raw()
        executed_qty = float(self._executed_qty_total or self.position_qty_base or 0.0)
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

    def _epsilon_qty(self) -> float:
        return float(getattr(self._settings, "epsilon_qty", 1e-6) or 1e-6)

    def _log_position_state(
        self,
        executed_qty: float,
        closed_qty: float,
        remaining_qty: float,
        entry_avg: Optional[float],
    ) -> None:
        snapshot = (executed_qty, closed_qty, remaining_qty, entry_avg)
        if self._last_pos_snapshot == snapshot:
            return
        tp_price, sl_price = self._compute_tp_sl_prices(entry_avg)
        avg_label = "—" if entry_avg is None else f"{entry_avg:.8f}"
        tp_label = "—" if tp_price is None else f"{tp_price:.8f}"
        sl_label = "—" if sl_price is None else f"{sl_price:.8f}"
        self._logger(
            "[POS] "
            f"executed={executed_qty:.8f} closed={closed_qty:.8f} "
            f"remaining={remaining_qty:.8f} avg={avg_label} "
            f"tp={tp_label} sl={sl_label}"
        )
        self._last_pos_snapshot = snapshot

    def _reconcile_position_from_fills(self) -> None:
        cycle = self.ledger.active_cycle
        executed = 0.0
        closed = 0.0
        remaining = 0.0
        entry_avg = None
        if cycle and cycle.status == CycleStatus.OPEN:
            executed = cycle.executed_qty
            closed = cycle.closed_qty
            remaining = cycle.remaining_qty
            entry_avg = cycle.entry_avg if cycle.entry_avg > 0 else None
        else:
            executed = self._executed_qty_total
            closed = self._closed_qty_total
            remaining = self._remaining_qty
            entry_avg = self._entry_avg_price or self.get_buy_price()

        self._executed_qty_total = executed
        self._closed_qty_total = closed
        self._remaining_qty = remaining
        self._aggregate_sold_qty = closed
        if entry_avg is not None:
            self._entry_avg_price = entry_avg
            self._last_buy_price = entry_avg
        if executed > self._epsilon_qty():
            now_ms = int(time.time() * 1000)
            if self.position is None:
                self.position = {
                    "buy_price": entry_avg,
                    "qty": remaining,
                    "opened_ts": now_ms,
                    "partial": True,
                    "initial_qty": executed,
                    "side": self._direction,
                }
            else:
                if entry_avg is not None:
                    self.position["buy_price"] = entry_avg
                self.position["qty"] = remaining
                if not self.position.get("opened_ts"):
                    self.position["opened_ts"] = now_ms
                self.position["initial_qty"] = executed
            base_qty = executed
            if self._profile.step_size:
                base_qty = self._round_down(base_qty, self._profile.step_size)
            self.position_qty_base = base_qty
        self._log_position_state(executed, closed, remaining, entry_avg)

    def _compute_tp_sl_prices(
        self, entry_avg: Optional[float] = None
    ) -> tuple[Optional[float], Optional[float]]:
        if entry_avg is None:
            entry_avg = self._entry_avg_price
        tick_size = self._profile.tick_size
        if entry_avg is None or entry_avg <= 0 or tick_size is None or tick_size <= 0:
            return None, None
        tp_ticks = int(self._settings.take_profit_ticks)
        sl_ticks = int(self._settings.stop_loss_ticks)
        tp_price = None
        sl_price = None
        position_side = self._position_side()
        if position_side not in {"LONG", "SHORT"}:
            return None, None
        if tp_ticks > 0:
            if position_side == "SHORT":
                raw_tp = entry_avg - (tp_ticks * tick_size)
                tp_price = self._round_to_tick(raw_tp, tick_size, mode="DOWN")
            else:
                raw_tp = entry_avg + (tp_ticks * tick_size)
                tp_price = self._round_to_tick(raw_tp, tick_size, mode="UP")
        if sl_ticks > 0:
            if position_side == "SHORT":
                raw_sl = entry_avg + (sl_ticks * tick_size)
                sl_price = self._round_to_tick(raw_sl, tick_size, mode="UP")
            else:
                raw_sl = entry_avg - (sl_ticks * tick_size)
                sl_price = self._round_to_tick(raw_sl, tick_size, mode="DOWN")

        executed_qty = self.get_executed_qty_total()
        tp_label = "—" if tp_price is None else f"{tp_price:.8f}"
        sl_label = "—" if sl_price is None else f"{sl_price:.8f}"
        self._logger(
            "[TRIGGERS] "
            f"dir={position_side} entry_avg={entry_avg:.8f} exec_qty={executed_qty:.8f} "
            f"tick={tick_size:.8f} tp_ticks={tp_ticks} sl_ticks={sl_ticks} "
            f"tp={tp_label} sl={sl_label}"
        )

        invalid = False
        if position_side == "SHORT":
            if tp_price is not None and tp_price >= entry_avg:
                invalid = True
            if sl_price is not None and sl_price <= entry_avg:
                invalid = True
        else:
            if tp_price is not None and tp_price <= entry_avg:
                invalid = True
            if sl_price is not None and sl_price >= entry_avg:
                invalid = True
        if invalid:
            self._logger(
                "[WARN][TRIGGERS] "
                f"dir={position_side} entry_avg={entry_avg:.8f} tp={tp_label} sl={sl_label}"
            )
            return None, None
        return tp_price, sl_price

    def _tp_trigger_price(
        self,
        entry_price: Optional[float],
        tick_size: Optional[float],
        ticks: int,
        position_side: str,
    ) -> Optional[float]:
        if entry_price is None or tick_size is None or tick_size <= 0 or ticks <= 0:
            return None
        if position_side == "SHORT":
            return entry_price - ticks * tick_size
        return entry_price + ticks * tick_size

    def _sl_trigger_price(
        self,
        entry_price: Optional[float],
        tick_size: Optional[float],
        ticks: int,
        position_side: str,
    ) -> Optional[float]:
        if entry_price is None or tick_size is None or tick_size <= 0 or ticks <= 0:
            return None
        if position_side == "SHORT":
            return entry_price + ticks * tick_size
        return entry_price - ticks * tick_size

    def _tp_follow_top_price(
        self,
        entry_price: Optional[float],
        best_ask: Optional[float],
        tick_size: Optional[float],
        ticks: int,
        position_side: str,
    ) -> Optional[float]:
        if (
            entry_price is None
            or best_ask is None
            or tick_size is None
            or tick_size <= 0
            or ticks <= 0
        ):
            return None
        if position_side == "SHORT":
            return None
        target_price = entry_price + ticks * tick_size
        raw_price = max(best_ask, target_price)
        rounded = TradeExecutor._round_to_step(raw_price, tick_size)
        if rounded < tick_size:
            rounded = tick_size
        return rounded

    def _tp_follow_top_price_buy(
        self,
        entry_price: Optional[float],
        best_bid: Optional[float],
        tick_size: Optional[float],
        ticks: int,
        position_side: str,
    ) -> Optional[float]:
        if (
            entry_price is None
            or best_bid is None
            or tick_size is None
            or tick_size <= 0
            or ticks <= 0
        ):
            return None
        if position_side != "SHORT":
            return None
        target_price = entry_price - ticks * tick_size
        raw_price = min(best_bid, target_price)
        rounded = TradeExecutor._round_to_step(raw_price, tick_size)
        if rounded < tick_size:
            rounded = tick_size
        return rounded

    def _tp_order_age_ms(self) -> Optional[int]:
        sell_order = self._get_active_sell_order()
        if not sell_order:
            return None
        reason = str(sell_order.get("reason") or "").upper()
        if reason not in {"TP", "TP_MAKER"}:
            return None
        created_ts = sell_order.get("created_ts")
        if created_ts is None:
            return None
        now_ms = int(time.time() * 1000)
        return max(now_ms - int(created_ts), 0)

    def _resolve_exit_order_type(self, intent: str, reason_upper: str) -> str:
        if intent == "TP":
            if reason_upper == "TP_FORCE":
                return "MARKET"
            return "LIMIT"
        if intent == "SL":
            return "MARKET"
        return self._normalize_order_type(self._settings.exit_order_type)

    def _exit_order_reason(self, intent: Optional[str]) -> str:
        normalized = self._normalize_exit_intent(intent)
        if normalized == "TP":
            return "TP_MAKER"
        if normalized == "SL":
            return "SL_HARD"
        return "MANUAL"

    def _log_exit_summary(
        self,
        exit_reason: str,
        entry_price: Optional[float],
        exit_price: Optional[float],
        qty: float,
    ) -> None:
        tick_size = self._profile.tick_size
        position_side = self._position_side()
        ticks_realized = None
        if (
            entry_price is not None
            and exit_price is not None
            and tick_size is not None
            and tick_size > 0
        ):
            if position_side == "SHORT":
                ticks_realized = (entry_price - exit_price) / tick_size
            else:
                ticks_realized = (exit_price - entry_price) / tick_size
        latency_ms = None
        if self._exit_started_ts is not None:
            latency_ms = int((time.monotonic() - self._exit_started_ts) * 1000.0)
        elif self.exit_intent_set_ts is not None:
            latency_ms = int((time.monotonic() - self.exit_intent_set_ts) * 1000.0)
        entry_label = "—" if entry_price is None else f"{entry_price:.8f}"
        exit_label = "—" if exit_price is None else f"{exit_price:.8f}"
        qty_label = "—" if qty <= 0 else f"{qty:.8f}"
        ticks_label = "—" if ticks_realized is None else f"{ticks_realized:.2f}"
        latency_label = "—" if latency_ms is None else str(latency_ms)
        self._logger(
            f"[EXIT] {self._direction_tag()} "
            f"exit_reason={exit_reason} entry_price={entry_label} "
            f"exit_price={exit_label} pos_qty={qty_label} "
            f"ticks_realized={ticks_label} latency_ms={latency_label}"
        )

    def _settlement_balance_retry(
        self, required_qty: float, base_free: float, base_locked: float
    ) -> tuple[float, float, int]:
        attempts = 0
        grace_ms = int(
            getattr(
                self._settings,
                "sell_refresh_grace_ms",
                getattr(self._settings, "settlement_grace_ms", 400),
            )
        )
        grace_s = max(0.05, grace_ms / 1000.0)
        for delay_s in (grace_s * 0.5, grace_s * 0.75, grace_s):
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
                f"[RECOVERY_SET] {self._direction_tag()} "
                f"residual={self._recovery_residual_qty:.8f} reason={reason}"
            )
            self._log_trade_snapshot(reason=f"recovery:{reason}")

    def _clear_recovery(self, reason: str) -> None:
        if self._recovery_active:
            self._logger(f"[RECOVERY_CLEAR] {self._direction_tag()} reason={reason}")
        self._recovery_residual_qty = 0.0
        self._recovery_active = False
        self._recovery_last_check_ts = None

    def _attempt_recovery_sell(self) -> None:
        if not self._recovery_active:
            return
        if (
            self.sell_place_inflight
            or self.sell_cancel_pending
            or self._has_active_order(self._exit_side())
        ):
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
            f"[RECOVERY_RETRY] {self._direction_tag()} "
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
        self._processed_fill_queue.clear()
        self._seen_fill_keys = set()
        self._seen_fill_queue.clear()
        self._last_progress_bid = None
        self._last_progress_ask = None
        self._data_blind_since_ts = None
        self.entry_active_order_id = None
        self.entry_active_price = None
        self.entry_cancel_pending = False
        self._entry_missing_since_ts = None
        self._current_cycle_id = None
        self._entry_target_qty = None
        self._cycle_started_ts = None
        self._cycle_start_ts_ms = None
        self._cycle_order_ids = set()
        self._log_cycle_cleanup(cycle_id)

    def set_margin_capabilities(
        self, margin_api_access: bool, borrow_allowed_by_api: Optional[bool]
    ) -> None:
        self._borrow_allowed_by_api = borrow_allowed_by_api

    def place_test_orders_margin(self) -> int:
        if self._inflight_trade_action:
            self.last_action = "inflight"
            return 0
        if self._entry_blocked_by_active_position():
            self.last_action = "entry_blocked_active_pos"
            return 0
        self._inflight_trade_action = True
        inflight_ms = int(getattr(self._settings, "inflight_deadline_ms", 2500))
        self._entry_inflight_deadline_ts = time.monotonic() + (inflight_ms / 1000.0)
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
            entry_side = self._entry_side()
            exit_side = self._exit_side()
            if self._has_active_role("ENTRY"):
                self._log_guard_active_order("ENTRY")
                self.last_action = "entry_active_guard"
                return 0
            if self._has_active_order(exit_side) or self.sell_active_order_id is not None:
                self._logger(
                    f"[ERROR] active {exit_side} detected during PLACE_{entry_side} -> SAFE_STOP"
                )
                self.abort_cycle_with_reason(reason="ACTIVE_SELL_ON_BUY", critical=True)
                return 0
            if self._has_active_order(entry_side) or self.position is not None:
                self._logger(f"[TRADE] place_test_orders | {entry_side} already active")
                self.last_action = "buy_active"
                return 0
            price_state, health_state = self._router.build_price_state()
            self._update_data_blind_state(price_state)
            mid = price_state.mid
            bid = price_state.bid
            ask = price_state.ask
            active_src = price_state.source
            if not self._can_trade_now(
                price_state,
                action="ENTRY",
                max_age_ms=self._trade_gate_age_ms("ENTRY"),
            ):
                self.last_action = "entry_blocked"
                return 0
            ws_bad = active_src == "WS" and self._entry_ws_bad(health_state.ws_connected)
            entry_context = self._entry_log_context(
                active_src,
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
            ref_price, ref_label = self._entry_reference(bid, ask)
            if mid is None or ref_price is None:
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

            entry_price = self._calculate_entry_price(ref_price)
            if entry_price is None:
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
            remaining_qty = self._remaining_to_target(entry_price)
            self._log_entry_target(entry_price, remaining_qty)
            qty = self._apply_dust_carry_to_entry_qty(remaining_qty)

            if qty <= 0:
                self._logger(f"[TRADE] place_test_orders | qty <= 0 tag={self.TAG}")
                return 0
            if qty < self._profile.min_qty:
                self._logger(
                    "[TRADE] place_test_orders | "
                    f"qty below minQty ({qty} < {self._profile.min_qty}) tag={self.TAG}"
                )
                return 0

            notional = qty * entry_price
            if self._profile.min_notional is None:
                self._logger("[TRADE] place_test_orders | minNotional unknown")
            elif notional < self._profile.min_notional:
                self._logger(
                    "[TRADE] place_test_orders | "
                    f"notional below minNotional ({notional} < {self._profile.min_notional}) "
                    f"tag={self.TAG}"
                )
                return 0

            buy_cost = qty * entry_price
            can_buy = True
            if quote_state["free"] < buy_cost:
                self._logger(f"[PRECHECK] insufficient {quote_asset} for {entry_side}")
                can_buy = False

            if not can_buy:
                self.last_action = "precheck_failed"
                self.orders_count = 0
                return 0

            self._logger(
                "[TRADE] place_test_orders | "
                f"mode={self._settings.account_mode} "
                f"lev_hint={self._settings.leverage_hint} "
                f"buy={entry_price:.5f} qty={qty:.5f} "
                f"tag={self.TAG}"
            )

            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            self._begin_cycle_entry(now_ms=timestamp)
            entry_client_id = self._build_client_order_id(
                entry_side, timestamp, role_tag=self._entry_role_tag()
            )
            order_type = self._normalize_order_type(self._settings.order_type)
            if order_type == "MARKET":
                self._logger(f"[TRADE] {entry_side} rejected: MARKET disabled")
                self.last_action = "market_buy_rejected"
                self._transition_state(TradeState.STATE_IDLE)
                return 0
            side_effect_type = self._normalize_side_effect_type(
                self._settings.side_effect_type
            )

            cycle_label = self._cycle_id_label()
            ref_value_label = "?" if ref_price is None else f"{ref_price:.8f}"
            self._logger(
                f"[PLACE_{entry_side}] cycle_id={cycle_label} price={ref_label} "
                f"{ref_label}={ref_value_label} "
                f"src={price_state.source}"
            )
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
            self.last_action = "placing_buy"
            self._start_entry_attempt()
            self._entry_reprice_last_ts = time.monotonic()
            self._log_entry_place(entry_price, bid, ask, entry_context)
            entry_order = self._place_margin_order(
                symbol=self._settings.symbol,
                side=entry_side,
                quantity=qty,
                price=entry_price,
                is_isolated=is_isolated,
                client_order_id=entry_client_id,
                side_effect=side_effect_type,
                order_type=order_type,
            )
            if not entry_order:
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
                f"[ORDER] {entry_side} placed cycle_id={cycle_label} qty={qty:.8f} "
                f"price={entry_price:.8f} id={entry_order.get('orderId')}"
            )
            self.mark_progress(reason="entry_place", reset_reconcile=True)
            entry_order_id = entry_order.get("orderId")
            if isinstance(entry_order_id, int):
                self._cycle_order_ids.add(entry_order_id)
                self._clear_start_inflight()
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
            self._reset_buy_retry()
            self._start_buy_wait()
            now_ms = int(time.time() * 1000)
            self.active_test_orders = [
                {
                    "orderId": entry_order_id,
                    "side": entry_side,
                    "price": entry_price,
                    "bid_at_place": bid,
                    "qty": qty,
                    "cum_qty": 0.0,
                    "avg_fill_price": 0.0,
                    "last_fill_price": None,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": entry_order.get("clientOrderId", entry_client_id),
                    "cycle_id": self._current_cycle_id,
                }
            ]
            self.entry_active_order_id = entry_order.get("orderId")
            self.entry_active_price = entry_price
            self.entry_cancel_pending = False
            self._entry_last_ref_bid = ref_price
            self._entry_last_ref_ts_ms = now_ms
            self._entry_order_price = entry_price
            self.orders_count = len(self.active_test_orders)
            self.pnl_cycle = None
            self.last_exit_reason = None
            self._last_buy_price = entry_price
            self._reset_exit_intent()
            return self.orders_count
        finally:
            if cycle_pending and not buy_placed:
                self._current_cycle_id = None
                self._cycle_start_ts_ms = None
                self._cycle_order_ids = set()
            self._inflight_trade_action = False
            self._entry_inflight_deadline_ts = None

    def start_cycle_run(self, direction: str = "LONG") -> int:
        if not self._set_start_inflight():
            self._logger("[START_IGNORED] reason=inflight")
            return 0
        self._set_direction(direction)
        self._logger(f"[START] {self._direction_tag()}")
        self._logger(
            "[PARAMS] "
            f"dir={self._direction} target_usd={float(self._settings.order_quote):.8f} "
            f"max_budget={float(self._settings.max_budget):.8f} "
            f"reserve={float(self._settings.budget_reserve):.8f} "
            f"tp_ticks={int(self._settings.take_profit_ticks)} "
            f"sl_ticks={int(self._settings.stop_loss_ticks)}"
        )
        if self.state not in {TradeState.STATE_IDLE, TradeState.STATE_STOPPED}:
            self._logger(f"[START_IGNORED] reason=state={self.state.value}")
            self._clear_start_inflight()
            return 0
        if self._entry_blocked_by_active_position():
            self._clear_start_inflight()
            return 0
        self.run_active = True
        self.cycles_done = 0
        self.cycles_target = self._normalize_cycle_target(self._settings.cycle_count)
        self._next_cycle_ready_ts = None
        self._transition_pipeline_state(PipelineState.ARM_ENTRY, reason="start_run")
        placed = self._attempt_cycle_start()
        if not placed:
            self._clear_start_inflight()
        if not placed:
            self._next_cycle_ready_ts = time.monotonic() + self._cycle_cooldown_s
            self._logger("[CYCLE_START] delayed: waiting for next cycle window")
        return placed

    def stop_run_by_user(self, reason: str = "user") -> None:
        self.stop_requested = True
        self.run_active = False
        self._next_cycle_ready_ts = None
        self._clear_start_inflight()
        self._logger(f"[STOP_REQUEST] reason={reason}")
        self._handle_stop_interrupt(reason=reason)

    def _cancel_open_orders(self, open_orders: Optional[list[dict]] = None) -> None:
        if open_orders is None:
            self._cancel_all_symbol_orders()
            return
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

    def _handle_stop_interrupt(
        self,
        open_orders: Optional[list[dict]] = None,
        reason: str = "user",
    ) -> bool:
        if not self.stop_requested:
            return False
        self.run_active = False
        self._next_cycle_ready_ts = None
        self._clear_start_inflight()
        self._logger(f"[STOP_INTERRUPT] reason={reason}")
        self._transition_state(TradeState.STATE_STOPPING)
        if open_orders is None:
            try:
                open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
            except httpx.HTTPStatusError as exc:
                self._log_binance_error(
                    "stop_open_orders", exc, {"symbol": self._settings.symbol}
                )
                open_orders = []
            except Exception as exc:
                self._logger(f"[STOP] open_orders error: {exc}")
                open_orders = []
        cancel_count = len(open_orders or [])
        self._cancel_open_orders(open_orders)
        open_left = 0
        last_snapshot: list[dict] = []
        for _ in range(3):
            try:
                last_snapshot = self._rest.get_margin_open_orders(self._settings.symbol)
            except httpx.HTTPStatusError as exc:
                self._log_binance_error(
                    "stop_open_orders_poll", exc, {"symbol": self._settings.symbol}
                )
                last_snapshot = []
            except Exception as exc:
                self._logger(f"[STOP] open_orders poll error: {exc}")
                last_snapshot = []
            open_left = len(last_snapshot)
            if open_left == 0:
                break
            time.sleep(0.3)
        self._logger(f"[STOP_SYNC] canceled={cancel_count} open_left={open_left}")
        cycle = self.ledger.active_cycle
        if cycle and cycle.status == CycleStatus.OPEN:
            if cycle.remaining_qty > self._epsilon_qty():
                if "STOP_WITH_REMAINING" not in (cycle.notes or ""):
                    cycle.notes = (
                        f"{cycle.notes},STOP_WITH_REMAINING"
                        if cycle.notes
                        else "STOP_WITH_REMAINING"
                    )
            else:
                self.ledger.close_cycle(cycle.cycle_id, notes="STOP_CLEAN")
        self._clear_entry_attempt()
        self._clear_entry_wait()
        self._clear_exit_wait()
        self._clear_cancel_wait()
        self._clear_wait_state()
        self._clear_entry_cancel_trackers()
        self.sell_cancel_pending = False
        self.entry_active_order_id = None
        self.sell_active_order_id = None
        self.entry_active_price = None
        self.active_test_orders = []
        self.orders_count = 0
        self.last_action = "STOPPED"
        self._trade_hold = False
        self._transition_state(TradeState.STATE_STOPPED)
        self.stop_requested = False
        return True

    def process_cycle_flow(self) -> int:
        if self._handle_stop_interrupt(reason="tick_stop"):
            return 0
        if not self.run_active:
            return 0
        if not self._is_continuous_run() and self.cycles_done >= self.cycles_target:
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
        self._transition_pipeline_state(PipelineState.ARM_ENTRY, reason="attempt_cycle_start")
        if self._entry_blocked_by_active_position():
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
        if self._is_continuous_run() or self.cycles_done < self.cycles_target:
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

    def _auto_next_cycle(self) -> None:
        self._transition_state(TradeState.STATE_IDLE)
        if not self.run_active:
            self._next_cycle_ready_ts = None
            self._logger("[CYCLE] done -> auto_next state=IDLE reason=stop_requested")
            self._transition_pipeline_state(PipelineState.IDLE, reason="stop_requested")
            return
        if not self._is_continuous_run() and self.cycles_done >= self.cycles_target:
            self.run_active = False
            self._next_cycle_ready_ts = None
            self._logger("[CYCLE] done -> auto_next state=IDLE reason=target_reached")
            self._transition_pipeline_state(PipelineState.IDLE, reason="target_reached")
            return
        price_state, _ = self._router.build_price_state()
        if price_state.data_blind:
            self._next_cycle_ready_ts = time.monotonic() + self._cycle_cooldown_s
            self._logger("[CYCLE] done -> auto_next state=IDLE reason=data_blind")
            self._transition_pipeline_state(PipelineState.NEXT_DEAL, reason="data_blind")
            return
        if self._next_cycle_ready_ts is None:
            self._next_cycle_ready_ts = time.monotonic() + self._cycle_cooldown_s
        self._logger("[CYCLE] done -> auto_next state=IDLE")
        self._transition_pipeline_state(PipelineState.NEXT_DEAL, reason="auto_next")

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

    def _enter_tick(self, open_orders: Optional[dict[int, dict]] = None) -> None:
        if open_orders is None:
            return
        self._handle_entry_follow_top(open_orders)

    def _exit_tick(self) -> None:
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
        self._watchdog_inflight()
        price_state, health_state = self._router.build_price_state()
        self._watchdog_exit_arm(price_state)
        if self._dust_accumulate and not self._entry_order_active():
            remaining_qty = self._remaining_qty
            if remaining_qty > self._epsilon_qty():
                entry_avg = self._entry_avg_price or self.get_buy_price()
                ref_price = self._resolve_dust_reference_price(
                    price_state.bid,
                    price_state.ask,
                    price_state.mid,
                    entry_avg,
                )
                current_notional = 0.0
                if ref_price is not None:
                    current_notional = remaining_qty * ref_price
                if self._attempt_dust_entry_topup(
                    price_state,
                    health_state,
                    current_notional,
                ):
                    return
        self._ensure_exit_orders(price_state, health_state)

    def _attempt_dust_entry_topup(
        self,
        price_state: PriceState,
        health_state: HealthState,
        current_notional: float,
    ) -> bool:
        if not self._profile.tick_size or not self._profile.step_size:
            return False
        min_notional = self._profile.min_notional
        if min_notional is None or min_notional <= 0:
            return False
        bid = price_state.bid
        ask = price_state.ask
        ref_price, ref_label = self._entry_reference(bid, ask)
        if ref_price is None:
            return False
        entry_price = self._calculate_entry_price(ref_price)
        if entry_price is None:
            return False
        _, quote_asset = self._split_symbol(self._settings.symbol)
        quote_state = self._get_margin_asset(quote_asset)
        if quote_state is None:
            return False
        order_quote = float(self._settings.order_quote)
        missing_notional = (min_notional * 1.2) - current_notional
        min_topup_quote = float(
            getattr(self._settings, "min_topup_quote", 0.0) or 0.0
        )
        topup_quote = max(missing_notional, min_topup_quote)
        topup_quote = min(topup_quote, order_quote * 0.25)
        max_budget = float(self._settings.max_budget)
        budget_reserve = float(self._settings.budget_reserve)
        allowed_budget = max(0.0, max_budget - budget_reserve)
        free_quote = float(quote_state.get("free", 0.0))
        if free_quote <= 0:
            if not self._should_dedup_log("dust_wait_funds", 2.0):
                self._logger("[DUST_WAIT_FUNDS] free_quote=0")
            return False
        topup_quote = min(topup_quote, allowed_budget, free_quote)
        if topup_quote <= 0:
            return False
        qty = self._round_down(topup_quote / entry_price, self._profile.step_size)
        if qty <= 0:
            return False
        ws_bad = price_state.source == "WS" and self._entry_ws_bad(health_state.ws_connected)
        entry_context = self._entry_log_context(
            price_state.source,
            ws_bad,
            health_state.ws_age_ms,
            health_state.http_age_ms,
        )
        if not self._should_dedup_log("dust_no_entry", 2.0):
            self._logger("[DUST_NO_ENTRY] placing ENTRY_TOPUP")
        self._logger(
            "[ENTRY_TOPUP] "
            f"price={ref_label} {ref_label}={ref_price:.8f} "
            f"quote={topup_quote:.8f} qty={qty:.8f} "
            f"src={price_state.source}"
        )
        if self._place_entry_topup_order(
            price=ref_price,
            qty=qty,
            bid=bid,
            ask=ask,
            entry_context=entry_context,
        ):
            self._logger(
                f"[DUST_TOPUP_PLACED] qty={qty:.8f} notional={(qty * entry_price):.8f}"
            )
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
            return True
        return False

    def _ensure_exit_orders(
        self,
        price_state: PriceState,
        health_state: HealthState,
        allow_replace: bool = True,
        skip_reconcile: bool = False,
    ) -> None:
        if not skip_reconcile:
            self._reconcile_position_from_fills()
        self._update_data_blind_state(price_state)
        self._note_effective_source(price_state.source)
        self._note_price_progress(price_state.bid, price_state.ask)
        if not self._can_trade_now(
            price_state,
            action="EXIT",
            max_age_ms=self._trade_gate_age_ms("EXIT"),
        ):
            # Invariant: do not transition into exit workflows while data is blind/stale.
            self.last_action = "exit_blocked"
            return
        remaining_qty = self._remaining_qty
        if remaining_qty <= self._epsilon_qty():
            return
        entry_avg = self._entry_avg_price
        tp_price, sl_price = self._compute_tp_sl_prices(entry_avg)
        bid = price_state.bid
        ask = price_state.ask
        mid = price_state.mid
        best_bid, best_ask = self._book_refs(price_state)
        position_side = self._position_side()
        exit_side = self._exit_side()
        if self.exit_intent is None:
            self._set_exit_intent("TP", mid=mid, ticks=int(self._settings.take_profit_ticks))

        sl_triggered = False
        if sl_price is not None:
            if position_side == "SHORT":
                sl_triggered = ask is not None and ask >= sl_price
            else:
                sl_triggered = bid is not None and bid <= sl_price
        if sl_triggered:
            best_bid_label = "—" if bid is None else f"{bid:.8f}"
            best_ask_label = "—" if ask is None else f"{ask:.8f}"
            entry_label = "—" if entry_avg is None else f"{entry_avg:.8f}"
            self._logger(
                "[SL_TRIGGER] "
                f"dir={self._direction} bid={best_bid_label} ask={best_ask_label} "
                f"entry_avg={entry_label} target={sl_price:.8f}"
            )
            self._logger(
                "[SELL_PLAN] "
                f"phase=SL reason=SL qty={remaining_qty:.8f} price={sl_price:.8f} "
                f"bid={best_bid_label} ask={best_ask_label} "
                f"mid={mid if mid is not None else '—'}"
            )
            self._handle_sl_trigger(remaining_qty, sl_price, bid, ask)
            return

        if self._adaptive_mode:
            return

        if tp_price is None:
            return

        now = time.monotonic()
        maker_deadline_ms = self._resolve_wait_deadline_ms("EXIT_MAKER_WAIT")
        elapsed_ms = (
            int((now - self._tp_maker_started_ts) * 1000.0)
            if self._tp_maker_started_ts is not None
            else 0
        )
        tick_size = self._profile.tick_size
        tp_triggered = False
        if tp_price is not None:
            if position_side == "SHORT":
                tp_triggered = best_bid is not None and best_bid <= tp_price
            else:
                tp_triggered = best_ask is not None and best_ask >= tp_price
        if not tp_triggered:
            return
        best_bid_label = "—" if best_bid is None else f"{best_bid:.8f}"
        best_ask_label = "—" if best_ask is None else f"{best_ask:.8f}"
        entry_label = "—" if entry_avg is None else f"{entry_avg:.8f}"
        self._logger(
            "[TP_TRIGGER] "
            f"dir={self._direction} bid={best_bid_label} ask={best_ask_label} "
            f"entry_avg={entry_label} target={tp_price:.8f}"
        )
        cross_reason = "timeout" if elapsed_ms >= maker_deadline_ms else None
        desired_phase = "CROSS" if cross_reason else "MAKER"
        if price_state.data_blind and desired_phase == "CROSS":
            if not self._should_dedup_log("exit_cross_blocked_data_blind", 2.0):
                self._logger("[EXIT_CROSS_BLOCKED] reason=data_blind")
            desired_phase = "MAKER"
        if price_state.data_blind and self._data_blind_since_ts is not None:
            blind_ms = int((time.monotonic() - self._data_blind_since_ts) * 1000.0)
            blind_exit_hard_ms = int(
                getattr(self._settings, "blind_exit_hard_ms", 15000)
            )
            if blind_ms >= blind_exit_hard_ms:
                if not self._should_dedup_log("exit_data_blind_hard", 2.0):
                    self._logger(
                        "[EXIT_DATA_BLIND_HARD] "
                        f"age_ms={blind_ms} action=safe_stop"
                    )
                if not self._has_active_order(self._exit_side()):
                    self._place_sell_order(
                        reason="TP_MAKER",
                        price_override=tp_price,
                        exit_intent="TP",
                        ref_bid=best_bid,
                        ref_ask=best_ask,
                    )
                self._enter_safe_stop(reason="DATA_BLIND_EXIT_HARD", allow_rest=False)
                return
        if desired_phase == "MAKER" and self._tp_exit_phase != "MAKER":
            self._logger("[TP_MAKER_ARMED]")
            self._logger(f"[EXIT] {self._direction_tag()} maker_armed")
        if desired_phase == "CROSS" and self._tp_exit_phase != "CROSS":
            elapsed_label = elapsed_ms
            reason_label = cross_reason or "trigger"
            self._logger(
                f"[TP_CROSS_ARMED] reason={reason_label} elapsed_ms={elapsed_label}"
            )
            self._logger(f"[EXIT] {self._direction_tag()} cross_armed reason={reason_label}")

        active_sell_order = self._get_active_sell_order()
        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            self._sell_last_seen_ts = now
        else:
            if self._sell_last_seen_ts is None:
                self._sell_last_seen_ts = now

        qty_to_sell = remaining_qty
        if self._profile.step_size:
            qty_to_sell = self._round_down(remaining_qty, self._profile.step_size)
        if qty_to_sell <= 0:
            return
        price_ref = self._resolve_dust_reference_price(bid, ask, mid, entry_avg)
        min_notional = self._profile.min_notional
        notional = None
        carry_notional = None
        if price_ref is not None:
            notional = qty_to_sell * price_ref
            carry_notional = remaining_qty * price_ref
        if (
            min_notional is not None
            and min_notional > 0
            and notional is not None
            and notional < min_notional
        ):
            self._dust_accumulate = False
            cycle = self.ledger.active_cycle
            if cycle and cycle.status == CycleStatus.OPEN:
                if "DUST_REMAINING" not in (cycle.notes or ""):
                    cycle.notes = f"{cycle.notes},DUST_REMAINING" if cycle.notes else "DUST_REMAINING"
                close_threshold = self._profile.step_size * 1.01 if self._profile.step_size else 0.0
                if cycle.remaining_qty <= close_threshold:
                    self._finalize_partial_close(reason="DUST_REMAINING")
                    return
                if not self._should_dedup_log("exit_blocked_min_notional", 1.0):
                    notional_label = "?" if notional is None else f"{notional:.8f}"
                    self._logger(
                        "[EXIT_BLOCKED_MINNOTIONAL] "
                        f"cycle={cycle.cycle_id} rem={cycle.remaining_qty:.8f} "
                        f"notional={notional_label} min_notional={min_notional:.8f}"
                    )
            return
        if self._should_carry_dust(remaining_qty, carry_notional, min_notional):
            if carry_notional is not None:
                self._apply_dust_carry(remaining_qty, carry_notional)
            return
        dust_threshold = None
        if min_notional is not None and min_notional > 0:
            dust_threshold = min_notional * 1.05
        if dust_threshold is not None and notional is not None and notional >= dust_threshold:
            if self._dust_accumulate:
                self._logger(f"[DUST_EXIT_READY] notional={notional:.8f}")
            self._dust_accumulate = False
        if (
            min_notional is not None
            and min_notional > 0
            and notional is not None
            and notional < min_notional
        ):
            self._dust_accumulate = True
        if self._dust_accumulate and min_notional is not None and min_notional > 0:
            if not self._should_dedup_log("dust_pos", 2.0):
                notional_label = "?" if notional is None else f"{notional:.8f}"
                self._logger(
                    "[DUST_POS] "
                    f"remaining_qty={qty_to_sell:.8f} notional={notional_label} "
                    f"min_notional={min_notional:.8f} action=accumulate"
                )
            if self._entry_order_active():
                if self.state != TradeState.STATE_ENTRY_WORKING:
                    self._transition_state(TradeState.STATE_ENTRY_WORKING)
                return
            current_notional = notional or 0.0
            if self._attempt_dust_entry_topup(price_state, health_state, current_notional):
                return
            return

        if active_sell_order and not self._sell_order_is_final(active_sell_order):
            existing_qty = float(active_sell_order.get("qty") or 0.0)
            existing_price = float(active_sell_order.get("price") or 0.0)
            reason = str(active_sell_order.get("reason") or "").upper()
            replace_needed = False
            if not math.isclose(
                existing_qty, qty_to_sell, rel_tol=0.0, abs_tol=self._epsilon_qty()
            ):
                replace_needed = True
            if desired_phase == "MAKER" and tp_price is not None and existing_price:
                if not math.isclose(
                    existing_price, tp_price, rel_tol=0.0, abs_tol=self._profile.tick_size or 0.0
                ):
                    replace_needed = True
            if desired_phase == "CROSS" and reason != "TP_CROSS":
                replace_needed = True
            if (
                replace_needed
                and allow_replace
                and not self.sell_cancel_pending
                and not self.sell_place_inflight
            ):
                order_id = active_sell_order.get("orderId")
                if order_id:
                    self._logger(
                        "[SELL_PLAN] "
                        f"phase=CANCEL reason=TP qty={qty_to_sell:.8f} "
                        f"price={tp_price:.8f} bid={bid if bid is not None else '—'} "
                        f"ask={ask if ask is not None else '—'} mid={mid if mid is not None else '—'}"
                    )
                    self._cancel_margin_order(
                        symbol=self._settings.symbol,
                        order_id=order_id,
                        is_isolated="TRUE" if self._settings.margin_isolated else "FALSE",
                        tag=active_sell_order.get("tag", self.TAG),
                    )
                    self.sell_cancel_pending = True
                    self._reset_cancel_wait("EXIT_CANCEL_WAIT")
                    return
            return

        heartbeat_ms = int(getattr(self._settings, "sell_refresh_grace_ms", 400))
        no_sell_elapsed_ms = (now - (self._sell_last_seen_ts or now)) * 1000.0
        if no_sell_elapsed_ms < heartbeat_ms and self.sell_cancel_pending:
            return
        if self.sell_place_inflight or self.sell_cancel_pending:
            return

        exit_reason = "TP_MAKER" if desired_phase == "MAKER" else "TP_CROSS"
        self._tp_exit_phase = desired_phase
        if desired_phase == "CROSS":
            if exit_side == "SELL":
                price_override = best_bid if best_bid is not None else tp_price
            else:
                price_override = best_ask if best_ask is not None else tp_price
        else:
            if tick_size is None or tick_size <= 0 or entry_avg is None:
                price_override = tp_price
            elif position_side == "SHORT":
                target_price = entry_avg - (int(self._settings.take_profit_ticks) * tick_size)
                raw_price = min(best_bid if best_bid is not None else target_price, target_price)
                price_override = self._round_to_step(raw_price, tick_size)
            else:
                target_price = entry_avg + (int(self._settings.take_profit_ticks) * tick_size)
                raw_price = max(best_ask if best_ask is not None else target_price, target_price)
                price_override = self._round_to_step(raw_price, tick_size)
        self._sell_last_seen_ts = now
        best_bid_label = "—" if best_bid is None else f"{best_bid:.8f}"
        best_ask_label = "—" if best_ask is None else f"{best_ask:.8f}"
        self._logger(
            "[SELL_PLAN] "
            f"phase={desired_phase} reason=TP qty={qty_to_sell:.8f} "
            f"price={price_override:.8f} bid={best_bid_label} "
            f"ask={best_ask_label} mid={mid if mid is not None else '—'}"
        )
        self._place_sell_order(
            reason=exit_reason,
            price_override=price_override,
            exit_intent="TP",
            qty_override=qty_to_sell,
            ref_bid=best_bid,
            ref_ask=best_ask,
            tick_size=self._profile.tick_size,
        )

    def evaluate_exit_conditions(self) -> None:
        if self._handle_stop_interrupt(reason="tick_stop"):
            return
        self._exit_tick()

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

    def _resolve_tp_watchdog_ms(self) -> int:
        configured = int(
            getattr(self._settings, "tp_missing_watchdog_ms", 1500) or 1500
        )
        return self._clamp_int(configured, 250, 20000)

    def _watchdog_exit_arm(self, price_state: PriceState) -> None:
        executed_qty = self.get_executed_qty_total()
        remaining_qty = self._resolve_remaining_qty_raw()
        if executed_qty <= self._epsilon_qty() or remaining_qty <= self._epsilon_qty():
            self._tp_missing_since_ts = None
            return
        if self._normalize_exit_intent(self.exit_intent or "") == "SL":
            self._tp_missing_since_ts = None
            return
        if self._count_active_tp_orders() > 0:
            self._tp_missing_since_ts = None
            return
        now = time.monotonic()
        if self._tp_missing_since_ts is None:
            self._tp_missing_since_ts = now
            return
        elapsed_ms = int((now - self._tp_missing_since_ts) * 1000.0)
        watchdog_ms = self._resolve_tp_watchdog_ms()
        if elapsed_ms < watchdog_ms:
            return
        self._logger(
            "[WATCHDOG_EXIT_ARM] "
            f"deal={self._resolve_deal_id(self._current_cycle_id)} "
            f"reason=no_tp_active exec={executed_qty:.8f} rem={remaining_qty:.8f}"
        )
        self._transition_pipeline_state(PipelineState.ARM_EXIT, reason="tp_watchdog")
        self._place_tp_for_fill(remaining_qty, price_state)
        self._tp_missing_since_ts = now

    def _book_refs(self, price_state: PriceState) -> tuple[Optional[float], Optional[float]]:
        bid = self.get_best_bid(price_state)
        ask = self.get_best_ask(price_state)
        if price_state.bid is None or price_state.ask is None:
            if not self._should_dedup_log("book_missing", 2.0):
                bid_label = "—" if price_state.bid is None else f"{price_state.bid:.8f}"
                ask_label = "—" if price_state.ask is None else f"{price_state.ask:.8f}"
                mid_label = "—" if price_state.mid is None else f"{price_state.mid:.8f}"
                self._logger(
                    f"[BOOK_MISSING] bid={bid_label} ask={ask_label} mid={mid_label}"
                )
        return bid, ask

    def _sl_breached(self, price_state: PriceState, sl_price: Optional[float]) -> bool:
        if sl_price is None:
            return False
        best_bid = price_state.bid
        best_ask = price_state.ask
        tick_size = self._profile.tick_size or 0.0
        allowed_ticks = int(
            getattr(self._settings, "exit_sl_emergency_ticks", self.EXIT_SL_EMERGENCY_TICKS)
        )
        buffer = allowed_ticks * tick_size if tick_size else 0.0
        position_side = self._position_side()
        if position_side == "SHORT":
            if best_ask is None:
                return False
            threshold = sl_price + buffer
            return best_ask >= threshold
        if best_bid is None:
            return False
        threshold = sl_price - buffer
        return best_bid <= threshold

    def _exit_elapsed_ms(self) -> Optional[int]:
        if self._exit_started_ts is None:
            return None
        return int((time.monotonic() - self._exit_started_ts) * 1000.0)

    def _resolve_exit_ref_side(self, intent: str) -> str:
        if intent == "TP" and self._tp_exit_phase == "CROSS":
            return "ask" if self._exit_side() == "BUY" else "bid"
        _, ref_side = self._resolve_exit_policy_for_side(intent)
        return ref_side

    @staticmethod
    def get_cross_price(
        direction: str, side_needed: str, book: dict[str, Optional[float]]
    ) -> Optional[float]:
        side = str(side_needed or "").upper()
        bid = book.get("bid")
        ask = book.get("ask")
        if side == "SELL":
            return bid if bid is not None else ask
        if side == "BUY":
            return ask if ask is not None else bid
        return None

    @staticmethod
    def _cap_tp_maker_price(
        side_needed: str,
        tp_price: Optional[float],
        best_bid: Optional[float],
        best_ask: Optional[float],
        tick_size: Optional[float],
    ) -> Optional[float]:
        if tp_price is None:
            return tp_price
        side = str(side_needed or "").upper()
        if side == "SELL" and best_ask is not None:
            raw_price = max(tp_price, best_ask)
            if tick_size is None or tick_size <= 0:
                return raw_price
            return TradeExecutor._round_to_step(raw_price, tick_size)
        if side == "BUY" and best_bid is not None:
            raw_price = min(tp_price, best_bid)
            if tick_size is None or tick_size <= 0:
                return raw_price
            return TradeExecutor._round_to_step(raw_price, tick_size)
        return tp_price

    def _compute_tp_cross_price(
        self,
        bid: Optional[float],
        tick_size: Optional[float],
        cross_ticks: int,
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if bid is None or tick_size is None or tick_size <= 0:
            return None, None, None, "ref_missing"
        raw_price = bid
        rounded = TradeExecutor._round_to_step(raw_price, tick_size)
        if rounded < tick_size:
            rounded = tick_size
        delta_ticks = (rounded - bid) / tick_size
        return rounded, bid, delta_ticks, "TP_CROSS"

    def _compute_tp_cross_price_buy(
        self,
        ask: Optional[float],
        tick_size: Optional[float],
        cross_ticks: int,
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if ask is None or tick_size is None or tick_size <= 0:
            return None, None, None, "ref_missing"
        raw_price = ask
        rounded = TradeExecutor._round_to_step(raw_price, tick_size)
        if rounded < tick_size:
            rounded = tick_size
        delta_ticks = (rounded - ask) / tick_size
        return rounded, ask, delta_ticks, "TP_CROSS"

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

    def _compute_exit_price_dynamic(
        self,
        intent: str,
        bid: Optional[float],
        ask: Optional[float],
        tick_size: Optional[float],
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if self._exit_side() == "BUY":
            if intent == "TP" and self._tp_exit_phase == "CROSS":
                _, _, cross_ticks = self._tp_hybrid_params()
                return self._compute_tp_cross_price_buy(ask, tick_size, cross_ticks)
            return TradeExecutor._compute_exit_buy_price(intent, bid, ask, tick_size)
        return self._compute_exit_sell_price_dynamic(intent, bid, ask, tick_size)

    @staticmethod
    def _normalize_exit_intent(intent: Optional[str]) -> str:
        upper = str(intent or "").upper()
        if upper in {"TP", "TP_MAKER", "TP_FORCE"}:
            return "TP"
        if upper in {"SL", "SL_HARD"}:
            return "SL"
        return "MANUAL"

    @staticmethod
    def _resolve_exit_policy(intent: str) -> tuple[str, str]:
        if intent == "TP":
            return "TP_MAKER", "ask"
        if intent == "SL":
            return "SL_HARD", "bid"
        return "MANUAL", "ask"

    @staticmethod
    def _resolve_exit_policy_buy(intent: str) -> tuple[str, str]:
        if intent == "TP":
            return "TP_MAKER", "bid"
        if intent == "SL":
            return "SL_HARD", "ask"
        return "MANUAL", "bid"

    def _resolve_exit_policy_for_side(self, intent: str) -> tuple[str, str]:
        if self._exit_side() == "BUY":
            return TradeExecutor._resolve_exit_policy_buy(intent)
        return TradeExecutor._resolve_exit_policy(intent)

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

    @staticmethod
    def _compute_exit_buy_price(
        intent: str,
        bid: Optional[float],
        ask: Optional[float],
        tick_size: Optional[float],
    ) -> tuple[Optional[float], Optional[float], Optional[float], str]:
        if tick_size is None or tick_size <= 0:
            return None, None, None, "tick_size_missing"
        policy, ref_side = TradeExecutor._resolve_exit_policy_buy(intent)
        ref_price = bid if ref_side == "bid" else ask
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
        price_state, _ = self._router.build_price_state()
        if not self._can_trade_now(
            price_state,
            action="ENTRY",
            max_age_ms=self._trade_gate_age_ms("ENTRY"),
        ):
            return False
        order_id = buy_order.get("orderId")
        self._logger(
            "[CANCEL_ENTRY_BEFORE_EXIT] "
            f"id={order_id} status={status_label} timeout_s={timeout_s:.1f}"
        )
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        if order_id:
            self._note_pending_replace(order_id)
            final_status = self._safe_cancel_and_sync(
                order_id=order_id,
                is_isolated=is_isolated,
                tag=buy_order.get("tag", self.TAG),
            )
            if final_status in {"FILLED", "PARTIALLY_FILLED"}:
                return True
            if final_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                self._handle_entry_cancel_confirmed(buy_order, order_id, final_status)
                return True
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
        exit_side = self._exit_side()
        entry_side = self._entry_side()
        if self.sell_place_inflight:
            self.last_action = "sell_place_inflight"
            return 0
        if self.sell_cancel_pending:
            self.last_action = "sell_cancel_pending"
            return 0
        reason_upper = reason.upper()
        intent = self._normalize_exit_intent(exit_intent or self.exit_intent or reason)
        allow_multiple_tp = (
            self._adaptive_mode and intent == "TP" and reason_upper in {"TP", "TP_MAKER"}
        )
        exit_order_type = self._resolve_exit_order_type(intent, reason_upper)
        is_cross_intent = (
            exit_order_type == "MARKET"
            or reason_upper in {"TP_CROSS", "TP_FORCE", "SL_HARD", "EMERGENCY", "RECOVERY"}
            or self._tp_exit_phase == "CROSS"
        )
        active_cycle_id = self._current_cycle_id or 0
        active_cross_meta = self._get_active_meta_for_role(
            active_cycle_id, "EXIT_CROSS"
        )
        if active_cross_meta is not None or self._has_active_role("EXIT_CROSS"):
            client_id = active_cross_meta.client_id if active_cross_meta else None
            self._log_role_active_skip(
                active_cycle_id, "EXIT_CROSS", client_id
            )
            self._log_guard_active_order("EXIT_CROSS")
            self.last_action = "exit_cross_active_guard"
            return 0
        active_tp_meta = self._get_active_meta_for_role(
            active_cycle_id, "EXIT_TP"
        )
        if (
            not allow_multiple_tp
            and (active_tp_meta is not None or self._has_active_role("EXIT_TP"))
            and not is_cross_intent
        ):
            client_id = active_tp_meta.client_id if active_tp_meta else None
            self._log_role_active_skip(
                active_cycle_id, "EXIT_TP", client_id
            )
            self._log_guard_active_order("EXIT_TP")
            self.last_action = "exit_tp_active_guard"
            return 0
        active_sell_order = self._get_active_sell_order()
        if (
            active_sell_order
            and not self._sell_order_is_final(active_sell_order)
            and not allow_multiple_tp
        ):
            self._logger("[SELL_SKIP_DUPLICATE] reason=active_sell")
            self._logger(f"[TRADE] close_position | {exit_side} already active")
            self.last_action = "sell_active"
            return 0
        if self._has_active_order(entry_side) and self._executed_qty_total <= self._epsilon_qty():
            if not self._cancel_entry_before_exit():
                self._logger("[CANCEL_ENTRY_FAILED] action=flatten")
                self._flatten_position("CANCEL_ENTRY_FAILED")
                return 0
            if self._has_active_order(entry_side):
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
        if not self._has_active_order(entry_side) and self.position is None:
            self._logger(f"[TRADE] close_position | no {entry_side} to close")
            self.last_action = "no_buy"
            return 0
        self.sell_place_inflight = True
        self.sell_last_attempt_ts = now
        self._inflight_trade_action = True
        inflight_ms = int(getattr(self._settings, "inflight_deadline_ms", 2500))
        self._sell_inflight_deadline_ts = now + (inflight_ms / 1000.0)
        try:
            price_state, health_state = self._router.build_price_state()
            self._update_data_blind_state(price_state)
            self._note_effective_source(price_state.source)
            mid = mid_override if mid_override is not None else price_state.mid
            bid = ref_bid if ref_bid is not None else price_state.bid
            ask = ref_ask if ref_ask is not None else price_state.ask
            if not self._can_trade_now(
                price_state,
                action="EXIT",
                max_age_ms=self._trade_gate_age_ms("EXIT"),
            ):
                self.last_action = "exit_blocked"
                return 0
            intent = self._normalize_exit_intent(exit_intent or self.exit_intent or reason)
            if intent == "TP" and price_override is None:
                tp_price, _ = self._compute_tp_sl_prices()
                if tp_price is not None:
                    price_override = tp_price
            if intent == "TP" and self._tp_exit_phase is None:
                self._tp_exit_phase = "MAKER"
            if self._exit_started_ts is None and intent in {"TP", "SL", "MANUAL"}:
                self._exit_started_ts = time.monotonic()
            tick_size = tick_size or self._profile.tick_size
            reason_upper = reason.upper()
            exit_order_type = self._resolve_exit_order_type(intent, reason_upper)
            if (
                exit_order_type == "LIMIT"
                and intent == "TP"
                and (self._tp_exit_phase == "CROSS" or reason_upper == "TP_CROSS")
            ):
                price_override = TradeExecutor.get_cross_price(
                    self._direction, exit_side, {"bid": bid, "ask": ask}
                )
            if price_state.data_blind:
                if exit_order_type == "MARKET" or reason_upper in {"TP_CROSS", "RECOVERY"}:
                    if not self._should_dedup_log("exit_blocked_data_blind", 2.0):
                        self._logger(
                            f"[EXIT_BLOCKED] {self._direction_tag()} "
                            "action=aggressive_exit reason=data_blind "
                            f"intent={intent} exit_order_type={exit_order_type}"
                        )
                    self.last_action = "data_blind_exit_blocked"
                    return 0
            if exit_order_type == "MARKET" and reason_upper not in {
                "EMERGENCY",
                "SL_HARD",
                "TP_FORCE",
                "PANIC",
                "RECOVERY",
            }:
                self._logger(f"[TRADE] {exit_side} rejected: MARKET disabled")
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
            if qty_override is None:
                qty = remaining_qty_raw
            else:
                qty = min(qty_override, remaining_qty_raw)
            if self._profile.step_size:
                qty = self._round_down(qty, self._profile.step_size)
            active_sell_order = self._get_active_sell_order()
            active_sell_pending = (
                active_sell_order is not None
                and not self._sell_order_is_final(active_sell_order)
            )
            tolerance = self._get_step_size_tolerance()
            if qty <= 0 or remaining_qty_raw <= tolerance:
                if (
                    remaining_qty_raw <= tolerance
                    and not active_sell_pending
                    and self._executed_qty_total > self._epsilon_qty()
                ):
                    self._logger("[SELL_ALREADY_CLOSED_BY_PARTIAL]")
                    self._finalize_partial_close(reason="REMAINING_ZERO")
                return 0
            self._log_trade_snapshot(reason=f"sell_place:{intent}")
            if self._profile.min_qty and qty < self._profile.min_qty:
                self._logger(
                    f"[PARTIAL_TOO_SMALL] qty={qty:.8f} minQty={self._profile.min_qty}"
                )
                if qty <= self._get_step_size_tolerance():
                    if not active_sell_pending:
                        self._logger("[SELL_ALREADY_CLOSED_BY_PARTIAL]")
                        self._finalize_partial_close(reason="PARTIAL")
                    return 0
                if exit_order_type == "MARKET":
                    return self._place_emergency_market_sell(qty)
                self.last_action = "partial_too_small"
                return 0
            price_ref = self._resolve_dust_reference_price(bid, ask, mid, self._entry_avg_price)
            notional = None if price_ref is None else qty * price_ref
            min_notional = self._profile.min_notional
            if (
                min_notional is not None
                and min_notional > 0
                and notional is not None
                and notional < min_notional
            ):
                self._dust_accumulate = False
                cycle = self.ledger.active_cycle
                if cycle and cycle.status == CycleStatus.OPEN:
                    if "DUST_REMAINING" not in (cycle.notes or ""):
                        cycle.notes = f"{cycle.notes},DUST_REMAINING" if cycle.notes else "DUST_REMAINING"
                    if self._profile.step_size:
                        close_threshold = self._profile.step_size * 1.01
                    else:
                        close_threshold = 0.0
                    if cycle.remaining_qty <= close_threshold:
                        self._finalize_partial_close(reason="DUST_REMAINING")
                        return 0
                    if not self._should_dedup_log("exit_blocked_min_notional", 1.0):
                        notional_label = f"{notional:.8f}" if notional is not None else "?"
                        self._logger(
                            "[EXIT_BLOCKED_MINNOTIONAL] "
                            f"cycle={cycle.cycle_id} rem={cycle.remaining_qty:.8f} "
                            f"notional={notional_label} min_notional={min_notional:.8f}"
                        )
                return 0
            notional = None if price_ref is None else remaining_qty_raw * price_ref
            if self._should_carry_dust(remaining_qty_raw, notional, self._profile.min_notional):
                if notional is not None:
                    self._apply_dust_carry(remaining_qty_raw, notional)
                return 0
            if self._is_dust_notional(qty, price_ref):
                min_notional = self._profile.min_notional or 0.0
                notional_label = "?" if price_ref is None else f"{qty * price_ref:.8f}"
                self._logger(
                    "[DUST_POS] "
                    f"remaining_qty={qty:.8f} notional={notional_label} "
                    f"min_notional={min_notional:.8f} action=accumulate"
                )
                self.last_action = "dust_accumulate"
                self._dust_accumulate = True
                self._transition_to_position_state()
                return 0

            if exit_side == "SELL":
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
                        if not self._should_dedup_log("balance_mismatch", 1.0):
                            self._logger(
                                "[BALANCE_MISMATCH] "
                                f"remaining={qty_to_sell:.8f} available={total_available:.8f} "
                                f"free={base_free:.8f} locked={base_locked:.8f}"
                            )
                        base_free, base_locked, attempts = self._settlement_balance_retry(
                            required_qty=qty_to_sell,
                            base_free=base_free,
                            base_locked=base_locked,
                        )
                        total_available = base_free + base_locked
                        if total_available < qty_to_sell:
                            if not self._should_dedup_log("balance_mismatch_retry", 1.0):
                                self._logger(
                                    "[BALANCE_MISMATCH] "
                                    f"after_retry remaining={qty_to_sell:.8f} "
                                    f"available={total_available:.8f} attempts={attempts}"
                                )
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
            exit_policy, sell_ref_label = self._resolve_exit_policy_for_side(intent)
            if reason_upper == "TP_FORCE":
                exit_policy = "TP_FORCE"
            if reason_upper == "SL_HARD":
                exit_policy = "SL_HARD"
            if exit_order_type == "LIMIT":
                if price_override is None:
                    position_side = self._position_side()
                    if intent == "TP" and position_side == "SHORT":
                        take_profit_ticks = int(self._settings.take_profit_ticks)
                        sell_price = self._tp_follow_top_price_buy(
                            entry_price=self.get_buy_price(),
                            best_bid=bid,
                            tick_size=tick_size or self._profile.tick_size,
                            ticks=take_profit_ticks,
                            position_side=position_side,
                        )
                        sell_ref_price = bid
                        sell_ref_label = "bid"
                        if (
                            sell_price is not None
                            and sell_ref_price is not None
                            and tick_size
                            and tick_size > 0
                        ):
                            delta_ticks_to_ref = (sell_price - sell_ref_price) / tick_size
                        exit_policy = "TP_MAKER"
                    elif intent == "TP" and position_side != "SHORT":
                        take_profit_ticks = int(self._settings.take_profit_ticks)
                        sell_price = self._tp_follow_top_price(
                            entry_price=self.get_buy_price(),
                            best_ask=ask,
                            tick_size=tick_size or self._profile.tick_size,
                            ticks=take_profit_ticks,
                            position_side=position_side,
                        )
                        sell_ref_price = ask
                        sell_ref_label = "ask"
                        if (
                            sell_price is not None
                            and sell_ref_price is not None
                            and tick_size
                            and tick_size > 0
                        ):
                            delta_ticks_to_ref = (sell_price - sell_ref_price) / tick_size
                        exit_policy = "TP_MAKER"
                    else:
                        (
                            sell_price,
                            sell_ref_price,
                            delta_ticks_to_ref,
                            exit_policy,
                        ) = self._compute_exit_price_dynamic(
                            intent=intent,
                            bid=bid,
                            ask=ask,
                            tick_size=tick_size or self._profile.tick_size,
                        )
                else:
                    sell_price = price_override
                    if intent == "TP" and exit_order_type == "LIMIT":
                        sell_price = self._cap_tp_maker_price(
                            exit_side, sell_price, bid, ask, tick_size or self._profile.tick_size
                        )
                        sell_ref_label = "ask"
                        sell_ref_price = ask
                    sell_ref_price = ask if sell_ref_label == "ask" else bid
                    if (
                        sell_ref_price is not None
                        and tick_size
                        and tick_size > 0
                        and sell_price is not None
                    ):
                        delta_ticks_to_ref = (sell_price - sell_ref_price) / tick_size
                if reason_upper == "TP_FORCE":
                    exit_policy = "TP_FORCE"
                if reason_upper == "SL_HARD":
                    exit_policy = "SL_HARD"
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

            exit_role = "EXIT_TP"
            if (
                exit_order_type == "MARKET"
                or reason_upper in {"TP_CROSS", "TP_FORCE", "SL_HARD", "RECOVERY"}
                or self._tp_exit_phase == "CROSS"
            ):
                exit_role = "EXIT_CROSS"
            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            exit_client_id = self._build_client_order_id(
                exit_side, timestamp + 1, role_tag=self._exit_role_tag(exit_role)
            )
            side_effect_type = (
                "AUTO_REPAY"
                if self._settings.allow_borrow and self._borrow_allowed_by_api is not False
                else None
            )
            order_type = exit_order_type
            if reason_upper == "TP_CROSS":
                order_type = "LIMIT"

            cycle_label = self._cycle_id_label()
            ref_label = "best_ask" if exit_side == "SELL" else "best_bid"
            ref_value = ask if exit_side == "SELL" else bid
            ref_value_label = "?" if ref_value is None else f"{ref_value:.8f}"
            ws_bad = price_state.source == "WS" and self._entry_ws_bad(health_state.ws_connected)
            ws_age_label = health_state.ws_age_ms if health_state.ws_age_ms is not None else "?"
            http_age_label = health_state.http_age_ms if health_state.http_age_ms is not None else "?"
            self._logger(
                f"[PLACE_{exit_side}] cycle_id={cycle_label} price={ref_label} {ref_label}={ref_value_label} "
                f"active_source={price_state.source} ws_age={ws_age_label} "
                f"http_age={http_age_label} ws_bad={ws_bad}"
            )
            self._transition_state(self._exit_state_for_intent(intent))
            if reason_upper in {"TP", "TP_MAKER", "TP_FORCE", "TP_CROSS"}:
                self.last_action = "placing_sell_tp"
            else:
                self.last_action = "placing_sell"
            price_label = "MARKET" if sell_price is None else f"{sell_price:.8f}"
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} side={exit_side} price={price_label} qty={qty:.8f} "
                f"policy={exit_policy}"
            )
            policy_label = "CROSS" if exit_role == "EXIT_CROSS" else exit_policy
            self._logger(
                f"[EXIT_{self._direction}] side={exit_side} policy={policy_label}"
            )
            if self._direction == "SHORT":
                exit_price_label = "MARKET" if sell_price is None else f"{sell_price:.8f}"
                self._logger(
                    f"[EXIT_SHORT] side={exit_side} policy={policy_label} price={exit_price_label}"
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
                f"cycle_id={cycle_label} side={exit_side} exit_intent={intent} exit_policy={exit_policy} "
                f"sell_ref={sell_ref_label} bid={bid_label} ask={ask_label} "
                f"mid={mid_label} sell_price={price_label} "
                f"delta_ticks_to_ref={delta_label} qty={qty:.8f}"
                f" elapsed_ms={elapsed_label} phase={phase_label}"
            )
            if self.position is not None and self.position.get("partial"):
                self._logger(f"[SELL_FOR_PARTIAL] qty={qty:.8f}")
            tp_key = None
            if exit_role == "EXIT_TP" and sell_price is not None:
                deal_id = self._resolve_deal_id(self._current_cycle_id)
                tp_key = self._build_tp_key(
                    deal_id,
                    self._direction,
                    exit_side,
                    qty,
                    sell_price,
                )
                if tp_key in self._active_tp_keys:
                    self._log_tp_skip_dup(self._current_cycle_id, tp_key)
                    self.last_action = "tp_skip_dup"
                    return 0
                self._log_tp_place(
                    self._current_cycle_id,
                    exit_side,
                    qty,
                    sell_price,
                    tp_key,
                )
            self._register_order_meta(
                client_id=exit_client_id,
                role=exit_role,
                side=exit_side,
                price=sell_price if sell_price is not None else mid or 0.0,
                qty=qty,
                tp_key=tp_key,
            )
            exit_order = self._place_margin_order(
                symbol=self._settings.symbol,
                side=exit_side,
                quantity=qty,
                price=sell_price,
                is_isolated=is_isolated,
                client_order_id=exit_client_id,
                side_effect=side_effect_type,
                order_type=order_type,
            )
            if not exit_order:
                self._update_order_status(exit_client_id, "REJECTED")
                if self._last_place_error_code == -2010:
                    return self._handle_sell_insufficient_balance(reason)
                if (
                    self._last_place_error_code == -1013
                    and self._last_place_error_msg
                    and "NOTIONAL" in self._last_place_error_msg.upper()
                ):
                    self._logger(
                        "[SELL_REJECT_NOTIONAL] -> treat_as_dust accumulate"
                    )
                    self.last_action = "sell_reject_notional"
                    self._dust_accumulate = True
                    self._transition_to_position_state()
                    return 0
                self._logger(f"[ORDER] {exit_side} rejected -> flatten")
                self.last_action = "place_failed"
                self._transition_state(TradeState.STATE_ERROR)
                self._flatten_position("SELL_REJECTED")
                return 0
            price_label = (
                f"{mid:.8f}" if sell_price is None else f"{sell_price:.8f}"
            )
            exit_order_id = exit_order.get("orderId")
            if isinstance(exit_order_id, int):
                self._cycle_order_ids.add(exit_order_id)
            self._register_order_role(exit_order_id, exit_role)
            self._bind_order_meta(exit_client_id, exit_order_id)
            self._apply_pending_replace(exit_role, exit_client_id, exit_order_id)
            if tp_key is not None:
                self._active_tp_keys.add(tp_key)
            if reason_upper in {"TP", "TP_MAKER", "TP_FORCE", "TP_CROSS", "SL", "SL_HARD"}:
                self._logger(
                    f"[ORDER] {exit_side} placed "
                    f"cycle_id={cycle_label} reason={reason_upper} type={order_type} "
                    f"price={price_label} qty={qty:.8f} id={exit_order_id}"
                )
            else:
                self._logger(
                    f"[ORDER] {exit_side} placed cycle_id={cycle_label} qty={qty:.8f} "
                    f"price={price_label} id={exit_order_id}"
                )
            self._transition_pipeline_state(
                PipelineState.WAIT_EXIT, reason=f"exit_place:{reason_upper}"
            )
            self._transition_state(self._exit_state_for_intent(intent))
            if reset_retry:
                self._reset_sell_retry()
            self._start_sell_wait()
            if exit_role == "EXIT_TP":
                self._reset_exit_wait("placed")
                self._tp_maker_started_ts = time.monotonic()
            elif reason_upper == "TP_CROSS":
                self._reset_exit_wait("switch_to_cross")
                self._tp_maker_started_ts = None
            now_ms = int(time.time() * 1000)
            self.sell_active_order_id = exit_order_id
            self.sell_cancel_pending = False
            self._last_sell_ref_price = sell_ref_price
            self._last_sell_ref_side = sell_ref_label
            self.active_test_orders.append(
                {
                    "orderId": exit_order_id,
                    "side": exit_side,
                    "price": sell_price,
                    "qty": qty,
                    "cum_qty": 0.0,
                    "sell_executed_qty": 0.0,
                    "created_ts": now_ms,
                    "updated_ts": now_ms,
                    "status": "NEW",
                    "sell_status": "NEW",
                    "tag": self.TAG,
                    "clientOrderId": exit_order.get("clientOrderId", exit_client_id),
                    "reason": reason_upper,
                    "cycle_id": self._current_cycle_id,
                    "role": exit_role,
                }
            )
            self.orders_count = len(self.active_test_orders)
            self.mark_progress(reason="exit_place", reset_reconcile=True)
            return 1
        finally:
            self._inflight_trade_action = False
            self.sell_place_inflight = False
            self._sell_inflight_deadline_ts = None

    def _place_emergency_market_sell(self, qty: float, force: bool = False) -> int:
        exit_side = self._exit_side()
        active_cycle_id = self._current_cycle_id or 0
        active_cross_meta = self._get_active_meta_for_role(
            active_cycle_id, "EXIT_CROSS"
        )
        if active_cross_meta is not None or self._has_active_role("EXIT_CROSS"):
            client_id = active_cross_meta.client_id if active_cross_meta else None
            self._log_role_active_skip(
                active_cycle_id,
                "EXIT_CROSS",
                client_id,
            )
            self._log_guard_active_order("EXIT_CROSS")
            self.last_action = "exit_cross_active_guard"
            return 0
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
        price_state, _ = self._router.build_price_state()
        if price_state.data_blind:
            if not self._should_dedup_log("market_exit_blocked_data_blind", 2.0):
                self._logger(
                    f"[EXIT_BLOCKED] {self._direction_tag()} "
                    "action=market_exit reason=data_blind"
                )
            self.last_action = "data_blind_exit_blocked"
            return 0
        self.sell_place_inflight = True
        self.sell_last_attempt_ts = time.monotonic()
        inflight_ms = int(getattr(self._settings, "inflight_deadline_ms", 2500))
        self._sell_inflight_deadline_ts = time.monotonic() + (inflight_ms / 1000.0)
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
                f"[PLACE_{exit_side}] cycle_id={cycle_label} price=best_ask best_ask={ask_label} "
                f"active_source={price_state.source} ws_age={ws_age_label} "
                f"http_age={http_age_label} ws_bad={ws_bad}"
            )
            intent = self._normalize_exit_intent(self.exit_intent or "MANUAL")
            exit_policy, sell_ref_label = self._resolve_exit_policy_for_side(intent)
            bid_label = "—" if price_state.bid is None else f"{price_state.bid:.8f}"
            ask_label = "—" if price_state.ask is None else f"{price_state.ask:.8f}"
            mid_label = "—" if price_state.mid is None else f"{price_state.mid:.8f}"
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} side={exit_side} price=MARKET qty={qty:.8f} "
                f"policy={exit_policy}"
            )
            self._logger(
                f"[EXIT_{self._direction}] side={exit_side} policy=CROSS"
            )
            self._logger(
                "[SELL_PLACE] "
                f"cycle_id={cycle_label} side={exit_side} exit_intent={intent} exit_policy={exit_policy} "
                f"sell_ref={sell_ref_label} bid={bid_label} ask={ask_label} "
                f"mid={mid_label} sell_price=MARKET "
                "delta_ticks_to_ref=— "
                f"qty={qty:.8f}"
            )
            is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
            timestamp = int(time.time() * 1000)
            sell_client_id = self._build_client_order_id(
                exit_side, timestamp + 1, role_tag=self._exit_role_tag("EXIT_CROSS")
            )
            side_effect_type = (
                "AUTO_REPAY"
                if self._settings.allow_borrow and self._borrow_allowed_by_api is not False
                else None
            )
            self._register_order_meta(
                client_id=sell_client_id,
                role="EXIT_CROSS",
                side=exit_side,
                price=price_state.mid or 0.0,
                qty=qty,
            )
            sell_order = self._place_margin_order(
                symbol=self._settings.symbol,
                side=exit_side,
                quantity=qty,
                price=None,
                is_isolated=is_isolated,
                client_order_id=sell_client_id,
                side_effect=side_effect_type,
                order_type="MARKET",
            )
            if not sell_order:
                self._update_order_status(sell_client_id, "REJECTED")
                self._logger(f"[ORDER] {exit_side} rejected")
                self.last_action = "place_failed"
                self._transition_to_position_state()
                return 0
            sell_order_id = sell_order.get("orderId")
            if isinstance(sell_order_id, int):
                self._cycle_order_ids.add(sell_order_id)
            self._register_order_role(sell_order_id, "EXIT_CROSS")
            self._bind_order_meta(sell_client_id, sell_order_id)
            self._apply_pending_replace("EXIT_CROSS", sell_client_id, sell_order_id)
            self._logger(
                f"[ORDER] {exit_side} placed "
                f"cycle_id={cycle_label} reason=EMERGENCY type=MARKET "
                f"qty={qty:.8f} id={sell_order_id}"
            )
            self._transition_state(self._exit_state_for_intent(self.exit_intent))
            self._reset_sell_retry()
            self._start_sell_wait()
            now_ms = int(time.time() * 1000)
            self.sell_active_order_id = sell_order_id
            self.sell_cancel_pending = False
            self.active_test_orders.append(
                {
                    "orderId": sell_order_id,
                    "side": exit_side,
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
                    "role": "EXIT_CROSS",
                }
            )
            self.orders_count = len(self.active_test_orders)
            self.mark_progress(reason="exit_place", reset_reconcile=True)
            return 1
        finally:
            self.sell_place_inflight = False
            self._sell_inflight_deadline_ts = None

    def _reset_sell_retry(self) -> None:
        self._sell_wait_started_ts = None
        self._sell_retry_count = 0
        self._clear_price_wait("EXIT")
        self._pending_sell_retry_reason = None
        self.sell_retry_pending = False

    def _start_sell_wait(self) -> None:
        self._sell_wait_started_ts = time.monotonic()
        wait_kind = "EXIT_CROSS_WAIT"
        if self._tp_exit_phase == "MAKER":
            wait_kind = "EXIT_MAKER_WAIT"
        self._set_exit_wait_state(wait_kind, force_reset=True)

    def _reset_buy_retry(self) -> None:
        self._buy_wait_started_ts = None
        self._buy_retry_count = 0
        self._clear_price_wait("ENTRY")
        self._entry_consecutive_fresh_reads = 0
        self._entry_last_seen_bid = None
        self._entry_last_ws_bad_ts = None

    def _start_buy_wait(self) -> None:
        self._buy_wait_started_ts = time.monotonic()
        self._set_entry_wait_state("ENTRY_WAIT", force_reset=True)

    def _start_entry_cancel(self, reason: str, age_ms: Optional[int] = None) -> bool:
        buy_order = self._get_active_entry_order()
        if not buy_order or self._entry_order_is_final(buy_order):
            return self._recover_stuck(
                reason="entry_cancel_missing",
                wait_kind="ENTRY_WAIT",
                age_ms=age_ms,
            )
        price_state, _ = self._router.build_price_state()
        if not self._can_trade_now(
            price_state,
            action="ENTRY",
            max_age_ms=self._trade_gate_age_ms("ENTRY"),
        ):
            return False
        order_id = buy_order.get("orderId")
        age_label = age_ms if age_ms is not None else "?"
        self._logger(
            f"[ENTRY_CANCEL] {self._direction_tag()} "
            f"reason={reason} order_id={order_id} age_ms={age_label}"
        )
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        if order_id:
            self._cancel_margin_order(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=buy_order.get("tag", self.TAG),
            )
        self.entry_cancel_pending = True
        self._entry_replace_after_cancel = True
        self._entry_cancel_requested_ts = time.monotonic()
        self._reset_cancel_wait("ENTRY_CANCEL_WAIT")
        return True

    def _attempt_entry_replace_from_cancel(self) -> None:
        price_state, health_state = self._router.build_price_state()
        if not self._can_trade_now(
            price_state,
            action="ENTRY",
            max_age_ms=self._trade_gate_age_ms("ENTRY"),
        ):
            return
        self._note_effective_source(price_state.source)
        bid = price_state.bid
        ask = price_state.ask
        ref_price, ref_label = self._entry_reference(bid, ask)
        entry_context = self._entry_log_context(
            price_state.source,
            price_state.source == "WS" and self._entry_ws_bad(health_state.ws_connected),
            health_state.ws_age_ms,
            health_state.http_age_ms,
        )
        if ref_price is None:
            if not self._should_dedup_log("entry_replace_no_ref", 2.0):
                self._logger(f"[ENTRY_REPLACE_WAIT] reason=no_{ref_label} {entry_context}")
            return
        remaining_qty = self._remaining_to_target(ref_price)
        if remaining_qty <= self._epsilon_qty():
            self._entry_replace_after_cancel = False
            return
        self._start_entry_attempt()
        self._start_buy_wait()
        placed = self._place_entry_order(
            price=ref_price,
            bid=bid,
            ask=ask,
            entry_context=entry_context,
            reason="deadline_replace",
        )
        if placed:
            self._entry_replace_after_cancel = False

    def _start_entry_attempt(self) -> None:
        if self._entry_attempt_started_ts is None:
            self._entry_attempt_started_ts = time.monotonic()
        self._last_entry_wait_payload = None
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
        self._last_entry_wait_payload = None

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

    @staticmethod
    def _normalize_entry_float(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return round(float(value), 8)

    def _maybe_log_entry_state(
        self,
        order_id: Optional[int],
        order_price: Optional[float],
        best_bid: Optional[float],
        last_ref_bid: Optional[float],
        moved_ticks: int,
        can_reprice: bool,
        reason: str,
        entry_context: str,
        source: str,
        ws_bad: bool,
    ) -> None:
        payload = (
            self.state.value,
            order_id,
            self._normalize_entry_float(order_price),
            self._normalize_entry_float(best_bid),
            moved_ticks,
            can_reprice,
            reason,
            source,
            ws_bad,
        )
        if payload == self._last_entry_state_payload:
            return
        self._last_entry_state_payload = payload
        cycle_label = self._cycle_id_label()
        order_price_label = "?" if not order_price or order_price <= 0 else f"{order_price:.8f}"
        best_label = "?" if best_bid is None else f"{best_bid:.8f}"
        ref_label = "?" if last_ref_bid is None else f"{last_ref_bid:.8f}"
        best_ref_label = self._entry_reference_label()
        can_reprice_label = "True" if can_reprice else "False"
        self._logger(
            "[ENTRY_STATE] "
            f"cycle_id={cycle_label} order_id={order_id} order_price={order_price_label} "
            f"{best_ref_label}={best_label} last_ref={ref_label} "
            f"moved_ticks={moved_ticks} can_reprice={can_reprice_label} "
            f"reason={reason} {entry_context}"
        )

    def _log_entry_degraded(
        self,
        ws_connected: bool,
        entry_context: str,
    ) -> None:
        ws_no_first_tick, ws_stale_ticks = self._router.get_ws_issue_counts()
        counts = (ws_no_first_tick, ws_stale_ticks)
        if counts == self._last_entry_degraded_counts:
            return
        now = time.monotonic()
        if now - self._last_entry_degraded_ts < 2.0:
            return
        self._last_entry_degraded_counts = counts
        self._last_entry_degraded_ts = now
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
        self._update_data_blind_state(price_state)
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
        if source == "HTTP" and health_state.http_age_ms is not None:
            price_state.mid_age_ms = health_state.http_age_ms
        if not self._can_trade_now(
            price_state,
            action="ENTRY",
            max_age_ms=self._trade_gate_age_ms("ENTRY"),
        ):
            return (
                False,
                bid,
                ask,
                mid_val,
                age_ms,
                source,
                "blocked",
                ws_bad,
                health_state.ws_age_ms,
                health_state.http_age_ms,
            )
        if source == "HTTP" and health_state.http_age_ms is not None:
            age_ms = health_state.http_age_ms
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
        entry_http_max_age_ms = int(
            getattr(self._settings, "entry_http_max_age_ms", entry_max_age_ms)
        )
        if (
            health_state.http_age_ms is not None
            and health_state.http_age_ms > entry_http_max_age_ms
        ):
            return (
                False,
                bid,
                ask,
                mid_val,
                health_state.http_age_ms,
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

    def _note_price_progress(self, bid: Optional[float], ask: Optional[float]) -> None:
        if bid is None or ask is None:
            return
        self._last_progress_bid = bid
        self._last_progress_ask = ask

    def _update_data_blind_state(self, price_state: PriceState) -> None:
        now = time.monotonic()
        if price_state.data_blind:
            if self._data_blind_since_ts is None:
                self._data_blind_since_ts = now
        else:
            self._data_blind_since_ts = None

    def _calculate_entry_price(self, price: float) -> Optional[float]:
        if not self._profile.tick_size:
            return None
        tick_size = self._profile.tick_size
        return self._round_to_step(price, tick_size)

    def _guard_short_entry_side(self, side: str) -> bool:
        if self._direction != "SHORT":
            return True
        if side == "SELL":
            return True
        self._logger(f"[SHORT_SIDE_BUG] role=ENTRY side={side}")
        return False

    def _log_entry_place(
        self,
        price: float,
        bid: Optional[float],
        ask: Optional[float],
        entry_context: str,
    ) -> None:
        entry_side = self._entry_side()
        if self._direction == "SHORT":
            self._logger(
                f"[ENTRY_SHORT] side={entry_side} ref=ask price={price:.8f}"
            )
        self._logger(f"[ENTRY_{self._direction}] side={entry_side} price={price:.8f}")
        bid_label = "?" if bid is None else f"{bid:.8f}"
        ask_label = "?" if ask is None else f"{ask:.8f}"
        cycle_label = self._cycle_id_label()
        self._logger(
            f"[ENTRY] {self._direction_tag()} placed cycle_id={cycle_label} price={price:.8f} "
            f"bid={bid_label} ask={ask_label} {entry_context}"
        )

    def _place_entry_order(
        self,
        price: float,
        bid: Optional[float],
        ask: Optional[float],
        entry_context: str,
        reason: str,
    ) -> bool:
        if reason not in {"reprice", "deadline_replace"}:
            cycle_id = self._current_cycle_id or 0
            active_meta = self._get_active_meta_for_role(cycle_id, "ENTRY")
            if active_meta is not None or self._has_active_role("ENTRY"):
                client_id = active_meta.client_id if active_meta else None
                self._log_role_active_skip(cycle_id, "ENTRY", client_id)
                self._log_guard_active_order("ENTRY")
                self.last_action = "entry_active_guard"
                return False
        if not self._profile.tick_size or not self._profile.step_size:
            return False
        new_price = self._calculate_entry_price(price)
        if new_price is None:
            return False
        remaining_qty = self._remaining_to_target(new_price)
        self._log_entry_target(new_price, remaining_qty)
        qty = self._apply_dust_carry_to_entry_qty(remaining_qty)
        if self._profile.min_qty and qty < self._profile.min_qty:
            return False
        if self._profile.min_notional is not None and qty * new_price < self._profile.min_notional:
            return False
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        timestamp = int(time.time() * 1000)
        self._mark_cycle_entry_start()
        entry_side = self._entry_side()
        if not self._guard_short_entry_side(entry_side):
            return False
        entry_client_id = self._build_client_order_id(
            entry_side, timestamp, role_tag=self._entry_role_tag()
        )
        side_effect_type = self._normalize_side_effect_type(
            self._settings.side_effect_type
        )
        self._register_order_meta(
            client_id=entry_client_id,
            role="ENTRY",
            side=entry_side,
            price=new_price,
            qty=qty,
        )
        self._log_entry_place(new_price, bid, ask, entry_context)
        entry_order = self._place_margin_order(
            symbol=self._settings.symbol,
            side=entry_side,
            quantity=qty,
            price=new_price,
            is_isolated=is_isolated,
            client_order_id=entry_client_id,
            side_effect=side_effect_type,
            order_type="LIMIT",
        )
        if not entry_order:
            self._update_order_status(entry_client_id, "REJECTED")
            return False
        entry_order_id = entry_order.get("orderId")
        if isinstance(entry_order_id, int):
            self._cycle_order_ids.add(entry_order_id)
            self._clear_start_inflight()
        self._bind_order_meta(entry_client_id, entry_order_id)
        self._apply_pending_replace("ENTRY", entry_client_id, entry_order_id)
        cycle_label = self._cycle_id_label()
        self._logger(
            f"[ORDER] {entry_side} placed cycle_id={cycle_label} qty={qty:.8f} "
            f"price={new_price:.8f} id={entry_order.get('orderId')}"
        )
        self._logger(
            f"[ENTRY_PLACE] cycle_id={cycle_label} side={entry_side} "
            f"price={new_price:.8f} qty={qty:.8f} reason={reason}"
        )
        if reason == "deadline_replace":
            self._logger(
                f"[ENTRY] {self._direction_tag()} replaced cycle_id={cycle_label} price={new_price:.8f}"
            )
        self._entry_reprice_last_ts = time.monotonic()
        self._entry_consecutive_fresh_reads = 0
        self._entry_order_price = new_price
        now_ms = int(time.time() * 1000)
        self._register_order_role(entry_order_id, "ENTRY")
        self.active_test_orders = [
            {
                "orderId": entry_order_id,
                "side": entry_side,
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
                "clientOrderId": entry_order.get("clientOrderId", entry_client_id),
                "cycle_id": self._current_cycle_id,
                "role": "ENTRY",
            }
        ]
        self._transition_pipeline_state(
            PipelineState.WAIT_ENTRY_FILL, reason="entry_placed"
        )
        self.entry_active_order_id = entry_order.get("orderId")
        self.entry_active_price = new_price
        self.entry_cancel_pending = False
        if isinstance(entry_order_id, int):
            reset_reason = "repriced" if reason in {"reprice", "deadline_replace"} else "placed"
            self._reset_entry_wait(reset_reason)
        ref_price, _ = self._entry_reference(bid, ask)
        if ref_price is not None:
            self._entry_last_ref_bid = ref_price
            self._entry_last_ref_ts_ms = now_ms
        self.orders_count = len(self.active_test_orders)
        progress_reason = "entry_place"
        if reason in {"reprice", "deadline_replace"}:
            progress_reason = "entry_reprice"
        self.mark_progress(reason=progress_reason, reset_reconcile=True)
        return True

    def _place_entry_topup_order(
        self,
        price: float,
        qty: float,
        bid: Optional[float],
        ask: Optional[float],
        entry_context: str,
    ) -> bool:
        if not self._profile.tick_size or not self._profile.step_size:
            return False
        new_price = self._calculate_entry_price(price)
        if new_price is None:
            return False
        qty = self._round_down(qty, self._profile.step_size)
        if qty <= 0:
            return False
        if self._profile.min_qty and qty < self._profile.min_qty:
            return False
        if self._profile.min_notional is not None and qty * new_price < self._profile.min_notional:
            return False
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        timestamp = int(time.time() * 1000)
        self._mark_cycle_entry_start()
        entry_side = self._entry_side()
        if not self._guard_short_entry_side(entry_side):
            return False
        entry_client_id = self._build_client_order_id(
            entry_side, timestamp, role_tag=self._entry_role_tag()
        )
        side_effect_type = self._normalize_side_effect_type(
            self._settings.side_effect_type
        )
        self._register_order_meta(
            client_id=entry_client_id,
            role="ENTRY",
            side=entry_side,
            price=new_price,
            qty=qty,
        )
        self._log_entry_place(new_price, bid, ask, entry_context)
        entry_order = self._place_margin_order(
            symbol=self._settings.symbol,
            side=entry_side,
            quantity=qty,
            price=new_price,
            is_isolated=is_isolated,
            client_order_id=entry_client_id,
            side_effect=side_effect_type,
            order_type="LIMIT",
        )
        if not entry_order:
            self._update_order_status(entry_client_id, "REJECTED")
            return False
        entry_order_id = entry_order.get("orderId")
        if isinstance(entry_order_id, int):
            self._cycle_order_ids.add(entry_order_id)
            self._clear_start_inflight()
        self._bind_order_meta(entry_client_id, entry_order_id)
        self._apply_pending_replace("ENTRY", entry_client_id, entry_order_id)
        cycle_label = self._cycle_id_label()
        self._logger(
            f"[ORDER] {entry_side} placed cycle_id={cycle_label} qty={qty:.8f} "
            f"price={new_price:.8f} id={entry_order.get('orderId')} role=ENTRY_TOPUP"
        )
        self._entry_reprice_last_ts = time.monotonic()
        self._entry_consecutive_fresh_reads = 0
        self._entry_order_price = new_price
        now_ms = int(time.time() * 1000)
        self._register_order_role(entry_order_id, "ENTRY")
        self.active_test_orders = [
            {
                "orderId": entry_order_id,
                "side": entry_side,
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
                "clientOrderId": entry_order.get("clientOrderId", entry_client_id),
                "cycle_id": self._current_cycle_id,
                "role": "ENTRY",
            }
        ]
        self.entry_active_order_id = entry_order.get("orderId")
        self.entry_active_price = new_price
        self.entry_cancel_pending = False
        ref_price, _ = self._entry_reference(bid, ask)
        if ref_price is not None:
            self._entry_last_ref_bid = ref_price
            self._entry_last_ref_ts_ms = now_ms
        self.orders_count = len(self.active_test_orders)
        self.mark_progress(reason="entry_topup", reset_reconcile=True)
        return True

    def _handle_entry_follow_top(self, open_orders: dict[int, dict]) -> None:
        if self.state != TradeState.STATE_ENTRY_WORKING:
            return
        buy_order = self._get_active_entry_order()
        if not buy_order:
            if self._entry_replace_after_cancel:
                self._attempt_entry_replace_from_cancel()
            return
        if buy_order.get("status") == "FILLED":
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
        self._note_effective_source(source)
        entry_context = self._entry_log_context(
            source,
            ws_bad,
            ws_age_ms,
            http_age_ms,
        )
        now = time.monotonic()
        if ws_bad and source == "WS":
            self._entry_last_ws_bad_ts = now
        ref_price, _ = self._entry_reference(bid, ask)
        if not ok or ref_price is None or mid is None:
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
                        f"{entry_context}",
                        level="DEBUG",
                        key="ENTRY_REPRICE",
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
                        f"{entry_context}",
                        level="DEBUG",
                        key="ENTRY_REPRICE",
                    )
            order_id = buy_order.get("orderId")
            order_price = float(buy_order.get("price") or 0.0)
            self._maybe_log_entry_state(
                order_id=order_id,
                order_price=order_price,
                best_bid=ref_price,
                last_ref_bid=self._entry_last_ref_bid,
                moved_ticks=0,
                can_reprice=False,
                reason=status,
                entry_context=entry_context,
                source=source,
                ws_bad=ws_bad,
            )
            return
        self._note_price_progress(bid, ask)
        self._clear_price_wait("ENTRY")
        elapsed_ms = self._entry_attempt_elapsed_ms()
        if elapsed_ms is not None:
            payload = (entry_context,)
            if payload != self._last_entry_wait_payload:
                self._last_entry_wait_payload = payload
                self._logger(
                    f"[ENTRY_WAIT] ms={elapsed_ms} {entry_context}",
                    level="DEBUG",
                    key="ENTRY_WAIT",
                )
        if not self._profile.tick_size or not self._profile.step_size:
            return
        current_price = float(buy_order.get("price") or 0.0)
        if current_price <= 0:
            return
        if self._remaining_to_target(current_price) <= self._epsilon_qty():
            order_id = buy_order.get("orderId")
            if order_id:
                is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
                final_status = self._safe_cancel_and_sync(
                    order_id=order_id,
                    is_isolated=is_isolated,
                    tag=buy_order.get("tag", self.TAG),
                    attempt_cancel=True,
                )
                if final_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                    buy_order["status"] = final_status
                    self.active_test_orders = [
                        entry
                        for entry in self.active_test_orders
                        if entry.get("orderId") != order_id
                    ]
                    self.orders_count = len(self.active_test_orders)
                    self.entry_active_order_id = None
                    self.entry_active_price = None
                    self.entry_cancel_pending = False
            return
        tick_size = self._profile.tick_size
        now_ms = int(time.time() * 1000)
        last_ref_bid = self._entry_last_ref_bid
        if last_ref_bid is None and ref_price is not None:
            self._entry_last_ref_bid = ref_price
            self._entry_last_ref_ts_ms = now_ms
            last_ref_bid = ref_price
        moved_ticks = 0
        ref_moved = False
        min_reprice_ticks = max(
            1, int(getattr(self._settings, "entry_reprice_min_ticks", 1) or 1)
        )
        if ref_price is not None and last_ref_bid is not None:
            moved_ticks = self._ticks_between(last_ref_bid, ref_price, tick_size)
            ref_moved = moved_ticks >= min_reprice_ticks
        if self._entry_order_price is None:
            self._entry_order_price = current_price
        self._entry_consecutive_fresh_reads = 0
        reprice_reason = "ref_moved" if ref_moved else "ref_not_moved"
        can_reprice = ref_moved
        new_price = None
        if ref_price is not None:
            new_price = self._round_to_step(ref_price, tick_size)
        if can_reprice:
            if new_price is None:
                reprice_reason = "price_invalid"
                can_reprice = False
            elif new_price == current_price:
                reprice_reason = "already_at_best_ref"
                can_reprice = False
        if can_reprice:
            cooldown_ms = int(
                getattr(self._settings, "entry_reprice_cooldown_ms", 1200) or 0
            )
            if cooldown_ms > 0 and self._entry_reprice_last_ts:
                elapsed_s = time.monotonic() - self._entry_reprice_last_ts
                if elapsed_s * 1000.0 < cooldown_ms:
                    can_reprice = False
                    reprice_reason = "cooldown"
        order_id = buy_order.get("orderId")
        order_price_label = f"{current_price:.8f}"
        reason_label = reprice_reason
        self._maybe_log_entry_state(
            order_id=order_id,
            order_price=current_price,
            best_bid=bid,
            last_ref_bid=last_ref_bid,
            moved_ticks=moved_ticks,
            can_reprice=can_reprice,
            reason=reason_label,
            entry_context=entry_context,
            source=source,
            ws_bad=ws_bad,
        )
        if not can_reprice:
            if reason_label == "ref_not_moved":
                now = time.monotonic()
                payload = (
                    self._normalize_entry_float(current_price),
                    self._normalize_entry_float(bid),
                )
                should_log = (
                    payload != self._entry_ref_not_moved_payload
                    or now - self._entry_ref_not_moved_log_ts >= 1.5
                )
                if not should_log:
                    return
                self._entry_ref_not_moved_payload = payload
                self._entry_ref_not_moved_log_ts = now
            else:
                if self._should_dedup_log("entry_reprice_skip", 0.8):
                    return
            new_price_label = "?" if new_price is None else f"{new_price:.8f}"
            self._logger(
                "[ENTRY_REPRICE] action=SKIP "
                f"reason={reason_label} old_price={order_price_label} "
                f"new_price={new_price_label} {entry_context}",
                level="DEBUG",
                key="ENTRY_REPRICE",
            )
            return
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
                attempt_cancel = order_id in open_orders
                if attempt_cancel:
                    self._note_pending_replace(order_id)
                final_status = self._safe_cancel_and_sync(
                    order_id=order_id,
                    is_isolated=is_isolated,
                    tag=buy_order.get("tag", self.TAG),
                    attempt_cancel=attempt_cancel,
                )
                if final_status in {"FILLED", "PARTIALLY_FILLED"}:
                    self._logger(
                        "[ENTRY_REPRICE_ABORT] "
                        f"old_id={order_id} status={final_status}"
                    )
                    self.entry_cancel_pending = False
                    return
                if final_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                    self.entry_cancel_pending = False
                elif final_status == "UNKNOWN":
                    self.entry_cancel_pending = True
                    self._reset_cancel_wait("ENTRY_CANCEL_WAIT")
                    return
                else:
                    self.entry_cancel_pending = True
                    self._reset_cancel_wait("ENTRY_CANCEL_WAIT")
                    return
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        self._logger(
            f"[ENTRY] {self._direction_tag()} repriced cycle_id={self._cycle_id_label()} "
            f"price={new_price:.8f}"
        )
        self._place_entry_order(
            price=new_price,
            bid=bid,
            ask=ask,
            entry_context=entry_context,
            reason="reprice",
        )

    def _maybe_handle_sell_reprice(self, open_orders: dict[int, dict]) -> None:
        if not self._in_exit_workflow():
            return
        sell_order = self._find_order(self._exit_side())
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
        sell_reason = str(sell_order.get("reason") or "").upper()
        if sell_reason == "TP_FORCE":
            return
        intent = self._normalize_exit_intent(self.exit_intent or sell_reason)
        if intent != "TP":
            return
        if not self._should_dedup_log("reprice_disabled", 2.0):
            self._logger("[REPRICE_SKIP reason=deterministic_tp]")
        return

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
        price_state, _ = self._router.build_price_state()
        self._update_data_blind_state(price_state)
        if not self._can_trade_now(
            price_state,
            action="EXIT",
            max_age_ms=self._trade_gate_age_ms("EXIT"),
        ):
            return False
        exit_side = self._exit_side()
        self._logger(
            f"[EXIT_STUCK_TIMEOUT] waited_ms={elapsed_ms} "
            f"limit_ms={max_wait_ms} side={exit_side}"
        )
        entry_avg = self._entry_avg_price
        tp_price, sl_price = self._compute_tp_sl_prices(entry_avg)
        sl_breached = self._sl_breached(price_state, sl_price)
        best_bid, best_ask = self._book_refs(price_state)
        position_side = self._position_side()
        tp_triggered = False
        if tp_price is not None:
            if position_side == "SHORT":
                tp_triggered = best_bid is not None and best_bid <= tp_price
            else:
                tp_triggered = best_ask is not None and best_ask >= tp_price
        max_cross_attempts = int(
            getattr(self._settings, "exit_cross_attempts", self.EXIT_CROSS_MAX_ATTEMPTS)
        )
        status_missing_ms = None
        if self._exit_missing_since_ts is not None:
            status_missing_ms = int(
                (time.monotonic() - self._exit_missing_since_ts) * 1000.0
            )
        if not sl_breached and not tp_triggered:
            if not self._should_dedup_log("exit_cross_blocked_tp_unmet", 2.0):
                self._logger("[EXIT_CROSS_BLOCKED] reason=tp_unmet_by_book")
            return False
        if not sl_breached or self._exit_cross_attempts < max_cross_attempts:
            if self._exit_cross_attempts >= max_cross_attempts:
                self._logger(
                    "[EXIT_CROSS_LIMIT] "
                    f"attempts={self._exit_cross_attempts} max={max_cross_attempts}"
                )
                return False
            self._exit_cross_attempts += 1
            attempt_label = self._exit_cross_attempts
            breach_label = "sl_breach" if sl_breached else "timeout"
            self._logger(
                "[EXIT_CROSS_RETRY] "
                f"attempt={attempt_label}/{max_cross_attempts} reason={breach_label}"
            )
            self._tp_exit_phase = "CROSS"
            if self._has_active_order(exit_side) or self.sell_active_order_id:
                self._cancel_active_sell(reason="TIMEOUT_CROSS")
            self._schedule_sell_retry("TP_CROSS")
            return True

        status_limit_ms = int(
            getattr(
                self._settings,
                "exit_status_missing_ms",
                self.EXIT_STATUS_MISSING_MS,
            )
        )
        status_missing = (
            status_missing_ms is not None and status_missing_ms >= status_limit_ms
        )
        if sl_breached and (status_missing or self._exit_cross_attempts >= max_cross_attempts):
            self._logger(
                "[EXIT_EMERGENCY] reason=sl_breach "
                f"missing_ms={status_missing_ms if status_missing_ms is not None else '—'} "
                f"attempts={self._exit_cross_attempts}/{max_cross_attempts}"
            )
            for order in list(self.active_test_orders):
                if order.get("side") != exit_side:
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
        return False

    def _watchdog_inflight(self) -> None:
        now = time.monotonic()
        if (
            self._inflight_trade_action
            and self._entry_inflight_deadline_ts is not None
            and now >= self._entry_inflight_deadline_ts
        ):
            self._logger("[WATCHDOG] reset inflight entry")
            self._inflight_trade_action = False
            self._entry_inflight_deadline_ts = None
            self._refresh_after_watchdog()
        if (
            self.sell_place_inflight
            and self._sell_inflight_deadline_ts is not None
            and now >= self._sell_inflight_deadline_ts
        ):
            self._logger("[WATCHDOG] reset inflight sell")
            self.sell_place_inflight = False
            self._inflight_trade_action = False
            self._sell_inflight_deadline_ts = None
            self._refresh_after_watchdog()

    def _maybe_idle_guard(self, now: float) -> None:
        if self._wait_state_kind is None or self._wait_state_enter_ts_ms is None:
            return
        idle_guard_ms = int(getattr(self._settings, "idle_guard_ms", 6000) or 6000)
        age_ms = int(now * 1000) - self._wait_state_enter_ts_ms
        if age_ms < idle_guard_ms:
            return
        if self._should_dedup_log(f"idle_guard:{self._wait_state_kind}", 2.0):
            return
        self._logger(
            "[IDLE_GUARD] "
            f"wait={self._wait_state_kind} age_ms={age_ms} "
            f"state={self.state.value} last_progress={self._last_progress_reason} "
            f"entry_active={self._entry_order_active()} "
            f"exit_active={self._has_active_order(self._exit_side())}"
        )

    def watchdog_tick(self) -> bool:
        if self._reconcile_inflight or self._safe_stop_issued:
            return False
        if self.state in {
            TradeState.STATE_IDLE,
            TradeState.STATE_FLAT,
            TradeState.STATE_ERROR,
            TradeState.STATE_SAFE_STOP,
            TradeState.STATE_STOPPED,
        }:
            return False
        now = time.monotonic()
        self._maybe_idle_guard(now)
        if self._check_wait_deadline(now):
            return True
        if self._check_progress_deadline(now):
            return True
        state_age_ms = self._state_age_ms(now)
        no_progress_ms = self._no_progress_ms(now)
        hard_stall_ms = int(getattr(self._settings, "hard_stall_ms", self.HARD_STALL_MS))
        if self.state == TradeState.STATE_ENTRY_WORKING:
            entry_order = self._get_active_entry_order()
            entry_order_active = entry_order is not None and not self._entry_order_is_final(
                entry_order
            )
            price_state, _ = self._router.build_price_state()
            self._update_data_blind_state(price_state)
            moved_ticks = 0
            if (
                entry_order_active
                and price_state.bid is not None
                and self.entry_active_price is not None
                and self._profile.tick_size
            ):
                moved_ticks = self._ticks_between(
                    self.entry_active_price, price_state.bid, self._profile.tick_size
                )
            if self._entry_missing_since_ts is not None:
                missing_ms = int((now - self._entry_missing_since_ts) * 1000.0)
                if missing_ms >= 1000:
                    self._logger(
                        "[WD] "
                        f"entry_missing_ms={missing_ms} state={self.state.value} -> RECONCILE"
                    )
                    return self._start_reconcile_from_watchdog(state_age_ms, no_progress_ms)
            if entry_order_active and moved_ticks == 0 and no_progress_ms < hard_stall_ms:
                return False
        if no_progress_ms < hard_stall_ms:
            return False
        return self._start_reconcile_from_watchdog(state_age_ms, no_progress_ms)

    def _start_reconcile_from_watchdog(self, state_age_ms: int, no_progress_ms: int) -> bool:
        if self._should_skip_recover_for_active_order(
            reason="watchdog_timeout",
            from_state=self.state,
        ):
            return False
        self._logger(
            "[WD] "
            f"stuck state={self.state.value} age_ms={state_age_ms} "
            f"no_progress_ms={no_progress_ms} last_progress={self._last_progress_reason} "
            "-> RECONCILE"
        )
        self._reconcile_inflight = True
        self._transition_state(TradeState.STATE_RECONCILE)
        return True

    def _recover_stuck(
        self,
        reason: str,
        wait_kind: Optional[str] = None,
        age_ms: Optional[int] = None,
    ) -> bool:
        if self._should_skip_recover_for_active_order(reason, self.state):
            return False
        from_state = self.state
        order_id = self.entry_active_order_id or self.sell_active_order_id
        age_label = age_ms if age_ms is not None else self._state_age_ms()
        self._last_recover_reason = reason
        self._last_recover_from = from_state
        self._logger(
            f"[RECOVER] {self._direction_tag()} "
            f"from={from_state.value} reason={reason} age_ms={age_label} "
            f"order_id={order_id}"
        )
        self._transition_state(TradeState.STATE_RECOVER_STUCK)

        snapshot = self.collect_reconcile_snapshot()
        if snapshot is None:
            self._reconcile_recovery_attempts += 1
            max_retries = int(getattr(self._settings, "max_reconcile_retries", 3) or 3)
            if self._reconcile_recovery_attempts >= max_retries:
                self._enter_safe_stop(reason="RECOVER_SNAPSHOT_ERROR", allow_rest=False)
            return False
        self._reconcile_recovery_attempts = 0
        open_orders = snapshot.get("open_orders") or []
        trades = snapshot.get("trades") or []
        balance = snapshot.get("balance")
        self.sync_open_orders(open_orders)
        trades_filtered = self._filter_trades_for_cycle(trades)
        executed, closed, remaining, entry_avg = self._build_reconcile_state(
            trades=trades_filtered,
            balance=balance,
        )
        self._apply_reconcile_snapshot(executed, closed, remaining, entry_avg)

        entry_side = self._entry_side()
        exit_side = self._exit_side()
        have_open_entry = any(
            str(order.get("side") or "").upper() == entry_side for order in open_orders
        )
        have_open_exit = any(
            str(order.get("side") or "").upper() == exit_side for order in open_orders
        )

        if from_state == TradeState.STATE_ENTRY_WORKING and have_open_entry and self.position is None:
            price_state, _ = self._router.build_price_state()
            self._note_effective_source(price_state.source)
            ref_price, _ = self._entry_reference(price_state.bid, price_state.ask)
            entry_order = next(
                (
                    order
                    for order in open_orders
                    if str(order.get("side") or "").upper() == entry_side
                ),
                None,
            )
            entry_price = None
            if entry_order is not None:
                entry_price = float(entry_order.get("price") or 0.0)
            if entry_price is None or entry_price <= 0:
                entry_price = self.entry_active_price
            moved_ticks = 0
            if (
                ref_price is not None
                and entry_price is not None
                and self._profile.tick_size
            ):
                moved_ticks = self._ticks_between(
                    entry_price, ref_price, self._profile.tick_size
                )
            if (
                moved_ticks >= self.PRICE_MOVE_TICK_THRESHOLD
                and not self._dust_accumulate
            ):
                if self._start_entry_cancel(reason="progress_deadline", age_ms=age_label):
                    self._transition_state(TradeState.STATE_ENTRY_WORKING)
                    self._set_entry_wait_state("ENTRY_WAIT")
                    decided = "entry_cancel_replace"
                    self._logger(
                        f"[RECOVER] {self._direction_tag()} "
                        f"snapshot open_orders={len(open_orders)} fills={len(trades)} decided={decided}"
                    )
                    return True
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
            self._set_entry_wait_state("ENTRY_WAIT")
            decided = "entry_wait"
            self._logger(
                f"[RECOVER] {self._direction_tag()} "
                f"snapshot open_orders={len(open_orders)} fills={len(trades)} decided={decided}"
            )
            return True

        if have_open_exit:
            self._tp_exit_phase = "MAKER"
            self._transition_state(self._exit_state_for_intent(self.exit_intent))
            self._set_exit_wait_state("EXIT_MAKER_WAIT")
            decided = "exit_wait"
        elif remaining <= self._epsilon_qty():
            self.position = None
            self._reset_position_tracking()
            self._transition_state(TradeState.STATE_IDLE)
            decided = "entry_place"
            if self.run_active:
                placed = self.place_test_orders_margin()
                if placed:
                    decided = "entry_placed"
        else:
            force_cross = reason == "progress_deadline_exit"
            if force_cross and not have_open_exit:
                price_state, _ = self._router.build_price_state()
                best_bid, best_ask = self._book_refs(price_state)
                entry_avg = self._entry_avg_price
                tp_price, _ = self._compute_tp_sl_prices(entry_avg)
                position_side = self._position_side()
                tp_triggered = False
                if tp_price is not None:
                    if position_side == "SHORT":
                        tp_triggered = best_bid is not None and best_bid <= tp_price
                    else:
                        tp_triggered = best_ask is not None and best_ask >= tp_price
                if not tp_triggered:
                    if not self._should_dedup_log("exit_cross_blocked_tp_unmet", 2.0):
                        self._logger("[EXIT_CROSS_BLOCKED] reason=tp_unmet_by_book")
                    force_cross = False
            if force_cross and not have_open_exit:
                self._tp_exit_phase = "CROSS"
                if self.exit_intent is None:
                    self._set_exit_intent("TP", mid=None, force=True)
                placed = self._place_sell_order(
                    reason="TP_CROSS",
                    exit_intent=self.exit_intent or "TP",
                )
                if placed:
                    decided = "force_exit_cross"
                else:
                    self._transition_state(self._exit_state_for_intent(self.exit_intent))
                    self._set_exit_wait_state("EXIT_CROSS_WAIT")
                    decided = "force_exit_cross_blocked"
            else:
                self._tp_exit_phase = "MAKER"
                price_state, health_state = self._router.build_price_state()
                self._ensure_exit_orders(
                    price_state,
                    health_state,
                    allow_replace=True,
                    skip_reconcile=True,
                )
                self._transition_state(self._exit_state_for_intent(self.exit_intent))
                self._set_exit_wait_state("EXIT_MAKER_WAIT")
                decided = "place_exit_maker"

        self._logger(
            f"[RECOVER] {self._direction_tag()} "
            f"snapshot open_orders={len(open_orders)} fills={len(trades)} decided={decided}"
        )
        return True

    def _refresh_after_watchdog(self) -> None:
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error(
                "watchdog_open_orders",
                exc,
                {"symbol": self._settings.symbol},
            )
            return
        except Exception as exc:
            self._logger(f"[WATCHDOG] open_orders error: {exc}")
            return
        self.sync_open_orders(open_orders)
        self._reconcile_position_from_fills()

    def _enter_safe_stop(self, reason: str, allow_rest: bool = True) -> None:
        if self._safe_stop_issued:
            return
        self._safe_stop_issued = True
        self.run_active = False
        self._next_cycle_ready_ts = None
        if allow_rest:
            self._logger(f"[SAFE_STOP] reason={reason}")
        else:
            self._logger(f"[SAFE_STOP] reason={reason} mode=local_only")
        self._transition_state(TradeState.STATE_SAFE_STOP)
        if allow_rest:
            self._cancel_all_symbol_orders()
            remaining_qty_raw = self._resolve_remaining_qty_raw()
            qty = remaining_qty_raw
            if self._profile.step_size:
                qty = self._round_down(remaining_qty_raw, self._profile.step_size)
            if qty > 0:
                self._logger(
                    f"[SAFE_STOP] action=market_close qty={qty:.8f} remaining={remaining_qty_raw:.8f}"
                )
                self._place_emergency_market_sell(qty, force=True)
        self._handle_cycle_completion(reason=f"SAFE_STOP:{reason}", critical=True)

    def _cancel_all_symbol_orders(self) -> None:
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("reconcile_openOrders", exc, {"symbol": self._settings.symbol})
            return
        except Exception as exc:
            self._logger(f"[RECONCILE] open_orders error: {exc}")
            return
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

    def _fetch_recent_trades(self) -> list[dict]:
        if not hasattr(self._rest, "get_margin_my_trades"):
            return []
        try:
            return self._rest.get_margin_my_trades(self._settings.symbol, limit=200)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("reconcile_trades", exc, {"symbol": self._settings.symbol})
        except Exception as exc:
            self._logger(f"[RECONCILE] trades error: {exc}")
        return []

    def _filter_trades_for_cycle(
        self,
        trades: list[dict],
    ) -> list[dict]:
        if not self._cycle_order_ids:
            return []
        if self._cycle_start_ts_ms is None:
            return []
        order_ids = set(self._cycle_order_ids)
        min_ts = self._cycle_start_ts_ms - 2000
        filtered: list[dict] = []
        for trade in trades:
            trade_time = trade.get("time") or trade.get("transactTime")
            order_id = trade.get("orderId")
            if isinstance(order_id, str) and order_id.isdigit():
                order_id = int(order_id)
            if isinstance(order_id, int):
                if order_id in order_ids:
                    filtered.append(trade)
                continue
            time_ok = isinstance(trade_time, (int, float)) and trade_time >= min_ts
            if time_ok:
                filtered.append(trade)
        return filtered

    def collect_reconcile_snapshot(self) -> Optional[dict]:
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except httpx.HTTPStatusError as exc:
            self._log_binance_error("reconcile_openOrders", exc, {"symbol": self._settings.symbol})
            return None
        except Exception as exc:
            self._logger(f"[RECONCILE] open_orders error: {exc}")
            return None
        self._last_open_orders = open_orders
        trades = self._fetch_recent_trades()
        balance = self._get_base_balance_snapshot()
        return {
            "open_orders": open_orders,
            "trades": trades,
            "balance": balance,
        }

    def mark_reconcile_failed(self, reason: str) -> None:
        self._reconcile_inflight = False
        self._logger(f"[RECONCILE] failed reason={reason}")

    def _apply_reconcile_snapshot(
        self,
        executed: float,
        closed: float,
        remaining: float,
        entry_avg: Optional[float],
    ) -> None:
        cycle = self.ledger.active_cycle
        if cycle and cycle.status == CycleStatus.OPEN:
            executed = cycle.executed_qty
            closed = cycle.closed_qty
            remaining = cycle.remaining_qty
            if cycle.entry_avg > 0:
                entry_avg = cycle.entry_avg
            self._executed_qty_total = executed
            self._closed_qty_total = closed
            self._remaining_qty = remaining
            if entry_avg is not None:
                self._entry_avg_price = entry_avg
                self._last_buy_price = entry_avg
        else:
            self._executed_qty_total = executed
            self._closed_qty_total = closed
            self._remaining_qty = remaining
            if entry_avg is not None:
                self._entry_avg_price = entry_avg
                self._last_buy_price = entry_avg
        if remaining > self._epsilon_qty():
            now_ms = int(time.time() * 1000)
            if self.position is None:
                self.position = {
                    "buy_price": entry_avg,
                    "qty": remaining,
                    "opened_ts": now_ms,
                    "partial": executed > 0,
                    "initial_qty": executed or remaining,
                    "side": self._direction,
                }
            else:
                if entry_avg is not None:
                    self.position["buy_price"] = entry_avg
                self.position["qty"] = remaining
                if executed:
                    self.position["initial_qty"] = executed
        self._log_position_state(executed, closed, remaining, entry_avg)

    def _build_reconcile_state(
        self,
        trades: list[dict],
        balance: Optional[dict],
    ) -> tuple[float, float, float, Optional[float]]:
        executed = 0.0
        closed = 0.0
        entry_notional = 0.0
        entry_avg = None
        entry_side = self._entry_side()
        exit_side = self._exit_side()
        if trades:
            for trade in trades:
                qty = float(trade.get("qty") or trade.get("executedQty") or 0.0)
                price = float(trade.get("price") or trade.get("avgPrice") or 0.0)
                side = str(trade.get("side") or "").upper()
                is_buyer = trade.get("isBuyer")
                is_entry_fill = None
                if side:
                    is_entry_fill = side == entry_side
                elif is_buyer is not None:
                    is_entry_fill = is_buyer if entry_side == "BUY" else not is_buyer
                if is_entry_fill is None:
                    continue
                if is_entry_fill:
                    executed += qty
                    entry_notional += price * qty
                else:
                    closed += qty
        else:
            for order in self.active_test_orders:
                side = str(order.get("side") or "").upper()
                cum_qty = float(order.get("cum_qty") or 0.0)
                if cum_qty <= 0:
                    continue
                if side == entry_side:
                    executed += cum_qty
                    avg_price = float(order.get("avg_fill_price") or 0.0)
                    if avg_price <= 0:
                        avg_price = float(order.get("price") or 0.0)
                    if avg_price > 0:
                        entry_notional += avg_price * cum_qty
                elif side == exit_side:
                    closed += cum_qty
        if executed > 0 and entry_notional > 0:
            entry_avg = entry_notional / executed
        elif executed > 0:
            entry_avg = self._entry_avg_price or self.get_buy_price()
        remaining = max(executed - closed, 0.0)
        if executed <= 0 and self.position is not None:
            executed = float(self.position.get("initial_qty") or self.position.get("qty") or 0.0)
            remaining = float(self.position.get("qty") or executed)
            entry_avg = self.position.get("buy_price") or entry_avg or self._entry_avg_price or self.get_buy_price()
        if executed <= 0 and balance and balance["total"] > self._epsilon_qty():
            executed = balance["total"]
            remaining = balance["total"]
            entry_avg = entry_avg or self._entry_avg_price or self.get_buy_price()
        if balance and balance["total"] > remaining + self._epsilon_qty():
            remaining = balance["total"]
        return executed, closed, remaining, entry_avg

    def _apply_open_orders_balance_only(
        self,
        open_orders: list[dict],
        balance: Optional[dict],
        reason: str,
    ) -> None:
        if not self._should_dedup_log(f"reconcile_openorders_only_{reason}", 5.0):
            base_total = balance["total"] if balance else 0.0
            self._logger(
                "[RECONCILE_OPENORDERS_ONLY] "
                f"reason={reason} open_orders={len(open_orders)} "
                f"base_total={base_total:.8f}"
            )
        have_open_entry = any(
            str(order.get("side") or "").upper() == self._entry_side()
            for order in open_orders
        )
        have_open_exit = any(
            str(order.get("side") or "").upper() == self._exit_side()
            for order in open_orders
        )
        if have_open_entry:
            self._transition_state(TradeState.STATE_ENTRY_WORKING)
            return
        if have_open_exit:
            self._transition_state(self._exit_state_for_intent(self.exit_intent))
            return
        base_total = balance["total"] if balance else 0.0
        min_qty = self._profile.min_qty or 0.0
        if base_total >= min_qty and base_total > self._epsilon_qty():
            entry_avg = self._entry_avg_price or self._last_buy_price or self.get_buy_price()
            now_ms = int(time.time() * 1000)
            if self.position is None:
                self.position = {
                    "buy_price": entry_avg,
                    "qty": base_total,
                    "opened_ts": now_ms,
                    "partial": True,
                    "initial_qty": base_total,
                    "side": self._direction,
                }
            else:
                if entry_avg is not None:
                    self.position["buy_price"] = entry_avg
                self.position["qty"] = base_total
                if not self.position.get("opened_ts"):
                    self.position["opened_ts"] = now_ms
                self.position.setdefault("initial_qty", base_total)
            self._remaining_qty = base_total
            self._set_position_qty_base(base_total)
            self._transition_to_position_state()
            price_state, health_state = self._router.build_price_state()
            self._ensure_exit_orders(
                price_state,
                health_state,
                allow_replace=True,
                skip_reconcile=True,
            )
            return
        self.position = None
        self._reset_position_tracking()
        self._transition_state(TradeState.STATE_FLAT)
        self._transition_state(TradeState.STATE_IDLE)

    def _resolve_exchange_qty(self, balance: Optional[dict]) -> float:
        if not balance:
            return 0.0
        total = float(balance.get("total", 0.0) or 0.0)
        borrowed = abs(float(balance.get("borrowed", 0.0) or 0.0))
        net = abs(float(balance.get("net", 0.0) or 0.0))
        if self._direction == "SHORT":
            return max(borrowed, net, total)
        return total

    def _restore_position_from_exchange(
        self,
        exchange_qty: float,
        entry_avg: Optional[float],
    ) -> None:
        if exchange_qty <= self._epsilon_qty():
            return
        cycle = self.ledger.active_cycle
        if not cycle or cycle.status != CycleStatus.OPEN:
            recovered_avg = (
                entry_avg
                or self._entry_avg_price
                or self._last_buy_price
                or self.get_buy_price()
                or 0.0
            )
            cycle_id = self.ledger.start_cycle(
                symbol=self._settings.symbol,
                direction=self._direction,
                entry_qty=exchange_qty,
                entry_avg=recovered_avg,
                notes="RECOVERED_FROM_EXCHANGE",
            )
            self._current_cycle_id = cycle_id
            self._cycle_id_counter = max(self._cycle_id_counter, cycle_id)
            self.ledger.on_entry_fill(cycle_id, exchange_qty, recovered_avg or 0.0)
            cycle = self.ledger.active_cycle
        if cycle and cycle.status == CycleStatus.OPEN:
            cycle.notes = "RECOVERED_FROM_EXCHANGE"
            self._sync_cycle_qty_from_ledger(cycle.cycle_id)
        self._executed_qty_total = exchange_qty
        self._closed_qty_total = 0.0
        self._remaining_qty = exchange_qty
        recovered_price = entry_avg or self._entry_avg_price or self.get_buy_price()
        if recovered_price is not None:
            self._entry_avg_price = recovered_price
            self._last_buy_price = recovered_price
        now_ms = int(time.time() * 1000)
        if self.position is None:
            self.position = {
                "buy_price": recovered_price,
                "qty": exchange_qty,
                "opened_ts": now_ms,
                "partial": True,
                "initial_qty": exchange_qty,
                "side": self._direction,
            }
        else:
            if recovered_price is not None:
                self.position["buy_price"] = recovered_price
            self.position["qty"] = exchange_qty
            self.position["partial"] = True
            self.position.setdefault("initial_qty", exchange_qty)

    def apply_reconcile_snapshot(self, snapshot: Optional[dict]) -> None:
        try:
            if snapshot is None:
                self.mark_reconcile_failed("snapshot_missing")
                return
            open_orders = snapshot.get("open_orders") or []
            balance = snapshot.get("balance")
            self.sync_open_orders(open_orders)
            restored, unresolved, stale_local = self._reconcile_registry_mapping(open_orders)
            if stale_local and not self._should_dedup_log("reconcile_stale_local", 2.0):
                self._logger(f"[RECON_STALE] marked={stale_local}")
            if restored:
                self._logger(f"[RECON_RESTORE] restored_orders={restored}")
            ledger_remaining = 0.0
            ledger_executed = 0.0
            cycle = self.ledger.active_cycle
            if cycle and cycle.status == CycleStatus.OPEN:
                ledger_remaining = cycle.remaining_qty
                ledger_executed = cycle.executed_qty
            exchange_total = self._resolve_exchange_qty(balance)
            action = "ok"
            if balance and abs(exchange_total - ledger_remaining) > self._epsilon_qty():
                ledger_before = ledger_remaining
                if exchange_total > self._epsilon_qty() and ledger_remaining <= self._epsilon_qty():
                    self._restore_position_from_exchange(
                        exchange_total,
                        entry_avg=self._entry_avg_price or self.get_buy_price(),
                    )
                    ledger_remaining = exchange_total
                    action = "restore"
                    self._logger(
                        "[RECON_RESTORE] "
                        f"exchange_qty={exchange_total:.8f} "
                        f"ledger_before={ledger_before:.8f} "
                        f"ledger_after={ledger_remaining:.8f}"
                    )
                    self._trade_hold = False
                else:
                    action = "hold"
                    self._trade_hold = True
            else:
                self._trade_hold = False
            self._logger(
                "[RECON] "
                f"exchange_qty={exchange_total:.8f} "
                f"ledger_exec={ledger_executed:.8f} ledger_rem={ledger_remaining:.8f} "
                f"action={action}"
            )
            if unresolved > 0 and restored == 0:
                self._reconcile_attempts += 1
                attempt_label = self._reconcile_attempts
                if not self._should_dedup_log("reconcile_restore_failed", 2.0):
                    self._logger(
                        "[RECON_RESTORE_FAIL] "
                        f"unknown_orders={unresolved} attempt={attempt_label}"
                    )
                if self._reconcile_attempts >= 3:
                    self._enter_safe_stop(reason="RECON_RESTORE_FAILED", allow_rest=False)
            else:
                self._reconcile_attempts = 0
            if self.state == TradeState.STATE_RECONCILE:
                self._transition_to_position_state()
        finally:
            self._reconcile_inflight = False

    def _reconcile_with_exchange(self) -> None:
        snapshot = self.collect_reconcile_snapshot()
        self.apply_reconcile_snapshot(snapshot)

    def _finalize_sell_timeout(self) -> None:
        self._sell_wait_started_ts = None
        self._sell_retry_count = 0
        self.sell_active_order_id = None
        self.sell_cancel_pending = False
        self.sell_retry_pending = False
        self._pending_sell_retry_reason = None
        self._exit_cross_attempts = 0
        exit_side = self._exit_side()
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("side") != exit_side
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
        if self._has_active_order(self._exit_side()):
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
        self._note_effective_source(source)
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
            ) = self._compute_exit_price_dynamic(
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
        if self._handle_stop_interrupt(open_orders=open_orders, reason="tick_stop"):
            return
        self._last_open_orders = open_orders
        self._update_open_orders_signature(open_orders)
        canceled_tp_orders = self._dedup_open_tp_orders(open_orders)
        self._sync_tp_keys_from_open_orders(open_orders, canceled_tp_orders)
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
                if order.get("side") == self._exit_side():
                    order["sell_status"] = status
                order["updated_ts"] = now_ms
        self.orders_count = len(self.active_test_orders)
        if self.sell_active_order_id is None:
            sell_order = self._find_order(self._exit_side())
            if sell_order:
                self.sell_active_order_id = sell_order.get("orderId")
        if self.entry_active_order_id is None:
            buy_order = self._find_order(self._entry_side())
            if buy_order:
                self.entry_active_order_id = buy_order.get("orderId")
                self.entry_active_price = buy_order.get("price")
        self._rebind_orphan_fills()
        entry_order = self._get_active_entry_order()
        if (
            self.state == TradeState.STATE_ENTRY_WORKING
            and entry_order is not None
            and not self._entry_order_is_final(entry_order)
        ):
            entry_id = entry_order.get("orderId")
            if entry_id not in open_map:
                if self._entry_missing_since_ts is None:
                    self._entry_missing_since_ts = time.monotonic()
            else:
                self._entry_missing_since_ts = None
        else:
            self._entry_missing_since_ts = None
        exit_order = self._get_active_sell_order()
        if (
            self._in_exit_workflow()
            and exit_order is not None
            and not self._sell_order_is_final(exit_order)
        ):
            exit_id = exit_order.get("orderId")
            if exit_id not in open_map:
                if self._exit_missing_since_ts is None:
                    self._exit_missing_since_ts = time.monotonic()
            else:
                self._exit_missing_since_ts = None
        else:
            self._exit_missing_since_ts = None
        self._enter_tick(open_map)
        if self._maybe_handle_sell_watchdog(open_map):
            return
        self._maybe_handle_sell_reprice(open_map)
        self._maybe_place_sell_retry()
        self._attempt_recovery_sell()
        self._watchdog_inflight()

    def handle_order_filled(
        self, order_id: int, side: str, price: float, qty: float, ts_ms: int
    ) -> None:
        self.mark_progress(reason="fill", reset_reconcile=True)
        order = next(
            (entry for entry in self.active_test_orders if entry.get("orderId") == order_id),
            None,
        )
        client_order_id = (
            str(order.get("clientOrderId"))
            if order and order.get("clientOrderId") is not None
            else None
        )
        meta = self._get_meta_for_fill(
            order_id,
            qty,
            price,
            client_order_id=client_order_id,
            side=side,
            kind="FILLED",
            ts_ms=ts_ms,
        )
        if meta is None:
            return
        trade_id = self._extract_trade_id(order)
        is_buyer_maker = self._extract_is_buyer_maker(order)
        if not self._should_apply_fill_dedup(
            order_id, side, qty, price, is_buyer_maker, ts_ms, trade_id
        ):
            return
        side_upper = str(side).upper()
        cycle_id = meta.cycle_id
        role = meta.role.value
        if order is None:
            delta = self._resolve_fill_delta(order_id, qty)
            if delta <= 0:
                self._update_registry_status_by_order(order_id, "FILLED")
                return
            if role == "ENTRY":
                self.ledger.on_entry_fill(cycle_id, delta, price)
                self._sync_cycle_qty_from_ledger(cycle_id)
                self._log_fill_applied(meta, delta, price)
                self._log_entry_fill(cycle_id, delta, price)
                price_state, _ = self._router.build_price_state()
                self._place_tp_for_fill(delta, price_state)
            elif role in {"EXIT_TP", "EXIT_CROSS"}:
                self.ledger.on_exit_fill(cycle_id, delta, price)
                self._sync_cycle_qty_from_ledger(cycle_id)
                self._log_fill_applied(meta, delta, price)
                if meta.role == OrderRole.EXIT_TP:
                    self._log_tp_fill(cycle_id, delta, price)
            self._update_registry_status_by_order(order_id, "FILLED")
            return
        if not self._should_process_fill(
            order_id, side_upper, qty, price, cycle_id, trade_id
        ):
            return
        if order.get("status") == "FILLED":
            return
        order["status"] = "FILLED"
        if order.get("side") == self._exit_side():
            order["sell_status"] = "FILLED"
        order["updated_ts"] = ts_ms
        order["price"] = price
        order["qty"] = qty
        self._apply_order_filled(order, dedup_checked=True, meta=meta)
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
        client_order_id = (
            str(order.get("clientOrderId"))
            if order and order.get("clientOrderId") is not None
            else None
        )
        meta = self._get_meta_for_fill(
            order_id,
            cum_qty,
            avg_price,
            client_order_id=client_order_id,
            side=side,
            kind="PARTIAL",
            ts_ms=ts_ms,
        )
        if meta is None:
            return
        trade_id = self._extract_trade_id(order)
        is_buyer_maker = self._extract_is_buyer_maker(order)
        if not self._should_apply_fill_dedup(
            order_id, side, cum_qty, avg_price, is_buyer_maker, ts_ms, trade_id
        ):
            return
        side_upper = str(side).upper()
        cycle_id = meta.cycle_id
        if order is None:
            fill_price = avg_price if avg_price > 0 else 0.0
            delta = self._resolve_fill_delta(order_id, cum_qty)
            if delta <= 0:
                return
            if meta.role == OrderRole.ENTRY:
                if fill_price > 0:
                    self.ledger.on_entry_fill(cycle_id, delta, fill_price)
                    self._sync_cycle_qty_from_ledger(cycle_id)
                    self._log_fill_applied(meta, delta, fill_price)
                    self._log_entry_fill(cycle_id, delta, fill_price)
                    price_state, _ = self._router.build_price_state()
                    self._place_tp_for_fill(delta, price_state)
                self._update_registry_status_by_order(order_id, "PARTIAL", cum_qty)
            elif meta.role in {OrderRole.EXIT_TP, OrderRole.EXIT_CROSS}:
                if fill_price > 0:
                    self.ledger.on_exit_fill(cycle_id, delta, fill_price)
                    self._sync_cycle_qty_from_ledger(cycle_id)
                    self._log_fill_applied(meta, delta, fill_price)
                    if meta.role == OrderRole.EXIT_TP:
                        self._log_tp_fill(cycle_id, delta, fill_price)
                self._update_registry_status_by_order(order_id, "PARTIAL", cum_qty)
            return
        if not self._should_process_fill(
            order_id, side_upper, cum_qty, avg_price, cycle_id, trade_id
        ):
            return
        role = meta.role.value
        entry_side = self._entry_side()
        exit_side = self._exit_side()
        if role == "ENTRY":
            order["cum_qty"] = cum_qty
            if avg_price > 0:
                order["avg_fill_price"] = avg_price
                order["last_fill_price"] = avg_price
            order["updated_ts"] = ts_ms
            delta = self._resolve_fill_delta(order_id, cum_qty)
            if delta <= 0:
                return
            if delta > 0:
                self.mark_progress(reason="partial_fill", reset_reconcile=True)
                self._log_cycle_fill(cycle_id, entry_side, delta, cum_qty)
                fill_price = avg_price if avg_price > 0 else order.get("price")
                if fill_price is not None:
                    self.ledger.on_entry_fill(cycle_id, delta, float(fill_price))
                    self._sync_cycle_qty_from_ledger(cycle_id)
                    self._log_fill_applied(meta, delta, float(fill_price))
                    self._log_entry_fill(cycle_id, delta, float(fill_price))
                self._update_registry_status_by_order(order_id, "PARTIAL", delta)
                if self.position is None:
                    self.position = {
                        "buy_price": fill_price,
                        "qty": cum_qty,
                        "opened_ts": ts_ms,
                        "partial": True,
                        "initial_qty": cum_qty,
                        "side": self._direction,
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
                self._reconcile_position_from_fills()
                self._transition_to_position_state()
                action, sell_qty = self._sync_sell_for_partial(delta)
                price_label = "—" if avg_price <= 0 else f"{avg_price:.8f}"
                self._logger(
                    f"[BUY_FILL] {self._direction_tag()} "
                    f"status=PARTIALLY_FILLED executed_qty={cum_qty:.8f} "
                    f"delta={delta:.8f} avg_price={price_label} "
                    f"-> action={action} qty={sell_qty:.8f}"
                )
                self._log_trade_snapshot(reason="entry_partial")
            pos_qty = self.position.get("qty") if self.position is not None else 0.0
            avg_value = avg_price
            if avg_value <= 0 and self.position is not None:
                avg_value = float(self.position.get("buy_price") or 0.0)
            last_logged = self._last_buy_partial_cum.get(order_id)
            if last_logged is None or not math.isclose(
                last_logged, cum_qty, rel_tol=0.0, abs_tol=1e-12
            ):
                self._last_buy_partial_cum[order_id] = cum_qty
                cycle_label = "—" if cycle_id is None else str(cycle_id)
                self._logger(
                    f"[BUY_PARTIAL] {self._direction_tag()} "
                    f"cycle_id={cycle_label} delta={delta:.8f} "
                    f"pos_qty={pos_qty:.8f} avg={avg_value:.8f}"
                )
            return
        if role in {"EXIT_TP", "EXIT_CROSS"}:
            prev_cum = float(order.get("cum_qty") or 0.0)
            prev_avg = float(order.get("avg_fill_price") or 0.0)
            delta = self._update_sell_execution(order, cum_qty)
            if delta > 0:
                self.mark_progress(reason="partial_fill", reset_reconcile=True)
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
            self._reconcile_position_from_fills()
            if delta > 0:
                self._log_cycle_fill(cycle_id, exit_side, delta, cum_qty)
                fill_price = avg_price if avg_price > 0 else order.get("price")
                if fill_price is not None:
                    self.ledger.on_exit_fill(cycle_id, delta, float(fill_price))
                    self._sync_cycle_qty_from_ledger(cycle_id)
                    self._log_fill_applied(meta, delta, float(fill_price))
                    if meta.role == OrderRole.EXIT_TP:
                        self._log_tp_fill(cycle_id, delta, float(fill_price))
                self._update_registry_status_by_order(order_id, "PARTIAL", delta)
                cycle_label = "—" if cycle_id is None else str(cycle_id)
                self._logger(
                    f"[SELL_PARTIAL] {self._direction_tag()} "
                    f"cycle_id={cycle_label} delta={delta:.8f} "
                    f"remaining={remaining_qty:.8f}"
                )
                self._sync_sell_for_partial(delta, allow_replace=False)
            if remaining_qty <= self._get_step_size_tolerance():
                sell_reason = str(order.get("reason") or "").upper()
                exit_intent = self._normalize_exit_intent(self.exit_intent or sell_reason)
                exit_reason = "PARTIAL"
                if sell_reason in {"SL", "SL_HARD"} or exit_intent == "SL":
                    exit_reason = "SL_HARD"
                    if not self._post_sl_reconcile(exit_side):
                        return
                self._finalize_partial_close(reason=exit_reason)
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
        if order.get("side") == self._exit_side():
            order["sell_status"] = status
        order["updated_ts"] = ts_ms
        side = str(order.get("side", "UNKNOWN")).upper()
        cycle_id = self._get_cycle_id_for_order(order)
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        self._logger(
            f"[ORDER] {side} {status} cycle_id={cycle_label} id={order_id}"
        )
        self._update_registry_status_by_order(order_id, status)
        self._clear_order_role(order_id)
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        entry_side = self._entry_side()
        exit_side = self._exit_side()
        if side == entry_side:
            self._reset_buy_retry()
            self._clear_entry_attempt()
            if self.entry_active_order_id == order_id:
                self._finalize_entry_ownership(reason=f"done:{status}")
            if self.position is None:
                self._transition_state(TradeState.STATE_IDLE)
            else:
                self._transition_to_position_state()
        elif side == exit_side:
            self._reset_sell_retry()
            self.sell_active_order_id = None
            self.sell_cancel_pending = False
            if self.position is not None:
                self._transition_to_position_state()
            else:
                self._transition_state(TradeState.STATE_IDLE)

    def _apply_order_filled(
        self,
        order: dict,
        dedup_checked: bool = False,
        meta: Optional[OrderMeta] = None,
    ) -> None:
        side = order.get("side")
        order_id = order.get("orderId")
        qty = float(order.get("qty") or 0.0)
        price = float(order.get("price") or 0.0)
        side_label = str(side or "UNKNOWN").upper()
        if meta is None:
            client_order_id = (
                str(order.get("clientOrderId"))
                if order.get("clientOrderId") is not None
                else None
            )
            meta = self._get_meta_for_fill(
                order_id,
                qty,
                price,
                client_order_id=client_order_id,
                side=side_label,
                kind="FILLED",
                ts_ms=int(order.get("updated_ts") or time.time() * 1000),
            )
        if meta is None:
            return
        cycle_id = meta.cycle_id
        if not dedup_checked:
            trade_id = self._extract_trade_id(order)
            is_buyer_maker = self._extract_is_buyer_maker(order)
            ts_ms = int(order.get("updated_ts") or time.time() * 1000)
            if not self._should_apply_fill_dedup(
                order_id, side_label, qty, price, is_buyer_maker, ts_ms, trade_id
            ):
                return
            if not self._should_process_fill(
                order_id, side_label, qty, price, cycle_id, trade_id
            ):
                return
        role = meta.role.value
        entry_side = self._entry_side()
        exit_side = self._exit_side()
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        if role == "ENTRY":
            order["cum_qty"] = qty
            self._logger(
                f"[ORDER] {entry_side} filled "
                f"cycle_id={cycle_label} qty={qty:.8f} "
                f"price={price:.8f}"
            )
            self._logger(
                f"[FILL] {self._direction_tag()} entry cycle_id={cycle_label} "
                f"qty={qty:.8f} price={price:.8f}"
            )
            delta = self._resolve_fill_delta(order_id, qty)
            if delta <= 0:
                self._update_registry_status_by_order(order_id, "FILLED")
                self._clear_order_role(order_id)
                return
            self._log_cycle_fill(cycle_id, entry_side, delta, qty)
            self.ledger.on_entry_fill(cycle_id, delta, price)
            self._sync_cycle_qty_from_ledger(cycle_id)
            self._log_fill_applied(meta, delta, price)
            self._log_entry_fill(cycle_id, delta, price)
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
                    "side": self._direction,
                }
                self._set_position_qty_base(qty)
                self._reset_exit_intent()
            self._reconcile_position_from_fills()
            self._transition_to_position_state()
            action, sell_qty = self._sync_sell_for_partial(delta)
            price_label = "—" if price <= 0 else f"{price:.8f}"
            self._logger(
                f"[BUY_FILL] {self._direction_tag()} "
                f"status=FILLED executed_qty={qty:.8f} delta={delta:.8f} "
                f"avg_price={price_label} -> action={action} qty={sell_qty:.8f}"
            )
            self._log_trade_snapshot(reason="entry_filled")
            self._clear_order_role(order_id)
        elif role in {"EXIT_TP", "EXIT_CROSS"}:
            delta = 0.0
            if qty > 0:
                prev_cum = float(order.get("cum_qty") or 0.0)
                delta = self._update_sell_execution(order, qty)
                if delta <= 0:
                    self._update_registry_status_by_order(order_id, "FILLED")
                    self._clear_order_role(order_id)
                    return
                if price > 0:
                    prev_avg = float(order.get("avg_fill_price") or 0.0)
                    prev_notional = prev_avg * prev_cum
                    new_notional = price * qty
                    notional_delta = max(new_notional - prev_notional, 0.0)
                    if notional_delta > 0:
                        self._aggregate_sold_notional += notional_delta
                    order["avg_fill_price"] = price
                    order["last_fill_price"] = price
            self._reconcile_position_from_fills()
            self._reset_sell_retry()
            self.sell_active_order_id = None
            self.sell_cancel_pending = False
            reason = str(order.get("reason") or "MANUAL").upper()
            exit_intent = self._normalize_exit_intent(self.exit_intent or reason)
            if reason == "TP":
                exit_reason = "TP_MAKER"
            elif reason in {"TP_MAKER", "TP_FORCE", "TP_CROSS"}:
                exit_reason = reason
            elif reason in {"SL", "SL_HARD"}:
                exit_reason = "SL_HARD"
            elif exit_intent == "SL":
                exit_reason = "SL_HARD"
            elif exit_intent == "TP":
                exit_reason = "TP_MAKER"
            else:
                exit_reason = "MANUAL"
            fill_price = order.get("price")
            buy_price = self.position.get("buy_price") if self.position else self._last_buy_price
            if exit_reason in {"TP_MAKER", "TP_FORCE", "TP_CROSS", "SL_HARD"}:
                fill_price_label = "—" if fill_price is None else f"{fill_price:.8f}"
                qty_label = "—" if qty <= 0 else f"{qty:.8f}"
                self._logger(
                    f"[ORDER] {exit_side} filled "
                    f"cycle_id={cycle_label} reason={exit_reason} "
                    f"fill_price={fill_price_label} qty={qty_label}"
                )
            else:
                fill_price_label = "—" if fill_price is None else f"{fill_price:.8f}"
                qty_label = "—" if qty <= 0 else f"{qty:.8f}"
                self._logger(
                    f"[ORDER] {exit_side} filled "
                    f"cycle_id={cycle_label} qty={qty_label} "
                    f"price={fill_price_label}"
                )
            fill_price_label = "—" if fill_price is None else f"{fill_price:.8f}"
            self._logger(
                f"[FILL] {self._direction_tag()} exit reason={exit_reason} "
                f"qty={qty:.8f} price={fill_price_label}"
            )
            if delta > 0:
                self._log_cycle_fill(cycle_id, exit_side, delta, qty)
                if fill_price is not None:
                    self.ledger.on_exit_fill(cycle_id, delta, float(fill_price))
                    self._sync_cycle_qty_from_ledger(cycle_id)
                    self._log_fill_applied(meta, delta, float(fill_price))
                    if meta.role == OrderRole.EXIT_TP:
                        self._log_tp_fill(cycle_id, delta, float(fill_price))
            remaining_qty = self._resolve_remaining_qty_raw()
            if (
                self._adaptive_mode
                and exit_reason in {"TP_MAKER", "TP_FORCE", "TP_CROSS"}
                and remaining_qty > self._get_step_size_tolerance()
            ):
                if self.position is not None:
                    self.position["qty"] = remaining_qty
                self._transition_to_position_state()
                self._clear_order_role(order_id)
                self._update_registry_status_by_order(order_id, "FILLED")
                return
            if exit_reason == "SL_HARD":
                if not self._post_sl_reconcile(exit_side):
                    self._clear_order_role(order_id)
                    self._update_registry_status_by_order(order_id, "FILLED")
                    return
            pnl = self._finalize_close(order)
            self.last_exit_reason = exit_reason
            if exit_reason in {"TP_MAKER", "TP_FORCE", "TP_CROSS", "SL_HARD"}:
                pnl_quote = "—" if pnl is None else f"{pnl:.8f}"
                pnl_bps = "—"
                if pnl is not None and buy_price and qty:
                    notion = buy_price * qty
                    if notion > 0:
                        pnl_bps = f"{(pnl / notion) * 10000:.2f}"
                self._logger(
                    f"[PNL] realized reason={exit_reason} pnl_quote={pnl_quote} pnl_bps={pnl_bps}"
                )
            else:
                realized = "—" if pnl is None else f"{pnl:.8f}"
                self._logger(f"[PNL] realized={realized}")
            self._transition_state(TradeState.STATE_FLAT)
            self._log_exit_summary(exit_reason, buy_price, fill_price, qty)
            total_qty = self._executed_qty_total if self._executed_qty_total > 0 else qty
            self._finalize_cycle_close(
                reason=exit_reason,
                pnl=pnl,
                qty=total_qty,
                buy_price=buy_price,
                cycle_id=cycle_id,
            )
            self._clear_order_role(order_id)
        self._update_registry_status_by_order(order_id, "FILLED")

    def _apply_entry_partial_fill(
        self,
        order_id: Optional[int],
        qty: float,
        price: Optional[float],
    ) -> None:
        if qty <= 0:
            return
        entry_side = self._entry_side()
        ts_ms = int(time.time() * 1000)
        meta = self._get_meta_for_fill(
            order_id,
            qty,
            price or 0.0,
            side=entry_side,
            kind="PARTIAL",
            ts_ms=ts_ms,
        )
        if meta is None:
            return
        cycle_id = meta.cycle_id
        avg_price = price if price is not None else 0.0
        if not self._should_apply_fill_dedup(
            order_id, entry_side, qty, avg_price, None, ts_ms, None
        ):
            return
        if not self._should_process_fill(order_id, entry_side, qty, avg_price, cycle_id):
            return
        delta = self._resolve_fill_delta(order_id, qty)
        if delta <= 0:
            return
        price_label = "?" if price is None else f"{price:.8f}"
        cycle_label = "—" if cycle_id is None else str(cycle_id)
        self._logger(
            f"[ORDER] {entry_side} partial timeout "
            f"cycle_id={cycle_label} qty={qty:.8f} price={price_label}"
        )
        self._log_cycle_fill(cycle_id, entry_side, delta, qty)
        if price is not None:
            self.ledger.on_entry_fill(cycle_id, delta, float(price))
            self._sync_cycle_qty_from_ledger(cycle_id)
            self._log_fill_applied(meta, delta, float(price))
            self._log_entry_fill(cycle_id, delta, float(price))
            self._transition_pipeline_state(
                PipelineState.ARM_EXIT, reason="entry_partial_fill"
            )
            price_state, _ = self._router.build_price_state()
            self._place_tp_for_fill(delta, price_state)
        self._update_registry_status_by_order(order_id, "PARTIAL", qty)
        self._reset_buy_retry()
        self._clear_entry_attempt()
        if self.position is None:
            self.position = {
                "buy_price": price,
                "qty": qty,
                "opened_ts": int(time.time() * 1000),
                "partial": True,
                "initial_qty": qty,
                "side": self._direction,
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
        self._reconcile_position_from_fills()
        action, sell_qty = self._sync_sell_for_partial(qty)
        price_label = "—" if price is None else f"{price:.8f}"
        self._logger(
            f"[BUY_FILL] {self._direction_tag()} "
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
            entry_price = self.position.get("buy_price", mid)
            qty = self.position.get("qty", 0.0)
            if self._position_side() == "SHORT":
                return (entry_price - mid) * qty
            return (mid - entry_price) * qty
        entry_order = self._find_order(self._entry_side())
        if entry_order and entry_order.get("status") == "NEW":
            entry_price = entry_order.get("price", mid)
            qty = entry_order.get("qty", 0.0)
            if self._direction == "SHORT":
                return (entry_price - mid) * qty
            return (mid - entry_price) * qty
        return None

    def update_ledger_unrealized(self, price_state: Optional[PriceState]) -> None:
        if price_state is None:
            return
        active_cycle = self.ledger.active_cycle
        if not active_cycle or active_cycle.status != CycleStatus.OPEN:
            return
        now = time.monotonic()
        if (now - self._last_ledger_unreal_ts) < 0.25:
            return
        self._last_ledger_unreal_ts = now
        mark_price = price_state.bid if self._direction == "LONG" else price_state.ask
        if mark_price is None:
            return
        self.ledger.update_unrealized(mark_price, mark_side="BOOK")

    def _finalize_close(self, sell_order: dict) -> Optional[float]:
        sell_price = sell_order.get("price")
        qty = sell_order.get("qty")
        buy_price = None
        if self.position is not None:
            buy_price = self.position.get("buy_price")
        if buy_price is None:
            buy_price = self._last_buy_price
        pnl = None
        if self._adaptive_mode:
            cycle = self.ledger.active_cycle
            if cycle and cycle.status == CycleStatus.OPEN and cycle.exit_qty > 0:
                if cycle.direction == "SHORT":
                    pnl = (cycle.entry_avg - cycle.exit_avg) * cycle.exit_qty
                else:
                    pnl = (cycle.exit_avg - cycle.entry_avg) * cycle.exit_qty
                pnl -= cycle.fees_quote
        if pnl is None and buy_price is not None and sell_price is not None and qty is not None:
            if self._position_side() == "SHORT":
                pnl = (buy_price - sell_price) * qty
            else:
                pnl = (sell_price - buy_price) * qty
        self._record_cycle_pnl(pnl)
        self.position = None
        self._reset_position_tracking()
        self.last_action = "closed"
        return pnl

    def _resolve_close_qty(self) -> float:
        return self._resolve_remaining_qty_raw()

    def _resolve_remaining_qty_raw(self) -> float:
        cycle = self.ledger.active_cycle
        if cycle and cycle.status == CycleStatus.OPEN:
            self._sync_cycle_qty_from_ledger(cycle.cycle_id)
            return max(cycle.remaining_qty, 0.0)
        if self._executed_qty_total > 0 or self._closed_qty_total > 0:
            remaining_qty = self._executed_qty_total - self._closed_qty_total
            return max(remaining_qty, 0.0)
        if self.position_qty_base is None:
            if self.position is not None:
                self._set_position_qty_base(self.position.get("initial_qty"))
            else:
                return 0.0
        base_qty = float(self.position_qty_base or 0.0)
        remaining_qty = base_qty - self._aggregate_sold_qty
        return max(remaining_qty, 0.0)

    def _resolve_dust_reference_price(
        self,
        bid: Optional[float],
        ask: Optional[float],
        mid: Optional[float],
        entry_avg: Optional[float],
    ) -> Optional[float]:
        if self._direction == "SHORT":
            return bid or mid or entry_avg
        return ask or mid or entry_avg

    def _is_dust_notional(self, qty: float, price_ref: Optional[float]) -> bool:
        min_notional = self._profile.min_notional
        if min_notional is None or min_notional <= 0:
            return False
        if price_ref is None or qty <= 0:
            return False
        qty_eval = qty
        if self._profile.step_size:
            qty_eval = self._round_down(qty, self._profile.step_size)
        notional = qty_eval * price_ref
        return notional < min_notional

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
        self._executed_qty_total = 0.0
        self._closed_qty_total = 0.0
        self._remaining_qty = 0.0
        self._entry_avg_price = None
        self._last_pos_snapshot = None
        self._tp_maker_started_ts = None
        self._sell_last_seen_ts = None
        self._dust_accumulate = False
        self._reset_exit_intent()
        self._clear_recovery(reason="reset")
        self._tp_armed = False

    def _reset_exit_intent(self) -> None:
        self.exit_intent = None
        self.exit_intent_set_ts = None
        self._last_sell_ref_price = None
        self._last_sell_ref_side = None
        self._exit_started_ts = None
        self._tp_exit_phase = None
        self._exit_reprice_last_ts = 0.0
        self._tp_maker_started_ts = None
        self._tp_armed = False
        self._reconcile_recovery_attempts = 0
        self._exit_cross_attempts = 0
        self._exit_missing_since_ts = None
        self._sl_close_retries = 0

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
        self._tp_maker_started_ts = None
        self._tp_armed = False
        self._logger(f"[EXIT_INTENT] {self._direction_tag()} {intent}")
        ticks_label = "?" if ticks is None else str(ticks)
        mid_label = "—" if mid is None else f"{mid:.8f}"
        self._logger(
            f"[EXIT_INTENT_SET] {self._direction_tag()} intent={intent} "
            f"ticks={ticks_label} mid={mid_label}"
        )

    def _cancel_active_sell(self, reason: str) -> None:
        if not self.sell_active_order_id and not self._has_active_order(self._exit_side()):
            return
        price_state, _ = self._router.build_price_state()
        if not self._can_trade_now(
            price_state,
            action="EXIT",
            max_age_ms=self._trade_gate_age_ms("EXIT"),
        ):
            return
        sell_order = self._get_active_sell_order()
        order_id = sell_order.get("orderId") if sell_order else self.sell_active_order_id
        if not order_id:
            return
        is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
        self._logger(f"[SELL_CANCEL] reason={reason} id={order_id}")
        self._note_pending_replace(order_id)
        final_status = self._safe_cancel_and_sync(
            order_id=order_id,
            is_isolated=is_isolated,
            tag=sell_order.get("tag", self.TAG) if sell_order else self.TAG,
        )
        self.sell_active_order_id = None
        if final_status in {"FILLED", "PARTIALLY_FILLED"}:
            self.sell_cancel_pending = False
            return
        self.sell_cancel_pending = True
        self._reset_cancel_wait("EXIT_CANCEL_WAIT")

    def _override_to_sl(self, bid: Optional[float], ask: Optional[float]) -> None:
        self._cancel_entry_before_exit(timeout_s=1.0)
        exit_side = self._exit_side()
        if self.sell_active_order_id or self._has_active_order(exit_side):
            sell_order = self._get_active_sell_order()
            if sell_order:
                order_id = sell_order.get("orderId")
                is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
                if order_id:
                    self._logger(f"[SL_OVERRIDE] cancel_sell id={order_id}")
                    final_status = self._safe_cancel_and_sync(
                        order_id=order_id,
                        is_isolated=is_isolated,
                        tag=sell_order.get("tag", self.TAG),
                    )
                    if final_status in {"FILLED", "PARTIALLY_FILLED"}:
                        self.sell_active_order_id = None
                        self.sell_cancel_pending = False
                        return
                self.sell_active_order_id = None
                self.sell_cancel_pending = False
        self._place_sell_order(
            reason="SL_HARD",
            price_override=bid if exit_side == "SELL" else ask,
            exit_intent="SL",
            ref_bid=bid,
            ref_ask=ask,
        )

    def _force_tp_exit(self, bid: Optional[float], ask: Optional[float]) -> None:
        self._cancel_entry_before_exit(timeout_s=1.0)
        self._logger("[TP_FORCE] action=aggressive_exit")
        exit_side = self._exit_side()
        if self.sell_active_order_id or self._has_active_order(exit_side):
            sell_order = self._get_active_sell_order()
            if sell_order:
                order_id = sell_order.get("orderId")
                is_isolated = "TRUE" if self._settings.margin_isolated else "FALSE"
                if order_id:
                    self._logger(f"[TP_FORCE] cancel_sell id={order_id}")
                    final_status = self._safe_cancel_and_sync(
                        order_id=order_id,
                        is_isolated=is_isolated,
                        tag=sell_order.get("tag", self.TAG),
                    )
                    if final_status in {"FILLED", "PARTIALLY_FILLED"}:
                        self.sell_active_order_id = None
                        self.sell_cancel_pending = False
                        return
                self.sell_active_order_id = None
                self.sell_cancel_pending = False
        self._set_exit_intent("TP", mid=None, force=True)
        self._place_sell_order(
            reason="TP_FORCE",
            price_override=bid if exit_side == "SELL" else ask,
            exit_intent="TP",
            ref_bid=bid,
            ref_ask=ask,
        )

    def _panic_exit(self, reason: str, bid: Optional[float], ask: Optional[float]) -> None:
        self._logger(f"[PANIC_EXIT] reason={reason}")
        self._cancel_entry_before_exit(timeout_s=1.0)
        exit_side = self._exit_side()
        price_override = bid if exit_side == "SELL" else ask
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
        order_id = order.get("orderId")
        if isinstance(order_id, int):
            delta = self._resolve_fill_delta(order_id, cum_qty)
        else:
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
        self._transition_pipeline_state(PipelineState.ARM_EXIT, reason="entry_fill")
        self._reconcile_position_from_fills()
        if self._adaptive_mode:
            price_state, _ = self._router.build_price_state()
            self._place_tp_for_fill(delta_qty, price_state)
            return "TP_PLACED", delta_qty
        remaining_qty_raw = self._remaining_qty
        qty_to_sell = remaining_qty_raw
        if self._profile.step_size:
            qty_to_sell = self._round_down(remaining_qty_raw, self._profile.step_size)
        if qty_to_sell <= 0 or remaining_qty_raw <= self._epsilon_qty():
            return "SKIP_EMPTY", qty_to_sell
        price_state, health_state = self._router.build_price_state()
        self._ensure_exit_orders(price_state, health_state, allow_replace=allow_replace)
        return "ENSURE_EXIT", qty_to_sell

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
            active_sell_order = self._get_active_sell_order()
            active_sell_pending = (
                active_sell_order is not None
                and not self._sell_order_is_final(active_sell_order)
            )
            if remaining_qty_raw <= self._get_step_size_tolerance() and not active_sell_pending:
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
        cycle = self.ledger.get_cycle(cycle_id)
        remaining_qty = (
            cycle.remaining_qty
            if cycle and cycle.status == CycleStatus.OPEN
            else self._remaining_qty
        )
        tolerance = self._get_step_size_tolerance()
        if remaining_qty > tolerance:
            self._logger(
                "[DEAL_CLOSE_BLOCKED] "
                f"reason=remaining qty={remaining_qty:.8f} tolerance={tolerance:.8f}"
            )
            self._transition_pipeline_state(PipelineState.WAIT_EXIT, reason="remaining")
            self._transition_to_position_state()
            return
        exchange_qty = self._resolve_exchange_qty(self._get_base_balance_snapshot())
        if exchange_qty > tolerance:
            self._logger(
                "[DEAL_CLOSE_BLOCKED] "
                f"reason=exchange_qty qty={exchange_qty:.8f} tolerance={tolerance:.8f}"
            )
            self._restore_position_from_exchange(exchange_qty, entry_avg=self._entry_avg_price)
            self._transition_pipeline_state(PipelineState.ARM_EXIT, reason="exchange_qty")
            self._transition_to_position_state()
            return
        if reason == "SL_HARD":
            self.losses += 1
        elif pnl is not None:
            if pnl > 0:
                self.wins += 1
            elif pnl < 0:
                self.losses += 1
        pnl_bps = None
        if pnl is not None and buy_price and qty:
            notion = buy_price * qty
            if notion > 0:
                pnl_bps = (pnl / notion) * 10000
        self._transition_pipeline_state(PipelineState.FINALIZE_DEAL, reason=reason)
        self._log_cycle_close(cycle_id, pnl, pnl_bps, reason)
        self.ledger.close_cycle(cycle_id, notes=reason)
        duration_ms = None
        if self._cycle_started_ts is not None:
            duration_ms = int((time.monotonic() - self._cycle_started_ts) * 1000.0)
        pnl_quote_label = "—" if pnl is None else f"{pnl:.8f}"
        pnl_bps_label = "—" if pnl_bps is None else f"{pnl_bps:.2f}"
        duration_label = "—" if duration_ms is None else str(duration_ms)
        self._logger(
            "[CYCLE_DONE] "
            f"pnl_quote={pnl_quote_label} pnl_bps={pnl_bps_label} "
            f"mode={reason} duration_ms={duration_label}"
        )
        self._handle_cycle_completion(reason=reason)
        self._transition_state(TradeState.STATE_FLAT)
        self._cleanup_cycle_state(cycle_id)
        self._auto_next_cycle()
        result_label = "LOSS" if reason == "SL_HARD" else None
        self._log_deal_close(cycle_id, pnl, result=result_label)

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
        cycle_id = self._get_cycle_id_for_order(self._find_order(self._exit_side()))
        if cycle_id is None:
            cycle_id = self._current_cycle_id
        qty = self._executed_qty_total or self.position_qty_base
        buy_price = self.position.get("buy_price") if self.position else self._last_buy_price
        pnl = None
        if qty and buy_price and self._aggregate_sold_notional > 0:
            if self._position_side() == "SHORT":
                pnl = (buy_price * qty) - self._aggregate_sold_notional
            else:
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
            return self._find_order(self._exit_side())
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
            return self._find_order(self._entry_side())
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
        self._clear_cancel_wait()
        if order_id is None:
            return
        self._update_registry_status_by_order(order_id, status)
        buy_order["status"] = status
        self.active_test_orders = [
            entry for entry in self.active_test_orders if entry.get("orderId") != order_id
        ]
        self.orders_count = len(self.active_test_orders)
        self._clear_order_role(order_id)
        if self.entry_active_order_id == order_id:
            self._finalize_entry_ownership(reason=f"cancel:{status}")
        if status in {"CANCELED", "REJECTED", "EXPIRED"}:
            self._reset_buy_retry()
            self._clear_entry_attempt()
        self._logger(f"[ENTRY_CANCEL_CONFIRMED] id={order_id} status={status}")
        self.mark_progress(reason="entry_cancel_confirmed", reset_reconcile=True)
        if (
            self._entry_replace_after_cancel
            and status in {"CANCELED", "REJECTED", "EXPIRED"}
        ):
            self._attempt_entry_replace_from_cancel()
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
            self._entry_replace_after_cancel = False
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
        self._set_cancel_wait_state("ENTRY_CANCEL_WAIT")
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
        self._set_cancel_wait_state("EXIT_CANCEL_WAIT")
        if not self._should_dedup_log(f"sell_cancel_wait:{order_id}", 2.0):
            self._logger(f"[SELL_CANCEL_WAIT] id={order_id}")

    def _finalize_sell_cancel(self, order_id: Optional[int]) -> None:
        if order_id is not None:
            self._logger(f"[SELL_CANCEL_CONFIRMED] id={order_id}")
            self._update_registry_status_by_order(order_id, "CANCELED")
        self.mark_progress(reason="sell_cancel_confirmed", reset_reconcile=True)
        self.sell_cancel_pending = False
        self._clear_cancel_wait()
        self.sell_active_order_id = None
        self.sell_retry_pending = False
        if order_id is not None:
            self.active_test_orders = [
                entry
                for entry in self.active_test_orders
                if entry.get("orderId") != order_id
            ]
            self.orders_count = len(self.active_test_orders)
            self._clear_order_role(order_id)
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
            if status:
                self._update_registry_status_by_order(order_id, status)
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
                    self._log_cycle_fill(cycle_id, self._exit_side(), delta, executed_qty)
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
        self.ledger.mark_error(notes=reason)
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
        self._clear_start_inflight()
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
        self._last_place_error_msg = None
        try:
            order = self._rest.create_margin_order(params)
            return order
        except httpx.HTTPStatusError as exc:
            code = None
            msg = None
            try:
                payload = exc.response.json() if exc.response else {}
                code = payload.get("code")
                msg = payload.get("msg")
            except Exception:
                pass
            self._last_place_error_code = code
            self._last_place_error_msg = msg
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

    def _apply_snapshot_fill(self, order_id: int, status: str, snapshot: dict) -> None:
        status_label = str(status).upper()
        side = str(snapshot.get("side") or "UNKNOWN")
        executed_qty = float(
            snapshot.get("executedQty", 0.0)
            or snapshot.get("executedQuantity", 0.0)
            or 0.0
        )
        cumulative_quote = snapshot.get("cumulativeQuoteQty", snapshot.get("cummulativeQuoteQty"))
        cumulative_quote_qty = (
            float(cumulative_quote)
            if cumulative_quote is not None
            else float(snapshot.get("cumulativeQuoteQty", 0.0) or 0.0)
        )
        avg_price = float(snapshot.get("avgPrice", 0.0) or 0.0)
        if executed_qty > 0 and cumulative_quote_qty > 0:
            avg_price = cumulative_quote_qty / executed_qty
        ts_ms = int(time.time() * 1000)
        if status_label == "FILLED":
            fill_qty = executed_qty
            if fill_qty <= 0:
                fill_qty = float(snapshot.get("origQty", 0.0) or snapshot.get("quantity", 0.0))
            fill_price = avg_price if avg_price > 0 else float(snapshot.get("price", 0.0) or 0.0)
            if fill_qty > 0:
                self.handle_order_filled(order_id, side, fill_price, fill_qty, ts_ms)
        elif status_label == "PARTIALLY_FILLED":
            if executed_qty > 0:
                self.handle_order_partial(order_id, side, executed_qty, avg_price, ts_ms)

    def _safe_cancel_and_sync(
        self,
        order_id: int,
        is_isolated: str,
        tag: str,
        attempt_cancel: bool = True,
    ) -> str:
        cancel_ok = False
        code = None
        if attempt_cancel:
            cancel_ok, code = self._cancel_margin_order_with_code(
                symbol=self._settings.symbol,
                order_id=order_id,
                is_isolated=is_isolated,
                tag=tag,
            )
        final_status = "CANCELED" if cancel_ok else "UNKNOWN"
        snapshot = None
        if not cancel_ok or not attempt_cancel or code == -2011:
            snapshot = self._get_margin_order_snapshot(order_id)
        if snapshot:
            final_status = str(snapshot.get("status") or "").upper()
            if final_status in {"FILLED", "PARTIALLY_FILLED"}:
                self._apply_snapshot_fill(order_id, final_status, snapshot)
            elif final_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                self._update_registry_status_by_order(order_id, final_status)
        self._logger(
            "[CANCEL_SYNC] "
            f"order_id={order_id} cancel_ok={cancel_ok} final_status={final_status}"
        )
        return final_status

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

    def _build_client_order_id(
        self, side: str, timestamp: int, role_tag: Optional[str] = None
    ) -> str:
        cycle_id = self._current_cycle_id or 0
        direction = self._direction
        role_label = role_tag or "GEN"
        nonce = self._build_client_nonce(timestamp, side)
        raw = (
            f"HDG-{self._settings.symbol}-{direction}-C{cycle_id}-{role_label}-{nonce}"
        )
        sanitized = self._sanitize_client_order_id(raw)
        if len(sanitized) > 36:
            sanitized = sanitized[:36]
        return sanitized

    def _build_client_nonce(self, timestamp: int, side: str) -> str:
        token = secrets.token_hex(3)
        return token[:6]

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
    def _round_to_tick(value: float, tick_size: float, mode: str = "NEAREST") -> float:
        if tick_size <= 0:
            return value
        scaled = value / tick_size
        if mode == "UP":
            return math.ceil(scaled) * tick_size
        if mode == "DOWN":
            return math.floor(scaled) * tick_size
        return round(scaled) * tick_size

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
            "borrowed": float(base_state.get("borrowed", 0.0) or 0.0),
            "net": float(base_state.get("netAsset", 0.0) or 0.0),
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
