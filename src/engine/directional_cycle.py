from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Callable, Deque, Optional

from src.core.models import CycleTelemetry, MarketDataMode, StrategyParams, SymbolFilters
from src.core.state_machine import BotState, BotStateMachine
from src.exchange.binance_margin import BinanceMarginExecution
from src.services.market_data import MarketDataService


TradeRowFn = Callable[[dict[str, Any]], None]


@dataclass
class CycleSnapshot:
    entry_mid: Optional[float] = None
    detect_mid: Optional[float] = None
    exit_mid: Optional[float] = None
    entry_long_price: Optional[float] = None
    entry_short_price: Optional[float] = None
    winner_side: Optional[str] = None
    loser_side: Optional[str] = None
    winner_raw_bps: Optional[float] = None
    winner_net_bps: Optional[float] = None
    loser_raw_bps: Optional[float] = None
    loser_net_bps: Optional[float] = None
    reason: Optional[str] = None
    ws_age_ms: Optional[float] = None
    http_age_ms: Optional[float] = None
    effective_source: Optional[str] = None
    effective_age_ms: Optional[float] = None
    data_stale: Optional[bool] = None
    waiting_for_data: Optional[bool] = None


class DirectionalCycle:
    def __init__(
        self,
        execution: BinanceMarginExecution,
        state_machine: BotStateMachine,
        strategy: StrategyParams,
        symbol_filters: SymbolFilters,
        market_data: MarketDataService,
        emit_log: Callable[..., None],
        emit_trade_row: TradeRowFn,
        emit_exposure: Callable[[bool], None],
        on_not_authorized: Callable[[str], None],
    ) -> None:
        self._execution = execution
        self._state_machine = state_machine
        self._strategy = strategy
        self._filters = symbol_filters
        self._emit_log = emit_log
        self._emit_trade_row = emit_trade_row
        self._emit_exposure = emit_exposure
        self._on_not_authorized = on_not_authorized

        self._market_data = market_data
        self._last_tick: Optional[dict[str, Any]] = None
        self._detect_mid_buffer: Deque[Decimal] = deque(maxlen=5)
        self._detect_window_ticks = max(5, int(self._strategy.detect_window_ticks))
        self._cycle_start: Optional[datetime] = None
        self._entry_mid: Optional[Decimal] = None
        self._entry_qty: Optional[float] = None
        self._detect_start_ts: Optional[float] = None
        self._winner_side: Optional[str] = None
        self._loser_side: Optional[str] = None
        self._detect_mid: Optional[Decimal] = None
        self._loser_exit_price: Optional[float] = None
        self._winner_exit_price: Optional[float] = None
        self._exit_mid: Optional[float] = None
        self._entry_long_price: Optional[float] = None
        self._entry_short_price: Optional[float] = None
        self._winner_raw_bps: Optional[float] = None
        self._winner_net_bps: Optional[float] = None
        self._loser_raw_bps: Optional[float] = None
        self._loser_net_bps: Optional[float] = None
        self._long_raw_bps: Optional[float] = None
        self._short_raw_bps: Optional[float] = None
        self._last_net_usd: Optional[float] = None
        self._last_cycle_net_bps: Optional[float] = None
        self._last_cycle_result: Optional[str] = None
        self._last_reason: Optional[str] = None
        self._ws_age_ms: Optional[float] = None
        self._http_age_ms: Optional[float] = None
        self._effective_source: Optional[str] = None
        self._effective_age_ms: Optional[float] = None
        self._data_stale: bool = True
        self._waiting_for_data: bool = False
        self._last_skip_log_ts = 0.0
        self._last_skip_reason: Optional[str] = None
        self._last_ride_log_ts = 0.0
        self._exposure_open = False
        self._last_data_log_source: Optional[str] = None
        self._last_data_log_ts = 0.0
        self._last_data_health_log_ts = 0.0
        self._armed_ts: Optional[float] = None
        self._last_non_impulse_skip_ts = 0.0
        self._last_impulse_block_warn_ts = 0.0
        self._last_impulse_degrade_log_ts = 0.0
        self._last_tick_rate_block_log_ts = 0.0
        self._last_tickrate_force_log_ts = 0.0
        self._last_detect_sample_log_ts = 0.0
        self._inflight_entry = False
        self._inflight_exit = False
        self._open_orders_count = 0
        self._pos_long_qty = 0.0
        self._pos_short_qty = 0.0
        self._pos_net_qty = 0.0
        self._last_action: Optional[str] = None
        self._last_position_refresh_ts = 0.0
        self._entry_pos_long_qty = 0.0
        self._entry_pos_short_qty = 0.0
        self._trades_count = 0
        self._phase_seq = 0
        self._ride_start_ts: Optional[float] = None
        self._last_transition_reason: Optional[str] = None
        self._wait_winner_start_ts: Optional[float] = None
        self._entry_actions: set[str] = set()
        self._market_data.set_detect_window_ticks(self._detect_window_ticks)

    @property
    def cycle_start(self) -> Optional[datetime]:
        return self._cycle_start

    @property
    def snapshot(self) -> CycleSnapshot:
        return CycleSnapshot(
            entry_mid=self._float_from_decimal(self._entry_mid),
            detect_mid=self._float_from_decimal(self._detect_mid),
            exit_mid=self._winner_exit_price,
            entry_long_price=self._entry_long_price,
            entry_short_price=self._entry_short_price,
            winner_side=self._winner_side,
            loser_side=self._loser_side,
            winner_raw_bps=self._winner_raw_bps,
            winner_net_bps=self._winner_net_bps,
            loser_raw_bps=self._loser_raw_bps,
            loser_net_bps=self._loser_net_bps,
            reason=self._last_reason,
            ws_age_ms=self._ws_age_ms,
            http_age_ms=self._http_age_ms,
            effective_source=self._effective_source,
            effective_age_ms=self._effective_age_ms,
            data_stale=self._data_stale,
            waiting_for_data=self._waiting_for_data,
        )

    def build_telemetry(
        self, state: BotState, active_cycle: bool, last_error: Optional[str]
    ) -> CycleTelemetry:
        metrics = self._compute_entry_metrics()
        now = datetime.now(timezone.utc)
        duration_s = None
        if self._cycle_start:
            duration_s = (now - self._cycle_start).total_seconds()
        winner_raw = self._winner_raw_bps
        loser_raw = self._loser_raw_bps
        winner_net = self._winner_net_bps
        loser_net = self._loser_net_bps
        total_raw = None
        total_net = None
        if winner_raw is not None and loser_raw is not None:
            total_raw = winner_raw + loser_raw
        if winner_net is not None and loser_net is not None:
            total_net = winner_net + loser_net
        return CycleTelemetry(
            cycle_id=self._state_machine.cycle_id,
            phase_seq=self._phase_seq,
            state=state.value,
            active_cycle=active_cycle,
            inflight_entry=self._inflight_entry,
            inflight_exit=self._inflight_exit,
            open_orders_count=self._open_orders_count,
            pos_long_qty=self._pos_long_qty,
            pos_short_qty=self._pos_short_qty,
            last_action=self._last_action,
            started_at=self._cycle_start,
            duration_s=duration_s,
            nominal_usd=self._strategy.usd_notional,
            fee_total_bps=self._strategy.fee_total_bps,
            target_net_bps=self._strategy.target_net_bps,
            max_loss_bps=self._strategy.max_loss_bps,
            max_spread_bps=self._strategy.max_spread_bps,
            min_tick_rate=self._strategy.min_tick_rate,
            cooldown_s=self._strategy.cooldown_s,
            detect_window_ticks=self._detect_window_ticks,
            detect_timeout_ms=self._strategy.detect_timeout_ms,
            tick_rate=metrics["tick_rate"],
            impulse_bps=metrics["impulse_bps"],
            spread_bps=metrics["spread_bps"],
            ws_age_ms=metrics["ws_age_ms"],
            effective_source=str(metrics["effective_source"]),
            last_mid=self._float_from_decimal(self._get_effective_mid_decimal()),
            entry_mid=self._float_from_decimal(self._entry_mid),
            exit_mid=self._exit_mid or self._winner_exit_price or self._loser_exit_price,
            winner_side=self._winner_side,
            loser_side=self._loser_side,
            long_raw_bps=self._long_raw_bps,
            short_raw_bps=self._short_raw_bps,
            winner_raw_bps=winner_raw,
            loser_raw_bps=loser_raw,
            winner_net_bps=winner_net,
            loser_net_bps=loser_net,
            total_raw_bps=total_raw,
            total_net_bps=total_net if total_net is not None else self._last_cycle_net_bps,
            result=self._last_cycle_result,
            pnl_usdt=self._last_net_usd,
            condition=self._condition_from_state(state),
            reason=self._last_reason,
            last_error=last_error,
        )

    def _condition_from_state(self, state: BotState) -> str:
        return {
            BotState.DETECT: "DETECT",
            BotState.ENTERED_LONG: "ENTERED_LONG",
            BotState.ENTERED_SHORT: "ENTERED_SHORT",
            BotState.WAIT_WINNER: "WAIT_WINNER",
            BotState.EXIT: "EXIT",
            BotState.FLATTEN: "FLATTEN",
            BotState.COOLDOWN: "COOLDOWN",
            BotState.ERROR: "ERROR",
            BotState.IDLE: "IDLE",
        }.get(state, state.value)

    def _set_phase(self, state: BotState, reason: str, action: Optional[str] = None) -> None:
        previous = self._state_machine.state
        if not self._state_machine.transition(state):
            self._emit_log(
                "FSM",
                "ERROR",
                "[FSM] invalid_transition",
                cycle_id=self._state_machine.cycle_id,
                from_state=previous.value,
                to_state=state.value,
                reason=reason,
            )
            return
        if previous != state:
            self._phase_seq += 1
            self._emit_log(
                "FSM",
                "INFO",
                f"[FSM] cycle_id={self._state_machine.cycle_id} FROM={previous.value} "
                f"TO={state.value} reason={reason}",
            )
        self._last_transition_reason = reason
        if action:
            self._last_action = action
        self._log_cycle_state(reason=reason, action=self._last_action)

    def _log_cycle_state(self, reason: str, action: Optional[str]) -> None:
        tick = self._market_data.get_last_effective_tick() or {}
        last_mid = tick.get("mid")
        total_raw = None
        total_net = None
        if self._winner_raw_bps is not None and self._loser_raw_bps is not None:
            total_raw = self._winner_raw_bps + self._loser_raw_bps
        if self._winner_net_bps is not None and self._loser_net_bps is not None:
            total_net = self._winner_net_bps + self._loser_net_bps
        self._emit_log(
            "CYCLE",
            "INFO",
            "[CYCLE]",
            id=self._state_machine.cycle_id,
            phase_seq=self._phase_seq,
            state=self._state_machine.state.value,
            reason=reason,
            pos_long=self._pos_long_qty,
            pos_short=self._pos_short_qty,
            open_orders=self._open_orders_count,
            last_mid=last_mid,
            raw=total_raw,
            net=total_net,
            action=action,
        )

    @property
    def exposure_open(self) -> bool:
        return self._exposure_open

    def update_strategy(self, strategy: StrategyParams) -> None:
        self._strategy = strategy
        self._detect_window_ticks = max(5, int(self._strategy.detect_window_ticks))
        self._market_data.set_detect_window_ticks(self._detect_window_ticks)
        self._detect_mid_buffer = deque(
            list(self._detect_mid_buffer)[-self._detect_window_ticks :],
            maxlen=self._detect_window_ticks,
        )

    def update_filters(self, symbol_filters: SymbolFilters) -> None:
        self._filters = symbol_filters

    def update_data_mode(self, mode: MarketDataMode) -> None:
        self._market_data.set_mode(mode)

    def update_ws_status(self, status: str) -> None:
        self._market_data.set_ws_connected(status == "CONNECTED")

    def update_tick(self, payload: dict[str, Any]) -> None:
        self._last_tick = self._market_data.get_last_effective_tick()
        snapshot = self._market_data.get_snapshot()
        self._ws_age_ms = snapshot.ws_age_ms
        self._http_age_ms = snapshot.http_age_ms
        self._effective_source = snapshot.effective_source
        self._effective_age_ms = snapshot.effective_age_ms
        self._data_stale = snapshot.data_stale
        if not self._data_stale:
            self._waiting_for_data = False
        self._maybe_log_data_source()
        self._maybe_log_data_health()
        if self._state_machine.state == BotState.WAIT_WINNER:
            if self._winner_side:
                self._evaluate_riding()
            else:
                self._evaluate_detecting()

    def attempt_entry(self) -> None:
        self._check_timeouts()
        if self._state_machine.active_cycle:
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] entry_blocked",
                reason="cycle_active",
                state=self._state_machine.state.value,
            )
            return
        if self._inflight_entry or self._inflight_exit:
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] entry_blocked",
                reason="inflight",
                inflight_entry=self._inflight_entry,
                inflight_exit=self._inflight_exit,
                state=self._state_machine.state.value,
            )
            return
        if self._state_machine.state == BotState.COOLDOWN:
            self._log_skip("cooldown")
            return
        if self._state_machine.state != BotState.DETECT:
            return
        if not self._market_data.get_last_ws_tick() and not self._market_data.get_last_http_tick():
            self._log_skip("no_tick")
            return
        if not self._filters.step_size or not self._filters.min_qty:
            self._log_skip("filters_missing")
            return
        if not self._is_flat(force=True):
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] start_blocked",
                reason="not_flat",
                state=self._state_machine.state.value,
                open_orders_count=self._open_orders_count,
                pos_long_qty=self._pos_long_qty,
                pos_short_qty=self._pos_short_qty,
            )
            self._last_action = "FLATTEN"
            self._controlled_flatten(reason="not_flat_before_start")
            return
        reason = self._entry_filter_reason()
        if reason:
            self._log_skip(reason)
            return
        self._start_cycle()

    def arm(self) -> None:
        snapshot = self._market_data.get_snapshot()
        last_ws_tick = self._market_data.get_last_ws_tick() or {}
        self._armed_ts = time.monotonic()
        self._last_non_impulse_skip_ts = self._armed_ts
        previous = self._state_machine.state
        self._emit_log(
            "INFO",
            "INFO",
            "snapshot_before_start",
            bid=snapshot.bid,
            ask=snapshot.ask,
            mid=snapshot.mid,
            ws_age_ms=snapshot.ws_age_ms,
            last_ws_tick_time=last_ws_tick.get("rx_time"),
            ws_connected=self._market_data.is_ws_connected(),
        )
        self._state_machine.arm()
        if previous != self._state_machine.state:
            self._emit_log(
                "FSM",
                "INFO",
                f"[FSM] cycle_id={self._state_machine.cycle_id} FROM={previous.value} "
                f"TO={self._state_machine.state.value} reason=arm",
            )

    def stop(self) -> None:
        previous = self._state_machine.state
        self._state_machine.stop()
        self._execution.cancel_open_orders()
        self._emergency_flatten(reason="stop")
        if previous != self._state_machine.state:
            self._emit_log(
                "FSM",
                "INFO",
                f"[FSM] cycle_id={self._state_machine.cycle_id} FROM={previous.value} "
                f"TO={self._state_machine.state.value} reason=stop",
            )

    def end_cooldown(self, auto_resume: bool) -> None:
        if not auto_resume:
            previous = self._state_machine.state
            self._state_machine.stop()
            if previous != self._state_machine.state:
                self._emit_log(
                    "FSM",
                    "INFO",
                    f"[FSM] cycle_id={self._state_machine.cycle_id} FROM={previous.value} "
                    f"TO={self._state_machine.state.value} reason=cooldown_stop",
                )
            return
        if not self._is_flat(force=True):
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] cooldown_blocked",
                reason="not_flat",
                open_orders_count=self._open_orders_count,
                pos_long_qty=self._pos_long_qty,
                pos_short_qty=self._pos_short_qty,
            )
            self._last_action = "FLATTEN"
            self._controlled_flatten(reason="cooldown_not_flat")
            return
        previous = self._state_machine.state
        self._state_machine.end_cooldown()
        if previous != self._state_machine.state:
            self._emit_log(
                "FSM",
                "INFO",
                f"[FSM] cycle_id={self._state_machine.cycle_id} FROM={previous.value} "
                f"TO={self._state_machine.state.value} reason=cooldown_end",
            )

    def emergency_flatten(self, reason: str) -> None:
        self._emergency_flatten(reason=reason)

    def emergency_test(self) -> None:
        self._execution.cancel_open_orders()
        if self._abort_if_not_authorized("emergency_test_cancel"):
            return
        account = self._execution.get_margin_account()
        if self._abort_if_not_authorized("emergency_test_account"):
            return
        self._emit_log(
            "INFO",
            "INFO",
            "[EMERGENCY_TEST] ok",
            account_loaded=bool(account),
        )

    def _start_cycle(self) -> None:
        if not self._state_machine.start_cycle():
            return
        self._cycle_start = datetime.now(timezone.utc)
        self._waiting_for_data = False
        self._inflight_entry = True
        self._inflight_exit = False
        self._phase_seq = 0
        self._ride_start_ts = None
        self._entry_mid = None
        self._entry_qty = None
        self._detect_start_ts = None
        self._winner_side = None
        self._loser_side = None
        self._detect_mid = None
        self._loser_exit_price = None
        self._winner_exit_price = None
        self._exit_mid = None
        self._entry_long_price = None
        self._entry_short_price = None
        self._winner_raw_bps = None
        self._winner_net_bps = None
        self._loser_raw_bps = None
        self._loser_net_bps = None
        self._long_raw_bps = None
        self._short_raw_bps = None
        self._last_reason = None
        self._last_net_usd = None
        self._last_cycle_net_bps = None
        self._last_cycle_result = None
        self._last_detect_sample_log_ts = 0.0
        self._last_action = None
        self._trades_count = 0
        self._entry_actions = set()
        self._wait_winner_start_ts = None
        self._reset_detect_buffer()
        self._emit_log(
            "CYCLE",
            "INFO",
            "[CYCLE] START",
            n=self._state_machine.cycle_id,
            side="HEDGE",
            symbol=self._execution.symbol,
        )
        self._emit_log(
            "ENTRY",
            "INFO",
            "[ENTRY] start",
            cycle_id=self._state_machine.cycle_id,
            symbol=self._execution.symbol,
        )
        self._emit_log("TRADE", "INFO", "ENTER cycle", cycle_id=self._state_machine.cycle_id)
        self._emit_log("STATE", "INFO", "ENTER_START", cycle_id=self._state_machine.cycle_id)
        if not self._enter_hedge():
            self._inflight_entry = False
            return
        self._inflight_entry = False
        self._set_phase(BotState.WAIT_WINNER, reason="enter_filled", action="ENTER_FILLED")
        self._detect_start_ts = time.monotonic()
        self._wait_winner_start_ts = self._detect_start_ts
        self._reset_detect_buffer()
        self._emit_log(
            "INFO",
            "INFO",
            "[DETECT] start",
            entry_mid=self._format_decimal(self._entry_mid),
            thr=self._strategy.winner_threshold_bps,
            win_ticks=self._detect_window_ticks,
        )
        self._emit_log("STATE", "INFO", "DETECT_START", cycle_id=self._state_machine.cycle_id)

    def _enter_hedge(self) -> bool:
        if self._inflight_entry is False:
            self._inflight_entry = True
        if self._state_machine.state not in {
            BotState.DETECT,
            BotState.ENTERED_LONG,
            BotState.ENTERED_SHORT,
        }:
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] enter_not_allowed",
                state=self._state_machine.state.value,
            )
            return False
        tick = self._market_data.get_last_ws_tick() or {}
        mid = float(tick.get("mid", 0.0))
        bid = float(tick.get("bid", 0.0))
        ask = float(tick.get("ask", 0.0))
        if mid <= 0 or bid <= 0 or ask <= 0:
            self._set_error("bad_mid")
            return False
        qty = self._strategy.usd_notional / mid
        qty = self._round_step(qty)
        if qty <= 0:
            self._set_error("qty_zero")
            return False
        if self._filters.min_qty and qty < self._filters.min_qty:
            self._set_error("min_qty")
            return False
        if self._filters.min_notional and qty * mid < self._filters.min_notional:
            self._set_error("min_notional")
            return False
        metrics = self._compute_entry_metrics()
        self._emit_log(
            "TRADE",
            "INFO",
            "[ENTRY] OK",
            data=metrics["effective_source"],
            spread_bps=f"{metrics['spread_bps']:.2f}",
            tick_rate=metrics["tick_rate"],
            impulse_bps=f"{metrics['impulse_bps']:.4f}",
            qty=qty,
            mid=f"{metrics['mid']:.2f}",
        )
        self._entry_qty = qty
        try:
            sell_mode, sell_price = self._order_params("SELL", bid, ask)
            buy_mode, buy_price = self._order_params("BUY", bid, ask)
            sell_client_id = self._build_client_order_id("SHORT", "ENTRY")
            buy_client_id = self._build_client_order_id("LONG", "ENTRY")
            sell_order = self._place_order(
                "SELL",
                qty,
                sell_mode,
                price=sell_price,
                side_effect_type="AUTO_BORROW_REPAY",
                client_order_id=sell_client_id,
                order_kind="OPEN_SHORT",
            )
            if not sell_order:
                self._log_partial("partial_hedge_entry", qty, None, sell_order)
                return False
            buy_order = self._place_order(
                "BUY",
                qty,
                buy_mode,
                price=buy_price,
                client_order_id=buy_client_id,
                order_kind="OPEN_LONG",
            )
        except Exception as exc:
            self._emit_log(
                "ERROR",
                "ERROR",
                "ENTER order exception",
                error=str(exc),
            )
            self._timeout_to_cooldown("enter_exception")
            return False
        buy_id = buy_order.get("orderId") if buy_order else None
        sell_id = sell_order.get("orderId") if sell_order else None
        self._emit_log(
            "ENTRY",
            "INFO",
            "[ENTRY] sent",
            cycle_id=self._state_machine.cycle_id,
            buy_client_id=buy_client_id,
            sell_client_id=sell_client_id,
            buy_id=buy_id,
            sell_id=sell_id,
            qty=qty,
        )
        self._last_action = "ENTER_SENT"
        self._emit_log(
            "TRADE",
            "INFO",
            "ENTER sent",
            buy_id=buy_id,
            sell_id=sell_id,
            qty=qty,
            type=self._strategy.order_mode,
        )
        if not buy_order or not sell_order:
            self._log_partial("partial_hedge_entry", qty, buy_order, sell_order)
            return False
        buy_fill = self._wait_for_fill_with_fallback(
            "BUY",
            buy_id,
            qty,
            buy_mode,
            buy_price,
            client_order_id=buy_client_id,
            timeout_ms=self._strategy.wait_fill_timeout_ms,
        )
        sell_fill = self._wait_for_fill_with_fallback(
            "SELL",
            sell_id,
            qty,
            sell_mode,
            sell_price,
            side_effect_type="AUTO_BORROW_REPAY",
            client_order_id=sell_client_id,
            timeout_ms=self._strategy.wait_fill_timeout_ms,
        )
        if not buy_fill or not sell_fill:
            buy_status = self._execution.get_order(int(buy_id)) if buy_id else None
            sell_status = self._execution.get_order(int(sell_id)) if sell_id else None
            self._log_partial("partial_hedge_entry", qty, buy_status, sell_status)
            return False
        self._set_phase(BotState.ENTERED_LONG, reason="entry_long_filled", action="ENTER_LONG")
        self._entry_long_price = buy_fill[0]
        self._set_phase(BotState.ENTERED_SHORT, reason="entry_short_filled", action="ENTER_SHORT")
        self._entry_short_price = sell_fill[0]
        effective_mid = self._get_effective_mid_decimal()
        if effective_mid is None:
            effective_mid = Decimal(str((self._entry_long_price + self._entry_short_price) / 2))
        self._entry_mid = effective_mid
        self._log_deal(buy_fill[2], "ENTER_LONG")
        self._last_action = "ENTRY_LONG"
        self._log_deal(sell_fill[2], "ENTER_SHORT")
        self._last_action = "ENTRY_SHORT"
        self._exposure_open = True
        self._emit_exposure(True)
        self._refresh_position_cache(force=True)
        if not self._wait_for_positions("entry_positions", self._strategy.wait_positions_timeout_ms):
            self._timeout_to_cooldown("entry_positions_timeout")
            return False
        self._entry_pos_long_qty = self._pos_long_qty
        self._entry_pos_short_qty = self._pos_short_qty
        self._emit_log(
            "POS",
            "INFO",
            "[POS] entry_snapshot",
            cycle_id=self._state_machine.cycle_id,
            pos_long_qty=self._pos_long_qty,
            pos_short_qty=self._pos_short_qty,
            open_orders_count=self._open_orders_count,
        )
        self._emit_log(
            "TRADE",
            "INFO",
            "ENTER filled",
            buy_price=self._entry_long_price,
            sell_price=self._entry_short_price,
            qty=qty,
        )
        self._emit_log(
            "ENTRY",
            "INFO",
            "[ENTRY] filled",
            cycle_id=self._state_machine.cycle_id,
            buy_price=self._entry_long_price,
            sell_price=self._entry_short_price,
            qty=qty,
        )
        self._emit_log("STATE", "INFO", "ENTER_RESULT", cycle_id=self._state_machine.cycle_id)
        return True

    def _evaluate_detecting(self) -> None:
        if not self._entry_mid or not self._detect_start_ts:
            return
        elapsed = (time.monotonic() - self._detect_start_ts) * 1000
        min_tick_rate = max(1, int(self._strategy.min_tick_rate))
        min_timeout_ms = (self._detect_window_ticks / min_tick_rate) * 1000 + 500
        detect_timeout_ms = max(float(self._strategy.detect_timeout_ms), min_timeout_ms)
        tick = self._market_data.get_last_effective_tick() or {}
        mid_now = self._mid_from_tick_decimal(tick)
        if mid_now is None:
            return
        self._append_detect_mid(mid_now)
        long_raw = self._calc_raw_bps("LONG", self._entry_mid, mid_now)
        short_raw = self._calc_raw_bps("SHORT", self._entry_mid, mid_now)
        self._long_raw_bps = long_raw
        self._short_raw_bps = short_raw
        winner_threshold = self._strategy.winner_threshold_bps
        best = max(long_raw, short_raw)
        if self._wait_winner_start_ts:
            wait_elapsed_ms = (time.monotonic() - self._wait_winner_start_ts) * 1000
            if wait_elapsed_ms >= self._strategy.max_wait_ms:
                if abs(best) < self._strategy.raw_bps_min_exit:
                    self._force_flatten("wait_winner_timeout", result="TIMEOUT")
                    return
        if elapsed > detect_timeout_ms and len(self._detect_mid_buffer) >= self._detect_window_ticks:
            self._handle_detect_timeout()
            return
        delta_mid_bps = None
        if len(self._detect_mid_buffer) >= self._detect_window_ticks:
            mid_first = self._detect_mid_buffer[0]
            mid_last = self._detect_mid_buffer[-1]
            if self._entry_mid and self._entry_mid > 0:
                delta_mid_bps = float(
                    (mid_last - mid_first) / self._entry_mid * Decimal("10000")
                )
        now = time.monotonic()
        if now - self._last_detect_sample_log_ts > 0.3:
            mid_disp = self._format_decimal(mid_now)
            self._emit_log(
                "INFO",
                "INFO",
                "[DETECT] sample",
                mid_raw=f"{float(mid_now):.6f}",
                mid_disp=mid_disp,
                long_raw=f"{long_raw:.4f}",
                short_raw=f"{short_raw:.4f}",
                delta_window_bps=f"{delta_mid_bps:.4f}" if delta_mid_bps is not None else None,
            )
            self._last_detect_sample_log_ts = now
        if len(self._detect_mid_buffer) < self._detect_window_ticks:
            if self._last_reason != "wait_window":
                self._last_reason = "wait_window"
            return
        if best < winner_threshold:
            return
        if long_raw >= short_raw:
            self._winner_side = "LONG"
            self._loser_side = "SHORT"
        else:
            self._winner_side = "SHORT"
            self._loser_side = "LONG"
        self._detect_mid = mid_now
        self._winner_raw_bps = best
        self._winner_net_bps = self._winner_raw_bps - self._strategy.fee_total_bps
        self._emit_log(
            "TRADE",
            "INFO",
            "DETECT winner",
            winner=self._winner_side,
            loser=self._loser_side,
            detect_mid=self._format_decimal(mid_now),
        )
        self._emit_log(
            "INFO",
            "INFO",
            "[DETECT] winner",
            side=self._winner_side,
            best_bps=f"{best:.4f}",
        )
        self._emit_log(
            "STATE",
            "INFO",
            "DETECT_WINNER",
            cycle_id=self._state_machine.cycle_id,
            winner=self._winner_side,
        )
        self._set_phase(BotState.WAIT_WINNER, reason="detect_winner", action="CUT_SENT")
        self._cut_loser()

    def _cut_loser(self) -> None:
        if not self._entry_qty or not self._loser_side:
            self._set_error("missing_loser")
            return
        try:
            self._refresh_position_cache(force=True)
            qty = (
                self._pos_short_qty
                if self._loser_side == "SHORT"
                else self._pos_long_qty
            )
            qty = self._normalize_qty(qty)
            if qty <= 0:
                self._timeout_to_cooldown("cut_qty_zero")
                return
            side = "BUY" if self._loser_side == "SHORT" else "SELL"
            side_effect = "AUTO_REPAY" if self._loser_side == "SHORT" else None
            order_mode, price = self._order_params(side)
            client_order_id = self._build_client_order_id("LOSE", "EXIT")
            order = self._place_order(
                side,
                qty,
                order_mode,
                price=price,
                side_effect_type=side_effect,
                client_order_id=client_order_id,
                order_kind="CLOSE_SHORT" if self._loser_side == "SHORT" else "CLOSE_LONG",
            )
            if not order:
                self._timeout_to_cooldown("cut_loser_failed")
                return
            fill = self._wait_for_fill_with_fallback(
                side,
                order.get("orderId"),
                qty,
                order_mode,
                price,
                side_effect_type=side_effect,
                client_order_id=client_order_id,
                timeout_ms=self._strategy.wait_fill_timeout_ms,
            )
            if not fill:
                self._timeout_to_cooldown("cut_loser_timeout")
                return
        except Exception as exc:
            self._emit_log(
                "ERROR",
                "ERROR",
                "CUT order exception",
                error=str(exc),
            )
            self._timeout_to_cooldown("cut_loser_exception")
            return
        self._loser_exit_price = fill[0]
        self._loser_raw_bps = self._calc_raw_bps(
            self._loser_side, self._entry_mid or Decimal("0"), self._loser_exit_price
        )
        self._loser_net_bps = (
            self._loser_raw_bps - self._strategy.fee_total_bps
            if self._loser_raw_bps is not None
            else None
        )
        self._log_deal(fill[2], "CUT_LOSER")
        self._last_action = "CUT_LOSER"
        self._refresh_position_cache(force=True)
        step = self._filters.step_size or 0.0
        reduced = True
        if self._loser_side == "SHORT":
            reduced = self._pos_short_qty <= max(self._entry_pos_short_qty - step, 0.0)
        elif self._loser_side == "LONG":
            reduced = self._pos_long_qty <= max(self._entry_pos_long_qty - step, 0.0)
        if not reduced:
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] cut_loser_not_reduced",
                loser_side=self._loser_side,
                pos_long_qty=self._pos_long_qty,
                pos_short_qty=self._pos_short_qty,
            )
            self._emergency_flatten(reason="cut_loser_not_reduced")
            self._set_error("cut_loser_not_reduced")
            return
        step = self._filters.step_size or 0.0
        if self._pos_long_qty > step and self._pos_short_qty > step:
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] cut_loser_multiple_positions",
                pos_long_qty=self._pos_long_qty,
                pos_short_qty=self._pos_short_qty,
            )
            self._timeout_to_cooldown("cut_loser_multi_pos")
            return
        self._emit_log(
            "TRADE",
            "INFO",
            "CUT loser filled",
            loser_side=self._loser_side,
            exit_price=self._loser_exit_price,
        )
        self._emit_log(
            "EXIT",
            "INFO",
            "[EXIT] loser_closed",
            cycle_id=self._state_machine.cycle_id,
            loser_side=self._loser_side,
            exit_price=self._loser_exit_price,
            client_order_id=client_order_id,
        )
        self._emit_log("STATE", "INFO", "CUT_DONE", cycle_id=self._state_machine.cycle_id)
        self._set_phase(BotState.WAIT_WINNER, reason="cut_done", action="CUT_FILLED")
        self._ride_start_ts = time.monotonic()
        self._evaluate_riding()

    def _evaluate_riding(self) -> None:
        if self._state_machine.state != BotState.WAIT_WINNER:
            return
        if not self._last_tick or not self._entry_mid or not self._winner_side:
            return
        snapshot = self._market_data.get_snapshot()
        effective_age_ms = snapshot.effective_age_ms
        tick = self._market_data.get_last_effective_tick() or self._last_tick
        mid_now = self._mid_from_tick_decimal(tick or {})
        if mid_now is None:
            return
        winner_raw_bps = self._calc_raw_bps(self._winner_side, self._entry_mid, mid_now)
        winner_net_bps = winner_raw_bps - self._strategy.fee_total_bps
        self._winner_raw_bps = winner_raw_bps
        self._winner_net_bps = winner_net_bps
        total_net_bps = winner_net_bps
        if self._loser_net_bps is not None:
            total_net_bps += self._loser_net_bps
        if self._wait_winner_start_ts:
            wait_elapsed_ms = (time.monotonic() - self._wait_winner_start_ts) * 1000
            if wait_elapsed_ms >= self._strategy.max_wait_ms:
                if abs(winner_raw_bps) < self._strategy.raw_bps_min_exit:
                    self._force_flatten("wait_winner_timeout", result="TIMEOUT")
                    return
        now = time.monotonic()
        if now - self._last_ride_log_ts > 0.5:
            self._emit_log(
                "TRADE",
                "INFO",
                "RIDE update",
                winner=self._winner_side,
                raw_bps=f"{winner_raw_bps:.2f}",
                net_bps=f"{total_net_bps:.2f}",
            )
            self._emit_log(
                "STATE",
                "INFO",
                "RIDE_UPDATE",
                cycle_id=self._state_machine.cycle_id,
                raw_bps=f"{winner_raw_bps:.2f}",
                net_bps=f"{total_net_bps:.2f}",
            )
            self._last_ride_log_ts = now
        if snapshot.data_stale or effective_age_ms > self._strategy.data_stale_exit_ms:
            self._timeout_to_cooldown("data_stale")
            return
        if self._ride_start_ts:
            ride_elapsed_ms = (now - self._ride_start_ts) * 1000
            if ride_elapsed_ms >= self._strategy.max_ride_ms:
                self._exit_winner("max_ride_timeout")
                return
        if winner_raw_bps <= -self._strategy.emergency_stop_bps:
            self._exit_winner("emergency_stop")
            return
        if total_net_bps >= self._strategy.target_net_bps:
            self._exit_winner("target")
            return
        if total_net_bps <= -self._strategy.max_loss_bps:
            self._exit_winner("stop_loss")

    def _exit_winner(self, note: str) -> None:
        if not self._entry_qty or not self._winner_side:
            return
        self._set_phase(BotState.EXIT, reason=note, action="EXIT_SENT")
        self._last_reason = note
        self._inflight_exit = True
        try:
            self._refresh_position_cache(force=True)
            qty = (
                self._pos_long_qty
                if self._winner_side == "LONG"
                else self._pos_short_qty
            )
            qty = self._normalize_qty(qty)
            if qty <= 0:
                self._timeout_to_cooldown("exit_qty_zero")
                return
            side = "SELL" if self._winner_side == "LONG" else "BUY"
            side_effect = "AUTO_REPAY" if self._winner_side == "SHORT" else None
            order_mode, price = self._order_params(side)
            client_order_id = self._build_client_order_id("WIN", "EXIT")
            order = self._place_order(
                side,
                qty,
                order_mode,
                price=price,
                side_effect_type=side_effect,
                client_order_id=client_order_id,
                order_kind="CLOSE_LONG" if self._winner_side == "LONG" else "CLOSE_SHORT",
            )
            if not order:
                self._timeout_to_cooldown("exit_winner_failed")
                return
            fill = self._wait_for_fill_with_fallback(
                side,
                order.get("orderId"),
                qty,
                order_mode,
                price,
                side_effect_type=side_effect,
                client_order_id=client_order_id,
                timeout_ms=self._strategy.wait_exit_timeout_ms,
            )
            if not fill:
                self._timeout_to_cooldown("exit_winner_timeout")
                return
        except Exception as exc:
            self._emit_log(
                "ERROR",
                "ERROR",
                "EXIT order exception",
                error=str(exc),
            )
            self._timeout_to_cooldown("exit_winner_exception")
            return
        self._winner_exit_price = fill[0]
        self._exit_mid = self._winner_exit_price
        self._winner_raw_bps = self._calc_raw_bps(
            self._winner_side, self._entry_mid or Decimal("0"), self._winner_exit_price
        )
        self._winner_net_bps = (
            self._winner_raw_bps - self._strategy.fee_total_bps
            if self._winner_raw_bps is not None
            else None
        )
        self._log_deal(fill[2], "EXIT_WINNER")
        self._last_action = "EXIT_WINNER"
        self._emit_log(
            "TRADE",
            "INFO",
            "EXIT winner filled",
            winner_side=self._winner_side,
            exit_price=self._winner_exit_price,
            note=note,
        )
        self._emit_log(
            "EXIT",
            "INFO",
            "[EXIT] winner_closed",
            cycle_id=self._state_machine.cycle_id,
            winner_side=self._winner_side,
            exit_price=self._winner_exit_price,
            note=note,
            client_order_id=client_order_id,
        )
        self._emit_log("STATE", "INFO", "EXIT_DONE", cycle_id=self._state_machine.cycle_id)
        self._emit_trade_summary(note=note)
        self._exposure_open = False
        self._emit_exposure(False)
        self._settle_borrow()
        if not self._wait_for_positions("exit_flat", self._strategy.wait_positions_timeout_ms):
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] exit_not_flat",
                open_orders_count=self._open_orders_count,
                pos_long_qty=self._pos_long_qty,
                pos_short_qty=self._pos_short_qty,
            )
            if not self._cleanup_close_winner():
                self._force_flatten("exit_not_flat", result="TIMEOUT")
                return
        self._finish_cycle("OK", reason=note)

    def _timeout_to_cooldown(self, reason: str) -> None:
        self._emit_log(
            "ERROR",
            "ERROR",
            "[TIMEOUT] fallback",
            cycle_id=self._state_machine.cycle_id,
            reason=reason,
        )
        self._force_flatten(reason, result="TIMEOUT")

    def _force_flatten(self, reason: str, result: str) -> None:
        self._last_reason = reason
        self._last_action = "FLATTEN_SENT"
        self._set_phase(BotState.FLATTEN, reason=reason, action="FLATTEN_SENT")
        self._execution.cancel_open_orders()
        if not self._abort_if_not_authorized("force_flatten_cancel_orders"):
            success = self._controlled_flatten(reason=reason)
            if not success:
                self._emergency_flatten(reason=reason)
        self._finish_cycle(result, reason=reason)

    def _finish_cycle(self, result: str, reason: Optional[str] = None) -> None:
        self._execution.cancel_open_orders()
        self._inflight_entry = False
        self._inflight_exit = False
        self._set_phase(BotState.COOLDOWN, reason="cycle_done")
        self._emit_log("STATE", "INFO", "CYCLE_DONE", cycle_id=self._state_machine.cycle_id)
        self._log_cycle_end(result, reason=reason)
        if result != "OK":
            reason_code = self._flatten_reason_code(reason or "")
            self._emit_log(
                "FSM",
                "WARN",
                f"[FSM] cycle_id={self._state_machine.cycle_id} result=FLATTEN "
                f"reason={reason_code}",
            )
        self._reset_cycle_state()

    def _flatten_reason_code(self, reason: str) -> str:
        reason_lower = reason.lower()
        if "timeout" in reason_lower:
            return "TIMEOUT"
        if "edge" in reason_lower or "raw_bps" in reason_lower or "noise" in reason_lower:
            return "NO_EDGE"
        if "loss" in reason_lower or "stop" in reason_lower:
            return "LOSS"
        return "NO_EDGE"

    def _reset_cycle_state(self) -> None:
        self._execution.cancel_open_orders()
        self._winner_side = None
        self._loser_side = None
        self._winner_raw_bps = None
        self._winner_net_bps = None
        self._loser_raw_bps = None
        self._loser_net_bps = None
        self._long_raw_bps = None
        self._short_raw_bps = None
        self._last_cycle_net_bps = None
        self._last_net_usd = None
        self._entry_actions = set()
        self._entry_mid = None
        self._entry_qty = None
        self._detect_mid = None
        self._exit_mid = None
        self._detect_start_ts = None
        self._ride_start_ts = None
        self._wait_winner_start_ts = None

    def _wait_for_positions(self, label: str, timeout_ms: float) -> bool:
        start = time.monotonic()
        while (time.monotonic() - start) * 1000 < timeout_ms:
            self._refresh_position_cache(force=True)
            if self._open_orders_count == 0 and abs(self._pos_net_qty) <= (
                self._filters.step_size or 0.0
            ):
                return True
            time.sleep(0.1)
        self._emit_log(
            "WARN",
            "WARN",
            "[WAIT] positions_timeout",
            cycle_id=self._state_machine.cycle_id,
            label=label,
            open_orders_count=self._open_orders_count,
            pos_long_qty=self._pos_long_qty,
            pos_short_qty=self._pos_short_qty,
        )
        return False

    def _cleanup_close_winner(self) -> bool:
        if not self._winner_side:
            return False
        self._refresh_position_cache(force=True)
        qty = (
            self._pos_long_qty
            if self._winner_side == "LONG"
            else self._pos_short_qty
        )
        qty = self._normalize_qty(qty)
        if qty <= 0:
            return True
        side = "SELL" if self._winner_side == "LONG" else "BUY"
        side_effect = "AUTO_REPAY" if self._winner_side == "SHORT" else None
        order = self._place_order(
            side,
            qty,
            "market",
            side_effect_type=side_effect,
            client_order_id=self._build_client_order_id("WIN", "CLEANUP"),
            order_kind="CLOSE_LONG" if self._winner_side == "LONG" else "CLOSE_SHORT",
        )
        if not order:
            return False
        fill = self._wait_for_fill_with_fallback(
            side,
            order.get("orderId"),
            qty,
            "market",
            None,
            side_effect_type=side_effect,
            client_order_id=order.get("clientOrderId"),
            timeout_ms=self._strategy.wait_exit_timeout_ms,
        )
        return bool(fill)

    def _no_winner_exit(self, reason: str, allow_flatten: bool, no_loss: bool) -> None:
        self._emit_log("TRADE", "INFO", "DETECT no winner", reason=reason)
        self._emit_log("STATE", "INFO", "DETECT_NO_WINNER", cycle_id=self._state_machine.cycle_id)
        result = "NO_WINNER"
        success = False
        if allow_flatten and no_loss:
            self._inflight_exit = True
            self._last_action = "FLATTEN"
            self._emit_log("INFO", "INFO", "[DETECT] timeout -> no_loss_flatten")
            success = self._close_no_winner_positions(reason=reason)
            result = "NO_WINNER_NO_LOSS" if success else "ERROR"
            if not success:
                self._emergency_flatten(reason="no_winner_no_loss_failed")
        elif allow_flatten:
            self._inflight_exit = True
            self._last_action = "FLATTEN"
            self._emit_log("INFO", "INFO", "[DETECT] timeout -> controlled_flatten")
            success = self._controlled_flatten(reason=reason)
            result = "NO_WINNER_ALLOWED" if success else "ERROR"
            if not success:
                self._emergency_flatten(reason="no_winner_flatten_failed")
        else:
            self._emit_log(
                "INFO",
                "INFO",
                "[DETECT] timeout -> no_flatten_allowed",
                policy=self._strategy.no_winner_policy,
            )
        self._last_reason = reason
        self._inflight_exit = False
        self._finish_cycle(result, reason=reason)

    def _log_partial(
        self,
        reason: str,
        qty: float,
        buy_order: Optional[dict[str, Any]],
        sell_order: Optional[dict[str, Any]],
    ) -> None:
        self._inflight_entry = False
        self._emit_log(
            "ERROR",
            "ERROR",
            "[ENTER] partial hedge -> emergency",
            reason=reason,
            qty=qty,
            buy_status=buy_order.get("status") if buy_order else None,
            buy_exec=buy_order.get("executedQty") if buy_order else None,
            buy_order_id=buy_order.get("orderId") if buy_order else None,
            sell_status=sell_order.get("status") if sell_order else None,
            sell_exec=sell_order.get("executedQty") if sell_order else None,
            sell_order_id=sell_order.get("orderId") if sell_order else None,
            error=self._execution.last_error,
            error_code=self._execution.last_error_code,
        )
        self._last_reason = reason
        self._emergency_flatten(reason=reason)
        self._state_machine.set_error(reason)
        self._finish_cycle("ENTER_FAILED", reason=reason)

    def _log_deal(self, order: dict[str, Any], tag: str) -> None:
        if not order:
            return
        self._trades_count += 1
        self._emit_log(
            "DEALS",
            "INFO",
            tag,
            order_id=order.get("orderId"),
            side=order.get("side"),
            qty=order.get("executedQty"),
            avg_price=order.get("avgPrice") or order.get("price"),
            status=order.get("status"),
        )

    def _emit_trade_summary(self, note: str) -> None:
        if not self._cycle_start or not self._entry_qty or not self._entry_mid:
            return
        now = datetime.now(timezone.utc)
        duration_ms = int((now - self._cycle_start).total_seconds() * 1000)
        winner_side = self._winner_side or "—"
        loser_side = self._loser_side or "—"
        loser_exit = self._loser_exit_price or 0.0
        winner_exit = self._winner_exit_price or 0.0
        winner_raw = self._calc_raw_bps(winner_side, self._entry_mid, winner_exit)
        loser_raw = self._calc_raw_bps(loser_side, self._entry_mid, loser_exit)
        winner_net = winner_raw - self._strategy.fee_total_bps
        loser_net = loser_raw - self._strategy.fee_total_bps
        net_total = winner_net + loser_net
        net_usd = (self._strategy.usd_notional / 10_000) * net_total
        self._last_net_usd = net_usd
        self._last_cycle_net_bps = net_total
        entry_mid = float(self._entry_mid)
        self._emit_trade_row(
            {
                "ts": now,
                "cycle_id": self._state_machine.cycle_id,
                "phase": "cycle_summary",
                "side": f"{winner_side}/{loser_side}",
                "qty": self._entry_qty,
                "entry_price": entry_mid,
                "exit_price": winner_exit or loser_exit,
                "raw_bps": winner_raw + loser_raw,
                "net_bps": net_total,
                "net_usd": net_usd,
                "duration_ms": duration_ms,
                "note": note,
            }
        )
        self._emit_log(
            "DEALS",
            "INFO",
            "Cycle summary",
            cycle_id=self._state_machine.cycle_id,
            winner=winner_side,
            loser=loser_side,
            raw_bps=winner_raw + loser_raw,
            net_bps=net_total,
            net_usd=net_usd,
            note=note,
        )

    def _no_winner_order_price(self, side: str) -> Optional[float]:
        tick = self._last_tick or self._market_data.get_last_effective_tick() or {}
        bid = float(tick.get("bid", 0.0))
        ask = float(tick.get("ask", 0.0))
        tick_size = self._filters.tick_size
        if bid <= 0 or ask <= 0 or tick_size <= 0:
            return None
        if side == "BUY":
            raw_price = (ask + tick_size) * (1 + self._strategy.slip_bps / 10_000)
            return self._normalize_price(side, raw_price)
        price = bid - tick_size
        if price <= 0:
            return None
        raw_price = price * (1 - self._strategy.slip_bps / 10_000)
        return self._normalize_price(side, raw_price)

    def _place_ioc_aggressive_limit(
        self,
        side: str,
        qty: float,
        price: float,
        side_effect_type: Optional[str],
        client_order_id: Optional[str],
        order_kind: Optional[str],
    ) -> Optional[dict[str, Any]]:
        if order_kind in {"OPEN_LONG", "OPEN_SHORT"} and self._state_machine.state not in {
            BotState.DETECT,
            BotState.ENTERED_LONG,
            BotState.ENTERED_SHORT,
        }:
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] open_order_blocked",
                cycle_id=self._state_machine.cycle_id,
                state=self._state_machine.state.value,
                kind=order_kind,
                side=side,
            )
            return None
        if order_kind in {"OPEN_LONG", "OPEN_SHORT"} and side in self._entry_actions:
            self._emit_log(
                "FSM",
                "ERROR",
                f"[FSM] cycle_id={self._state_machine.cycle_id} duplicate_entry "
                f"side={side} blocked",
            )
            return None
        order = self._execution.place_order(
            side,
            qty,
            "aggressive_limit",
            price=price,
            side_effect_type=side_effect_type,
            time_in_force="IOC",
            client_order_id=client_order_id,
        )
        self._emit_log(
            "ORDER",
            "INFO",
            "[ORDER] sent",
            cycle_id=self._state_machine.cycle_id,
            kind=order_kind,
            side=side,
            qty=qty,
            order_mode="aggressive_limit",
            price=price,
            tif="IOC",
            reduceOnly=False,
            sideEffectType=side_effect_type,
            client_order_id=client_order_id,
        )
        self._abort_if_not_authorized("place_ioc_order")
        if order_kind in {"OPEN_LONG", "OPEN_SHORT"} and order:
            self._entry_actions.add(side)
        return order

    def _close_no_winner_positions(self, reason: str) -> bool:
        if not self._entry_qty or not self._entry_mid:
            self._set_error("no_winner_missing_entry")
            return False
        self._set_phase(BotState.FLATTEN, reason=reason, action="FLATTEN_SENT")
        order_mode = self._strategy.order_mode
        buy_side_effect = "AUTO_REPAY"
        sell_side_effect = "NO_SIDE_EFFECT"
        buy_price = None
        sell_price = None
        buy_client_id = self._build_client_order_id("LONG", "FLATTEN")
        sell_client_id = self._build_client_order_id("SHORT", "FLATTEN")
        if order_mode == "aggressive_limit":
            buy_price = self._no_winner_order_price("BUY")
            sell_price = self._no_winner_order_price("SELL")
            if buy_price is None or sell_price is None:
                self._emit_log(
                    "INFO",
                    "INFO",
                    "[DETECT] no_loss fallback to market",
                    reason="invalid_limit_price",
                )
                order_mode = "market"
        if order_mode == "aggressive_limit":
            buy_order = self._place_ioc_aggressive_limit(
                "BUY",
                self._entry_qty,
                buy_price or 0.0,
                buy_side_effect,
                buy_client_id,
                order_kind="CLOSE_SHORT",
            )
            sell_order = self._place_ioc_aggressive_limit(
                "SELL",
                self._entry_qty,
                sell_price or 0.0,
                sell_side_effect,
                sell_client_id,
                order_kind="CLOSE_LONG",
            )
        else:
            buy_order = self._place_order(
                "BUY",
                self._entry_qty,
                "market",
                side_effect_type=buy_side_effect,
                client_order_id=buy_client_id,
                order_kind="CLOSE_SHORT",
            )
            sell_order = self._place_order(
                "SELL",
                self._entry_qty,
                "market",
                side_effect_type=sell_side_effect,
                client_order_id=sell_client_id,
                order_kind="CLOSE_LONG",
            )
        if self._abort_if_not_authorized("no_winner_place_orders"):
            return False
        buy_fill = self._wait_for_fill_with_fallback(
            "BUY",
            buy_order.get("orderId") if buy_order else None,
            self._entry_qty,
            order_mode,
            buy_price,
            side_effect_type=buy_side_effect,
            client_order_id=buy_client_id,
            timeout_ms=self._strategy.wait_fill_timeout_ms,
        )
        sell_fill = self._wait_for_fill_with_fallback(
            "SELL",
            sell_order.get("orderId") if sell_order else None,
            self._entry_qty,
            order_mode,
            sell_price,
            side_effect_type=sell_side_effect,
            client_order_id=sell_client_id,
            timeout_ms=self._strategy.wait_fill_timeout_ms,
        )
        if not buy_fill or not sell_fill:
            self._emit_log(
                "ERROR",
                "ERROR",
                "[DETECT] no_loss fill failed",
                buy_filled=bool(buy_fill),
                sell_filled=bool(sell_fill),
            )
            return False
        buy_exit = buy_fill[0]
        sell_exit = sell_fill[0]
        self._emit_log(
            "EXIT",
            "INFO",
            "[EXIT] flatten_filled",
            cycle_id=self._state_machine.cycle_id,
            buy_exit=buy_exit,
            sell_exit=sell_exit,
            reason=reason,
        )
        long_raw = self._calc_raw_bps("LONG", self._entry_mid, sell_exit)
        short_raw = self._calc_raw_bps("SHORT", self._entry_mid, buy_exit)
        self._long_raw_bps = long_raw
        self._short_raw_bps = short_raw
        fees_est_bps = self._strategy.fee_total_bps * 2
        net_total = (long_raw - self._strategy.fee_total_bps) + (
            short_raw - self._strategy.fee_total_bps
        )
        net_usd = (self._strategy.usd_notional / 10_000) * net_total
        self._last_cycle_net_bps = net_total
        self._last_net_usd = net_usd
        self._winner_exit_price = sell_exit
        self._loser_exit_price = buy_exit
        self._exit_mid = (sell_exit + buy_exit) / 2
        self._emit_log(
            "TRADE",
            "INFO",
            "NO_WINNER exit filled",
            reason=reason,
            long_exit=sell_exit,
            short_exit=buy_exit,
            fees_est_bps=fees_est_bps,
            net_bps=f"{net_total:.4f}",
            pnl_usdt=net_usd,
        )
        duration_ms = 0
        if self._cycle_start:
            duration_ms = int(
                (datetime.now(timezone.utc) - self._cycle_start).total_seconds() * 1000
            )
        self._emit_trade_row(
            {
                "ts": datetime.now(timezone.utc),
                "cycle_id": self._state_machine.cycle_id,
                "phase": "cycle_summary",
                "side": "NO_WINNER",
                "qty": self._entry_qty,
                "entry_price": float(self._entry_mid),
                "exit_price": (sell_exit + buy_exit) / 2,
                "raw_bps": long_raw + short_raw,
                "net_bps": net_total,
                "net_usd": net_usd,
                "duration_ms": duration_ms,
                "note": reason,
            }
        )
        self._exposure_open = False
        self._emit_exposure(False)
        self._settle_borrow()
        return True

    def _log_cycle_end(self, result: str, reason: Optional[str] = None) -> None:
        if not self._cycle_start:
            return
        self._last_cycle_result = result
        now = datetime.now(timezone.utc)
        duration_ms = int((now - self._cycle_start).total_seconds() * 1000)
        winner_side = self._winner_side or "—"
        self._emit_log(
            "CYCLE",
            "INFO",
            "[CYCLE] END",
            n=self._state_machine.cycle_id,
            result=result,
            pnl_usdt=self._last_net_usd,
            winner=winner_side,
            reason=reason or self._last_reason,
            duration_ms=duration_ms,
        )
        self._emit_log(
            "CYCLE",
            "INFO",
            "[CYCLE_SUMMARY]",
            id=self._state_machine.cycle_id,
            result=result,
            pnl_est=self._last_net_usd,
            fees_est=self._strategy.fee_total_bps,
            duration_ms=duration_ms,
            trades_count=self._trades_count,
        )
        self._reset_detect_buffer()

    def _entry_filter_reason(self) -> Optional[str]:
        metrics = self._compute_entry_metrics()
        commission_bps = float(self._strategy.fee_total_bps)
        raw_bps = float(metrics["impulse_bps"])
        raw_bps_min_enter = commission_bps * 1.5
        if commission_bps == 7:
            raw_bps_min_enter = max(raw_bps_min_enter, 11.0)
        expected_net_bps = raw_bps - commission_bps
        if abs(raw_bps) <= commission_bps:
            return "noise_zone"
        if raw_bps < raw_bps_min_enter:
            return "raw_bps_min"
        if expected_net_bps <= 0:
            return "no_edge"
        if metrics["data_stale"]:
            return "data_stale"
        if metrics["spread_bps"] > self._strategy.max_spread_bps:
            return "spread"
        if metrics["tick_rate"] < self._strategy.min_tick_rate:
            return "tick_rate"
        if self._strategy.use_impulse_filter and self._strategy.impulse_min_bps > 0:
            impulse_reason: Optional[str] = None
            if not metrics["impulse_ready"]:
                impulse_reason = "wait_ticks"
            elif metrics["impulse_bps"] < self._strategy.impulse_min_bps:
                impulse_reason = "impulse"
            if impulse_reason:
                if self._should_degrade_impulse(metrics):
                    self._maybe_log_impulse_degraded(metrics)
                else:
                    return impulse_reason
        if metrics["mid"] > 0 and not self._leverage_ok(metrics["mid"]):
            return "leverage_limit"
        return None

    def _should_degrade_impulse(self, metrics: dict[str, float | bool]) -> bool:
        if not self._strategy.use_impulse_filter:
            return False
        if self._strategy.impulse_grace_ms <= 0:
            return False
        if self._armed_ts is None:
            return False
        elapsed_ms = (time.monotonic() - self._armed_ts) * 1000
        if elapsed_ms < self._strategy.impulse_grace_ms:
            return False
        if metrics["data_stale"]:
            return False
        if metrics["spread_bps"] > self._strategy.max_spread_bps:
            return False
        if metrics["tick_rate"] < self._strategy.min_tick_rate:
            return False
        return True

    def _maybe_log_impulse_degraded(self, metrics: dict[str, float | bool]) -> None:
        now = time.monotonic()
        if now - self._last_impulse_degrade_log_ts < 1.0:
            return
        self._last_impulse_degrade_log_ts = now
        self._emit_log(
            "TRADE",
            "INFO",
            "[ENTRY] impulse_degraded",
            grace_ms=self._strategy.impulse_grace_ms,
            impulse_bps=f"{metrics['impulse_bps']:.4f}",
            tick_rate=metrics["tick_rate"],
            spread_bps=f"{metrics['spread_bps']:.2f}",
            mode=self._strategy.impulse_degrade_mode,
        )

    def _compute_entry_metrics(self) -> dict[str, float | bool]:
        tick = (
            self._market_data.get_last_effective_tick()
            or self._market_data.get_last_ws_tick()
            or self._market_data.get_last_http_tick()
            or {}
        )
        bid = float(tick.get("bid", 0.0))
        ask = float(tick.get("ask", 0.0))
        mid = float(tick.get("mid", 0.0))
        spread = float(tick.get("spread_bps", 0.0))
        now_ms = time.monotonic() * 1000
        rx_count_1s = self._market_data.get_ws_rx_count_1s(now_ms=now_ms)
        tick_rate = rx_count_1s
        impulse_bps = 0.0
        ref_mid = 0.0
        delta_mid = 0.0
        impulse_ready = False
        prev_mid_raw, last_mid_raw = self._market_data.get_ws_mid_raws()
        if prev_mid_raw is not None and last_mid_raw is not None:
            impulse_ready = True
            ref_mid_raw = prev_mid_raw
            mid_now_raw = last_mid_raw
            if ref_mid_raw > 0:
                delta_raw = mid_now_raw - ref_mid_raw
                impulse_bps = float(
                    (abs(delta_raw) / ref_mid_raw) * Decimal("10000")
                )
                ref_mid = float(ref_mid_raw)
                delta_mid = float(delta_raw)
        snapshot = self._market_data.get_snapshot()
        ws_age_ms = snapshot.ws_age_ms
        http_age_ms = snapshot.http_age_ms
        effective_source = snapshot.effective_source
        data_stale = snapshot.data_stale
        ws_connected = self._market_data.is_ws_connected()
        if (
            ws_connected
            and ws_age_ms is not None
            and ws_age_ms < 500
            and tick_rate == 0
        ):
            tick_rate = 1
            now = time.monotonic()
            if now - self._last_tickrate_force_log_ts >= 1.0:
                self._last_tickrate_force_log_ts = now
                self._emit_log(
                    "INFO",
                    "INFO",
                    "[TICKRATE] forced_alive",
                    ws_age_ms=ws_age_ms,
                )
        return {
            "spread_bps": spread,
            "tick_rate": tick_rate,
            "rx_count_1s": rx_count_1s,
            "impulse_bps": impulse_bps,
            "impulse_ready": impulse_ready,
            "ref_mid": ref_mid,
            "delta_mid": delta_mid,
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "ws_age_ms": ws_age_ms,
            "http_age_ms": http_age_ms,
            "effective_source": effective_source,
            "data_stale": data_stale,
            "ws_connected": ws_connected,
        }

    def _to_decimal(self, value: Decimal | float | int) -> Decimal:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def _float_from_decimal(self, value: Optional[Decimal]) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    def _format_decimal(self, value: Optional[Decimal], precision: int = 2) -> Optional[str]:
        if value is None:
            return None
        return f"{float(value):.{precision}f}"

    def _mid_from_tick_decimal(self, tick: dict[str, Any]) -> Optional[Decimal]:
        mid_raw = tick.get("mid_raw")
        if mid_raw is not None:
            mid_value = self._to_decimal(mid_raw)
            if mid_value > 0:
                return mid_value
        bid_raw = tick.get("bid_raw")
        ask_raw = tick.get("ask_raw")
        if bid_raw is not None and ask_raw is not None:
            bid = self._to_decimal(bid_raw)
            ask = self._to_decimal(ask_raw)
        else:
            bid = self._to_decimal(tick.get("bid", 0.0))
            ask = self._to_decimal(tick.get("ask", 0.0))
        if bid <= 0 or ask <= 0:
            return None
        return (bid + ask) / Decimal("2")

    def _get_effective_mid_decimal(self) -> Optional[Decimal]:
        mid_raw = self._market_data.get_mid_raw()
        if mid_raw is not None and mid_raw > 0:
            return mid_raw
        tick = self._market_data.get_last_effective_tick() or {}
        return self._mid_from_tick_decimal(tick)

    def _reset_detect_buffer(self) -> None:
        self._detect_mid_buffer = deque(maxlen=self._detect_window_ticks)

    def _append_detect_mid(self, mid: Decimal) -> None:
        if mid <= 0:
            return
        if not self._detect_mid_buffer or mid != self._detect_mid_buffer[-1]:
            self._detect_mid_buffer.append(mid)

    def _handle_detect_timeout(self) -> None:
        self._force_flatten("detect_timeout", result="TIMEOUT")

    def _round_step(self, qty: float) -> float:
        step = self._filters.step_size
        if step <= 0:
            return qty
        return float(int(qty / step) * step)

    def _normalize_qty(self, qty: float) -> float:
        return max(self._round_step(qty), 0.0)

    def _qty_ok(self, qty: float, mid: float) -> bool:
        if qty <= 0:
            return False
        if self._filters.min_qty and qty < self._filters.min_qty:
            return False
        if self._filters.min_notional and mid > 0:
            return qty * mid >= self._filters.min_notional
        return True

    def _abort_if_not_authorized(self, context: str) -> bool:
        if self._execution.last_error_code != -1002:
            return False
        self._emit_log(
            "ERROR",
            "ERROR",
            "API not authorized for margin request",
            context=context,
            error=self._execution.last_error,
            error_code=self._execution.last_error_code,
        )
        self._on_not_authorized(context)
        return True

    def _calc_raw_bps(self, side: str, from_mid: Decimal | float, to_mid: Decimal | float) -> float:
        from_value = self._to_decimal(from_mid)
        to_value = self._to_decimal(to_mid)
        if from_value <= 0 or to_value <= 0:
            return 0.0
        if side == "LONG":
            return float((to_value - from_value) / from_value * Decimal("10000"))
        if side == "SHORT":
            return float((from_value - to_value) / from_value * Decimal("10000"))
        return 0.0

    def _order_params(
        self, side: str, bid: Optional[float] = None, ask: Optional[float] = None
    ) -> tuple[str, Optional[float]]:
        order_mode = self._strategy.order_mode
        if order_mode != "aggressive_limit":
            return order_mode, None
        tick = self._last_tick or {}
        bid = bid or float(tick.get("bid", 0.0))
        ask = ask or float(tick.get("ask", 0.0))
        tick_size = self._filters.tick_size
        if bid <= 0 or ask <= 0 or tick_size <= 0:
            self._emit_log(
                "ERROR",
                "ERROR",
                "Aggressive limit fallback to market",
                side=side,
            )
            return "market", None
        if side == "BUY":
            raw_price = ask * (1 + self._strategy.slip_bps / 10_000)
        else:
            raw_price = bid * (1 - self._strategy.slip_bps / 10_000)
        normalized = self._normalize_price(side, raw_price)
        if not normalized or normalized <= 0:
            self._emit_log(
                "ERROR",
                "ERROR",
                "Aggressive limit fallback to market",
                side=side,
                reason="invalid_limit_price",
            )
            return "market", None
        return order_mode, normalized

    def _normalize_price(self, side: str, price: float) -> Optional[float]:
        tick_size = self._filters.tick_size
        if tick_size <= 0:
            return price
        price_dec = Decimal(str(price))
        tick_dec = Decimal(str(tick_size))
        if tick_dec <= 0 or price_dec <= 0:
            return None
        rounding = ROUND_CEILING if side == "BUY" else ROUND_FLOOR
        steps = (price_dec / tick_dec).to_integral_value(rounding=rounding)
        normalized = steps * tick_dec
        if normalized <= 0:
            return None
        return float(normalized)

    def _place_order(
        self,
        side: str,
        qty: float,
        order_mode: str,
        price: Optional[float] = None,
        side_effect_type: Optional[str] = None,
        client_order_id: Optional[str] = None,
        order_kind: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        if order_kind in {"OPEN_LONG", "OPEN_SHORT"} and self._state_machine.state not in {
            BotState.DETECT,
            BotState.ENTERED_LONG,
            BotState.ENTERED_SHORT,
        }:
            self._emit_log(
                "GUARD",
                "WARN",
                "[GUARD] open_order_blocked",
                cycle_id=self._state_machine.cycle_id,
                state=self._state_machine.state.value,
                kind=order_kind,
                side=side,
            )
            return None
        if order_kind in {"OPEN_LONG", "OPEN_SHORT"} and side in self._entry_actions:
            self._emit_log(
                "FSM",
                "ERROR",
                f"[FSM] cycle_id={self._state_machine.cycle_id} duplicate_entry "
                f"side={side} blocked",
            )
            return None
        time_in_force = "GTC" if order_mode == "aggressive_limit" else None
        order = self._execution.place_order(
            side,
            qty,
            order_mode,
            price=price,
            side_effect_type=side_effect_type,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
        )
        self._emit_log(
            "ORDER",
            "INFO",
            "[ORDER] sent",
            cycle_id=self._state_machine.cycle_id,
            kind=order_kind,
            side=side,
            qty=qty,
            order_mode=order_mode,
            price=price,
            tif=time_in_force,
            reduceOnly=False,
            sideEffectType=side_effect_type,
            client_order_id=client_order_id,
        )
        self._abort_if_not_authorized("place_order")
        if order_kind in {"OPEN_LONG", "OPEN_SHORT"} and order:
            self._entry_actions.add(side)
        return order

    def _wait_for_fill_with_fallback(
        self,
        side: str,
        order_id: Optional[int],
        qty: float,
        order_mode: str,
        price: Optional[float],
        side_effect_type: Optional[str] = None,
        client_order_id: Optional[str] = None,
        timeout_ms: Optional[int] = None,
    ) -> Optional[tuple[float, float, dict[str, Any]]]:
        timeout_s = (timeout_ms or 3000) / 1000
        if order_mode != "aggressive_limit":
            return self._execution.wait_for_fill(order_id, timeout_s=timeout_s)
        fill = self._execution.wait_for_fill(order_id, timeout_s=min(0.4, timeout_s))
        if fill:
            return fill
        if order_id:
            self._execution.cancel_order(int(order_id))
            if self._abort_if_not_authorized("cancel_order"):
                return None
        fallback_client_id = None
        if client_order_id:
            fallback_client_id = f"{client_order_id}-FALLBACK"
        market_order = self._execution.place_order(
            side,
            qty,
            "market",
            side_effect_type=side_effect_type,
            client_order_id=fallback_client_id,
        )
        self._emit_log(
            "ORDER",
            "INFO",
            "[ORDER] fallback",
            cycle_id=self._state_machine.cycle_id,
            kind="FALLBACK",
            side=side,
            qty=qty,
            order_mode="market",
            client_order_id=fallback_client_id,
        )
        if self._abort_if_not_authorized("place_order_fallback"):
            return None
        if not market_order:
            return None
        return self._execution.wait_for_fill(
            market_order.get("orderId"),
            timeout_s=timeout_s,
        )

    def _current_data_source(self) -> str:
        return self._market_data.get_snapshot().effective_source

    def _maybe_log_data_source(self) -> None:
        source = self._current_data_source()
        now = time.monotonic()
        if self._last_data_log_source == source:
            return
        self._last_data_log_source = source
        self._last_data_log_ts = now
        snapshot = self._market_data.get_snapshot()
        last_ws_time = None
        last_http_time = None
        last_ws_tick = self._market_data.get_last_ws_tick()
        last_http_tick = self._market_data.get_last_http_tick()
        if last_ws_tick:
            last_ws_time = last_ws_tick.get("rx_time")
        if last_http_tick:
            last_http_time = last_http_tick.get("rx_time")
        self._emit_log(
            "INFO",
            "INFO",
            "DATA effective_source",
            source=source,
            ws_age_ms=snapshot.ws_age_ms,
            http_age_ms=snapshot.http_age_ms,
            ws_fresh_ms=self._market_data.ws_fresh_ms,
            http_fresh_ms=self._market_data.http_fresh_ms,
            last_ws_tick_time=last_ws_time,
            last_http_tick_time=last_http_time,
        )

    def _maybe_log_data_health(self) -> None:
        if self._state_machine.state == BotState.IDLE:
            return
        now = time.monotonic()
        if now - self._last_data_health_log_ts < 2.0:
            return
        self._last_data_health_log_ts = now
        snapshot = self._market_data.get_snapshot()
        last_tick = self._market_data.get_last_effective_tick() or {}
        self._emit_log(
            "INFO",
            "INFO",
            "DATA runtime",
            ws_connected=self._market_data.is_ws_connected(),
            ws_age_ms=snapshot.ws_age_ms,
            last_ws_rx_monotonic_ms=self._market_data.get_last_ws_rx_monotonic_ms(),
            last_tick_mid=last_tick.get("mid"),
            effective_source=snapshot.effective_source,
            data_stale=snapshot.data_stale,
        )

    def _leverage_ok(self, mid: float) -> bool:
        if mid <= 0:
            return False
        notional_leg = self._entry_qty or (self._strategy.usd_notional / mid)
        total_notional = 2 * notional_leg * mid
        margin_account = self._execution.get_margin_account() or {}
        spot_account = self._execution.get_spot_account() or {}
        assets = margin_account.get("userAssets", [])
        margin_usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        margin_free = float(margin_usdt.get("free", 0.0)) if margin_usdt else 0.0
        spot_balances = spot_account.get("balances", [])
        spot_usdt = next((item for item in spot_balances if item.get("asset") == "USDT"), None)
        spot_free = float(spot_usdt.get("free", 0.0)) if spot_usdt else 0.0
        equity_usdt = margin_free + spot_free
        if equity_usdt <= 0:
            return False
        return total_notional <= equity_usdt * max(1, self._strategy.leverage_max)

    def _set_error(self, reason: str) -> None:
        self._last_reason = reason
        self._state_machine.set_error(reason)

    def _build_client_order_id(self, leg: str, phase: str) -> str:
        return f"DHS-{self._execution.symbol}-{self._state_machine.cycle_id}-{leg}-{phase}"

    def _refresh_position_cache(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_position_refresh_ts < 1.0:
            return
        self._last_position_refresh_ts = now
        open_orders = self._execution.get_open_orders()
        if self._abort_if_not_authorized("check_open_orders"):
            return
        self._open_orders_count = len(open_orders)
        margin_account = self._execution.get_margin_account()
        if self._abort_if_not_authorized("check_margin_account"):
            return
        assets = (margin_account or {}).get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        net_qty = float(btc.get("netAsset", 0.0)) if btc else 0.0
        self._pos_net_qty = net_qty
        self._pos_long_qty = max(net_qty, 0.0)
        self._pos_short_qty = max(-net_qty, 0.0)
        self._emit_log(
            "POS",
            "INFO",
            "[POS] snapshot",
            pos_long_qty=self._pos_long_qty,
            pos_short_qty=self._pos_short_qty,
            open_orders_count=self._open_orders_count,
        )

    def _is_flat(self, force: bool = False) -> bool:
        self._refresh_position_cache(force=force)
        step = self._filters.step_size or 0.0
        return self._open_orders_count == 0 and abs(self._pos_net_qty) <= step

    def _has_open_exposure(self) -> bool:
        return not self._is_flat(force=True)

    def _controlled_flatten(self, reason: str) -> bool:
        self._last_reason = reason
        self._inflight_exit = True
        self._last_action = "FLATTEN"
        self._set_phase(BotState.FLATTEN, reason=reason, action="FLATTEN_SENT")
        self._emit_log(
            "STATE",
            "INFO",
            "CONTROLLED_FLATTEN_START",
            cycle_id=self._state_machine.cycle_id,
            reason=reason,
        )
        self._execution.cancel_open_orders()
        if self._abort_if_not_authorized("controlled_cancel_orders"):
            return False
        snapshot = self._execution.get_margin_account() or {}
        if self._abort_if_not_authorized("controlled_snapshot"):
            return False
        assets = snapshot.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        btc_free = float(btc.get("free", 0.0)) if btc else 0.0
        btc_borrowed = float(btc.get("borrowed", 0.0)) if btc else 0.0
        btc_net = float(btc.get("netAsset", 0.0)) if btc else 0.0
        usdt_free = float(usdt.get("free", 0.0)) if usdt else 0.0
        usdt_borrowed = float(usdt.get("borrowed", 0.0)) if usdt else 0.0
        mid = self._float_from_decimal(self._get_effective_mid_decimal()) or 0.0
        step = self._filters.step_size or 0.0
        sell_client_id = self._build_client_order_id("SHORT", "FLATTEN")
        buy_client_id = self._build_client_order_id("LONG", "FLATTEN")
        if btc_net > step:
            sell_qty = self._normalize_qty(btc_net)
            if self._qty_ok(sell_qty, mid):
                sell_order = self._place_order(
                    "SELL",
                    sell_qty,
                    "market",
                    side_effect_type="NO_SIDE_EFFECT",
                    client_order_id=sell_client_id,
                    order_kind="CLOSE_LONG",
                )
                if self._abort_if_not_authorized("controlled_sell"):
                    return False
                if not sell_order:
                    self._set_error("controlled_sell_failed")
        if btc_net < -step:
            buy_qty = self._normalize_qty(abs(btc_net))
            if self._qty_ok(buy_qty, mid):
                buy_order = self._place_order(
                    "BUY",
                    buy_qty,
                    "market",
                    side_effect_type="AUTO_REPAY",
                    client_order_id=buy_client_id,
                    order_kind="CLOSE_SHORT",
                )
                if self._abort_if_not_authorized("controlled_buy"):
                    return False
                if not buy_order:
                    self._set_error("controlled_buy_failed")
        repay_snapshot = self._execution.get_margin_account() or {}
        if self._abort_if_not_authorized("controlled_repay_snapshot"):
            return False
        repay_assets = repay_snapshot.get("userAssets", [])
        repay_btc = next((item for item in repay_assets if item.get("asset") == "BTC"), None)
        repay_usdt = next(
            (item for item in repay_assets if item.get("asset") == "USDT"), None
        )
        repay_btc_borrowed = float(repay_btc.get("borrowed", 0.0)) if repay_btc else 0.0
        repay_btc_free = float(repay_btc.get("free", 0.0)) if repay_btc else 0.0
        repay_usdt_borrowed = float(repay_usdt.get("borrowed", 0.0)) if repay_usdt else 0.0
        repay_usdt_free = float(repay_usdt.get("free", 0.0)) if repay_usdt else 0.0
        if repay_btc_borrowed > 0 and repay_btc_free > 0:
            repay_amount = min(repay_btc_borrowed, repay_btc_free)
            self._execution.repay_asset("BTC", repay_amount)
            if self._abort_if_not_authorized("controlled_repay_btc"):
                return False
        if repay_usdt_borrowed > 0 and repay_usdt_free > 0:
            repay_amount = min(repay_usdt_borrowed, repay_usdt_free)
            self._execution.repay_asset("USDT", repay_amount)
            if self._abort_if_not_authorized("controlled_repay_usdt"):
                return False
        final_snapshot = self._execution.get_margin_account() or {}
        if self._abort_if_not_authorized("controlled_final_snapshot"):
            return False
        final_open_orders = self._execution.get_open_orders()
        if self._abort_if_not_authorized("controlled_final_open_orders"):
            return False
        final_assets = final_snapshot.get("userAssets", [])
        final_btc = next((item for item in final_assets if item.get("asset") == "BTC"), None)
        final_usdt = next((item for item in final_assets if item.get("asset") == "USDT"), None)
        final_btc_borrowed = float(final_btc.get("borrowed", 0.0)) if final_btc else 0.0
        final_btc_net = float(final_btc.get("netAsset", 0.0)) if final_btc else 0.0
        final_usdt_borrowed = float(final_usdt.get("borrowed", 0.0)) if final_usdt else 0.0
        self._emit_log(
            "INFO",
            "INFO",
            "CONTROLLED_FLATTEN_DONE",
            reason=reason,
            exposure_btc=final_btc_net,
            borrowed_btc=final_btc_borrowed,
            borrowed_usdt=final_usdt_borrowed,
            open_orders_count=len(final_open_orders),
        )
        self._emit_log(
            "EXIT",
            "INFO",
            "[EXIT] flatten_done",
            cycle_id=self._state_machine.cycle_id,
            reason=reason,
            open_orders_count=len(final_open_orders),
        )
        success = True
        if (
            abs(final_btc_net) > step
            or final_btc_borrowed > step
            or final_usdt_borrowed > step
            or final_open_orders
        ):
            self._emit_log(
                "ERROR",
                "ERROR",
                "CONTROLLED_FLATTEN_FAILED",
                exposure_btc=final_btc_net,
                borrowed_btc=final_btc_borrowed,
                borrowed_usdt=final_usdt_borrowed,
                open_orders_count=len(final_open_orders),
            )
            self._state_machine.set_error("controlled_flatten_failed")
            success = False
        self._exposure_open = False
        self._emit_exposure(False)
        if success:
            self._refresh_position_cache(force=True)
            self._inflight_exit = False
        return success

    def _emergency_flatten(self, reason: str) -> None:
        self._last_reason = reason
        self._inflight_exit = True
        self._last_action = "FLATTEN"
        self._set_phase(BotState.FLATTEN, reason=reason, action="FLATTEN_SENT")
        if self._abort_if_not_authorized("emergency_start"):
            return
        open_orders = self._execution.get_open_orders()
        if self._abort_if_not_authorized("emergency_open_orders"):
            return
        for order in open_orders:
            order_id = order.get("orderId")
            if order_id:
                self._execution.cancel_order(int(order_id))
                if self._abort_if_not_authorized("emergency_cancel_order"):
                    return
        margin_account = self._execution.get_margin_account() or {}
        if self._abort_if_not_authorized("emergency_account_snapshot"):
            return
        assets = margin_account.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        btc_free = float(btc.get("free", 0.0)) if btc else 0.0
        btc_borrowed = float(btc.get("borrowed", 0.0)) if btc else 0.0
        btc_net = float(btc.get("netAsset", 0.0)) if btc else 0.0
        usdt_borrowed = float(usdt.get("borrowed", 0.0)) if usdt else 0.0
        net_short_btc = max(btc_borrowed, -btc_net)
        mid = float((self._market_data.get_last_effective_tick() or {}).get("mid", 0.0))
        sell_client_id = self._build_client_order_id("SHORT", "FLATTEN")
        buy_client_id = self._build_client_order_id("LONG", "FLATTEN")
        if btc_free > 0:
            sell_qty = self._normalize_qty(btc_free)
            if self._qty_ok(sell_qty, mid):
                self._place_order(
                    "SELL",
                    sell_qty,
                    "market",
                    client_order_id=sell_client_id,
                    order_kind="CLOSE_LONG",
                )
                if self._abort_if_not_authorized("emergency_sell"):
                    return
        if net_short_btc > 0:
            buy_qty = self._normalize_qty(net_short_btc)
            if self._qty_ok(buy_qty, mid):
                self._place_order(
                    "BUY",
                    buy_qty,
                    "market",
                    side_effect_type="AUTO_REPAY",
                    client_order_id=buy_client_id,
                    order_kind="CLOSE_SHORT",
                )
                if self._abort_if_not_authorized("emergency_buy"):
                    return
        repay_snapshot = self._execution.get_margin_account() or {}
        if self._abort_if_not_authorized("emergency_repay_snapshot"):
            return
        repay_assets = repay_snapshot.get("userAssets", [])
        repay_btc = next((item for item in repay_assets if item.get("asset") == "BTC"), None)
        repay_usdt = next((item for item in repay_assets if item.get("asset") == "USDT"), None)
        repay_btc_borrowed = float(repay_btc.get("borrowed", 0.0)) if repay_btc else 0.0
        repay_btc_free = float(repay_btc.get("free", 0.0)) if repay_btc else 0.0
        repay_usdt_borrowed = float(repay_usdt.get("borrowed", 0.0)) if repay_usdt else 0.0
        repay_usdt_free = float(repay_usdt.get("free", 0.0)) if repay_usdt else 0.0
        if repay_btc_borrowed > 0 and repay_btc_free > 0:
            repay_amount = min(repay_btc_borrowed, repay_btc_free)
            self._execution.repay_asset("BTC", repay_amount)
            if self._abort_if_not_authorized("emergency_repay_btc"):
                return
        if repay_usdt_borrowed > 0 and repay_usdt_free > 0:
            repay_amount = min(repay_usdt_borrowed, repay_usdt_free)
            self._execution.repay_asset("USDT", repay_amount)
            if self._abort_if_not_authorized("emergency_repay_usdt"):
                return
        final_snapshot = self._execution.get_margin_account() or {}
        if self._abort_if_not_authorized("emergency_final_snapshot"):
            return
        final_open_orders = self._execution.get_open_orders()
        if self._abort_if_not_authorized("emergency_final_open_orders"):
            return
        final_assets = final_snapshot.get("userAssets", [])
        final_btc = next((item for item in final_assets if item.get("asset") == "BTC"), None)
        final_usdt = next((item for item in final_assets if item.get("asset") == "USDT"), None)
        final_btc_borrowed = float(final_btc.get("borrowed", 0.0)) if final_btc else 0.0
        final_btc_net = float(final_btc.get("netAsset", 0.0)) if final_btc else 0.0
        final_usdt_borrowed = float(final_usdt.get("borrowed", 0.0)) if final_usdt else 0.0
        step = self._filters.step_size or 0.0
        self._emit_log(
            "ERROR",
            "ERROR",
            "EMERGENCY FLATTEN",
            reason=reason,
            exposure_btc=final_btc_net,
            borrowed_btc=final_btc_borrowed,
            borrowed_usdt=final_usdt_borrowed,
            open_orders_count=len(final_open_orders),
            error=self._execution.last_error,
            error_code=self._execution.last_error_code,
        )
        self._emit_log("STATE", "INFO", "EMERGENCY_FLATTEN", cycle_id=self._state_machine.cycle_id)
        self._exposure_open = False
        self._emit_exposure(False)
        if (
            abs(final_btc_net) <= step
            and final_btc_borrowed <= step
            and final_usdt_borrowed <= step
            and not final_open_orders
        ):
            self._inflight_exit = False

    def _settle_borrow(self) -> None:
        margin_account = self._execution.get_margin_account() or {}
        if self._abort_if_not_authorized("settle_snapshot"):
            return
        assets = margin_account.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        if btc:
            btc_borrowed = float(btc.get("borrowed", 0.0))
            if btc_borrowed > 0:
                self._execution.repay_asset("BTC", btc_borrowed)
                if self._abort_if_not_authorized("settle_repay_btc"):
                    return
        if usdt:
            usdt_borrowed = float(usdt.get("borrowed", 0.0))
            if usdt_borrowed > 0:
                self._execution.repay_asset("USDT", usdt_borrowed)
                if self._abort_if_not_authorized("settle_repay_usdt"):
                    return

    def _check_timeouts(self) -> None:
        if self._state_machine.state == BotState.WAIT_WINNER:
            if self._winner_side:
                self._evaluate_riding()
            else:
                self._evaluate_detecting()
        if self._state_machine.state == BotState.WAIT_WINNER and self._ride_start_ts:
            elapsed_ms = (time.monotonic() - self._ride_start_ts) * 1000
            if elapsed_ms >= self._strategy.max_ride_ms:
                self._exit_winner("max_ride_timeout")

    def _log_skip(self, reason: str) -> None:
        now = time.monotonic()
        if reason == "tick_rate" and now - self._last_tick_rate_block_log_ts < 1.0:
            return
        if reason == self._last_skip_reason and now - self._last_skip_log_ts < 0.5:
            return
        self._last_skip_log_ts = now
        self._last_skip_reason = reason
        if reason == "tick_rate":
            self._last_tick_rate_block_log_ts = now
        self._last_reason = reason
        self._waiting_for_data = reason == "data_stale"
        if reason not in {"impulse", "wait_ticks"}:
            self._last_non_impulse_skip_ts = now
        metrics = self._compute_entry_metrics()
        self._data_stale = bool(metrics["data_stale"])
        self._effective_source = str(metrics["effective_source"])
        self._ws_age_ms = float(metrics["ws_age_ms"])
        self._http_age_ms = float(metrics["http_age_ms"])
        if self._effective_source == "WS":
            self._effective_age_ms = self._ws_age_ms
        elif self._effective_source == "HTTP":
            self._effective_age_ms = self._http_age_ms
        else:
            self._effective_age_ms = 9999.0
        if reason == "data_stale":
            self._log_wait_data(metrics)
        self._maybe_warn_impulse_block(now, reason)
        skip_flags = []
        if metrics["data_stale"]:
            skip_flags.append("data")
        if metrics["spread_bps"] > self._strategy.max_spread_bps:
            skip_flags.append("spread")
        if metrics["tick_rate"] < self._strategy.min_tick_rate:
            skip_flags.append("tick")
        if self._strategy.use_impulse_filter and self._strategy.impulse_min_bps > 0:
            if not metrics["impulse_ready"] or metrics["impulse_bps"] < self._strategy.impulse_min_bps:
                skip_flags.append("impulse")
        if reason == "leverage_limit":
            skip_flags.append("leverage")
        self._emit_log(
            "TRADE",
            "INFO",
            "SKIP entry",
            reason=reason,
            blocked_by=reason,
            skip_flags=skip_flags,
            spread_bps=f"{metrics['spread_bps']:.2f}",
            spread_limit=self._strategy.max_spread_bps,
            tick_rate=metrics["tick_rate"],
            tick_rate_limit=self._strategy.min_tick_rate,
            impulse_bps=f"{metrics['impulse_bps']:.2f}",
            impulse_limit=self._strategy.impulse_min_bps,
            ref_mid=f"{metrics['ref_mid']:.2f}",
            delta_mid=f"{metrics['delta_mid']:.6f}",
            mid=f"{metrics['mid']:.2f}",
            bid=f"{metrics['bid']:.2f}",
            ask=f"{metrics['ask']:.2f}",
            ws_age_ms=metrics["ws_age_ms"],
            ws_connected=metrics["ws_connected"],
            rx_count_1s=metrics["rx_count_1s"],
            http_age_ms=metrics["http_age_ms"],
            effective_source=metrics["effective_source"],
            ws_fresh_ms=self._market_data.ws_fresh_ms,
            http_fresh_ms=self._market_data.http_fresh_ms,
            data_stale=metrics["data_stale"],
        )

    def _maybe_warn_impulse_block(self, now: float, reason: str) -> None:
        if reason not in {"impulse", "wait_ticks"}:
            return
        if not self._strategy.use_impulse_filter or self._strategy.impulse_min_bps <= 0:
            return
        if self._armed_ts is None:
            return
        if now - self._armed_ts <= 10.0:
            return
        if now - self._last_non_impulse_skip_ts <= 10.0:
            return
        if now - self._last_impulse_block_warn_ts < 5.0:
            return
        self._last_impulse_block_warn_ts = now
        self._emit_log(
            "WARN",
            "WARN",
            "Impulse filter blocks entry; consider lowering impulse_limit or check impulse calc.",
            impulse_limit=self._strategy.impulse_min_bps,
        )

    def _log_wait_data(self, metrics: dict[str, float | bool]) -> None:
        now = time.monotonic()
        if now - self._last_data_health_log_ts < 2.0:
            return
        self._last_data_health_log_ts = now
        self._emit_log(
            "INFO",
            "INFO",
            "DATA health",
            ws_connected=self._market_data.is_ws_connected(),
            ws_age_ms=metrics["ws_age_ms"],
            http_enabled=self._market_data.mode != MarketDataMode.WS_ONLY,
            http_age_ms=metrics["http_age_ms"],
            effective_source=metrics["effective_source"],
        )
