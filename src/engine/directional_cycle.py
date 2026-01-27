from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
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
        self._entry_long_price: Optional[float] = None
        self._entry_short_price: Optional[float] = None
        self._winner_raw_bps: Optional[float] = None
        self._winner_net_bps: Optional[float] = None
        self._loser_raw_bps: Optional[float] = None
        self._loser_net_bps: Optional[float] = None
        self._last_net_usd: Optional[float] = None
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
            state=state.value,
            active_cycle=active_cycle,
            started_at=self._cycle_start,
            duration_s=duration_s,
            nominal_usd=self._strategy.usd_notional,
            fee_total_bps=self._strategy.fee_total_bps,
            target_net_bps=self._strategy.target_net_bps,
            max_loss_bps=self._strategy.max_loss_bps,
            max_spread_bps=self._strategy.max_spread_bps,
            min_tick_rate=self._strategy.min_tick_rate,
            cooldown_s=self._strategy.cooldown_s,
            tick_rate=metrics["tick_rate"],
            impulse_bps=metrics["impulse_bps"],
            spread_bps=metrics["spread_bps"],
            ws_age_ms=metrics["ws_age_ms"],
            effective_source=str(metrics["effective_source"]),
            entry_mid=self._float_from_decimal(self._entry_mid),
            exit_mid=self._winner_exit_price or self._loser_exit_price,
            winner_side=self._winner_side,
            loser_side=self._loser_side,
            winner_raw_bps=winner_raw,
            loser_raw_bps=loser_raw,
            winner_net_bps=winner_net,
            loser_net_bps=loser_net,
            total_raw_bps=total_raw,
            total_net_bps=total_net,
            condition=self._condition_from_state(state),
            reason=self._last_reason,
            last_error=last_error,
        )

    def _condition_from_state(self, state: BotState) -> str:
        return {
            BotState.ENTERING: "ENTER",
            BotState.DETECTING: "DETECT_WAIT",
            BotState.CUTTING: "CUT_LOSER",
            BotState.RIDING: "RIDE_TARGET",
            BotState.EXITING: "EXIT",
            BotState.CONTROLLED_FLATTEN: "FLATTEN",
            BotState.COOLDOWN: "COOLDOWN",
            BotState.ERROR: "ERROR",
            BotState.ARMED: "ARMED",
            BotState.IDLE: "IDLE",
        }.get(state, state.value)

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
        if self._state_machine.state == BotState.DETECTING:
            self._evaluate_detecting()
        elif self._state_machine.state == BotState.RIDING:
            self._evaluate_riding()

    def attempt_entry(self) -> None:
        self._check_timeouts()
        if self._state_machine.state == BotState.COOLDOWN:
            self._log_skip("cooldown")
            return
        if self._state_machine.state != BotState.ARMED:
            return
        if not self._market_data.get_last_ws_tick() and not self._market_data.get_last_http_tick():
            self._log_skip("no_tick")
            return
        if not self._filters.step_size or not self._filters.min_qty:
            self._log_skip("filters_missing")
            return
        if self._has_open_exposure():
            self._log_skip("exposure")
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

    def stop(self) -> None:
        self._state_machine.stop()
        self._execution.cancel_open_orders()
        self._emergency_flatten(reason="stop")

    def end_cooldown(self, auto_resume: bool) -> None:
        if not auto_resume:
            self._state_machine.stop()
            return
        self._state_machine.end_cooldown()

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
        self._entry_mid = None
        self._entry_qty = None
        self._detect_start_ts = None
        self._winner_side = None
        self._loser_side = None
        self._detect_mid = None
        self._loser_exit_price = None
        self._winner_exit_price = None
        self._entry_long_price = None
        self._entry_short_price = None
        self._winner_raw_bps = None
        self._winner_net_bps = None
        self._loser_raw_bps = None
        self._loser_net_bps = None
        self._last_reason = None
        self._last_net_usd = None
        self._last_detect_sample_log_ts = 0.0
        self._reset_detect_buffer()
        self._emit_log(
            "CYCLE",
            "INFO",
            "[CYCLE] START",
            n=self._state_machine.cycle_id,
            side="HEDGE",
            symbol=self._execution.symbol,
        )
        self._emit_log("TRADE", "INFO", "ENTER cycle", cycle_id=self._state_machine.cycle_id)
        self._emit_log("STATE", "INFO", "ENTER_START", cycle_id=self._state_machine.cycle_id)
        if not self._enter_hedge():
            return
        self._state_machine.state = BotState.DETECTING
        self._detect_start_ts = time.monotonic()
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
            sell_order = self._place_order(
                "SELL",
                qty,
                sell_mode,
                price=sell_price,
                side_effect_type="AUTO_BORROW_REPAY",
            )
            if not sell_order:
                self._log_partial("partial_hedge_entry", qty, None, sell_order)
                return False
            buy_order = self._place_order("BUY", qty, buy_mode, price=buy_price)
        except Exception as exc:
            self._emit_log(
                "ERROR",
                "ERROR",
                "ENTER order exception",
                error=str(exc),
            )
            self._emergency_flatten(reason="enter_exception")
            return False
        buy_id = buy_order.get("orderId") if buy_order else None
        sell_id = sell_order.get("orderId") if sell_order else None
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
        buy_fill = self._wait_for_fill_with_fallback("BUY", buy_id, qty, buy_mode, buy_price)
        sell_fill = self._wait_for_fill_with_fallback(
            "SELL",
            sell_id,
            qty,
            sell_mode,
            sell_price,
            side_effect_type="AUTO_BORROW_REPAY",
        )
        if not buy_fill or not sell_fill:
            buy_status = self._execution.get_order(int(buy_id)) if buy_id else None
            sell_status = self._execution.get_order(int(sell_id)) if sell_id else None
            self._log_partial("partial_hedge_entry", qty, buy_status, sell_status)
            return False
        self._entry_long_price = buy_fill[0]
        self._entry_short_price = sell_fill[0]
        effective_mid = self._get_effective_mid_decimal()
        if effective_mid is None:
            effective_mid = Decimal(str((self._entry_long_price + self._entry_short_price) / 2))
        self._entry_mid = effective_mid
        self._log_deal(buy_fill[2], "ENTER_LONG")
        self._log_deal(sell_fill[2], "ENTER_SHORT")
        self._exposure_open = True
        self._emit_exposure(True)
        self._emit_log(
            "TRADE",
            "INFO",
            "ENTER filled",
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
        if elapsed > self._strategy.detect_timeout_ms:
            self._handle_detect_timeout()
            return
        tick = self._market_data.get_last_effective_tick() or {}
        mid_now = self._mid_from_tick_decimal(tick)
        if mid_now is None:
            return
        self._append_detect_mid(mid_now)
        long_raw = self._calc_raw_bps("LONG", self._entry_mid, mid_now)
        short_raw = self._calc_raw_bps("SHORT", self._entry_mid, mid_now)
        winner_threshold = self._strategy.winner_threshold_bps
        best = max(long_raw, short_raw)
        delta_mid_bps = None
        if len(self._detect_mid_buffer) >= self._detect_window_ticks:
            mid_first = self._detect_mid_buffer[0]
            mid_last = self._detect_mid_buffer[-1]
            if mid_first > 0:
                delta_mid_bps = float((mid_last - mid_first) / mid_first * Decimal("10000"))
        now = time.monotonic()
        if now - self._last_detect_sample_log_ts > 0.3:
            self._emit_log(
                "INFO",
                "INFO",
                "[DETECT] sample",
                mid=self._format_decimal(mid_now),
                long_raw=f"{long_raw:.4f}",
                short_raw=f"{short_raw:.4f}",
                delta_window_bps=f"{delta_mid_bps:.4f}" if delta_mid_bps is not None else None,
            )
            self._last_detect_sample_log_ts = now
        if best < winner_threshold:
            if delta_mid_bps is None or abs(delta_mid_bps) < winner_threshold / 2:
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
        self._state_machine.state = BotState.CUTTING
        self._cut_loser()

    def _cut_loser(self) -> None:
        if not self._entry_qty or not self._loser_side:
            self._set_error("missing_loser")
            return
        side = "BUY" if self._loser_side == "SHORT" else "SELL"
        side_effect = "AUTO_REPAY" if self._loser_side == "SHORT" else None
        order_mode, price = self._order_params(side)
        order = self._place_order(
            side,
            self._entry_qty,
            order_mode,
            price=price,
            side_effect_type=side_effect,
        )
        if not order:
            self._emergency_flatten(reason="cut_loser_failed")
            self._set_error("cut_loser_failed")
            return
        fill = self._wait_for_fill_with_fallback(
            side,
            order.get("orderId"),
            self._entry_qty,
            order_mode,
            price,
            side_effect_type=side_effect,
        )
        if not fill:
            self._emergency_flatten(reason="cut_loser_timeout")
            self._set_error("cut_loser_timeout")
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
        self._emit_log(
            "TRADE",
            "INFO",
            "CUT loser filled",
            loser_side=self._loser_side,
            exit_price=self._loser_exit_price,
        )
        self._emit_log("STATE", "INFO", "CUT_DONE", cycle_id=self._state_machine.cycle_id)
        self._state_machine.state = BotState.RIDING
        self._evaluate_riding()

    def _evaluate_riding(self) -> None:
        if self._state_machine.state != BotState.RIDING:
            return
        if not self._last_tick or not self._entry_mid or not self._winner_side:
            return
        effective_age_ms = self._market_data.get_snapshot().effective_age_ms
        tick = self._market_data.get_last_effective_tick() or self._last_tick
        mid_now = self._mid_from_tick_decimal(tick or {})
        if mid_now is None:
            return
        winner_raw_bps = self._calc_raw_bps(self._winner_side, self._entry_mid, mid_now)
        winner_net_bps = winner_raw_bps - self._strategy.fee_total_bps
        self._winner_raw_bps = winner_raw_bps
        self._winner_net_bps = winner_net_bps
        now = time.monotonic()
        if now - self._last_ride_log_ts > 0.5:
            self._emit_log(
                "TRADE",
                "INFO",
                "RIDE update",
                winner=self._winner_side,
                raw_bps=f"{winner_raw_bps:.2f}",
                net_bps=f"{winner_net_bps:.2f}",
            )
            self._emit_log(
                "STATE",
                "INFO",
                "RIDE_UPDATE",
                cycle_id=self._state_machine.cycle_id,
                raw_bps=f"{winner_raw_bps:.2f}",
                net_bps=f"{winner_net_bps:.2f}",
            )
            self._last_ride_log_ts = now
        if effective_age_ms > 1500:
            self._exit_winner("data_stale")
            return
        if winner_raw_bps <= -self._strategy.emergency_stop_bps:
            self._exit_winner("emergency_stop")
            return
        if winner_net_bps >= self._strategy.target_net_bps:
            self._exit_winner("target")
            return
        if winner_raw_bps <= -self._strategy.max_loss_bps:
            self._exit_winner("stop_loss")

    def _exit_winner(self, note: str) -> None:
        if not self._entry_qty or not self._winner_side:
            return
        self._state_machine.state = BotState.EXITING
        self._last_reason = note
        side = "SELL" if self._winner_side == "LONG" else "BUY"
        side_effect = "AUTO_REPAY" if self._winner_side == "SHORT" else None
        order_mode, price = self._order_params(side)
        order = self._place_order(
            side,
            self._entry_qty,
            order_mode,
            price=price,
            side_effect_type=side_effect,
        )
        if not order:
            self._emergency_flatten(reason="exit_winner_failed")
            self._set_error("exit_winner_failed")
            return
        fill = self._wait_for_fill_with_fallback(
            side,
            order.get("orderId"),
            self._entry_qty,
            order_mode,
            price,
            side_effect_type=side_effect,
        )
        if not fill:
            self._emergency_flatten(reason="exit_winner_timeout")
            self._set_error("exit_winner_timeout")
            return
        self._winner_exit_price = fill[0]
        self._winner_raw_bps = self._calc_raw_bps(
            self._winner_side, self._entry_mid or Decimal("0"), self._winner_exit_price
        )
        self._winner_net_bps = (
            self._winner_raw_bps - self._strategy.fee_total_bps
            if self._winner_raw_bps is not None
            else None
        )
        self._log_deal(fill[2], "EXIT_WINNER")
        self._emit_log(
            "TRADE",
            "INFO",
            "EXIT winner filled",
            winner_side=self._winner_side,
            exit_price=self._winner_exit_price,
            note=note,
        )
        self._emit_log("STATE", "INFO", "EXIT_DONE", cycle_id=self._state_machine.cycle_id)
        self._emit_trade_summary(note=note)
        self._exposure_open = False
        self._emit_exposure(False)
        self._settle_borrow()
        self._state_machine.finish_cycle()
        self._emit_log("STATE", "INFO", "CYCLE_DONE", cycle_id=self._state_machine.cycle_id)
        self._log_cycle_end("OK", reason=note)

    def _no_winner_exit(self, reason: str, allow_flatten: bool) -> None:
        self._emit_log("TRADE", "INFO", "DETECT no winner", reason=reason)
        self._emit_log("STATE", "INFO", "DETECT_NO_WINNER", cycle_id=self._state_machine.cycle_id)
        if allow_flatten:
            self._emit_log("INFO", "INFO", "[DETECT] timeout -> controlled_flatten")
            success = self._controlled_flatten(reason=reason)
            result = "NO_WINNER_ALLOWED" if success else "ERROR"
        else:
            self._emergency_flatten(reason=reason)
            result = "NO_WINNER"
        self._last_reason = reason
        if self._state_machine.state != BotState.ERROR:
            self._state_machine.finish_cycle()
            self._emit_log("STATE", "INFO", "CYCLE_DONE", cycle_id=self._state_machine.cycle_id)
        self._log_cycle_end(result, reason=reason)

    def _log_partial(
        self,
        reason: str,
        qty: float,
        buy_order: Optional[dict[str, Any]],
        sell_order: Optional[dict[str, Any]],
    ) -> None:
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
        self._state_machine.finish_cycle()
        self._log_cycle_end("ENTER_FAILED", reason=reason)

    def _log_deal(self, order: dict[str, Any], tag: str) -> None:
        if not order:
            return
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

    def _log_cycle_end(self, result: str, reason: Optional[str] = None) -> None:
        if not self._cycle_start:
            return
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
        self._reset_detect_buffer()

    def _entry_filter_reason(self) -> Optional[str]:
        metrics = self._compute_entry_metrics()
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
        allow_flatten = self._strategy.allow_no_winner_flatten and self._exposure_open
        self._no_winner_exit("detect_timeout", allow_flatten=allow_flatten)

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
            return order_mode, ask * (1 + self._strategy.slip_bps / 10_000)
        return order_mode, bid * (1 - self._strategy.slip_bps / 10_000)

    def _place_order(
        self,
        side: str,
        qty: float,
        order_mode: str,
        price: Optional[float] = None,
        side_effect_type: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        time_in_force = "GTC" if order_mode == "aggressive_limit" else None
        order = self._execution.place_order(
            side,
            qty,
            order_mode,
            price=price,
            side_effect_type=side_effect_type,
            time_in_force=time_in_force,
        )
        self._abort_if_not_authorized("place_order")
        return order

    def _wait_for_fill_with_fallback(
        self,
        side: str,
        order_id: Optional[int],
        qty: float,
        order_mode: str,
        price: Optional[float],
        side_effect_type: Optional[str] = None,
    ) -> Optional[tuple[float, float, dict[str, Any]]]:
        if order_mode != "aggressive_limit":
            return self._execution.wait_for_fill(order_id, timeout_s=3.0)
        fill = self._execution.wait_for_fill(order_id, timeout_s=0.4)
        if fill:
            return fill
        if order_id:
            self._execution.cancel_order(int(order_id))
            if self._abort_if_not_authorized("cancel_order"):
                return None
        market_order = self._execution.place_order(
            side,
            qty,
            "market",
            side_effect_type=side_effect_type,
        )
        if self._abort_if_not_authorized("place_order_fallback"):
            return None
        if not market_order:
            return None
        return self._execution.wait_for_fill(market_order.get("orderId"), timeout_s=3.0)

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

    def _has_open_exposure(self) -> bool:
        open_orders = self._execution.get_open_orders()
        if self._abort_if_not_authorized("check_open_orders"):
            return True
        if open_orders:
            return True
        margin_account = self._execution.get_margin_account()
        if self._abort_if_not_authorized("check_margin_account"):
            return True
        if not margin_account:
            return False
        assets = margin_account.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        step = self._filters.step_size or 0.0
        if btc:
            btc_free = float(btc.get("free", 0.0))
            btc_borrowed = float(btc.get("borrowed", 0.0))
            if btc_free > step or btc_borrowed > step:
                return True
        if usdt:
            usdt_borrowed = float(usdt.get("borrowed", 0.0))
            if usdt_borrowed > 0:
                return True
        return False

    def _controlled_flatten(self, reason: str) -> bool:
        self._last_reason = reason
        self._state_machine.state = BotState.CONTROLLED_FLATTEN
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
        if btc_net > step:
            sell_qty = self._normalize_qty(btc_net)
            if self._qty_ok(sell_qty, mid):
                sell_order = self._execution.place_order(
                    "SELL", sell_qty, "market", side_effect_type="NO_SIDE_EFFECT"
                )
                if self._abort_if_not_authorized("controlled_sell"):
                    return False
                if not sell_order:
                    self._set_error("controlled_sell_failed")
        if btc_net < -step:
            buy_qty = self._normalize_qty(abs(btc_net))
            if self._qty_ok(buy_qty, mid):
                buy_order = self._execution.place_order(
                    "BUY", buy_qty, "market", side_effect_type="AUTO_REPAY"
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
        return success

    def _emergency_flatten(self, reason: str) -> None:
        self._last_reason = reason
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
        if btc_free > 0:
            sell_qty = self._normalize_qty(btc_free)
            if self._qty_ok(sell_qty, mid):
                self._execution.place_order("SELL", sell_qty, "market")
                if self._abort_if_not_authorized("emergency_sell"):
                    return
        if net_short_btc > 0:
            buy_qty = self._normalize_qty(net_short_btc)
            if self._qty_ok(buy_qty, mid):
                self._execution.place_order(
                    "BUY",
                    buy_qty,
                    "market",
                    side_effect_type="AUTO_REPAY",
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
        if self._state_machine.state == BotState.DETECTING:
            self._evaluate_detecting()

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
