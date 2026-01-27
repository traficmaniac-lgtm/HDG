from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Optional

from src.core.models import StrategyParams, SymbolFilters
from src.core.state_machine import BotState, BotStateMachine
from src.exchange.binance_margin import BinanceMarginExecution


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


class DirectionalCycle:
    def __init__(
        self,
        execution: BinanceMarginExecution,
        state_machine: BotStateMachine,
        strategy: StrategyParams,
        symbol_filters: SymbolFilters,
        emit_log: Callable[..., None],
        emit_trade_row: TradeRowFn,
        emit_exposure: Callable[[bool], None],
    ) -> None:
        self._execution = execution
        self._state_machine = state_machine
        self._strategy = strategy
        self._filters = symbol_filters
        self._emit_log = emit_log
        self._emit_trade_row = emit_trade_row
        self._emit_exposure = emit_exposure

        self._last_tick: Optional[dict[str, Any]] = None
        self._last_ws_tick: Optional[dict[str, Any]] = None
        self._last_http_tick: Optional[dict[str, Any]] = None
        self._last_tick_source: Optional[str] = None
        self._tick_history: Deque[tuple[float, float]] = deque(maxlen=500)
        self._prev_ws_mid: Optional[float] = None
        self._last_ws_mid: Optional[float] = None
        self._cycle_start: Optional[datetime] = None
        self._entry_mid: Optional[float] = None
        self._entry_qty: Optional[float] = None
        self._entry_tick_count = 0
        self._detect_start_ts: Optional[float] = None
        self._winner_side: Optional[str] = None
        self._loser_side: Optional[str] = None
        self._detect_mid: Optional[float] = None
        self._loser_exit_price: Optional[float] = None
        self._winner_exit_price: Optional[float] = None
        self._entry_long_price: Optional[float] = None
        self._entry_short_price: Optional[float] = None
        self._winner_raw_bps: Optional[float] = None
        self._winner_net_bps: Optional[float] = None
        self._loser_raw_bps: Optional[float] = None
        self._loser_net_bps: Optional[float] = None
        self._last_reason: Optional[str] = None
        self._ws_age_ms: Optional[float] = None
        self._last_skip_log_ts = 0.0
        self._last_skip_reason: Optional[str] = None
        self._last_ride_log_ts = 0.0
        self._exposure_open = False
        self._last_data_log_source: Optional[str] = None

    @property
    def cycle_start(self) -> Optional[datetime]:
        return self._cycle_start

    @property
    def snapshot(self) -> CycleSnapshot:
        return CycleSnapshot(
            entry_mid=self._entry_mid,
            detect_mid=self._detect_mid,
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
        )

    @property
    def exposure_open(self) -> bool:
        return self._exposure_open

    def update_strategy(self, strategy: StrategyParams) -> None:
        self._strategy = strategy

    def update_filters(self, symbol_filters: SymbolFilters) -> None:
        self._filters = symbol_filters

    def update_tick(self, payload: dict[str, Any]) -> None:
        source = str(payload.get("source", "WS"))
        rx_time_ms = payload.get("rx_time_ms")
        if rx_time_ms is None:
            rx_time_ms = time.monotonic() * 1000
            payload["rx_time_ms"] = rx_time_ms
        if source == "WS":
            self._last_ws_tick = payload
            self._last_tick = payload
            now = time.monotonic()
            mid = float(payload.get("mid", 0.0))
            if mid > 0:
                self._prev_ws_mid = self._last_ws_mid if self._last_ws_mid is not None else mid
                self._last_ws_mid = mid
                self._tick_history.append((now, mid))
            if self._state_machine.state == BotState.DETECTING:
                self._evaluate_detecting()
            elif self._state_machine.state == BotState.RIDING:
                self._evaluate_riding()
        else:
            self._last_http_tick = payload
        self._ws_age_ms = self._data_stale_ms()
        self._last_tick_source = self._current_data_source()
        self._maybe_log_data_source()

    def attempt_entry(self) -> None:
        self._check_timeouts()
        if self._state_machine.state == BotState.COOLDOWN:
            self._log_skip("cooldown")
            return
        if self._state_machine.state != BotState.ARMED:
            return
        if not self._last_ws_tick and not self._last_http_tick:
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

    def _start_cycle(self) -> None:
        if not self._state_machine.start_cycle():
            return
        tick = self._last_ws_tick or self._last_tick or {}
        self._cycle_start = datetime.now(timezone.utc)
        self._entry_mid = float(tick.get("mid", 0.0))
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
        self._entry_tick_count = len(self._tick_history)
        self._last_reason = None
        self._emit_log("TRADE", "INFO", "ENTER cycle", cycle_id=self._state_machine.cycle_id)
        if not self._enter_hedge():
            return
        self._state_machine.state = BotState.DETECTING
        self._detect_start_ts = time.monotonic()
        self._emit_log("TRADE", "INFO", "DETECT start", cycle_id=self._state_machine.cycle_id)

    def _enter_hedge(self) -> bool:
        tick = self._last_ws_tick or {}
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
        self._entry_qty = qty
        try:
            sell_mode, sell_price = self._order_params("SELL", bid, ask)
            buy_mode, buy_price = self._order_params("BUY", bid, ask)
            sell_order = self._place_order(
                "SELL",
                qty,
                sell_mode,
                price=sell_price,
                side_effect_type="AUTO_BORROW",
            )
            if not sell_order:
                self._log_partial("partial_hedge_entry", qty)
                self._set_error("partial_hedge_entry")
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
            self._set_error("partial_hedge_entry")
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
            self._log_partial("partial_hedge_entry", qty)
            return False
        buy_fill = self._wait_for_fill_with_fallback("BUY", buy_id, qty, buy_mode, buy_price)
        sell_fill = self._wait_for_fill_with_fallback(
            "SELL",
            sell_id,
            qty,
            sell_mode,
            sell_price,
            side_effect_type="AUTO_BORROW",
        )
        if not buy_fill or not sell_fill:
            self._log_partial("partial_hedge_entry", qty)
            return False
        self._entry_long_price = buy_fill[0]
        self._entry_short_price = sell_fill[0]
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
        return True

    def _evaluate_detecting(self) -> None:
        if not self._entry_mid or not self._detect_start_ts:
            return
        elapsed = (time.monotonic() - self._detect_start_ts) * 1000
        if elapsed > self._strategy.detect_timeout_ms:
            self._no_winner_exit("timeout")
            return
        ticks_seen = len(self._tick_history) - self._entry_tick_count
        if ticks_seen < self._strategy.detect_window_ticks:
            return
        tick = self._last_tick or {}
        mid_now = float(tick.get("mid", 0.0))
        if mid_now <= 0:
            return
        long_raw = (mid_now - self._entry_mid) / self._entry_mid * 10_000
        short_raw = (self._entry_mid - mid_now) / self._entry_mid * 10_000
        winner_threshold = self._strategy.winner_threshold_bps
        if max(long_raw, short_raw) < winner_threshold:
            return
        if long_raw >= short_raw:
            self._winner_side = "LONG"
            self._loser_side = "SHORT"
        else:
            self._winner_side = "SHORT"
            self._loser_side = "LONG"
        self._detect_mid = mid_now
        self._winner_raw_bps = max(long_raw, short_raw)
        self._winner_net_bps = self._winner_raw_bps - self._strategy.fee_total_bps
        self._emit_log(
            "TRADE",
            "INFO",
            "DETECT winner",
            winner=self._winner_side,
            loser=self._loser_side,
            detect_mid=f"{mid_now:.2f}",
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
            self._loser_side, self._entry_mid or 0.0, self._loser_exit_price
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
        self._state_machine.state = BotState.RIDING
        self._evaluate_riding()

    def _evaluate_riding(self) -> None:
        if self._state_machine.state != BotState.RIDING:
            return
        if not self._last_tick or not self._entry_mid or not self._winner_side:
            return
        tick = self._last_tick
        mid_now = float(tick.get("mid", 0.0))
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
            self._last_ride_log_ts = now
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
            self._winner_side, self._entry_mid or 0.0, self._winner_exit_price
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
        self._emit_trade_summary(note=note)
        self._exposure_open = False
        self._emit_exposure(False)
        self._settle_borrow()
        self._state_machine.finish_cycle()

    def _no_winner_exit(self, reason: str) -> None:
        self._emit_log("TRADE", "INFO", "DETECT no winner", reason=reason)
        self._emergency_flatten(reason=reason)
        self._last_reason = reason
        self._state_machine.finish_cycle()

    def _log_partial(self, reason: str, qty: float) -> None:
        self._emit_log(
            "ERROR",
            "ERROR",
            "Partial hedge on entry",
            reason=reason,
            qty=qty,
            error=self._execution.last_error,
            error_code=self._execution.last_error_code,
        )
        self._last_reason = reason
        self._emergency_flatten(reason=reason)
        self._set_error(reason)

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
        self._emit_trade_row(
            {
                "ts": now,
                "cycle_id": self._state_machine.cycle_id,
                "phase": "cycle_summary",
                "side": f"{winner_side}/{loser_side}",
                "qty": self._entry_qty,
                "entry_price": self._entry_mid,
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

    def _entry_filter_reason(self) -> Optional[str]:
        metrics = self._compute_entry_metrics()
        if metrics["data_stale"]:
            return "data_stale"
        if metrics["spread_bps"] > self._strategy.max_spread_bps:
            return "spread"
        if metrics["tick_rate"] < self._strategy.min_tick_rate:
            return "tick_rate"
        if metrics["impulse_bps"] < self._strategy.impulse_min_bps:
            return "impulse"
        if metrics["mid"] > 0 and not self._leverage_ok(metrics["mid"]):
            return "leverage_limit"
        return None

    def _compute_entry_metrics(self) -> dict[str, float | bool]:
        tick = self._last_ws_tick or self._last_http_tick or {}
        bid = float(tick.get("bid", 0.0))
        ask = float(tick.get("ask", 0.0))
        mid = float(tick.get("mid", 0.0))
        spread = float(tick.get("spread_bps", 0.0))
        now = time.monotonic()
        cutoff = now - 1.0
        ticks = [tick_data for tick_data in self._tick_history if tick_data[0] >= cutoff]
        tick_rate = len(ticks) / 1.0 if ticks else 0.0
        impulse_bps = 0.0
        if self._prev_ws_mid and self._last_ws_mid and self._prev_ws_mid > 0:
            impulse_bps = abs(self._last_ws_mid - self._prev_ws_mid) / self._prev_ws_mid * 10_000
        ws_age_ms = self._data_stale_ms()
        data_source = self._current_data_source()
        data_stale = ws_age_ms > 500 or data_source != "WS"
        return {
            "spread_bps": spread,
            "tick_rate": tick_rate,
            "impulse_bps": impulse_bps,
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "ws_age_ms": ws_age_ms,
            "data_stale": data_stale,
        }

    def _data_stale_ms(self) -> float:
        tick = self._last_ws_tick or {}
        rx_time_ms = tick.get("rx_time_ms")
        if isinstance(rx_time_ms, (int, float)):
            return max(0.0, time.monotonic() * 1000 - rx_time_ms)
        return 9999.0

    def _round_step(self, qty: float) -> float:
        step = self._filters.step_size
        if step <= 0:
            return qty
        return float(int(qty / step) * step)

    def _calc_raw_bps(self, side: str, from_mid: float, to_mid: float) -> float:
        if from_mid <= 0 or to_mid <= 0:
            return 0.0
        if side == "LONG":
            return (to_mid - from_mid) / from_mid * 10_000
        return (from_mid - to_mid) / from_mid * 10_000

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
        return self._execution.place_order(
            side,
            qty,
            order_mode,
            price=price,
            side_effect_type=side_effect_type,
            time_in_force=time_in_force,
        )

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
        market_order = self._execution.place_order(
            side,
            qty,
            "market",
            side_effect_type=side_effect_type,
        )
        if not market_order:
            return None
        return self._execution.wait_for_fill(market_order.get("orderId"), timeout_s=3.0)

    def _current_data_source(self) -> str:
        ws_age_ms = self._data_stale_ms()
        if self._last_ws_tick and ws_age_ms <= 500:
            return "WS"
        if self._last_http_tick:
            return "HTTP"
        return "NONE"

    def _maybe_log_data_source(self) -> None:
        source = self._current_data_source()
        if self._last_data_log_source == source:
            return
        self._last_data_log_source = source
        last_ws_time = None
        last_http_time = None
        if self._last_ws_tick:
            last_ws_time = self._last_ws_tick.get("rx_time")
        if self._last_http_tick:
            last_http_time = self._last_http_tick.get("rx_time")
        self._emit_log(
            "INFO",
            "INFO",
            "DATA source update",
            source=source,
            ws_age_ms=self._data_stale_ms(),
            last_ws_tick_time=last_ws_time,
            last_http_tick_time=last_http_time,
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
        if open_orders:
            return True
        margin_account = self._execution.get_margin_account()
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

    def _emergency_flatten(self, reason: str) -> None:
        self._last_reason = reason
        self._execution.cancel_open_orders()
        margin_account = self._execution.get_margin_account() or {}
        assets = margin_account.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        if btc:
            btc_free = float(btc.get("free", 0.0))
            if btc_free > 0:
                self._execution.place_order(
                    "SELL",
                    btc_free,
                    "market",
                )
            btc_borrowed = float(btc.get("borrowed", 0.0))
            if btc_borrowed > 0:
                self._execution.place_order(
                    "BUY",
                    btc_borrowed,
                    "market",
                    side_effect_type="AUTO_REPAY",
                )
                self._execution.repay_asset("BTC", btc_borrowed)
        if usdt:
            usdt_borrowed = float(usdt.get("borrowed", 0.0))
            if usdt_borrowed > 0:
                self._execution.repay_asset("USDT", usdt_borrowed)
        self._emit_log(
            "ERROR",
            "ERROR",
            "EMERGENCY FLATTEN",
            reason=reason,
            btc_free=float(btc.get("free", 0.0)) if btc else 0.0,
            btc_borrowed=float(btc.get("borrowed", 0.0)) if btc else 0.0,
            usdt_borrowed=float(usdt.get("borrowed", 0.0)) if usdt else 0.0,
            error=self._execution.last_error,
            error_code=self._execution.last_error_code,
        )
        self._exposure_open = False
        self._emit_exposure(False)

    def _settle_borrow(self) -> None:
        margin_account = self._execution.get_margin_account() or {}
        assets = margin_account.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        if btc:
            btc_borrowed = float(btc.get("borrowed", 0.0))
            if btc_borrowed > 0:
                self._execution.repay_asset("BTC", btc_borrowed)
        if usdt:
            usdt_borrowed = float(usdt.get("borrowed", 0.0))
            if usdt_borrowed > 0:
                self._execution.repay_asset("USDT", usdt_borrowed)

    def _check_timeouts(self) -> None:
        if self._state_machine.state == BotState.DETECTING:
            self._evaluate_detecting()

    def _log_skip(self, reason: str) -> None:
        now = time.monotonic()
        if reason == self._last_skip_reason and now - self._last_skip_log_ts < 0.5:
            return
        self._last_skip_log_ts = now
        self._last_skip_reason = reason
        self._last_reason = reason
        metrics = self._compute_entry_metrics()
        self._emit_log(
            "TRADE",
            "INFO",
            "SKIP entry",
            reason=reason,
            spread_bps=f"{metrics['spread_bps']:.2f}",
            spread_limit=self._strategy.max_spread_bps,
            tick_rate=f"{metrics['tick_rate']:.2f}",
            tick_rate_limit=self._strategy.min_tick_rate,
            impulse_bps=f"{metrics['impulse_bps']:.2f}",
            impulse_limit=self._strategy.impulse_min_bps,
            mid=f"{metrics['mid']:.2f}",
            bid=f"{metrics['bid']:.2f}",
            ask=f"{metrics['ask']:.2f}",
            ws_age_ms=metrics["ws_age_ms"],
            data_stale=metrics["data_stale"],
        )
