from __future__ import annotations

import faulthandler
import io
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Optional

from PySide6.QtCore import QObject, Signal, Slot

from src.core.models import StrategyParams
from src.core.state_machine import BotState, BotStateMachine
from src.services.binance_rest import BinanceRestClient
from src.services.http_fallback import HttpFallback


@dataclass
class SymbolFilters:
    step_size: float = 0.0
    min_qty: float = 0.0
    min_notional: float = 0.0


class TradeEngine(QObject):
    log = Signal(dict)
    status_update = Signal(dict)
    balance_update = Signal(dict)
    depth_snapshot = Signal(dict)
    tick_update = Signal(dict)
    trade_row = Signal(dict)
    cycle_update = Signal(dict)
    connection_checked = Signal(dict)
    exposure_update = Signal(dict)

    def __init__(self) -> None:
        super().__init__()
        self._rest_client = BinanceRestClient()
        self._http_fallback = HttpFallback()
        self._state_machine = BotStateMachine()
        self._strategy = StrategyParams()
        self._symbol_filters = SymbolFilters()
        self._connected = False
        self._margin_permission_ok = False
        self._margin_btc_free = 0.0
        self._margin_btc_borrowed = 0.0
        self._last_tick: Optional[dict[str, Any]] = None
        self._tick_history: Deque[tuple[float, float]] = deque(maxlen=200)
        self._cycle_start: Optional[datetime] = None
        self._entry_price_long: Optional[float] = None
        self._entry_price_short: Optional[float] = None
        self._entry_qty: Optional[float] = None
        self._winner_side: Optional[str] = None
        self._loser_side: Optional[str] = None
        self._loser_exit_price: Optional[float] = None
        self._winner_exit_price: Optional[float] = None
        self._exposure_open = False
        self._entry_tick_count = 0
        self._last_skip_log_ts = 0.0
        self._last_skip_reason: Optional[str] = None
        self._watchdog_stop = threading.Event()
        self._ui_heartbeat = time.monotonic()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, name="gui-watchdog", daemon=True
        )
        self._watchdog_thread.start()

    def close(self) -> None:
        self._watchdog_stop.set()
        self._http_fallback.close()
        self._rest_client.close()

    @Slot(float)
    def update_ui_heartbeat(self, timestamp: float) -> None:
        self._ui_heartbeat = timestamp

    @Slot(dict)
    def on_tick(self, payload: dict) -> None:
        self._last_tick = payload
        now = time.monotonic()
        mid = float(payload.get("mid", 0.0))
        if mid > 0:
            self._tick_history.append((now, mid))
        self.tick_update.emit(payload)
        if self._state_machine.state == BotState.HOLD_WINNER:
            self._evaluate_winner_hold()

    @Slot(dict)
    def set_strategy(self, payload: dict) -> None:
        self._strategy = StrategyParams(**payload)
        self._emit_cycle_state()

    @Slot(dict)
    def set_connection(self, payload: dict) -> None:
        api_key = payload.get("api_key", "")
        api_secret = payload.get("api_secret", "")
        self._rest_client.close()
        self._rest_client = BinanceRestClient(api_key=api_key, api_secret=api_secret)
        self._connected = bool(api_key and api_secret)
        if self._connected:
            self._state_machine.connect_ok()
        else:
            self._state_machine.disconnect()
        self._emit_cycle_state()

    @Slot(bool)
    def validate_access(self, test_only: bool = False) -> None:
        account = self._rest_client.get_spot_account()
        margin_account = self._rest_client.get_margin_account()
        ok = bool(account and margin_account)
        self._margin_permission_ok = bool(margin_account)
        self.connection_checked.emit(
            {
                "ok": ok,
                "margin_permission_ok": self._margin_permission_ok,
                "spot_account": account,
                "margin_account": margin_account,
                "test_only": test_only,
                "error": self._rest_client.last_error,
            }
        )

    @Slot()
    def refresh_balance(self) -> None:
        spot_account = self._rest_client.get_spot_account()
        margin_account = self._rest_client.get_margin_account()
        self._margin_permission_ok = bool(margin_account)
        payload = {
            "spot_account": spot_account,
            "margin_account": margin_account,
            "margin_permission_ok": self._margin_permission_ok,
            "error": self._rest_client.last_error,
        }
        if margin_account:
            assets = margin_account.get("userAssets", [])
            btc = next((item for item in assets if item.get("asset") == "BTC"), None)
            if btc:
                self._margin_btc_free = float(btc.get("free", 0.0))
                self._margin_btc_borrowed = float(btc.get("borrowed", 0.0))
        self.balance_update.emit(payload)
        self._emit_exposure_status()

    @Slot()
    def load_exchange_info(self) -> None:
        info = self._rest_client.get_exchange_info("BTCUSDT")
        if not info:
            self._emit_log("ERROR", "ERROR", "Не удалось загрузить информацию биржи")
            return
        symbols = info.get("symbols", [])
        if not symbols:
            return
        filters = symbols[0].get("filters", [])
        lot_filter = next(
            (item for item in filters if item.get("filterType") == "LOT_SIZE"), None
        )
        min_notional_filter = next(
            (item for item in filters if item.get("filterType") == "MIN_NOTIONAL"),
            None,
        )
        if lot_filter:
            try:
                self._symbol_filters.step_size = float(lot_filter.get("stepSize", 0.0))
                self._symbol_filters.min_qty = float(lot_filter.get("minQty", 0.0))
            except ValueError:
                self._symbol_filters.step_size = 0.0
        if min_notional_filter:
            try:
                self._symbol_filters.min_notional = float(
                    min_notional_filter.get("minNotional", 0.0)
                )
            except ValueError:
                self._symbol_filters.min_notional = 0.0
        self.status_update.emit({"exchange_info": info})

    @Slot()
    def load_orderbook_snapshot(self) -> None:
        depth = self._rest_client.get_depth("BTCUSDT", limit=20)
        if not depth:
            self._emit_log("ERROR", "ERROR", "Не удалось загрузить стакан")
            return
        self.depth_snapshot.emit(depth)

    @Slot()
    def fetch_http_fallback(self) -> None:
        data = self._http_fallback.get_book_ticker("BTCUSDT")
        if data:
            self.tick_update.emit(data)

    @Slot()
    def attempt_entry(self) -> None:
        if self._state_machine.state == BotState.COOLDOWN:
            self._log_skip("cooldown")
            return
        if self._state_machine.state != BotState.READY:
            return
        if (
            not self._connected
            or not self._margin_permission_ok
            or not self._last_tick
            or self._symbol_filters.step_size <= 0
            or self._symbol_filters.min_qty <= 0
        ):
            self._log_skip("not_ready")
            return
        if self._exposure_open or self._margin_btc_free > 0 or self._margin_btc_borrowed > 0:
            self._log_skip("exposure")
            return
        reason = self._entry_filter_reason()
        if reason:
            self._log_skip(reason)
            return
        self._start_cycle()

    def _start_cycle(self) -> None:
        if not self._state_machine.start_cycle():
            return
        self._rest_client.cancel_open_orders("BTCUSDT")
        self._cycle_start = datetime.now(timezone.utc)
        self._entry_price_long = None
        self._entry_price_short = None
        self._entry_qty = None
        self._winner_side = None
        self._loser_side = None
        self._loser_exit_price = None
        self._winner_exit_price = None
        self._entry_tick_count = len(self._tick_history)
        self._emit_cycle_state()
        self._emit_log("TRADE", "INFO", "ENTER cycle", cycle_id=self._state_machine.cycle_id)
        if not self._enter_hedge():
            return
        self._state_machine.state = BotState.DETECTING
        self._emit_cycle_state()
        self._detect_winner()

    @Slot()
    def stop(self) -> None:
        self._state_machine.stop()
        self._emit_cycle_state()

    @Slot()
    def emergency_flatten(self) -> None:
        self._emergency_flatten(reason="manual")

    @Slot()
    def end_cooldown(self) -> None:
        self._state_machine.end_cooldown()
        self._emit_cycle_state()

    def _enter_hedge(self) -> bool:
        tick = self._last_tick or {}
        mid = float(tick.get("mid", 0.0))
        if mid <= 0:
            self._state_machine.set_error("bad_mid")
            self._emit_cycle_state()
            return False
        qty = self._strategy.usd_notional / mid
        qty = self._round_step(qty)
        if qty <= 0:
            self._state_machine.set_error("qty_zero")
            self._emit_cycle_state()
            return False
        if self._symbol_filters.min_qty and qty < self._symbol_filters.min_qty:
            self._state_machine.set_error("min_qty")
            self._emit_cycle_state()
            return False
        if self._symbol_filters.min_notional and qty * mid < self._symbol_filters.min_notional:
            self._state_machine.set_error("min_notional")
            self._emit_cycle_state()
            return False
        self._entry_qty = qty
        with ThreadPoolExecutor(max_workers=2) as executor:
            buy_future = executor.submit(self._place_order_margin_threadsafe, "BUY", qty)
            sell_future = executor.submit(self._place_order_margin_threadsafe, "SELL", qty)
            try:
                buy_order = buy_future.result(timeout=2.0)
                sell_order = sell_future.result(timeout=2.0)
            except Exception:
                buy_order = None
                sell_order = None
        buy_id = buy_order.get("orderId") if buy_order else None
        sell_id = sell_order.get("orderId") if sell_order else None
        self._emit_log(
            "TRADE",
            "INFO",
            "ENTER sent both orders",
            buy_id=buy_id,
            sell_id=sell_id,
            qty=qty,
            type="MARKET",
        )
        if not buy_order or not sell_order:
            self._emit_log(
                "TRADE",
                "INFO",
                "ENTER result",
                long="FILLED" if buy_order else "REJECTED",
                short="FILLED" if sell_order else "REJECTED",
                reason="partial_hedge",
            )
            self._emit_log(
                "ERROR",
                "ERROR",
                "Partial hedge on entry (order rejected)",
                buy_order=bool(buy_order),
                sell_order=bool(sell_order),
                buy_id=buy_id,
                sell_id=sell_id,
                qty=qty,
                error=self._rest_client.last_error,
                error_code=self._rest_client.last_error_code,
            )
            self._emergency_flatten(reason="partial_hedge")
            self._state_machine.set_error("partial_hedge")
            self._emit_cycle_state()
            return False
        buy_fill = self._wait_for_fill(buy_id)
        sell_fill = self._wait_for_fill(sell_id)
        if not buy_fill or not sell_fill:
            self._emit_log(
                "TRADE",
                "INFO",
                "ENTER result",
                long="FILLED" if buy_fill else "REJECTED",
                short="FILLED" if sell_fill else "REJECTED",
                reason="partial_hedge",
            )
            self._emit_log(
                "ERROR",
                "ERROR",
                "Partial hedge on entry (fill missing)",
                buy_filled=bool(buy_fill),
                sell_filled=bool(sell_fill),
                buy_id=buy_id,
                sell_id=sell_id,
                qty=qty,
                error=self._rest_client.last_error,
                error_code=self._rest_client.last_error_code,
            )
            self._emergency_flatten(reason="partial_hedge")
            self._state_machine.set_error("partial_hedge")
            self._emit_cycle_state()
            return False
        self._entry_price_long = buy_fill[0]
        self._entry_price_short = sell_fill[0]
        self._exposure_open = True
        self._emit_exposure_status()
        self._emit_log(
            "TRADE",
            "INFO",
            "ENTER filled",
            buy_price=self._entry_price_long,
            sell_price=self._entry_price_short,
            qty=qty,
        )
        self._emit_cycle_state()
        return True

    def _detect_winner(self) -> None:
        start = time.monotonic()
        timeout_s = self._strategy.detect_timeout_ms / 1000
        window_bps = max(1.0, 0.25 * self._strategy.max_loss_bps)
        winner_threshold = max(self._strategy.winner_threshold_bps_raw, window_bps)
        while time.monotonic() - start < timeout_s:
            if not self._last_tick or not self._entry_price_long or not self._entry_price_short:
                time.sleep(0.05)
                continue
            if len(self._tick_history) - self._entry_tick_count < (
                self._strategy.direction_detect_window_ticks
            ):
                time.sleep(0.05)
                continue
            mid = float(self._last_tick.get("mid", 0.0))
            entry_mid = (self._entry_price_long + self._entry_price_short) / 2
            if entry_mid <= 0:
                continue
            d_bps = (mid / entry_mid - 1) * 10_000
            if d_bps >= winner_threshold:
                self._winner_side = "LONG"
                self._loser_side = "SHORT"
                break
            if d_bps <= -winner_threshold:
                self._winner_side = "SHORT"
                self._loser_side = "LONG"
                break
            time.sleep(0.05)
        if not self._winner_side:
            self._emergency_flatten(reason="no_impulse")
            self._state_machine.finish_cycle()
            self._emit_cycle_state()
            return
        self._state_machine.state = BotState.CUT_LOSER
        self._emit_cycle_state()
        self._emit_log(
            "TRADE",
            "INFO",
            "DETECT winner",
            winner=self._winner_side,
            loser=self._loser_side,
        )
        self._cut_loser()

    def _cut_loser(self) -> None:
        if not self._entry_qty or not self._loser_side:
            self._state_machine.set_error("missing_loser")
            self._emit_cycle_state()
            return
        side = "BUY" if self._loser_side == "SHORT" else "SELL"
        order = self._place_order_margin(side, self._entry_qty)
        if not order:
            self._emergency_flatten(reason="cut_loser_failed")
            self._state_machine.set_error("cut_loser_failed")
            self._emit_cycle_state()
            return
        fill = self._wait_for_fill(order.get("orderId"))
        if not fill:
            self._emergency_flatten(reason="cut_loser_timeout")
            self._state_machine.set_error("cut_loser_timeout")
            self._emit_cycle_state()
            return
        self._loser_exit_price = fill[0]
        self._emit_log(
            "TRADE",
            "INFO",
            "CUT loser filled",
            loser_side=self._loser_side,
            exit_price=self._loser_exit_price,
        )
        self._state_machine.state = BotState.HOLD_WINNER
        self._emit_cycle_state()
        self._evaluate_winner_hold()

    def _evaluate_winner_hold(self) -> None:
        if self._state_machine.state != BotState.HOLD_WINNER:
            return
        if not self._last_tick or not self._entry_qty:
            return
        winner_side = self._winner_side or ""
        entry_price = (
            self._entry_price_long if winner_side == "LONG" else self._entry_price_short
        )
        if not entry_price:
            return
        current_price = float(self._last_tick.get("mid", 0.0))
        winner_raw_bps = self._calc_raw_bps(winner_side, entry_price, current_price)
        winner_net_bps = winner_raw_bps - self._strategy.fee_total_bps
        if winner_raw_bps <= -self._strategy.emergency_stop_bps:
            self._state_machine.state = BotState.EXIT_WINNER
            self._emit_cycle_state()
            self._exit_winner(note="emergency_stop")
            return
        if winner_net_bps >= self._strategy.target_net_bps:
            self._state_machine.state = BotState.EXIT_WINNER
            self._emit_cycle_state()
            self._exit_winner(note="target")

    def _exit_winner(self, note: str) -> None:
        if not self._entry_qty or not self._winner_side:
            return
        side = "SELL" if self._winner_side == "LONG" else "BUY"
        order = self._place_order_margin(side, self._entry_qty)
        if not order:
            self._emergency_flatten(reason="exit_winner_failed")
            self._state_machine.set_error("exit_winner_failed")
            self._emit_cycle_state()
            return
        fill = self._wait_for_fill(order.get("orderId"))
        if not fill:
            self._emergency_flatten(reason="exit_winner_timeout")
            self._state_machine.set_error("exit_winner_timeout")
            self._emit_cycle_state()
            return
        self._winner_exit_price = fill[0]
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
        self._emit_exposure_status()
        self._state_machine.finish_cycle()
        self._emit_cycle_state()

    def _emit_trade_summary(self, note: str) -> None:
        if not self._cycle_start or not self._entry_qty:
            return
        now = datetime.now(timezone.utc)
        duration_ms = int((now - self._cycle_start).total_seconds() * 1000)
        long_entry = self._entry_price_long or 0.0
        short_entry = self._entry_price_short or 0.0
        winner_side = self._winner_side or "—"
        loser_side = self._loser_side or "—"
        loser_exit = self._loser_exit_price or 0.0
        winner_exit = self._winner_exit_price or 0.0
        if winner_side == "LONG":
            winner_raw = self._calc_raw_bps("LONG", long_entry, winner_exit)
        else:
            winner_raw = self._calc_raw_bps("SHORT", short_entry, winner_exit)
        if loser_side == "LONG":
            loser_raw = self._calc_raw_bps("LONG", long_entry, loser_exit)
        else:
            loser_raw = self._calc_raw_bps("SHORT", short_entry, loser_exit)
        winner_net = winner_raw - self._strategy.fee_total_bps
        loser_net = loser_raw - self._strategy.fee_total_bps
        net_total = winner_net + loser_net
        net_usd = (self._strategy.usd_notional / 10_000) * net_total
        self.trade_row.emit(
            {
                "ts": now,
                "cycle_id": self._state_machine.cycle_id,
                "phase": "cycle_summary",
                "side": f"{winner_side}/{loser_side}",
                "qty": self._entry_qty,
                "entry_price": (long_entry + short_entry) / 2,
                "exit_price": (winner_exit + loser_exit) / 2,
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

    def _place_order_margin(self, side: str, qty: float) -> Optional[dict[str, Any]]:
        payload = {
            "symbol": "BTCUSDT",
            "side": side,
            "type": "MARKET",
            "quantity": f"{qty:.6f}",
        }
        return self._rest_client.place_order_margin(payload)

    def _place_order_margin_threadsafe(
        self, side: str, qty: float
    ) -> Optional[dict[str, Any]]:
        client = BinanceRestClient(
            api_key=self._rest_client.api_key,
            api_secret=self._rest_client.api_secret,
            spot_base_url=self._rest_client.spot_base_url,
        )
        payload = {
            "symbol": "BTCUSDT",
            "side": side,
            "type": "MARKET",
            "quantity": f"{qty:.6f}",
        }
        order = client.place_order_margin(payload)
        client.close()
        return order

    def _wait_for_fill(self, order_id: Optional[int]) -> Optional[tuple[float, float]]:
        if not order_id:
            return None
        timeout_s = 2.0
        start = time.monotonic()
        while time.monotonic() - start < timeout_s:
            order = self._rest_client.get_order("BTCUSDT", int(order_id))
            if order and order.get("status") == "FILLED":
                executed_qty = float(order.get("executedQty", 0.0) or 0.0)
                cumm_quote = float(order.get("cummulativeQuoteQty", 0.0) or 0.0)
                if executed_qty > 0 and cumm_quote > 0:
                    return cumm_quote / executed_qty, executed_qty
                break
            if order and order.get("status") in {"CANCELED", "REJECTED", "EXPIRED"}:
                return None
            time.sleep(0.1)
        return None

    def _entry_filter_reason(self) -> Optional[str]:
        metrics = self._compute_entry_metrics()
        if metrics["spread_bps"] > self._strategy.max_spread_bps:
            return "spread"
        if metrics["tick_rate"] < self._strategy.min_tick_rate:
            return "tick_rate"
        if metrics["impulse_bps"] < self._strategy.impulse_min_bps:
            return "impulse"
        return None

    def _has_open_exposure(self) -> bool:
        open_orders = self._rest_client.get_open_margin_orders("BTCUSDT") or []
        if open_orders:
            return True
        margin_account = self._rest_client.get_margin_account()
        if not margin_account:
            return False
        assets = margin_account.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        if btc:
            btc_free = float(btc.get("free", 0.0))
            btc_borrowed = float(btc.get("borrowed", 0.0))
            if btc_free > 0 or btc_borrowed > 0:
                return True
        if usdt:
            usdt_borrowed = float(usdt.get("borrowed", 0.0))
            if usdt_borrowed > 0:
                return True
        return False

    def _emergency_flatten(self, reason: str) -> None:
        self._rest_client.cancel_open_orders("BTCUSDT")
        margin_account = self._rest_client.get_margin_account() or {}
        assets = margin_account.get("userAssets", [])
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        if btc:
            btc_free = float(btc.get("free", 0.0))
            if btc_free > 0:
                self._place_order_margin("SELL", btc_free)
            btc_borrowed = float(btc.get("borrowed", 0.0))
            if btc_borrowed > 0:
                self._rest_client.repay_margin_asset("BTC", btc_borrowed)
        if usdt:
            usdt_borrowed = float(usdt.get("borrowed", 0.0))
            if usdt_borrowed > 0:
                self._rest_client.repay_margin_asset("USDT", usdt_borrowed)
        self._emit_log("TRADE", "INFO", "ACTION: EMERGENCY FLATTEN", reason=reason)
        self._emit_log(
            "ERROR",
            "ERROR",
            "EMERGENCY FLATTEN",
            reason=reason,
            btc_free=float(btc.get("free", 0.0)) if btc else 0.0,
            btc_borrowed=float(btc.get("borrowed", 0.0)) if btc else 0.0,
            usdt_borrowed=float(usdt.get("borrowed", 0.0)) if usdt else 0.0,
            error=self._rest_client.last_error,
            error_code=self._rest_client.last_error_code,
        )
        self._exposure_open = False
        self._emit_exposure_status()

    def _calc_raw_bps(self, side: str, entry: float, exit_price: float) -> float:
        if entry <= 0 or exit_price <= 0:
            return 0.0
        if side == "LONG":
            return (exit_price / entry - 1) * 10_000
        return (entry / exit_price - 1) * 10_000

    def _round_step(self, qty: float) -> float:
        step = self._symbol_filters.step_size
        if step <= 0:
            return qty
        return float(int(qty / step) * step)

    def _emit_cycle_state(self) -> None:
        self.cycle_update.emit(
            {
                "state": self._state_machine.state.value,
                "active_cycle": self._state_machine.active_cycle,
                "cycle_id": self._state_machine.cycle_id,
                "start_time": self._cycle_start,
                "entry_price_long": self._entry_price_long,
                "entry_price_short": self._entry_price_short,
                "winner_side": self._winner_side or "—",
                "loser_side": self._loser_side or "—",
            }
        )

    def _emit_exposure_status(self) -> None:
        exposure = self._exposure_open or self._margin_btc_free > 0 or self._margin_btc_borrowed > 0
        self.exposure_update.emit({"open_exposure": exposure})

    def _watchdog_loop(self) -> None:
        while not self._watchdog_stop.is_set():
            time.sleep(2.0)
            lag = time.monotonic() - self._ui_heartbeat
            if lag > 2.0:
                dump = self._dump_threads()
                self._emit_log("ERROR", "ERROR", "Фриз GUI", lag_s=f"{lag:.2f}", dump=dump)

    def _dump_threads(self) -> str:
        buffer = io.StringIO()
        buffer.write("Thread dump:\n")
        for thread in threading.enumerate():
            buffer.write(f"- {thread.name} (alive={thread.is_alive()})\n")
        buffer.write("\nStack:\n")
        faulthandler.dump_traceback(file=buffer)
        return buffer.getvalue()

    def _emit_log(self, category: str, level: str, message: str, **fields: object) -> None:
        self.log.emit(
            {
                "category": category,
                "level": level,
                "message": message,
                "fields": fields,
            }
        )

    def _log_skip(self, reason: str) -> None:
        now = time.monotonic()
        if reason == self._last_skip_reason and now - self._last_skip_log_ts < 0.5:
            return
        self._last_skip_log_ts = now
        self._last_skip_reason = reason
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
        )

    def _compute_entry_metrics(self) -> dict[str, float]:
        tick = self._last_tick or {}
        spread = float(tick.get("spread_bps", 0.0))
        bid = float(tick.get("bid", 0.0))
        ask = float(tick.get("ask", 0.0))
        mid = float(tick.get("mid", 0.0))
        now = time.monotonic()
        cutoff = now - 0.5
        ticks = [tick_data for tick_data in self._tick_history if tick_data[0] >= cutoff]
        tick_rate = len(ticks) / 0.5 if ticks else 0.0
        impulse_cutoff = now - 0.4
        first_tick = next((t for t in ticks if t[0] >= impulse_cutoff), None)
        impulse_bps = 0.0
        if first_tick and ticks:
            start_mid = first_tick[1]
            end_mid = ticks[-1][1]
            if start_mid > 0:
                impulse_bps = abs(end_mid / start_mid - 1) * 10_000
        rx_time = tick.get("rx_time")
        ws_age_ms = 0.0
        if isinstance(rx_time, datetime):
            ws_age_ms = (datetime.now(timezone.utc) - rx_time).total_seconds() * 1000
        return {
            "spread_bps": spread,
            "tick_rate": tick_rate,
            "impulse_bps": impulse_bps,
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "ws_age_ms": ws_age_ms,
        }
