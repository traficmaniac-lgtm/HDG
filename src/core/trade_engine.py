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
    log = Signal(str)
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
            self.log.emit("Не удалось загрузить информацию биржи")
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
            self.log.emit("Не удалось загрузить стакан")
            return
        self.depth_snapshot.emit(depth)

    @Slot()
    def fetch_http_fallback(self) -> None:
        data = self._http_fallback.get_book_ticker("BTCUSDT")
        if data:
            self.tick_update.emit(data)

    @Slot()
    def start_cycle(self) -> None:
        if not self._state_machine.start_cycle():
            return
        if not self._connected:
            self._state_machine.set_error("not_connected")
            self._emit_cycle_state()
            return
        if not self._last_tick:
            self._state_machine.set_error("no_tick")
            self._emit_cycle_state()
            return
        self._rest_client.cancel_open_orders("BTCUSDT")
        if not self._impulse_ok():
            self.log.emit("Пропуск входа: причина=flat/spread/tick_rate")
            self._state_machine.stop()
            self._emit_cycle_state()
            return
        if not self._margin_permission_ok:
            self._state_machine.set_error("margin_permission_missing")
            self._emit_cycle_state()
            return
        if self._has_open_exposure():
            self.log.emit("ЕСТЬ ОТКРЫТАЯ ПОЗИЦИЯ")
            self._state_machine.set_error("open_exposure")
            self._emit_cycle_state()
            return
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
        if not buy_order or not sell_order:
            self._emergency_flatten(reason="partial_hedge")
            self._state_machine.set_error("partial_hedge")
            self._emit_cycle_state()
            return False
        buy_fill = self._wait_for_fill(buy_order.get("orderId"))
        sell_fill = self._wait_for_fill(sell_order.get("orderId"))
        if not buy_fill or not sell_fill:
            self._emergency_flatten(reason="partial_hedge")
            self._state_machine.set_error("partial_hedge")
            self._emit_cycle_state()
            return False
        self._entry_price_long = buy_fill[0]
        self._entry_price_short = sell_fill[0]
        self._exposure_open = True
        self._emit_exposure_status()
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

    def _impulse_ok(self) -> bool:
        if not self._last_tick:
            return False
        spread = float(self._last_tick.get("spread_bps", 0.0))
        if spread > self._strategy.max_spread_bps:
            return False
        now = time.monotonic()
        cutoff = now - 0.5
        ticks = [tick for tick in self._tick_history if tick[0] >= cutoff]
        tick_rate = len(ticks) / 0.5 if ticks else 0.0
        if tick_rate < self._strategy.min_tick_rate:
            return False
        impulse_cutoff = now - 0.4
        first_tick = next((t for t in ticks if t[0] >= impulse_cutoff), None)
        if not first_tick:
            return False
        start_mid = first_tick[1]
        end_mid = ticks[-1][1]
        if start_mid <= 0:
            return False
        impulse_bps = abs(end_mid / start_mid - 1) * 10_000
        return impulse_bps >= self._strategy.impulse_min_bps

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
        self.log.emit(f"АВАРИЙНОЕ ЗАКРЫТИЕ: {reason}")
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
                self.log.emit(f"Фриз GUI ({lag:.2f}s)\n{dump}")

    def _dump_threads(self) -> str:
        buffer = io.StringIO()
        buffer.write("Thread dump:\n")
        for thread in threading.enumerate():
            buffer.write(f"- {thread.name} (alive={thread.is_alive()})\n")
        buffer.write("\nStack:\n")
        faulthandler.dump_traceback(file=buffer)
        return buffer.getvalue()
