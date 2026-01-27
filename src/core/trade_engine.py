from __future__ import annotations

import sys
import threading
import time
import traceback
from datetime import datetime, timezone

from PySide6.QtCore import QObject, Signal, Slot

from src.core.models import MarketDataMode, StrategyParams, SymbolFilters
from src.core.state_machine import BotStateMachine
from src.engine.directional_cycle import DirectionalCycle
from src.exchange.binance_margin import BinanceMarginExecution
from src.services.binance_rest import BinanceRestClient
from src.services.http_fallback import HttpFallback
from src.services.market_data import MarketDataService


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

    def __init__(self, market_data: MarketDataService | None = None) -> None:
        super().__init__()
        self._rest_client = BinanceRestClient()
        self._http_fallback = HttpFallback()
        self._market_data = market_data or MarketDataService()
        self._state_machine = BotStateMachine()
        self._strategy = StrategyParams()
        self._symbol_filters = SymbolFilters()
        self._data_mode = MarketDataMode.HYBRID
        self._ws_status = "DISCONNECTED"
        self._connected = False
        self._margin_permission_ok = False
        self._margin_btc_free = 0.0
        self._margin_btc_borrowed = 0.0
        self._margin_exec = BinanceMarginExecution(self._rest_client)
        self._cycle = DirectionalCycle(
            execution=self._margin_exec,
            state_machine=self._state_machine,
            strategy=self._strategy,
            symbol_filters=self._symbol_filters,
            market_data=self._market_data,
            emit_log=self._emit_log,
            emit_trade_row=self._handle_trade_row,
            emit_exposure=self._emit_exposure_status,
        )
        self._cycle.update_data_mode(self._data_mode)
        self._cycle.update_ws_status(self._ws_status)
        self._watchdog_stop = threading.Event()
        self._auto_loop = False
        self._stop_requested = False
        self._cycles_done = 0
        self._max_cycles = self._strategy.max_cycles
        self._heartbeat_lock = threading.Lock()
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
        with self._heartbeat_lock:
            self._ui_heartbeat = timestamp

    @Slot(dict)
    def on_tick(self, payload: dict) -> None:
        self._cycle.update_tick(payload)
        self.tick_update.emit(payload)
        self._emit_cycle_state()

    @Slot(dict)
    def set_strategy(self, payload: dict) -> None:
        self._strategy = StrategyParams(**payload)
        self._cycle.update_strategy(self._strategy)
        self._max_cycles = self._strategy.max_cycles
        self._emit_cycle_state()

    @Slot(dict)
    def set_connection(self, payload: dict) -> None:
        api_key = payload.get("api_key", "")
        api_secret = payload.get("api_secret", "")
        self._rest_client.close()
        self._rest_client = BinanceRestClient(api_key=api_key, api_secret=api_secret)
        self._margin_exec = BinanceMarginExecution(self._rest_client)
        self._cycle = DirectionalCycle(
            execution=self._margin_exec,
            state_machine=self._state_machine,
            strategy=self._strategy,
            symbol_filters=self._symbol_filters,
            market_data=self._market_data,
            emit_log=self._emit_log,
            emit_trade_row=self._handle_trade_row,
            emit_exposure=self._emit_exposure_status,
        )
        self._cycle.update_data_mode(self._data_mode)
        self._cycle.update_ws_status(self._ws_status)
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
        self._emit_exposure_status(self._cycle.exposure_open)

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
        price_filter = next(
            (item for item in filters if item.get("filterType") == "PRICE_FILTER"), None
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
        if price_filter:
            try:
                self._symbol_filters.tick_size = float(price_filter.get("tickSize", 0.0))
            except ValueError:
                self._symbol_filters.tick_size = 0.0
        self._cycle.update_filters(self._symbol_filters)
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
            self._market_data.update_tick(data)
            self.on_tick(data)

    @Slot(str)
    def set_data_mode(self, mode: str) -> None:
        try:
            self._data_mode = MarketDataMode(mode)
        except ValueError:
            self._data_mode = MarketDataMode.HYBRID
        self._cycle.update_data_mode(self._data_mode)
        self._emit_cycle_state()

    @Slot(str)
    def set_ws_status(self, status: str) -> None:
        self._ws_status = status
        self._cycle.update_ws_status(status)

    @Slot()
    def attempt_entry(self) -> None:
        if not self._connected or not self._margin_permission_ok:
            return
        if not self._auto_loop or self._stop_requested:
            return
        self._cycle.attempt_entry()
        self._emit_cycle_state()

    @Slot()
    def start_trading(self) -> None:
        self._auto_loop = True
        self._stop_requested = False
        self._cycles_done = 0
        self._cycle.arm()
        self._emit_cycle_state()

    @Slot()
    def stop(self) -> None:
        self._auto_loop = False
        self._stop_requested = True
        self._cycle.stop()
        self._emit_cycle_state()

    @Slot()
    def emergency_flatten(self) -> None:
        self._cycle.emergency_flatten(reason="manual")
        self._emit_cycle_state()

    @Slot(bool)
    def end_cooldown(self, auto_resume: bool) -> None:
        self._cycle.end_cooldown(auto_resume)
        self._emit_cycle_state()

    def _emit_cycle_state(self) -> None:
        snapshot = self._cycle.snapshot
        self.cycle_update.emit(
            {
                "state": self._state_machine.state.value,
                "active_cycle": self._state_machine.active_cycle,
                "cycle_id": self._state_machine.cycle_id,
                "start_time": self._cycle.cycle_start,
                "entry_mid": snapshot.entry_mid,
                "detect_mid": snapshot.detect_mid,
                "exit_mid": snapshot.exit_mid,
                "entry_price_long": snapshot.entry_long_price,
                "entry_price_short": snapshot.entry_short_price,
                "winner_side": snapshot.winner_side or "—",
                "loser_side": snapshot.loser_side or "—",
                "winner_raw_bps": snapshot.winner_raw_bps,
                "winner_net_bps": snapshot.winner_net_bps,
                "loser_raw_bps": snapshot.loser_raw_bps,
                "loser_net_bps": snapshot.loser_net_bps,
                "reason": snapshot.reason,
                "ws_age_ms": snapshot.ws_age_ms,
                "http_age_ms": snapshot.http_age_ms,
                "effective_source": snapshot.effective_source,
                "effective_age_ms": snapshot.effective_age_ms,
                "data_stale": snapshot.data_stale,
                "waiting_for_data": snapshot.waiting_for_data,
            }
        )

    def _emit_exposure_status(self, exposure_open: bool) -> None:
        exposure = exposure_open or self._margin_btc_free > 0 or self._margin_btc_borrowed > 0
        self.exposure_update.emit({"open_exposure": exposure})

    def _handle_trade_row(self, payload: dict) -> None:
        self.trade_row.emit(payload)
        if payload.get("phase") != "cycle_summary":
            return
        self._cycles_done += 1
        if self._max_cycles > 0 and self._cycles_done >= self._max_cycles:
            self._emit_log(
                "INFO",
                "INFO",
                "[ENGINE] max_cycles reached -> stop",
                cycles_done=self._cycles_done,
                max_cycles=self._max_cycles,
            )
            self.stop()

    def _watchdog_loop(self) -> None:
        while not self._watchdog_stop.is_set():
            time.sleep(2.0)
            with self._heartbeat_lock:
                heartbeat = self._ui_heartbeat
            lag = time.monotonic() - heartbeat
            if lag > 2.0:
                dump = self._dump_threads()
                self._emit_log("ERROR", "ERROR", "Фриз GUI", lag_s=f"{lag:.2f}", dump=dump)

    def _dump_threads(self) -> str:
        buffer_lines = ["Thread dump:"]
        frames = sys._current_frames()
        for thread in threading.enumerate():
            buffer_lines.append(f"- {thread.name} (alive={thread.is_alive()})")
            frame = frames.get(thread.ident)
            if frame is None:
                buffer_lines.append("  <no frame available>")
                continue
            stack = traceback.format_stack(frame)
            buffer_lines.append("  Stack:")
            buffer_lines.extend(f"    {line.rstrip()}" for line in stack)
            buffer_lines.append("")
        return "\n".join(buffer_lines)

    def _emit_log(self, category: str, level: str, message: str, **fields: object) -> None:
        self.log.emit(
            {
                "category": category,
                "level": level,
                "message": message,
                "fields": fields,
            }
        )
