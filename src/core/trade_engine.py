from __future__ import annotations

import sys
import threading
import time
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

from PySide6.QtCore import QObject, Signal, Slot

from src.core.models import CycleViewModel, MarketDataMode, StrategyParams, SymbolFilters
from src.core.state_machine import BotState, BotStateMachine
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
    cycle_updated = Signal(object)
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
        self._trade_gate: Optional[str] = None
        self._not_authorized_abort_done = False
        self._cycle = DirectionalCycle(
            execution=self._margin_exec,
            state_machine=self._state_machine,
            strategy=self._strategy,
            symbol_filters=self._symbol_filters,
            market_data=self._market_data,
            emit_log=self._emit_log,
            emit_trade_row=self._handle_trade_row,
            emit_exposure=self._emit_exposure_status,
            on_not_authorized=self._abort_not_authorized,
        )
        self._cycle.update_data_mode(self._data_mode)
        self._cycle.update_ws_status(self._ws_status)
        self._watchdog_stop = threading.Event()
        self._auto_loop = False
        self._stop_requested = False
        self._cycles_done = 0
        self._max_cycles = self._strategy.max_cycles
        self._last_cycle_telemetry: Optional[dict] = None
        self._last_cycle_emit_ts = 0.0
        self._last_cycle_emit_state = self._state_machine.state
        self._last_active_cycle = False
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
        self._trade_gate = None
        self._not_authorized_abort_done = False
        self._last_cycle_telemetry = None
        self._last_active_cycle = False
        self._cycle = DirectionalCycle(
            execution=self._margin_exec,
            state_machine=self._state_machine,
            strategy=self._strategy,
            symbol_filters=self._symbol_filters,
            market_data=self._market_data,
            emit_log=self._emit_log,
            emit_trade_row=self._handle_trade_row,
            emit_exposure=self._emit_exposure_status,
            on_not_authorized=self._abort_not_authorized,
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

    def fetch_open_orders(self, symbol: str) -> list[dict]:
        return self._rest_client.get_open_margin_orders(symbol) or []

    def fetch_position_state(self, symbol: str) -> dict[str, float]:
        base_asset = symbol.replace("USDT", "")
        margin_account = self._rest_client.get_margin_account() or {}
        assets = margin_account.get("userAssets", [])
        asset = next((item for item in assets if item.get("asset") == base_asset), None)
        net_asset = float(asset.get("netAsset", 0.0)) if asset else 0.0
        long_qty = max(net_asset, 0.0)
        short_qty = max(-net_asset, 0.0)
        return {"long_qty": long_qty, "short_qty": short_qty, "net_qty": net_asset}

    def is_flat(self, symbol: str, tolerance: float = 0.0) -> bool:
        open_orders = self.fetch_open_orders(symbol)
        pos = self.fetch_position_state(symbol)
        net_qty = pos.get("net_qty", 0.0)
        return not open_orders and abs(net_qty) <= tolerance

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
        if self._trade_gate == "ERROR_NOT_AUTHORIZED":
            return
        if not self._auto_loop or self._stop_requested:
            return
        self._cycle.attempt_entry()
        self._emit_cycle_state()

    @Slot()
    def start_trading(self) -> None:
        if self._trade_gate == "ERROR_NOT_AUTHORIZED":
            return
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

    @Slot()
    def emergency_test(self) -> None:
        self._cycle.emergency_test()
        self._emit_cycle_state()

    @Slot(bool)
    def end_cooldown(self, auto_resume: bool) -> None:
        self._cycle.end_cooldown(auto_resume)
        self._emit_cycle_state()

    def _emit_cycle_state(self, force: bool = False) -> None:
        state = self._state_machine.state
        active_cycle = self._state_machine.active_cycle
        now = time.monotonic()
        state_changed = state != self._last_cycle_emit_state
        if not force:
            if state in {BotState.DETECTING, BotState.RIDING} and not state_changed:
                if now - self._last_cycle_emit_ts < 0.25:
                    return
            elif not state_changed and now - self._last_cycle_emit_ts < 0.1:
                return
        telemetry = self._cycle.build_telemetry(state, active_cycle, self._state_machine.last_error)
        telemetry_dict = asdict(telemetry)
        if not active_cycle and self._last_active_cycle:
            self._last_cycle_telemetry = telemetry_dict
        if not active_cycle and self._last_cycle_telemetry:
            display_telemetry = self._last_cycle_telemetry
        else:
            display_telemetry = telemetry_dict
        snapshot = self._cycle.snapshot
        self.cycle_update.emit(
            {
                "state": state.value,
                "active_cycle": active_cycle,
                "cycle_id": self._state_machine.cycle_id,
                "start_time": self._cycle.cycle_start,
                "telemetry": display_telemetry,
                "last_cycle": self._last_cycle_telemetry,
                "data_stale": snapshot.data_stale,
                "waiting_for_data": snapshot.waiting_for_data,
                "effective_age_ms": snapshot.effective_age_ms,
            }
        )
        view_model = CycleViewModel(
            state=state.value,
            cycle_id=self._state_machine.cycle_id,
            active_cycle=active_cycle,
            inflight_entry=telemetry.inflight_entry,
            inflight_exit=telemetry.inflight_exit,
            open_orders_count=telemetry.open_orders_count,
            pos_long_qty=telemetry.pos_long_qty,
            pos_short_qty=telemetry.pos_short_qty,
            last_action=telemetry.last_action,
            start_ts=self._cycle.cycle_start,
            duration_s=telemetry.duration_s,
            entry_mid=telemetry.entry_mid,
            last_mid=telemetry.last_mid,
            exit_mid=telemetry.exit_mid,
            impulse_bps=telemetry.impulse_bps,
            tick_rate=telemetry.tick_rate,
            spread_bps=telemetry.spread_bps,
            ws_age_ms=telemetry.ws_age_ms,
            effective_source=telemetry.effective_source,
            effective_age_ms=snapshot.effective_age_ms,
            long_raw_bps=telemetry.long_raw_bps,
            short_raw_bps=telemetry.short_raw_bps,
            winner=telemetry.winner_side,
            loser=telemetry.loser_side,
            reason=telemetry.reason,
            target_net_bps=telemetry.target_net_bps,
            fee_bps=telemetry.fee_total_bps,
            max_loss_bps=telemetry.max_loss_bps,
            detect_window_ticks=telemetry.detect_window_ticks,
            detect_timeout_ms=telemetry.detect_timeout_ms,
            cooldown_s=telemetry.cooldown_s,
            result=telemetry.result,
            pnl_usdt=telemetry.pnl_usdt,
            net_bps_total=telemetry.total_net_bps,
            data_stale=snapshot.data_stale,
            waiting_for_data=snapshot.waiting_for_data,
        )
        self.cycle_updated.emit(view_model)
        self._last_cycle_emit_ts = now
        self._last_cycle_emit_state = state
        self._last_active_cycle = active_cycle

    def _emit_exposure_status(self, exposure_open: bool) -> None:
        exposure = exposure_open or self._margin_btc_free > 0 or self._margin_btc_borrowed > 0
        self.exposure_update.emit({"open_exposure": exposure})

    def _abort_not_authorized(self, context: str) -> None:
        if self._not_authorized_abort_done:
            return
        self._not_authorized_abort_done = True
        self._trade_gate = "ERROR_NOT_AUTHORIZED"
        self._auto_loop = False
        self._stop_requested = True
        self._margin_permission_ok = False
        path = self._rest_client.last_error_path or "<unknown>"
        params = self._rest_client.last_error_params or {}
        safe_params = {
            key: value
            for key, value in params.items()
            if str(key).lower() not in {"signature", "api_key", "apikey"}
        }
        print(f"\033[31m[NOT_AUTHORIZED] endpoint={path} params={safe_params}\033[0m")
        self.balance_update.emit(
            {
                "spot_account": None,
                "margin_account": None,
                "margin_permission_ok": False,
                "error": self._rest_client.last_error,
            }
        )
        self._emit_log(
            "ERROR",
            "ERROR",
            "API key not authorized for MARGIN endpoint",
            context=context,
            endpoint=path,
            params=safe_params,
            guidance="Enable Margin + IP whitelist",
        )
        self._state_machine.set_error("ERROR_NOT_AUTHORIZED")
        self._emit_cycle_state()

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
