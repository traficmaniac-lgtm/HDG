from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QSpinBox,
    QDoubleSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QCheckBox,
)

from src.core.config_store import SettingsStore
from src.core.logger import QtLogEmitter, setup_logger
from src.core.models import ConnectionMode, ConnectionSettings, MarketTick, StrategyParams
from src.core.state_machine import BotState, BotStateMachine
from src.gui.settings_tab import SettingsTab
from src.gui.widgets import make_card
from src.services.binance_rest import BinanceRestClient
from src.services.http_fallback import HttpFallback
from src.services.orderbook import OrderBook
from src.services.ws_market import MarketDataThread

DEBUG = os.getenv("DEBUG", "false").lower() == "true"


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.version = "0.2.0"
        self.setWindowTitle(f"Directional Hedge Scalper v{self.version} — BTCUSDT")
        self.setMinimumSize(1200, 720)

        self.settings_store = SettingsStore()
        self.connection_settings = self.settings_store.load()
        self.strategy_params = StrategyParams()
        self.state_machine = BotStateMachine()
        self.market_tick = MarketTick()
        self.connected = False
        self.data_source = "WS"
        self.ws_status = "DISCONNECTED"
        self.cycle_start_time: Optional[datetime] = None

        self.sim_entry_price: Optional[float] = None
        self.sim_entry_qty: Optional[float] = None
        self.sim_exit_price: Optional[float] = None
        self.sim_raw_bps: Optional[float] = None
        self.sim_net_bps: Optional[float] = None
        self.sim_condition: str = "—"

        self.log_emitter = QtLogEmitter()
        self.logger = setup_logger(Path("logs"), self.log_emitter)
        self.logger.info("[APP] version=0.2.0")

        self.ws_thread = MarketDataThread("btcusdt")
        self.ws_thread.price_update.connect(self.on_price_update)
        self.ws_thread.depth_update.connect(self.on_depth_update)
        self.ws_thread.status_update.connect(self.on_ws_status)

        self.http_client = HttpFallback()
        self.rest_client = BinanceRestClient()

        self.http_timer = QTimer(self)
        self.http_timer.setInterval(1000)
        self.http_timer.timeout.connect(self.fetch_http_fallback)

        self.balance_timer = QTimer(self)
        self.balance_timer.setInterval(7000)
        self.balance_timer.timeout.connect(self.refresh_balance)

        self.cooldown_timer = QTimer(self)
        self.cooldown_timer.setSingleShot(True)
        self.cooldown_timer.timeout.connect(self.on_cooldown_complete)

        self.orderbook = OrderBook()
        self.orderbook_ready = False
        self.last_balance_update: Optional[datetime] = None
        self.account_permissions = "—"

        self._build_ui()
        self._wire_signals()
        self._initialize_defaults()
        self._load_settings_into_form()
        self._update_ui_state()

        self.log_emitter.message.connect(self.append_log)

    def closeEvent(self, event) -> None:
        self.logger.info("Shutting down GUI")
        self.stop_ws_thread()
        self.http_client.close()
        self.rest_client.close()
        event.accept()

    def _build_ui(self) -> None:
        root = QWidget()
        layout = QVBoxLayout(root)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        layout.addWidget(self._build_status_bar())

        body = QHBoxLayout()
        body.setSpacing(12)
        body.addWidget(self._build_left_panel())
        body.addWidget(self._build_right_tabs(), stretch=1)

        layout.addLayout(body)
        self.setCentralWidget(root)

    def _build_status_bar(self) -> QWidget:
        bar = QFrame()
        bar.setFrameShape(QFrame.StyledPanel)
        bar.setStyleSheet(
            "QFrame { background-color: #151515; border: 1px solid #2a2a2a; border-radius: 4px; }"
        )
        layout = QHBoxLayout(bar)
        layout.setContentsMargins(10, 6, 10, 6)

        self.connection_label = QLabel("Connection: DISCONNECTED")
        self.data_label = QLabel("Data: WS")
        self.tick_age_label = QLabel("Tick age: — ms")
        self.last_update_label = QLabel("Last update: —")

        layout.addWidget(self.connection_label)
        layout.addSpacing(12)
        layout.addWidget(self.data_label)
        layout.addSpacing(12)
        layout.addWidget(self.tick_age_label)
        layout.addStretch(1)
        layout.addWidget(self.last_update_label)
        return bar

    def _build_left_panel(self) -> QWidget:
        wrapper = QWidget()
        wrapper.setFixedWidth(340)
        layout = QVBoxLayout(wrapper)
        layout.setSpacing(12)

        layout.addWidget(make_card("Market", self._build_market_card()))
        layout.addWidget(make_card("Account", self._build_account_card()))
        layout.addWidget(make_card("Controls", self._build_controls_card()))
        layout.addStretch(1)
        return wrapper

    def _build_market_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        self.symbol_combo = QComboBox()
        self.symbol_combo.addItems(["BTCUSDT"])
        layout.addWidget(QLabel("Symbol"))
        layout.addWidget(self.symbol_combo)

        self.price_source_label = QLabel("Source: Auto (WS→HTTP)")
        layout.addWidget(self.price_source_label)

        self.mid_label = QLabel("—")
        self.mid_label.setFont(QFont("Arial", 24, QFont.Bold))
        self.mid_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.mid_label)

        self.bid_label = QLabel("Bid: —")
        self.ask_label = QLabel("Ask: —")
        self.spread_label = QLabel("Spread: — bps")
        self.card_tick_age_label = QLabel("Tick age: — ms")
        layout.addWidget(self.bid_label)
        layout.addWidget(self.ask_label)
        layout.addWidget(self.spread_label)
        layout.addWidget(self.card_tick_age_label)
        return widget

    def _build_account_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        self.account_connection_label = QLabel("Connection: DISCONNECTED")
        self.account_data_label = QLabel("Data: —")
        self.balance_label = QLabel("USDT Balance: —")
        self.permissions_label = QLabel("Permissions: —")
        self.balance_updated_label = QLabel("Updated: —")
        self.balance_updated_label.setStyleSheet("color: #8c8c8c;")

        layout.addWidget(self.account_connection_label)
        layout.addWidget(self.account_data_label)
        layout.addWidget(self.balance_label)
        layout.addWidget(self.permissions_label)
        layout.addWidget(self.balance_updated_label)
        return widget

    def _build_controls_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        arm_layout = QHBoxLayout()
        self.arm_button = QPushButton("ARM")
        self.disarm_button = QPushButton("DISARM")
        arm_layout.addWidget(self.arm_button)
        arm_layout.addWidget(self.disarm_button)
        layout.addLayout(arm_layout)

        cycle_layout = QHBoxLayout()
        self.start_cycle_button = QPushButton("START CYCLE")
        self.stop_button = QPushButton("STOP")
        cycle_layout.addWidget(self.start_cycle_button)
        cycle_layout.addWidget(self.stop_button)
        layout.addLayout(cycle_layout)

        self.emergency_button = QPushButton("EMERGENCY FLATTEN")
        layout.addWidget(self.emergency_button)

        self.entry_allowed_label = QLabel("entry allowed: —")
        layout.addWidget(self.entry_allowed_label)

        self.dev_end_cycle_button = QPushButton("End cycle (dev)")
        if not DEBUG:
            self.dev_end_cycle_button.hide()
        layout.addWidget(self.dev_end_cycle_button)
        return widget

    def _build_right_tabs(self) -> QWidget:
        tabs = QTabWidget()
        tabs.addTab(self._build_trading_tab(), "Trading Window")
        tabs.addTab(self._build_parameters_tab(), "Parameters")
        tabs.addTab(self._build_trades_tab(), "Trades")
        tabs.addTab(self._build_logs_tab(), "Logs")
        tabs.addTab(self._build_stats_tab(), "Statistics")
        self.settings_tab = SettingsTab()
        tabs.addTab(self.settings_tab, "Settings")
        return tabs

    def _build_trading_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(12)

        self.state_panel = QFrame()
        self.state_panel.setFrameShape(QFrame.StyledPanel)
        state_layout = QGridLayout(self.state_panel)
        state_layout.setContentsMargins(10, 10, 10, 10)

        self.state_label = QLabel("State: IDLE")
        self.active_cycle_label = QLabel("active_cycle: false")
        self.cycle_id_label = QLabel("cycle_id: 0")
        self.cycle_start_label = QLabel("start_time: —")
        self.cycle_elapsed_label = QLabel("elapsed_s: —")

        state_layout.addWidget(self.state_label, 0, 0)
        state_layout.addWidget(self.active_cycle_label, 0, 1)
        state_layout.addWidget(self.cycle_id_label, 0, 2)
        state_layout.addWidget(self.cycle_start_label, 1, 0)
        state_layout.addWidget(self.cycle_elapsed_label, 1, 1)

        self.bps_panel = QFrame()
        self.bps_panel.setFrameShape(QFrame.StyledPanel)
        bps_layout = QGridLayout(self.bps_panel)
        bps_layout.setContentsMargins(10, 10, 10, 10)

        self.raw_bps_label = QLabel("raw_bps: —")
        self.net_bps_label = QLabel("net_bps: —")
        self.fee_label = QLabel("fee_total_bps: —")
        self.target_label = QLabel("target_net_bps: —")
        self.max_loss_label = QLabel("max_loss_bps: —")

        bps_layout.addWidget(self.raw_bps_label, 0, 0)
        bps_layout.addWidget(self.net_bps_label, 0, 1)
        bps_layout.addWidget(self.fee_label, 1, 0)
        bps_layout.addWidget(self.target_label, 1, 1)
        bps_layout.addWidget(self.max_loss_label, 1, 2)

        self.filters_panel = QFrame()
        self.filters_panel.setFrameShape(QFrame.StyledPanel)
        filters_layout = QGridLayout(self.filters_panel)
        filters_layout.setContentsMargins(10, 10, 10, 10)

        self.max_spread_label = QLabel("max_spread_bps: —")
        self.min_volume_label = QLabel("min_volume_threshold: —")
        self.cooldown_label = QLabel("cooldown_s: —")

        filters_layout.addWidget(self.max_spread_label, 0, 0)
        filters_layout.addWidget(self.min_volume_label, 0, 1)
        filters_layout.addWidget(self.cooldown_label, 0, 2)

        self.sim_panel = QFrame()
        self.sim_panel.setFrameShape(QFrame.StyledPanel)
        sim_layout = QGridLayout(self.sim_panel)
        sim_layout.setContentsMargins(10, 10, 10, 10)

        self.sim_side_label = QLabel("sim_side: —")
        self.sim_notional_label = QLabel("usd_notional: —")
        self.sim_entry_label = QLabel("entry_price: —")
        self.sim_exit_label = QLabel("exit_mark_price: —")
        self.sim_raw_label = QLabel("raw_bps: —")
        self.sim_net_label = QLabel("net_bps: —")
        self.sim_condition_label = QLabel("condition: —")

        sim_layout.addWidget(self.sim_side_label, 0, 0)
        sim_layout.addWidget(self.sim_notional_label, 0, 1)
        sim_layout.addWidget(self.sim_entry_label, 1, 0)
        sim_layout.addWidget(self.sim_exit_label, 1, 1)
        sim_layout.addWidget(self.sim_raw_label, 2, 0)
        sim_layout.addWidget(self.sim_net_label, 2, 1)
        sim_layout.addWidget(self.sim_condition_label, 2, 2)

        layout.addWidget(self.state_panel)
        layout.addWidget(self.bps_panel)
        layout.addWidget(self.filters_panel)
        layout.addWidget(self.sim_panel)
        layout.addStretch(1)
        return widget

    def _build_parameters_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)

        self.order_mode_combo = QComboBox()
        self.order_mode_combo.addItems(["market", "aggressive_limit"])

        self.sim_side_combo = QComboBox()
        self.sim_side_combo.addItems(["BUY", "SELL"])

        self.usd_notional_spin = QDoubleSpinBox()
        self.usd_notional_spin.setRange(1.0, 1000.0)
        self.usd_notional_spin.setDecimals(2)

        self.max_loss_spin = QSpinBox()
        self.max_loss_spin.setRange(3, 6)

        self.fee_spin = QDoubleSpinBox()
        self.fee_spin.setRange(0.0, 20.0)
        self.fee_spin.setDecimals(2)

        self.target_net_spin = QSpinBox()
        self.target_net_spin.setRange(8, 15)

        self.max_spread_spin = QDoubleSpinBox()
        self.max_spread_spin.setRange(0.1, 50.0)
        self.max_spread_spin.setDecimals(2)

        self.cooldown_spin = QSpinBox()
        self.cooldown_spin.setRange(2, 5)

        self.direction_window_combo = QComboBox()
        self.direction_window_combo.addItems(["1", "2", "3"])

        self.burst_volume_spin = QDoubleSpinBox()
        self.burst_volume_spin.setRange(0.0, 1_000_000.0)
        self.burst_volume_spin.setDecimals(2)

        self.auto_loop_checkbox = QCheckBox("auto_loop")

        form.addRow("order_mode", self.order_mode_combo)
        form.addRow("sim_side", self.sim_side_combo)
        form.addRow("usd_notional", self.usd_notional_spin)
        form.addRow("max_loss_bps", self.max_loss_spin)
        form.addRow("fee_total_bps", self.fee_spin)
        form.addRow("target_net_bps", self.target_net_spin)
        form.addRow("max_spread_bps", self.max_spread_spin)
        form.addRow("cooldown_s", self.cooldown_spin)
        form.addRow("direction_detect_window_ticks", self.direction_window_combo)
        form.addRow("burst_volume_threshold", self.burst_volume_spin)
        form.addRow("", self.auto_loop_checkbox)

        layout.addLayout(form)

        btn_layout = QHBoxLayout()
        self.apply_params_button = QPushButton("APPLY")
        self.reset_params_button = QPushButton("RESET DEFAULTS")
        btn_layout.addStretch(1)
        btn_layout.addWidget(self.apply_params_button)
        btn_layout.addWidget(self.reset_params_button)
        layout.addLayout(btn_layout)
        layout.addStretch(1)
        return widget

    def _build_trades_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self.trades_table = QTableWidget(0, 11)
        self.trades_table.setHorizontalHeaderLabels(
            [
                "ts",
                "cycle_id",
                "side",
                "entry_price",
                "exit_price",
                "raw_bps",
                "fees_bps",
                "net_bps",
                "net_usd",
                "duration_s",
                "note",
            ]
        )
        self.trades_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.trades_table)
        if DEBUG:
            self.add_test_trade_button = QPushButton("Add test row")
            layout.addWidget(self.add_test_trade_button)
        return widget

    def _build_logs_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        filter_layout = QHBoxLayout()
        self.log_filter_combo = QComboBox()
        self.log_filter_combo.addItems(["INFO", "WARN", "ERROR"])
        filter_layout.addWidget(QLabel("Filter:"))
        filter_layout.addWidget(self.log_filter_combo)
        filter_layout.addStretch(1)

        self.open_logs_button = QPushButton("Open logs folder")
        filter_layout.addWidget(self.open_logs_button)

        self.log_output = QPlainTextEdit()
        self.log_output.setReadOnly(True)

        layout.addLayout(filter_layout)
        layout.addWidget(self.log_output)
        return widget

    def _build_stats_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.addWidget(QLabel("Statistics will be available in later versions."))
        return widget

    def _wire_signals(self) -> None:
        self.settings_tab.mode_futures.toggled.connect(self.on_mode_changed)
        self.settings_tab.connect_button.clicked.connect(self.connect_to_binance)
        self.settings_tab.disconnect_button.clicked.connect(self.disconnect_from_binance)
        self.settings_tab.save_button.clicked.connect(self.save_settings)
        self.settings_tab.test_button.clicked.connect(self.test_connection)
        self.settings_tab.clear_button.clicked.connect(self.clear_settings_fields)

        self.arm_button.clicked.connect(self.on_arm)
        self.disarm_button.clicked.connect(self.on_disarm)
        self.start_cycle_button.clicked.connect(self.on_start_cycle)
        self.stop_button.clicked.connect(self.on_stop)
        self.emergency_button.clicked.connect(self.on_emergency_flatten)
        self.dev_end_cycle_button.clicked.connect(self.on_end_cycle)
        self.apply_params_button.clicked.connect(self.apply_params)
        self.reset_params_button.clicked.connect(self.reset_params)
        if DEBUG:
            self.add_test_trade_button.clicked.connect(self.add_test_trade)
        self.open_logs_button.clicked.connect(self.open_logs_folder)

    def on_mode_changed(self) -> None:
        self.settings_tab.leverage_combo.setEnabled(self.settings_tab.mode_futures.isChecked())

    def connect_to_binance(self) -> None:
        self._sync_settings_from_form()
        if not self.connection_settings.api_key or not self.connection_settings.api_secret:
            QMessageBox.warning(self, "Missing Keys", "Please enter API key and secret.")
            return

        if self.connection_settings.save_local:
            self.settings_store.save(self.connection_settings)
            self.logger.info("Settings saved to config/settings.json")

        self.rest_client.close()
        self.rest_client = BinanceRestClient(
            api_key=self.connection_settings.api_key,
            api_secret=self.connection_settings.api_secret,
        )

        if not self._validate_account_access():
            self.connected = False
            self._update_ui_state()
            return

        self.connected = True
        self.state_machine.connect_ok()
        self.load_orderbook_snapshot()
        self.start_ws_thread()
        self.balance_timer.start()
        self.logger.info("Connected (read-only) to Binance")
        self._update_ui_state()

    def disconnect_from_binance(self) -> None:
        self.connected = False
        self.state_machine.disconnect()
        self.stop_ws_thread()
        self.balance_timer.stop()
        self.orderbook = OrderBook()
        self.orderbook_ready = False
        self.logger.info("Disconnected from Binance")
        self._update_ui_state()

    def test_connection(self) -> None:
        self._sync_settings_from_form()
        if not self.connection_settings.api_key or not self.connection_settings.api_secret:
            QMessageBox.warning(self, "Missing Keys", "Please enter API key and secret.")
            return
        self.rest_client.close()
        self.rest_client = BinanceRestClient(
            api_key=self.connection_settings.api_key,
            api_secret=self.connection_settings.api_secret,
        )
        ok = self._validate_account_access(test_only=True)
        if ok:
            QMessageBox.information(self, "Connection", "Read-only access confirmed.")
        else:
            QMessageBox.warning(self, "Connection", "Failed to validate account access.")

    def save_settings(self) -> None:
        self._sync_settings_from_form()
        if self.connection_settings.save_local:
            self.settings_store.save(self.connection_settings)
            QMessageBox.information(self, "Settings", "Saved locally to config/settings.json")
            self.logger.info("Settings saved to config/settings.json")
        else:
            QMessageBox.information(self, "Settings", "Local save disabled.")

    def clear_settings_fields(self) -> None:
        self.settings_tab.api_key_input.clear()
        self.settings_tab.api_secret_input.clear()
        self.logger.info("Settings cleared")

    def on_arm(self) -> None:
        self.state_machine.arm(self.connected)
        self.logger.info("ARM requested")
        self._update_ui_state()

    def on_disarm(self) -> None:
        self.state_machine.disconnect()
        self.logger.info("DISARM requested")
        self._update_ui_state()

    def on_start_cycle(self) -> None:
        if not self._entry_allowed():
            QMessageBox.warning(self, "Guard", "Entry guards blocked the cycle start.")
            return
        started = self.state_machine.start_cycle()
        if not started:
            return
        entry = self.orderbook.vwap_for_notional(
            self.strategy_params.sim_side, self.strategy_params.usd_notional
        )
        if not entry:
            QMessageBox.warning(self, "Orderbook", "Insufficient depth for simulation.")
            self.state_machine.stop()
            return
        self.sim_entry_price, self.sim_entry_qty = entry
        self.sim_exit_price = None
        self.sim_condition = "SIM ENTRY"
        self.cycle_start_time = datetime.now(timezone.utc)
        self.logger.info("Cycle started (read-only simulation)")
        self._update_sim_labels()
        self._update_ui_state()

    def on_stop(self) -> None:
        self.state_machine.stop()
        self.logger.info("Cycle stopped")
        self._update_ui_state()

    def on_emergency_flatten(self) -> None:
        self.logger.warning("EMERGENCY FLATTEN requested (stub)")

    def on_end_cycle(self) -> None:
        if self.state_machine.state != BotState.RUNNING:
            return
        self._finish_sim_cycle(note="dev")

    def on_cooldown_complete(self) -> None:
        self.state_machine.end_cooldown()
        self._update_ui_state()
        if self.strategy_params.auto_loop and self._entry_allowed():
            self.on_start_cycle()

    def apply_params(self) -> None:
        self.strategy_params.order_mode = self.order_mode_combo.currentText()
        self.strategy_params.sim_side = self.sim_side_combo.currentText()
        self.strategy_params.usd_notional = self.usd_notional_spin.value()
        self.strategy_params.max_loss_bps = self.max_loss_spin.value()
        self.strategy_params.fee_total_bps = self.fee_spin.value()
        self.strategy_params.target_net_bps = self.target_net_spin.value()
        self.strategy_params.max_spread_bps = self.max_spread_spin.value()
        self.strategy_params.cooldown_s = self.cooldown_spin.value()
        self.strategy_params.direction_detect_window_ticks = int(
            self.direction_window_combo.currentText()
        )
        self.strategy_params.burst_volume_threshold = self.burst_volume_spin.value()
        self.strategy_params.auto_loop = self.auto_loop_checkbox.isChecked()
        self.logger.info("Parameters applied")
        self._update_ui_state()

    def reset_params(self) -> None:
        self.strategy_params = StrategyParams()
        self._sync_params_to_form()
        self._update_ui_state()

    def _sync_params_to_form(self) -> None:
        self.order_mode_combo.setCurrentText(self.strategy_params.order_mode)
        self.sim_side_combo.setCurrentText(self.strategy_params.sim_side)
        self.usd_notional_spin.setValue(self.strategy_params.usd_notional)
        self.max_loss_spin.setValue(self.strategy_params.max_loss_bps)
        self.fee_spin.setValue(self.strategy_params.fee_total_bps)
        self.target_net_spin.setValue(self.strategy_params.target_net_bps)
        self.max_spread_spin.setValue(self.strategy_params.max_spread_bps)
        self.cooldown_spin.setValue(self.strategy_params.cooldown_s)
        self.direction_window_combo.setCurrentText(
            str(self.strategy_params.direction_detect_window_ticks)
        )
        self.burst_volume_spin.setValue(self.strategy_params.burst_volume_threshold)
        self.auto_loop_checkbox.setChecked(self.strategy_params.auto_loop)

    def on_price_update(self, payload: dict) -> None:
        self.market_tick.bid = payload["bid"]
        self.market_tick.ask = payload["ask"]
        self.market_tick.mid = payload["mid"]
        self.market_tick.spread_bps = payload["spread_bps"]
        self.market_tick.event_time = payload["event_time"]
        self.market_tick.rx_time = payload["rx_time"]
        self._update_market_labels()
        self._update_ui_state()

    def on_depth_update(self, payload: dict) -> None:
        self.orderbook.apply_snapshot(payload["bids"], payload["asks"])
        self.orderbook_ready = self.orderbook.is_ready()
        if self.state_machine.state == BotState.RUNNING:
            self._update_simulation_metrics()
        self._update_ui_state()

    def on_ws_status(self, status: str) -> None:
        self.ws_status = status
        self.data_source = "WS"
        if status == "DEGRADED":
            self.data_source = "HTTP"
            if not self.http_timer.isActive():
                self.http_timer.start()
        elif status == "DISCONNECTED":
            self.data_source = "HTTP"
            if not self.http_timer.isActive():
                self.http_timer.start()
        else:
            if self.http_timer.isActive():
                self.http_timer.stop()
        self._update_ui_state()

    def fetch_http_fallback(self) -> None:
        data = self.http_client.get_book_ticker("BTCUSDT")
        if not data:
            return
        self.on_price_update(data)

    def load_orderbook_snapshot(self) -> None:
        depth = self.rest_client.get_depth("BTCUSDT", limit=20)
        if not depth:
            self.logger.warning("Failed to load orderbook snapshot")
            return
        self.orderbook.apply_snapshot(depth.get("bids", []), depth.get("asks", []))
        self.orderbook_ready = self.orderbook.is_ready()

    def start_ws_thread(self) -> None:
        if self.ws_thread.isRunning():
            return
        self.ws_thread.start()

    def stop_ws_thread(self) -> None:
        if self.ws_thread.isRunning():
            self.ws_thread.stop()
            self.ws_thread.wait(2000)

    def refresh_balance(self) -> None:
        if not self.connected:
            return
        if self.connection_settings.mode == ConnectionMode.FUTURES:
            balance = self.rest_client.get_futures_balance()
            if not balance:
                self.logger.warning("Failed to refresh futures balance")
                return
            usdt = next((item for item in balance if item.get("asset") == "USDT"), None)
            if usdt:
                available = float(usdt.get("availableBalance", 0.0))
                total = float(usdt.get("balance", 0.0))
                self.balance_label.setText(
                    f"USDT Balance: {available:,.2f} free / {total:,.2f} total"
                )
        else:
            account = self.rest_client.get_spot_account()
            if not account:
                self.logger.warning("Failed to refresh spot account")
                return
            self._update_balance_from_spot_account(account)
        self.last_balance_update = datetime.now(timezone.utc)
        self._update_ui_state()

    def _update_balance_from_spot_account(self, account: dict) -> None:
        balances = account.get("balances", [])
        usdt = next((item for item in balances if item.get("asset") == "USDT"), None)
        if usdt:
            free = float(usdt.get("free", 0.0))
            locked = float(usdt.get("locked", 0.0))
            total = free + locked
            self.balance_label.setText(f"USDT Balance: {free:,.2f} free / {total:,.2f} total")

    def _update_market_labels(self) -> None:
        self.mid_label.setText(f"{self.market_tick.mid:,.2f}")
        self.bid_label.setText(f"Bid: {self.market_tick.bid:,.2f}")
        self.ask_label.setText(f"Ask: {self.market_tick.ask:,.2f}")
        self.spread_label.setText(f"Spread: {self.market_tick.spread_bps:.2f} bps")
        now = datetime.now(timezone.utc)
        age_ms = (now - self.market_tick.rx_time).total_seconds() * 1000
        self.tick_age_label.setText(f"Tick age: {age_ms:.0f} ms")
        self.card_tick_age_label.setText(f"Tick age: {age_ms:.0f} ms")
        self.last_update_label.setText(
            f"Last update: {self.market_tick.rx_time.strftime('%H:%M:%S.%f')[:-3]}"
        )

    def _update_simulation_metrics(self) -> None:
        if not self.sim_entry_price or not self.sim_entry_qty:
            return
        exit_side = "SELL" if self.strategy_params.sim_side == "BUY" else "BUY"
        exit_price = self.orderbook.vwap_for_qty(exit_side, self.sim_entry_qty)
        if not exit_price:
            return
        self.sim_exit_price = exit_price
        if self.strategy_params.sim_side == "BUY":
            raw_bps = (exit_price / self.sim_entry_price - 1) * 10_000
        else:
            raw_bps = (self.sim_entry_price / exit_price - 1) * 10_000
        net_bps = raw_bps - self.strategy_params.fee_total_bps
        self.sim_raw_bps = raw_bps
        self.sim_net_bps = net_bps
        if net_bps >= self.strategy_params.target_net_bps:
            self._finish_sim_cycle(note="SIM TARGET")
            return
        if raw_bps <= -self.strategy_params.max_loss_bps:
            self._finish_sim_cycle(note="SIM STOP")
            return
        self.sim_condition = "SIM RUNNING"
        self._update_sim_labels()

    def _finish_sim_cycle(self, note: str) -> None:
        if self.state_machine.state != BotState.RUNNING:
            return
        self.state_machine.finish_cycle()
        entry = self.sim_entry_price or self.market_tick.mid
        exit_price = self.sim_exit_price or self.market_tick.mid
        raw_bps = self.sim_raw_bps or 0.0
        net_bps = self.sim_net_bps or raw_bps - self.strategy_params.fee_total_bps
        net_usd = (net_bps / 10_000) * self.strategy_params.usd_notional
        self.add_trade_row(
            side=self.strategy_params.sim_side,
            entry_price=entry,
            exit_price=exit_price,
            raw_bps=raw_bps,
            fees_bps=self.strategy_params.fee_total_bps,
            net_bps=net_bps,
            net_usd=net_usd,
            duration_s=self._elapsed_seconds(),
            note=note,
        )
        self.sim_condition = note
        self.cooldown_timer.start(self.strategy_params.cooldown_s * 1000)
        self.logger.info(f"Cycle finished ({note})")
        self._update_sim_labels()
        self._update_ui_state()

    def _update_sim_labels(self) -> None:
        self.sim_side_label.setText(f"sim_side: {self.strategy_params.sim_side}")
        self.sim_notional_label.setText(f"usd_notional: {self.strategy_params.usd_notional:.2f}")
        entry = "—" if not self.sim_entry_price else f"{self.sim_entry_price:,.2f}"
        exit_mark = "—" if not self.sim_exit_price else f"{self.sim_exit_price:,.2f}"
        self.sim_entry_label.setText(f"entry_price: {entry}")
        self.sim_exit_label.setText(f"exit_mark_price: {exit_mark}")
        raw = "—" if self.sim_raw_bps is None else f"{self.sim_raw_bps:.2f}"
        net = "—" if self.sim_net_bps is None else f"{self.sim_net_bps:.2f}"
        self.sim_raw_label.setText(f"raw_bps: {raw}")
        self.sim_net_label.setText(f"net_bps: {net}")
        self.sim_condition_label.setText(f"condition: {self.sim_condition}")

    def _update_state_labels(self) -> None:
        self.state_label.setText(f"State: {self.state_machine.state.value}")
        self.active_cycle_label.setText(
            f"active_cycle: {str(self.state_machine.active_cycle).lower()}"
        )
        self.cycle_id_label.setText(f"cycle_id: {self.state_machine.cycle_id}")
        if self.state_machine.state == BotState.RUNNING and self.cycle_start_time:
            start_str = self.cycle_start_time.strftime("%H:%M:%S")
            self.cycle_start_label.setText(f"start_time: {start_str}")
            self.cycle_elapsed_label.setText(f"elapsed_s: {self._elapsed_seconds():.1f}")
        else:
            self.cycle_start_label.setText("start_time: —")
            self.cycle_elapsed_label.setText("elapsed_s: —")

    def _update_ui_state(self) -> None:
        connection_text = "CONNECTED" if self.connected else "DISCONNECTED"
        self.connection_label.setText(f"Connection: {connection_text}")
        self.account_connection_label.setText(f"Connection: {connection_text}")
        if self.connected:
            self.connection_label.setStyleSheet("color: #27ae60;")
            self.account_connection_label.setStyleSheet("color: #27ae60;")
        else:
            self.connection_label.setStyleSheet("color: #e74c3c;")
            self.account_connection_label.setStyleSheet("color: #e74c3c;")

        if self.ws_status == "CONNECTED":
            data_status = "WS OK"
            color = "color: #27ae60;"
        elif self.ws_status == "DEGRADED":
            data_status = "WS DEGRADED"
            color = "color: #f1c40f;"
        else:
            data_status = "HTTP"
            color = "color: #f1c40f;"
        self.data_label.setText(f"Data: {data_status}")
        self.data_label.setStyleSheet(color)
        self.account_data_label.setText(f"Data: {data_status}")
        self.account_data_label.setStyleSheet(color)

        self.settings_tab.connect_button.setEnabled(not self.connected)
        self.settings_tab.disconnect_button.setEnabled(self.connected)
        self.arm_button.setEnabled(self.connected)
        self.disarm_button.setEnabled(self.connected)
        self.start_cycle_button.setEnabled(self._entry_allowed())
        self.stop_button.setEnabled(self.state_machine.state in {BotState.RUNNING, BotState.COOLDOWN})

        self.price_source_label.setText("Source: Auto (WS→HTTP)")
        self.fee_label.setText(f"fee_total_bps: {self.strategy_params.fee_total_bps:.2f}")
        self.target_label.setText(f"target_net_bps: {self.strategy_params.target_net_bps}")
        self.max_loss_label.setText(f"max_loss_bps: {self.strategy_params.max_loss_bps}")
        self.max_spread_label.setText(f"max_spread_bps: {self.strategy_params.max_spread_bps}")
        self.min_volume_label.setText(
            f"min_volume_threshold: {self.strategy_params.burst_volume_threshold:.2f}"
        )
        self.cooldown_label.setText(f"cooldown_s: {self.strategy_params.cooldown_s}")
        self.permissions_label.setText(f"Permissions: {self.account_permissions}")
        if self.last_balance_update:
            self.balance_updated_label.setText(
                f"Updated: {self.last_balance_update.strftime('%H:%M:%S')}"
            )

        entry_allowed = self._entry_allowed()
        self.entry_allowed_label.setText(f"entry allowed: {str(entry_allowed).lower()}")
        self.entry_allowed_label.setStyleSheet(
            "color: #27ae60;" if entry_allowed else "color: #e74c3c;"
        )
        self._update_state_labels()
        self._update_sim_labels()

    def append_log(self, message: str) -> None:
        filter_level = self.log_filter_combo.currentText()
        expected = "WARNING" if filter_level == "WARN" else filter_level
        if f"| {expected} |" not in message:
            return
        self.log_output.appendPlainText(message)

    def open_logs_folder(self) -> None:
        logs_path = Path("logs").resolve()
        QMessageBox.information(self, "Logs", f"Logs folder: {logs_path}")

    def add_trade_row(
        self,
        side: str,
        entry_price: float,
        exit_price: float,
        raw_bps: float,
        fees_bps: float,
        net_bps: float,
        net_usd: float,
        duration_s: float,
        note: str,
    ) -> None:
        row = self.trades_table.rowCount()
        self.trades_table.insertRow(row)
        values = [
            datetime.now(timezone.utc).strftime("%H:%M:%S"),
            str(self.state_machine.cycle_id),
            side,
            f"{entry_price:.2f}",
            f"{exit_price:.2f}",
            f"{raw_bps:.2f}",
            f"{fees_bps:.2f}",
            f"{net_bps:.2f}",
            f"{net_usd:.2f}",
            f"{duration_s:.1f}",
            note,
        ]
        for col, value in enumerate(values):
            self.trades_table.setItem(row, col, QTableWidgetItem(value))

    def add_test_trade(self) -> None:
        self.add_trade_row(
            side="TEST",
            entry_price=65000.0,
            exit_price=65010.0,
            raw_bps=1.5,
            fees_bps=self.strategy_params.fee_total_bps,
            net_bps=1.5 - self.strategy_params.fee_total_bps,
            net_usd=0.1,
            duration_s=1.0,
            note="debug",
        )

    def _elapsed_seconds(self) -> float:
        if not self.cycle_start_time:
            return 0.0
        return (datetime.now(timezone.utc) - self.cycle_start_time).total_seconds()

    def _initialize_defaults(self) -> None:
        self._sync_params_to_form()
        self.cycle_start_time = None
        self.sim_entry_price = None
        self.sim_entry_qty = None
        self.sim_exit_price = None
        self.sim_raw_bps = None
        self.sim_net_bps = None

    def _sync_settings_from_form(self) -> None:
        self.connection_settings.api_key = self.settings_tab.api_key_input.text().strip()
        self.connection_settings.api_secret = self.settings_tab.api_secret_input.text().strip()
        self.connection_settings.mode = (
            ConnectionMode.FUTURES
            if self.settings_tab.mode_futures.isChecked()
            else ConnectionMode.MARGIN
        )
        self.connection_settings.leverage = int(self.settings_tab.leverage_combo.currentText()[0])
        self.connection_settings.save_local = self.settings_tab.save_checkbox.isChecked()

    def _load_settings_into_form(self) -> None:
        self.settings_tab.api_key_input.setText(self.connection_settings.api_key)
        self.settings_tab.api_secret_input.setText(self.connection_settings.api_secret)
        if self.connection_settings.mode == ConnectionMode.FUTURES:
            self.settings_tab.mode_futures.setChecked(True)
        else:
            self.settings_tab.mode_margin.setChecked(True)
        self.settings_tab.leverage_combo.setCurrentText(f"{self.connection_settings.leverage}x")
        self.settings_tab.save_checkbox.setChecked(self.connection_settings.save_local)
        self.on_mode_changed()

    def _validate_account_access(self, test_only: bool = False) -> bool:
        if self.connection_settings.mode == ConnectionMode.FUTURES:
            account = self.rest_client.get_futures_account()
            if not account:
                self._handle_connection_error("Futures account access denied")
                return False
            balance = self.rest_client.get_futures_balance()
            if not balance:
                self._handle_connection_error("Futures balance unavailable")
                return False
            self.account_permissions = "FUTURES OK"
            if not test_only:
                self.refresh_balance()
            return True

        account = self.rest_client.get_spot_account()
        if not account:
            self._handle_connection_error("Spot account access denied")
            return False
        can_trade = account.get("canTrade")
        if can_trade is False:
            self.account_permissions = "READ-ONLY"
        else:
            self.account_permissions = "SPOT OK"
        if not test_only:
            self._update_balance_from_spot_account(account)
        return True

    def _handle_connection_error(self, message: str) -> None:
        self.logger.error(message)
        self.state_machine.set_error(message)
        QMessageBox.warning(self, "Connection Error", message)

    def _entry_allowed(self) -> bool:
        return (
            self.connected
            and self.state_machine.state == BotState.READY
            and self.orderbook_ready
            and self.market_tick.spread_bps <= self.strategy_params.max_spread_bps
        )

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if not hasattr(self, "_initialized"):
            self._initialize_defaults()
            self._initialized = True


if __name__ == "__main__":
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()
