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
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QRadioButton,
    QSpinBox,
    QDoubleSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QCheckBox,
)

from src.core.logger import QtLogEmitter, setup_logger
from src.core.models import ConnectionMode, ConnectionSettings, MarketTick, StrategyParams
from src.core.state_machine import BotState, BotStateMachine
from src.gui.widgets import make_card
from src.services.http_fallback import HttpFallback
from src.services.ws_market import MarketDataThread

DEBUG = os.getenv("DEBUG", "false").lower() == "true"


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Directional Hedge Scalper — BTCUSDT")
        self.setMinimumSize(1200, 720)

        self.connection_settings = ConnectionSettings()
        self.strategy_params = StrategyParams()
        self.state_machine = BotStateMachine()
        self.market_tick = MarketTick()
        self.connected = False
        self.data_source = "WS"
        self.ws_status = "DISCONNECTED"
        self.cycle_start_time: Optional[datetime] = None
        self.entry_price_long: Optional[float] = None
        self.entry_price_short: Optional[float] = None

        self.log_emitter = QtLogEmitter()
        self.logger = setup_logger(Path("logs"), self.log_emitter)

        self.ws_thread = MarketDataThread("btcusdt")
        self.ws_thread.price_update.connect(self.on_price_update)
        self.ws_thread.status_update.connect(self.on_ws_status)

        self.http_client = HttpFallback()
        self.http_timer = QTimer(self)
        self.http_timer.setInterval(1000)
        self.http_timer.timeout.connect(self.fetch_http_fallback)

        self.cooldown_timer = QTimer(self)
        self.cooldown_timer.setSingleShot(True)
        self.cooldown_timer.timeout.connect(self.on_cooldown_complete)

        self._build_ui()
        self._wire_signals()
        self._update_ui_state()

        self.log_emitter.message.connect(self.append_log)

    def closeEvent(self, event) -> None:
        self.logger.info("Shutting down GUI")
        self.stop_ws_thread()
        self.http_client.close()
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
        wrapper.setFixedWidth(330)
        layout = QVBoxLayout(wrapper)
        layout.setSpacing(12)

        layout.addWidget(make_card("Binance Access", self._build_access_card()))
        layout.addWidget(make_card("Symbol", self._build_symbol_card()))
        layout.addWidget(make_card("Controls", self._build_controls_card()))
        layout.addStretch(1)
        return wrapper

    def _build_access_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        mode_layout = QHBoxLayout()
        self.mode_margin = QRadioButton("Spot Margin")
        self.mode_futures = QRadioButton("Futures")
        self.mode_margin.setChecked(True)
        mode_layout.addWidget(self.mode_margin)
        mode_layout.addWidget(self.mode_futures)
        layout.addLayout(mode_layout)

        leverage_layout = QHBoxLayout()
        self.leverage_label = QLabel("Leverage:")
        self.leverage_combo = QComboBox()
        self.leverage_combo.addItems(["1x", "2x", "3x"])
        self.leverage_combo.setEnabled(False)
        leverage_layout.addWidget(self.leverage_label)
        leverage_layout.addWidget(self.leverage_combo)
        layout.addLayout(leverage_layout)

        self.api_key_input = QLineEdit()
        self.api_key_input.setPlaceholderText("API Key")
        self.api_secret_input = QLineEdit()
        self.api_secret_input.setPlaceholderText("API Secret")
        self.api_secret_input.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.api_key_input)
        layout.addWidget(self.api_secret_input)

        self.save_env_checkbox = QCheckBox("Save to .env.local")
        layout.addWidget(self.save_env_checkbox)

        btn_layout = QHBoxLayout()
        self.connect_button = QPushButton("CONNECT")
        self.disconnect_button = QPushButton("DISCONNECT")
        btn_layout.addWidget(self.connect_button)
        btn_layout.addWidget(self.disconnect_button)
        layout.addLayout(btn_layout)

        self.keys_note = QLabel("Keys are used only locally.")
        self.keys_note.setStyleSheet("color: #8c8c8c;")
        layout.addWidget(self.keys_note)
        return widget

    def _build_symbol_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        self.symbol_combo = QComboBox()
        self.symbol_combo.addItems(["BTCUSDT"])
        layout.addWidget(QLabel("Symbol:"))
        layout.addWidget(self.symbol_combo)

        self.price_source_label = QLabel("Price source: Auto (WS→HTTP)")
        layout.addWidget(self.price_source_label)

        self.mid_label = QLabel("—")
        self.mid_label.setFont(QFont("Arial", 24, QFont.Bold))
        self.mid_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.mid_label)

        self.bid_label = QLabel("Bid: —")
        self.ask_label = QLabel("Ask: —")
        self.spread_label = QLabel("Spread: — bps")
        layout.addWidget(self.bid_label)
        layout.addWidget(self.ask_label)
        layout.addWidget(self.spread_label)
        return widget

    def _build_controls_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        self.arm_button = QPushButton("ARM")
        self.disarm_button = QPushButton("DISARM")
        self.start_cycle_button = QPushButton("START CYCLE")
        self.stop_button = QPushButton("STOP")
        self.emergency_button = QPushButton("EMERGENCY FLATTEN")
        self.dev_end_cycle_button = QPushButton("End cycle (dev)")
        if not DEBUG:
            self.dev_end_cycle_button.hide()

        layout.addWidget(self.arm_button)
        layout.addWidget(self.disarm_button)
        layout.addWidget(self.start_cycle_button)
        layout.addWidget(self.stop_button)
        layout.addWidget(self.emergency_button)
        layout.addWidget(self.dev_end_cycle_button)
        return widget

    def _build_right_tabs(self) -> QWidget:
        tabs = QTabWidget()
        tabs.addTab(self._build_trading_tab(), "Trading Window")
        tabs.addTab(self._build_parameters_tab(), "Parameters")
        tabs.addTab(self._build_trades_tab(), "Trades")
        tabs.addTab(self._build_logs_tab(), "Logs")
        tabs.addTab(self._build_stats_tab(), "Statistics")
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
        self.winner_label = QLabel("winner_side: —")
        self.loser_label = QLabel("loser_side: —")

        state_layout.addWidget(self.state_label, 0, 0)
        state_layout.addWidget(self.active_cycle_label, 0, 1)
        state_layout.addWidget(self.cycle_id_label, 0, 2)
        state_layout.addWidget(self.cycle_start_label, 1, 0)
        state_layout.addWidget(self.cycle_elapsed_label, 1, 1)
        state_layout.addWidget(self.winner_label, 1, 2)
        state_layout.addWidget(self.loser_label, 2, 0)

        self.bps_panel = QFrame()
        self.bps_panel.setFrameShape(QFrame.StyledPanel)
        bps_layout = QGridLayout(self.bps_panel)
        bps_layout.setContentsMargins(10, 10, 10, 10)

        self.raw_long_label = QLabel("raw_bps_long: —")
        self.raw_short_label = QLabel("raw_bps_short: —")
        self.net_bps_label = QLabel("net_bps: —")
        self.fee_label = QLabel("fee_total_bps: —")
        self.target_label = QLabel("target_net_bps: —")
        self.max_loss_label = QLabel("max_loss_bps: —")

        bps_layout.addWidget(self.raw_long_label, 0, 0)
        bps_layout.addWidget(self.raw_short_label, 0, 1)
        bps_layout.addWidget(self.net_bps_label, 0, 2)
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
        self.entry_allowed_label = QLabel("entry allowed: —")

        filters_layout.addWidget(self.max_spread_label, 0, 0)
        filters_layout.addWidget(self.min_volume_label, 0, 1)
        filters_layout.addWidget(self.cooldown_label, 0, 2)
        filters_layout.addWidget(self.entry_allowed_label, 1, 0)

        layout.addWidget(self.state_panel)
        layout.addWidget(self.bps_panel)
        layout.addWidget(self.filters_panel)
        layout.addStretch(1)
        return widget

    def _build_parameters_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)

        self.order_mode_combo = QComboBox()
        self.order_mode_combo.addItems(["market", "aggressive_limit"])

        self.usd_per_leg_spin = QDoubleSpinBox()
        self.usd_per_leg_spin.setRange(1.0, 1000.0)
        self.usd_per_leg_spin.setDecimals(2)

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

        form.addRow("order_mode", self.order_mode_combo)
        form.addRow("usd_per_leg", self.usd_per_leg_spin)
        form.addRow("max_loss_bps", self.max_loss_spin)
        form.addRow("fee_total_bps", self.fee_spin)
        form.addRow("target_net_bps", self.target_net_spin)
        form.addRow("max_spread_bps", self.max_spread_spin)
        form.addRow("cooldown_s", self.cooldown_spin)
        form.addRow("direction_detect_window_ticks", self.direction_window_combo)
        form.addRow("burst_volume_threshold", self.burst_volume_spin)

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
        self.mode_futures.toggled.connect(self.on_mode_changed)
        self.connect_button.clicked.connect(self.connect_to_binance)
        self.disconnect_button.clicked.connect(self.disconnect_from_binance)
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
        self.leverage_combo.setEnabled(self.mode_futures.isChecked())

    def connect_to_binance(self) -> None:
        self.connection_settings.api_key = self.api_key_input.text().strip()
        self.connection_settings.api_secret = self.api_secret_input.text().strip()
        self.connection_settings.mode = (
            ConnectionMode.FUTURES if self.mode_futures.isChecked() else ConnectionMode.MARGIN
        )
        self.connection_settings.leverage = int(self.leverage_combo.currentText()[0])
        self.connection_settings.save_to_env = self.save_env_checkbox.isChecked()

        if not self.connection_settings.api_key or not self.connection_settings.api_secret:
            QMessageBox.warning(self, "Missing Keys", "Please enter API key and secret.")
            return

        if self.connection_settings.save_to_env:
            self.save_env_file()

        self.connected = True
        self.state_machine.connect_ok()
        self.start_ws_thread()
        self.logger.info("Connected (stub) to Binance")
        self._update_ui_state()

    def disconnect_from_binance(self) -> None:
        self.connected = False
        self.state_machine.disconnect()
        self.stop_ws_thread()
        self.logger.info("Disconnected from Binance")
        self._update_ui_state()

    def on_arm(self) -> None:
        self.state_machine.arm(self.connected)
        self.logger.info("ARM requested")
        self._update_ui_state()

    def on_disarm(self) -> None:
        self.state_machine.disconnect()
        self.logger.info("DISARM requested")
        self._update_ui_state()

    def on_start_cycle(self) -> None:
        if self.market_tick.mid <= 0:
            QMessageBox.warning(self, "No Price", "Waiting for market price.")
            return
        if self.market_tick.spread_bps > self.strategy_params.max_spread_bps:
            QMessageBox.warning(self, "Spread Too High", "Spread guard blocked entry.")
            return
        started = self.state_machine.start_cycle()
        if not started:
            return
        self.logger.info("Cycle started (simulation)")
        self.cycle_start_time = datetime.now(timezone.utc)
        self.entry_price_long = self.market_tick.mid
        self.entry_price_short = self.market_tick.mid
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
        self.state_machine.finish_cycle()
        self.logger.info("Cycle finished (dev)")
        self.add_trade_row(
            side="SIM",
            entry_price=self.entry_price_long or self.market_tick.mid,
            exit_price=self.market_tick.mid,
            raw_bps=self._compute_raw_bps(self.entry_price_long, self.market_tick.mid),
            fees_bps=self.strategy_params.fee_total_bps,
            net_bps=self._compute_raw_bps(self.entry_price_long, self.market_tick.mid)
            - self.strategy_params.fee_total_bps,
            net_usd=0.0,
            duration_s=self._elapsed_seconds(),
            note="dev",
        )
        self.cooldown_timer.start(self.strategy_params.cooldown_s * 1000)
        self._update_ui_state()

    def on_cooldown_complete(self) -> None:
        self.state_machine.end_cooldown()
        self._update_ui_state()

    def apply_params(self) -> None:
        self.strategy_params.order_mode = self.order_mode_combo.currentText()
        self.strategy_params.usd_per_leg = self.usd_per_leg_spin.value()
        self.strategy_params.max_loss_bps = self.max_loss_spin.value()
        self.strategy_params.fee_total_bps = self.fee_spin.value()
        self.strategy_params.target_net_bps = self.target_net_spin.value()
        self.strategy_params.max_spread_bps = self.max_spread_spin.value()
        self.strategy_params.cooldown_s = self.cooldown_spin.value()
        self.strategy_params.direction_detect_window_ticks = int(
            self.direction_window_combo.currentText()
        )
        self.strategy_params.burst_volume_threshold = self.burst_volume_spin.value()
        self.logger.info("Parameters applied")
        self._update_ui_state()

    def reset_params(self) -> None:
        self.strategy_params = StrategyParams()
        self._sync_params_to_form()
        self._update_ui_state()

    def _sync_params_to_form(self) -> None:
        self.order_mode_combo.setCurrentText(self.strategy_params.order_mode)
        self.usd_per_leg_spin.setValue(self.strategy_params.usd_per_leg)
        self.max_loss_spin.setValue(self.strategy_params.max_loss_bps)
        self.fee_spin.setValue(self.strategy_params.fee_total_bps)
        self.target_net_spin.setValue(self.strategy_params.target_net_bps)
        self.max_spread_spin.setValue(self.strategy_params.max_spread_bps)
        self.cooldown_spin.setValue(self.strategy_params.cooldown_s)
        self.direction_window_combo.setCurrentText(
            str(self.strategy_params.direction_detect_window_ticks)
        )
        self.burst_volume_spin.setValue(self.strategy_params.burst_volume_threshold)

    def on_price_update(self, payload: dict) -> None:
        self.market_tick.bid = payload["bid"]
        self.market_tick.ask = payload["ask"]
        self.market_tick.mid = payload["mid"]
        self.market_tick.spread_bps = payload["spread_bps"]
        self.market_tick.event_time = payload["event_time"]
        self.market_tick.rx_time = payload["rx_time"]
        self._update_market_labels()
        self._update_cycle_metrics()
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

    def start_ws_thread(self) -> None:
        if self.ws_thread.isRunning():
            return
        self.ws_thread.start()

    def stop_ws_thread(self) -> None:
        if self.ws_thread.isRunning():
            self.ws_thread.stop()
            self.ws_thread.wait(2000)

    def _update_market_labels(self) -> None:
        self.mid_label.setText(f"{self.market_tick.mid:,.2f}")
        self.bid_label.setText(f"Bid: {self.market_tick.bid:,.2f}")
        self.ask_label.setText(f"Ask: {self.market_tick.ask:,.2f}")
        self.spread_label.setText(f"Spread: {self.market_tick.spread_bps:.2f} bps")
        now = datetime.now(timezone.utc)
        age_ms = (now - self.market_tick.rx_time).total_seconds() * 1000
        self.tick_age_label.setText(f"Tick age: {age_ms:.0f} ms")
        self.last_update_label.setText(
            f"Last update: {self.market_tick.rx_time.strftime('%H:%M:%S.%f')[:-3]}"
        )

    def _update_cycle_metrics(self) -> None:
        if self.state_machine.state == BotState.RUNNING:
            raw_long = self._compute_raw_bps(self.entry_price_long, self.market_tick.mid)
            raw_short = self._compute_raw_bps(self.entry_price_short, self.market_tick.mid)
            self.raw_long_label.setText(f"raw_bps_long: {raw_long:.2f}")
            self.raw_short_label.setText(f"raw_bps_short: {raw_short:.2f}")
            net = max(raw_long, raw_short) - self.strategy_params.fee_total_bps
            self.net_bps_label.setText(f"net_bps: {net:.2f}")
        self._update_state_labels()

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
        self.winner_label.setText("winner_side: —")
        self.loser_label.setText("loser_side: —")

    def _update_ui_state(self) -> None:
        connection_text = "CONNECTED" if self.connected else "DISCONNECTED"
        self.connection_label.setText(f"Connection: {connection_text}")
        if self.connected:
            self.connection_label.setStyleSheet("color: #27ae60;")
        else:
            self.connection_label.setStyleSheet("color: #e74c3c;")

        if self.ws_status == "CONNECTED":
            data_status = "WS OK"
            self.data_label.setStyleSheet("color: #27ae60;")
        elif self.ws_status == "DEGRADED":
            data_status = "WS DEGRADED"
            self.data_label.setStyleSheet("color: #f1c40f;")
        else:
            data_status = "HTTP"
            self.data_label.setStyleSheet("color: #f1c40f;")
        self.data_label.setText(f"Data: {data_status}")

        self.connect_button.setEnabled(not self.connected)
        self.disconnect_button.setEnabled(self.connected)
        self.arm_button.setEnabled(self.connected)
        self.disarm_button.setEnabled(self.connected)
        self.start_cycle_button.setEnabled(
            self.state_machine.state == BotState.READY and self.connected
        )
        self.stop_button.setEnabled(self.state_machine.state in {BotState.RUNNING, BotState.COOLDOWN})

        self.price_source_label.setText("Price source: Auto (WS→HTTP)")
        self.fee_label.setText(f"fee_total_bps: {self.strategy_params.fee_total_bps:.2f}")
        self.target_label.setText(f"target_net_bps: {self.strategy_params.target_net_bps}")
        self.max_loss_label.setText(f"max_loss_bps: {self.strategy_params.max_loss_bps}")
        self.max_spread_label.setText(f"max_spread_bps: {self.strategy_params.max_spread_bps}")
        self.min_volume_label.setText(
            f"min_volume_threshold: {self.strategy_params.burst_volume_threshold:.2f}"
        )
        self.cooldown_label.setText(f"cooldown_s: {self.strategy_params.cooldown_s}")

        entry_allowed = (
            self.state_machine.state == BotState.READY
            and self.market_tick.spread_bps <= self.strategy_params.max_spread_bps
        )
        self.entry_allowed_label.setText(
            f"entry allowed: {str(entry_allowed).lower()}"
        )
        self.entry_allowed_label.setStyleSheet(
            "color: #27ae60;" if entry_allowed else "color: #e74c3c;"
        )
        self._update_state_labels()

    def append_log(self, message: str) -> None:
        filter_level = self.log_filter_combo.currentText()
        expected = "WARNING" if filter_level == "WARN" else filter_level
        if f"| {expected} |" not in message:
            return
        self.log_output.appendPlainText(message)

    def open_logs_folder(self) -> None:
        logs_path = Path("logs").resolve()
        QMessageBox.information(self, "Logs", f"Logs folder: {logs_path}")

    def save_env_file(self) -> None:
        env_path = Path(".env.local")
        mode = self.connection_settings.mode.value
        env_path.write_text(
            "\n".join(
                [
                    f"BINANCE_API_KEY={self.connection_settings.api_key}",
                    f"BINANCE_API_SECRET={self.connection_settings.api_secret}",
                    f"BINANCE_MODE={mode}",
                    f"BINANCE_LEVERAGE={self.connection_settings.leverage}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        self.logger.info("Saved keys to .env.local")

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

    def _compute_raw_bps(self, entry: Optional[float], mid: float) -> float:
        if not entry or entry <= 0:
            return 0.0
        return (mid / entry - 1) * 10_000

    def _elapsed_seconds(self) -> float:
        if not self.cycle_start_time:
            return 0.0
        return (datetime.now(timezone.utc) - self.cycle_start_time).total_seconds()

    def _initialize_defaults(self) -> None:
        self._sync_params_to_form()
        self.cycle_start_time = None
        self.entry_price_long = None
        self.entry_price_short = None

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
