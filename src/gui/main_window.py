from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QThread, QTimer, Qt, Signal
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFileDialog,
    QFrame,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLCDNumber,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSizePolicy,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.core.version import VERSION
from src.core.config_store import SettingsStore, StrategyParamsStore
from src.core.logger import LogBus, setup_logger
from src.core.models import (
    ConnectionMode,
    ConnectionSettings,
    MarketDataMode,
    MarketTick,
    StrategyParams,
)
from src.core.state_machine import BotState, BotStateMachine
from src.core.trade_engine import TradeEngine
from src.gui.parameters_tab import ParametersTab
from src.gui.settings_tab import SettingsTab
from src.gui.widgets import make_card
from src.services.orderbook import OrderBook
from src.services.market_data import MarketDataService
from src.services.ws_market import MarketDataThread

DEBUG = os.getenv("DEBUG", "false").lower() == "true"


class MainWindow(QMainWindow):
    request_validate = Signal(bool)
    request_refresh_balance = Signal()
    request_exchange_info = Signal()
    request_orderbook_snapshot = Signal()
    request_http_fallback = Signal()
    request_attempt_entry = Signal()
    request_stop_cycle = Signal()
    request_start_trading = Signal()
    request_emergency_flatten = Signal()
    request_emergency_test = Signal()
    request_set_strategy = Signal(dict)
    request_set_connection = Signal(dict)
    request_end_cooldown = Signal(bool)
    request_ui_heartbeat = Signal(float)
    request_on_tick = Signal(dict)
    request_set_data_mode = Signal(str)
    request_set_ws_status = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.version = VERSION
        self.setWindowTitle(f"Хедж-скальпер Directional v{self.version} — BTCUSDT")
        self.setMinimumSize(1200, 720)

        self.settings_store = SettingsStore()
        self.connection_settings = self.settings_store.load()
        self.strategy_store = StrategyParamsStore()
        self.strategy_params = StrategyParams()
        self.state_machine = BotStateMachine()
        self.market_tick = MarketTick()
        self.connected = False
        self.data_source = "WS"
        self.data_mode = MarketDataMode.HYBRID
        self.ws_status = "DISCONNECTED"
        self.cycle_start_time: Optional[datetime] = None
        self.run_mode = False

        self.sim_entry_price: Optional[float] = None
        self.sim_entry_qty: Optional[float] = None
        self.sim_exit_price: Optional[float] = None
        self.sim_last_mid: Optional[float] = None
        self.sim_total_raw_bps: Optional[float] = None
        self.sim_total_net_bps: Optional[float] = None
        self.sim_condition: str = "—"
        self.sim_winner_raw_bps: Optional[float] = None
        self.sim_winner_net_bps: Optional[float] = None
        self.sim_loser_raw_bps: Optional[float] = None
        self.sim_loser_net_bps: Optional[float] = None
        self.sim_long_raw_bps: Optional[float] = None
        self.sim_short_raw_bps: Optional[float] = None
        self.sim_result: Optional[str] = None
        self.sim_pnl_usdt: Optional[float] = None
        self.sim_detect_window_ticks: Optional[int] = None
        self.sim_detect_timeout_ms: Optional[int] = None
        self.sim_reason: Optional[str] = None
        self.sim_ws_age_ms: Optional[float] = None
        self.sim_tick_rate: Optional[float] = None
        self.sim_impulse_bps: Optional[float] = None
        self.sim_spread_bps: Optional[float] = None
        self.effective_source = "NONE"
        self.effective_age_ms: Optional[float] = None
        self.ws_age_ms: Optional[float] = None
        self.http_age_ms: Optional[float] = None
        self.data_stale = True
        self.waiting_for_data = False

        self.logger = setup_logger(Path("logs"))
        self.log_bus = LogBus(self.logger)
        self.log_entries: list[dict] = []
        self.log_bus.entry.connect(self.on_log_entry)
        self.log_bus.log("INFO", "INFO", "APP start", version=self.version)

        self.market_data = MarketDataService()
        self.ws_thread = MarketDataThread("btcusdt", market_data=self.market_data)
        self.ws_thread.price_update.connect(self.on_price_update)
        self.ws_thread.price_update.connect(self.request_on_tick)
        self.ws_thread.depth_update.connect(self.on_depth_update)
        self.ws_thread.status_update.connect(self.on_ws_status)

        self.engine_thread = QThread(self)
        self.trade_engine = TradeEngine(market_data=self.market_data)
        self.trade_engine.moveToThread(self.engine_thread)
        self.engine_thread.start()

        self.http_timer = QTimer(self)
        self.http_timer.setInterval(1000)
        self.http_timer.timeout.connect(self.request_http_fallback)

        self.balance_timer = QTimer(self)
        self.balance_timer.setInterval(10_000)
        self.balance_timer.timeout.connect(self.request_refresh_balance)

        self.cooldown_timer = QTimer(self)
        self.cooldown_timer.setSingleShot(True)
        self.cooldown_timer.timeout.connect(self.on_cooldown_complete)

        self.orderbook = OrderBook()
        self.orderbook_ready = False
        self.last_balance_update: Optional[datetime] = None
        self.account_permissions = "—"
        self.margin_permission_ok = False
        self.margin_usdt_free = 0.0
        self.margin_usdt_borrowed = 0.0
        self.margin_usdt_interest = 0.0
        self.margin_usdt_net = 0.0
        self.margin_btc_free = 0.0
        self.margin_btc_borrowed = 0.0
        self.spot_usdt_free = 0.0

        self._build_ui()
        self._wire_signals()
        self._initialize_defaults()
        self._load_settings_into_form()
        self._update_ui_state()


        self.request_set_strategy.emit(self.strategy_params.__dict__)

        self.heartbeat_timer = QTimer(self)
        self.heartbeat_timer.setInterval(500)
        self.heartbeat_timer.timeout.connect(self._emit_heartbeat)
        self.heartbeat_timer.start()

        self.run_timer = QTimer(self)
        self.run_timer.setInterval(150)
        self.run_timer.timeout.connect(self._run_loop_check)
        self.run_timer.start()

        self._auto_connect()

    def closeEvent(self, event) -> None:
        self.log_bus.log("INFO", "INFO", "GUI shutdown requested")
        self.stop_ws_thread()
        self.trade_engine.close()
        self.engine_thread.quit()
        self.engine_thread.wait(2000)
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

        self.connection_label = QLabel("Связь: ОТКЛЮЧЕНО")
        self.data_label = QLabel("Данные: WS")
        self.tick_age_label = QLabel("Возраст тика: — мс")
        self.last_update_label = QLabel("Последнее обновление: —")

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

        market_card = make_card("Рынок", self._build_market_card())
        account_card = make_card("Аккаунт", self._build_account_card())
        controls_card = make_card("Управление", self._build_controls_card())
        for card in (market_card, account_card, controls_card):
            card.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Minimum)
        layout.addWidget(market_card)
        layout.addWidget(account_card)
        layout.addWidget(controls_card)
        layout.addStretch(1)
        return wrapper

    def _build_market_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        self.symbol_combo = QComboBox()
        self.symbol_combo.addItems(["BTCUSDT"])
        layout.addWidget(QLabel("Символ"))
        layout.addWidget(self.symbol_combo)

        self.source_mode_combo = QComboBox()
        self.source_mode_combo.addItem("WS only", MarketDataMode.WS_ONLY.value)
        self.source_mode_combo.addItem("HTTP only", MarketDataMode.HTTP_ONLY.value)
        self.source_mode_combo.addItem("Hybrid (WS→HTTP)", MarketDataMode.HYBRID.value)
        layout.addWidget(QLabel("Источник данных"))
        layout.addWidget(self.source_mode_combo)

        self.price_source_label = QLabel("Режим: Hybrid (WS→HTTP)")
        layout.addWidget(self.price_source_label)

        self.mid_display = QLCDNumber()
        self.mid_display.setDigitCount(12)
        self.mid_display.setSegmentStyle(QLCDNumber.Flat)
        self.mid_display.setFixedHeight(48)
        self.mid_display.setMinimumWidth(240)
        self.mid_display.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        layout.addWidget(self.mid_display)

        grid = QFormLayout()
        grid.setLabelAlignment(Qt.AlignLeft)
        grid.setFormAlignment(Qt.AlignLeft | Qt.AlignTop)
        grid.setHorizontalSpacing(8)
        grid.setVerticalSpacing(4)
        self.bid_value = QLabel("—")
        self.ask_value = QLabel("—")
        self.spread_value = QLabel("—")
        self.effective_source_value = QLabel("—")
        self.card_tick_age_value = QLabel("—")
        grid.addRow("Бид", self.bid_value)
        grid.addRow("Аск", self.ask_value)
        grid.addRow("Спред (bps)", self.spread_value)
        grid.addRow("Effective source", self.effective_source_value)
        grid.addRow("Возраст (мс)", self.card_tick_age_value)
        layout.addLayout(grid)
        return widget

    def _build_account_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        self.account_connection_label = QLabel("Связь: ОТКЛЮЧЕНО")
        self.account_data_label = QLabel("Данные: —")
        self.margin_balance_label = QLabel("USDT (Маржа): —")
        self.spot_balance_label = QLabel("USDT (Spot): —")
        self.permissions_label = QLabel("Права: —")
        self.balance_updated_label = QLabel("Обновлено: —")
        self.balance_updated_label.setStyleSheet("color: #8c8c8c;")

        layout.addWidget(self.account_connection_label)
        layout.addWidget(self.account_data_label)
        layout.addWidget(self.margin_balance_label)
        layout.addWidget(self.spot_balance_label)
        layout.addWidget(self.permissions_label)
        layout.addWidget(self.balance_updated_label)
        return widget

    def _build_controls_card(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(6)

        cycle_layout = QHBoxLayout()
        self.start_cycle_button = QPushButton("СТАРТ")
        self.stop_button = QPushButton("СТОП")
        cycle_layout.addWidget(self.start_cycle_button)
        cycle_layout.addWidget(self.stop_button)
        layout.addLayout(cycle_layout)

        self.emergency_button = QPushButton("ЗАКРЫТЬ ВСЁ")
        layout.addWidget(self.emergency_button)
        self.emergency_test_button = QPushButton("Emergency Test")
        layout.addWidget(self.emergency_test_button)

        self.exposure_label = QLabel("позиция: нет")
        self.exposure_label.setStyleSheet("color: #8c8c8c;")
        layout.addWidget(self.exposure_label)

        self.entry_allowed_label = QLabel("вход разрешён: —")
        layout.addWidget(self.entry_allowed_label)

        self.dev_end_cycle_button = QPushButton("Завершить цикл (dev)")
        if not DEBUG:
            self.dev_end_cycle_button.hide()
        layout.addWidget(self.dev_end_cycle_button)
        return widget

    def _build_right_tabs(self) -> QWidget:
        tabs = QTabWidget()
        tabs.addTab(self._build_trading_tab(), "Торговля")
        self.parameters_tab = ParametersTab()
        tabs.addTab(self.parameters_tab, "Параметры")
        tabs.addTab(self._build_trades_tab(), "Сделки")
        tabs.addTab(self._build_logs_tab(), "Логи")
        tabs.addTab(self._build_stats_tab(), "Статистика")
        self.settings_tab = SettingsTab()
        tabs.addTab(self.settings_tab, "Настройки")
        return tabs

    def _build_trading_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(12)

        self.state_panel = QFrame()
        self.state_panel.setFrameShape(QFrame.StyledPanel)
        state_layout = QGridLayout(self.state_panel)
        state_layout.setContentsMargins(10, 10, 10, 10)

        self.state_label = QLabel("Состояние: IDLE")
        self.active_cycle_label = QLabel("активный цикл: false")
        self.cycle_id_label = QLabel("цикл_id: 0")
        self.cycle_start_label = QLabel("старт: —")
        self.cycle_elapsed_label = QLabel("длительность_с: —")

        state_layout.addWidget(self.state_label, 0, 0)
        state_layout.addWidget(self.active_cycle_label, 0, 1)
        state_layout.addWidget(self.cycle_id_label, 0, 2)
        state_layout.addWidget(self.cycle_start_label, 1, 0)
        state_layout.addWidget(self.cycle_elapsed_label, 1, 1)

        self.bps_panel = QFrame()
        self.bps_panel.setFrameShape(QFrame.StyledPanel)
        bps_layout = QGridLayout(self.bps_panel)
        bps_layout.setContentsMargins(10, 10, 10, 10)

        self.raw_bps_label = QLabel("raw_bps всего: —")
        self.net_bps_label = QLabel("net_bps всего: —")
        self.winner_raw_label = QLabel("raw_bps победителя: —")
        self.winner_net_label = QLabel("net_bps победителя: —")
        self.loser_raw_label = QLabel("raw_bps лузера: —")
        self.loser_net_label = QLabel("net_bps лузера: —")
        self.long_raw_label = QLabel("raw_bps long: —")
        self.short_raw_label = QLabel("raw_bps short: —")
        self.result_label = QLabel("result: —")
        self.pnl_label = QLabel("pnl_usdt: —")
        self.fee_label = QLabel("комиссия bps: —")
        self.target_label = QLabel("цель net bps: —")
        self.max_loss_label = QLabel("макс. убыток bps: —")

        bps_layout.addWidget(self.raw_bps_label, 0, 0)
        bps_layout.addWidget(self.net_bps_label, 0, 1)
        bps_layout.addWidget(self.fee_label, 0, 2)
        bps_layout.addWidget(self.winner_raw_label, 1, 0)
        bps_layout.addWidget(self.winner_net_label, 1, 1)
        bps_layout.addWidget(self.target_label, 1, 2)
        bps_layout.addWidget(self.loser_raw_label, 2, 0)
        bps_layout.addWidget(self.loser_net_label, 2, 1)
        bps_layout.addWidget(self.max_loss_label, 2, 2)
        bps_layout.addWidget(self.long_raw_label, 3, 0)
        bps_layout.addWidget(self.short_raw_label, 3, 1)
        bps_layout.addWidget(self.result_label, 3, 2)
        bps_layout.addWidget(self.pnl_label, 4, 0)

        self.filters_panel = QFrame()
        self.filters_panel.setFrameShape(QFrame.StyledPanel)
        filters_layout = QGridLayout(self.filters_panel)
        filters_layout.setContentsMargins(10, 10, 10, 10)

        self.max_spread_label = QLabel("макс. спред bps: —")
        self.min_volume_label = QLabel("мин. тик-рейт: —")
        self.cooldown_label = QLabel("cooldown сек: —")
        self.detect_window_label = QLabel("detect_ticks: —")
        self.detect_timeout_label = QLabel("detect_timeout_ms: —")

        filters_layout.addWidget(self.max_spread_label, 0, 0)
        filters_layout.addWidget(self.min_volume_label, 0, 1)
        filters_layout.addWidget(self.cooldown_label, 0, 2)
        filters_layout.addWidget(self.detect_window_label, 1, 0)
        filters_layout.addWidget(self.detect_timeout_label, 1, 1)

        self.sim_panel = QFrame()
        self.sim_panel.setFrameShape(QFrame.StyledPanel)
        sim_layout = QGridLayout(self.sim_panel)
        sim_layout.setContentsMargins(10, 10, 10, 10)

        self.sim_side_label = QLabel("победитель/лузер: —")
        self.sim_notional_label = QLabel("номинал USD: —")
        self.sim_entry_label = QLabel("вход_mid: —")
        self.sim_last_label = QLabel("последний_mid: —")
        self.sim_exit_label = QLabel("выход_mid: —")
        self.sim_reason_label = QLabel("reason: —")
        self.sim_ws_age_label = QLabel("ws_age_ms: —")
        self.sim_source_label = QLabel("source: —")
        self.sim_condition_label = QLabel("условие: —")
        self.sim_tick_label = QLabel("tick_rate: —")
        self.sim_impulse_label = QLabel("impulse_bps: —")
        self.sim_spread_label = QLabel("spread_bps: —")

        sim_layout.addWidget(self.sim_side_label, 0, 0)
        sim_layout.addWidget(self.sim_notional_label, 0, 1)
        sim_layout.addWidget(self.sim_condition_label, 0, 2)
        sim_layout.addWidget(self.sim_entry_label, 1, 0)
        sim_layout.addWidget(self.sim_exit_label, 1, 1)
        sim_layout.addWidget(self.sim_reason_label, 1, 2)
        sim_layout.addWidget(self.sim_last_label, 2, 0)
        sim_layout.addWidget(self.sim_tick_label, 2, 1)
        sim_layout.addWidget(self.sim_impulse_label, 2, 2)
        sim_layout.addWidget(self.sim_spread_label, 3, 0)
        sim_layout.addWidget(self.sim_ws_age_label, 3, 1)
        sim_layout.addWidget(self.sim_source_label, 3, 2)

        layout.addWidget(self.state_panel)
        layout.addWidget(self.bps_panel)
        layout.addWidget(self.filters_panel)
        layout.addWidget(self.sim_panel)
        layout.addStretch(1)
        return widget

    def _build_trades_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self.trades_table = QTableWidget(0, 12)
        self.trades_table.setHorizontalHeaderLabels(
            [
                "время",
                "цикл_id",
                "фаза",
                "сторона",
                "кол-во",
                "вход",
                "выход",
                "raw_bps",
                "net_bps",
                "net_usd",
                "длительность_мс",
                "заметка",
            ]
        )
        self.trades_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.trades_table)
        layout.addWidget(QLabel("Сделки (лог)"))
        self.deals_log_output = QPlainTextEdit()
        self.deals_log_output.setReadOnly(True)
        self.deals_log_output.setMaximumHeight(160)
        layout.addWidget(self.deals_log_output)
        if DEBUG:
            self.add_test_trade_button = QPushButton("Добавить тестовую строку")
            layout.addWidget(self.add_test_trade_button)
        return widget

    def _build_logs_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        filter_layout = QHBoxLayout()
        self.log_filter_combo = QComboBox()
        self.log_filter_combo.addItems(["ВСЕ", "Инфо", "Ошибки", "Торговля", "Сделки"])
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["ВСЕ", "DEBUG", "INFO", "WARN", "ERROR"])
        self.log_search_input = QLineEdit()
        self.log_search_input.setPlaceholderText("Поиск по тексту")
        filter_layout.addWidget(QLabel("Категория:"))
        filter_layout.addWidget(self.log_filter_combo)
        filter_layout.addWidget(QLabel("Уровень:"))
        filter_layout.addWidget(self.log_level_combo)
        filter_layout.addWidget(self.log_search_input)
        filter_layout.addStretch(1)

        self.save_logs_button = QPushButton("Сохранить лог")
        self.open_logs_button = QPushButton("Открыть папку логов")
        filter_layout.addWidget(self.save_logs_button)
        filter_layout.addWidget(self.open_logs_button)

        self.log_output = QPlainTextEdit()
        self.log_output.setReadOnly(True)

        layout.addLayout(filter_layout)
        layout.addWidget(self.log_output)
        self._refresh_log_view()
        return widget

    def _build_stats_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.addWidget(QLabel("Статистика будет доступна в следующих версиях."))
        return widget

    def _wire_signals(self) -> None:
        self.settings_tab.save_button.clicked.connect(self.save_settings)
        self.settings_tab.test_button.clicked.connect(self.test_connection)
        self.settings_tab.clear_button.clicked.connect(self.clear_settings_fields)

        self.start_cycle_button.clicked.connect(self.on_start_cycle)
        self.stop_button.clicked.connect(self.on_stop)
        self.emergency_button.clicked.connect(self.on_emergency_flatten)
        self.emergency_test_button.clicked.connect(self.on_emergency_test)
        self.dev_end_cycle_button.clicked.connect(self.on_end_cycle)
        self.parameters_tab.apply_params_button.clicked.connect(self.apply_params)
        self.parameters_tab.reset_params_button.clicked.connect(self.reset_params)
        self.source_mode_combo.currentIndexChanged.connect(self.on_source_mode_changed)
        if DEBUG:
            self.add_test_trade_button.clicked.connect(self.add_test_trade)
        self.save_logs_button.clicked.connect(self.save_logs_to_file)
        self.open_logs_button.clicked.connect(self.open_logs_folder)
        self.log_filter_combo.currentTextChanged.connect(self._refresh_log_view)
        self.log_level_combo.currentTextChanged.connect(self._refresh_log_view)
        self.log_search_input.textChanged.connect(self._refresh_log_view)

        self.request_validate.connect(self.trade_engine.validate_access)
        self.request_refresh_balance.connect(self.trade_engine.refresh_balance)
        self.request_exchange_info.connect(self.trade_engine.load_exchange_info)
        self.request_orderbook_snapshot.connect(self.trade_engine.load_orderbook_snapshot)
        self.request_http_fallback.connect(self.trade_engine.fetch_http_fallback)
        self.request_attempt_entry.connect(self.trade_engine.attempt_entry)
        self.request_stop_cycle.connect(self.trade_engine.stop)
        self.request_start_trading.connect(self.trade_engine.start_trading)
        self.request_emergency_flatten.connect(self.trade_engine.emergency_flatten)
        self.request_emergency_test.connect(self.trade_engine.emergency_test)
        self.request_set_strategy.connect(self.trade_engine.set_strategy)
        self.request_set_connection.connect(self.trade_engine.set_connection)
        self.request_end_cooldown.connect(self.trade_engine.end_cooldown)
        self.request_set_data_mode.connect(self.trade_engine.set_data_mode)
        self.request_set_ws_status.connect(self.trade_engine.set_ws_status)
        self.request_ui_heartbeat.connect(
            self.trade_engine.update_ui_heartbeat, Qt.ConnectionType.DirectConnection
        )
        self.request_on_tick.connect(self.trade_engine.on_tick)

        self.trade_engine.tick_update.connect(self.on_price_update)
        self.trade_engine.depth_snapshot.connect(self.on_depth_snapshot)
        self.trade_engine.balance_update.connect(self.on_balance_update)
        self.trade_engine.connection_checked.connect(self.on_connection_checked)
        self.trade_engine.cycle_updated.connect(self.on_cycle_update)
        self.trade_engine.trade_row.connect(self.on_trade_row)
        self.trade_engine.exposure_update.connect(self.on_exposure_update)
        self.trade_engine.log.connect(self.on_engine_log)

    def test_connection(self) -> None:
        self._sync_settings_from_form()
        if not self.connection_settings.api_key or not self.connection_settings.api_secret:
            QMessageBox.warning(self, "Ключи", "Введите API ключ и секрет.")
            return
        self.request_set_connection.emit(
            {
                "api_key": self.connection_settings.api_key,
                "api_secret": self.connection_settings.api_secret,
            }
        )
        self.request_validate.emit(True)

    def save_settings(self) -> None:
        self._sync_settings_from_form()
        if self.connection_settings.save_local:
            self.settings_store.save(self.connection_settings)
            QMessageBox.information(
                self, "Настройки", "Сохранено локально в config/settings.json"
            )
            self.log_bus.log("INFO", "INFO", "Settings saved", path="config/settings.json")
        else:
            QMessageBox.information(self, "Настройки", "Локальное сохранение отключено.")
        self._auto_connect()

    def clear_settings_fields(self) -> None:
        self.settings_tab.api_key_input.clear()
        self.settings_tab.api_secret_input.clear()
        self.log_bus.log("INFO", "INFO", "Settings cleared")
        self._auto_connect()

    def on_start_cycle(self) -> None:
        if not self._can_run():
            QMessageBox.warning(self, "Защита", "Нет доступа для старта авто-торговли.")
            return
        self.run_mode = True
        self.strategy_params.max_cycles = self.parameters_tab.max_cycles_spin.value()
        self.request_set_strategy.emit(self.strategy_params.__dict__)
        self.strategy_store.save_strategy_params(
            self.strategy_params, self.symbol_combo.currentText()
        )
        self.log_bus.log(
            "INFO",
            "INFO",
            "Trade params saved",
            path="config/strategy_params.json",
            action="start",
        )
        self.log_bus.log("INFO", "INFO", "AUTO trading started")
        self.request_start_trading.emit()
        self._update_ui_state()
        self._run_loop_check()

    def on_stop(self) -> None:
        self.run_mode = False
        self.log_bus.log("INFO", "INFO", "AUTO trading stopped by user")
        self.request_stop_cycle.emit()
        self._update_ui_state()

    def on_emergency_flatten(self) -> None:
        self.request_emergency_flatten.emit()

    def on_emergency_test(self) -> None:
        self.request_emergency_test.emit()

    def on_end_cycle(self) -> None:
        if not self.state_machine.active_cycle:
            return
        self.request_stop_cycle.emit()

    def on_cooldown_complete(self) -> None:
        auto_resume = self.run_mode and self.strategy_params.auto_loop
        self.request_end_cooldown.emit(auto_resume)
        if not auto_resume:
            self.run_mode = False
        self._update_ui_state()
        if auto_resume:
            self._run_loop_check()

    def apply_params(self) -> None:
        self.strategy_params.order_mode = (
            self.parameters_tab.order_mode_combo.currentData()
            or self.parameters_tab.order_mode_combo.currentText()
        )
        self.strategy_params.usd_notional = self.parameters_tab.usd_notional_spin.value()
        self.strategy_params.leverage_max = self.parameters_tab.leverage_max_spin.value()
        self.strategy_params.slip_bps = self.parameters_tab.slip_bps_spin.value()
        self.strategy_params.max_loss_bps = self.parameters_tab.max_loss_spin.value()
        self.strategy_params.fee_total_bps = self.parameters_tab.fee_spin.value()
        self.strategy_params.target_net_bps = self.parameters_tab.target_net_spin.value()
        self.strategy_params.max_spread_bps = self.parameters_tab.max_spread_spin.value()
        self.strategy_params.min_tick_rate = self.parameters_tab.min_tick_rate_spin.value()
        self.strategy_params.detect_timeout_ms = self.parameters_tab.detect_timeout_spin.value()
        self.strategy_params.use_impulse_filter = (
            self.parameters_tab.use_impulse_checkbox.isChecked()
        )
        self.strategy_params.impulse_min_bps = self.parameters_tab.impulse_min_spin.value()
        self.strategy_params.impulse_grace_ms = self.parameters_tab.impulse_grace_spin.value()
        self.strategy_params.winner_threshold_bps = (
            self.parameters_tab.winner_threshold_spin.value()
        )
        self.strategy_params.emergency_stop_bps = (
            self.parameters_tab.emergency_stop_spin.value()
        )
        self.strategy_params.cooldown_s = self.parameters_tab.cooldown_spin.value()
        self.strategy_params.detect_window_ticks = int(
            self.parameters_tab.direction_window_combo.currentText()
        )
        self.strategy_params.burst_volume_threshold = (
            self.parameters_tab.burst_volume_spin.value()
        )
        self.strategy_params.allow_no_winner_flatten = (
            self.parameters_tab.allow_no_winner_checkbox.isChecked()
        )
        self.strategy_params.no_winner_policy = (
            self.parameters_tab.no_winner_policy_combo.currentData()
            or self.parameters_tab.no_winner_policy_combo.currentText()
        )
        self.strategy_params.auto_loop = self.parameters_tab.auto_loop_checkbox.isChecked()
        self.strategy_params.max_cycles = self.parameters_tab.max_cycles_spin.value()
        self.strategy_store.save_strategy_params(
            self.strategy_params, self.symbol_combo.currentText()
        )
        self.log_bus.log(
            "INFO",
            "INFO",
            "Trade params saved",
            path="config/strategy_params.json",
            action="apply",
        )
        self.request_set_strategy.emit(self.strategy_params.__dict__)
        self._update_ui_state()

    def reset_params(self) -> None:
        self.strategy_params = StrategyParams()
        self._sync_params_to_form()
        self.strategy_store.save_strategy_params(
            self.strategy_params, self.symbol_combo.currentText()
        )
        self.request_set_strategy.emit(self.strategy_params.__dict__)
        self._update_ui_state()

    def _sync_params_to_form(self) -> None:
        index = self.parameters_tab.order_mode_combo.findData(self.strategy_params.order_mode)
        if index >= 0:
            self.parameters_tab.order_mode_combo.setCurrentIndex(index)
        self.parameters_tab.usd_notional_spin.setValue(self.strategy_params.usd_notional)
        self.parameters_tab.leverage_max_spin.setValue(self.strategy_params.leverage_max)
        self.parameters_tab.slip_bps_spin.setValue(self.strategy_params.slip_bps)
        self.parameters_tab.max_loss_spin.setValue(self.strategy_params.max_loss_bps)
        self.parameters_tab.fee_spin.setValue(self.strategy_params.fee_total_bps)
        self.parameters_tab.target_net_spin.setValue(self.strategy_params.target_net_bps)
        self.parameters_tab.max_spread_spin.setValue(self.strategy_params.max_spread_bps)
        self.parameters_tab.min_tick_rate_spin.setValue(self.strategy_params.min_tick_rate)
        self.parameters_tab.detect_timeout_spin.setValue(self.strategy_params.detect_timeout_ms)
        self.parameters_tab.use_impulse_checkbox.setChecked(
            self.strategy_params.use_impulse_filter
        )
        self.parameters_tab.impulse_min_spin.setValue(self.strategy_params.impulse_min_bps)
        self.parameters_tab.impulse_grace_spin.setValue(self.strategy_params.impulse_grace_ms)
        self.parameters_tab.winner_threshold_spin.setValue(
            self.strategy_params.winner_threshold_bps
        )
        self.parameters_tab.emergency_stop_spin.setValue(self.strategy_params.emergency_stop_bps)
        self.parameters_tab.cooldown_spin.setValue(self.strategy_params.cooldown_s)
        detect_ticks = max(5, self.strategy_params.detect_window_ticks)
        self.parameters_tab.direction_window_combo.setCurrentText(str(detect_ticks))
        self.parameters_tab.burst_volume_spin.setValue(
            self.strategy_params.burst_volume_threshold
        )
        self.parameters_tab.allow_no_winner_checkbox.setChecked(
            self.strategy_params.allow_no_winner_flatten
        )
        policy_index = self.parameters_tab.no_winner_policy_combo.findData(
            self.strategy_params.no_winner_policy
        )
        if policy_index >= 0:
            self.parameters_tab.no_winner_policy_combo.setCurrentIndex(policy_index)
        self.parameters_tab.auto_loop_checkbox.setChecked(self.strategy_params.auto_loop)
        self.parameters_tab.max_cycles_spin.setValue(self.strategy_params.max_cycles)

    def on_price_update(self, payload: dict) -> None:
        self.market_tick.bid = payload["bid"]
        self.market_tick.ask = payload["ask"]
        self.market_tick.mid = payload["mid"]
        self.market_tick.spread_bps = payload["spread_bps"]
        if "bid_raw" in payload:
            self.market_tick.bid_raw = payload["bid_raw"]
        if "ask_raw" in payload:
            self.market_tick.ask_raw = payload["ask_raw"]
        if "mid_raw" in payload:
            self.market_tick.mid_raw = payload["mid_raw"]
        self.market_tick.event_time = payload["event_time"]
        self.market_tick.rx_time = payload["rx_time"]
        self._update_market_labels()
        self._update_ui_state()

    def on_depth_update(self, payload: dict) -> None:
        self.orderbook.apply_snapshot(payload["bids"], payload["asks"])
        self.orderbook_ready = self.orderbook.is_ready()
        self._update_ui_state()

    def on_ws_status(self, status: str) -> None:
        self.ws_status = status
        self.request_set_ws_status.emit(status)
        self._update_http_timer()
        self._update_ui_state()

    def on_source_mode_changed(self) -> None:
        mode_value = self.source_mode_combo.currentData()
        try:
            self.data_mode = MarketDataMode(mode_value)
        except ValueError:
            self.data_mode = MarketDataMode.HYBRID
        self.request_set_data_mode.emit(self.data_mode.value)
        self._update_http_timer()
        self._update_ui_state()

    def _update_http_timer(self) -> None:
        if self.data_mode == MarketDataMode.WS_ONLY:
            if self.http_timer.isActive():
                self.http_timer.stop()
            return
        if self.data_mode == MarketDataMode.HTTP_ONLY:
            if not self.http_timer.isActive():
                self.http_timer.start()
            return
        if self.ws_status in {"DEGRADED", "DISCONNECTED"}:
            if not self.http_timer.isActive():
                self.http_timer.start()
        else:
            if self.http_timer.isActive():
                self.http_timer.stop()

    def on_depth_snapshot(self, payload: dict) -> None:
        self.orderbook.apply_snapshot(payload.get("bids", []), payload.get("asks", []))
        self.orderbook_ready = self.orderbook.is_ready()
        self._update_ui_state()

    def on_balance_update(self, payload: dict) -> None:
        spot_account = payload.get("spot_account")
        margin_account = payload.get("margin_account")
        if spot_account:
            self._update_balance_from_spot_account(spot_account)
        if margin_account:
            self._update_balance_from_margin_account(margin_account)
        self.margin_permission_ok = bool(payload.get("margin_permission_ok"))
        can_trade = spot_account.get("canTrade") if spot_account else None
        if not margin_account:
            self.account_permissions = "МАРЖА: НЕТ ДОСТУПА"
        elif can_trade is False:
            self.account_permissions = "МАРЖА ОК (SPOT: READ-ONLY)"
        else:
            self.account_permissions = "МАРЖА ОК"
        self.last_balance_update = datetime.now(timezone.utc)
        self._update_ui_state()

    def on_connection_checked(self, payload: dict) -> None:
        ok = payload.get("ok")
        test_only = payload.get("test_only")
        if "margin_permission_ok" in payload:
            self.margin_permission_ok = bool(payload.get("margin_permission_ok"))
        if test_only:
            if ok:
                QMessageBox.information(self, "Связь", "Доступ подтверждён.")
            else:
                QMessageBox.warning(self, "Связь", "Не удалось подтвердить доступ.")
            return
        if not ok:
            self.connected = False
            self.state_machine.set_error(payload.get("error") or "connection_failed")
            QMessageBox.warning(self, "Связь", "Не удалось подключиться к Binance.")
            self._update_ui_state()
            return
        self.connected = True
        self.state_machine.connect_ok()
        self.start_ws_thread()
        self.balance_timer.start()
        self.request_exchange_info.emit()
        self.request_orderbook_snapshot.emit()
        self.request_refresh_balance.emit()
        self.log_bus.log("INFO", "INFO", "Подключение установлено")
        self._update_ui_state()

    def on_cycle_update(self, view_model) -> None:
        try:
            self.state_machine.state = BotState(view_model.state)
        except ValueError:
            self.state_machine.state = BotState.ERROR
        self.state_machine.active_cycle = bool(view_model.active_cycle)
        self.state_machine.cycle_id = int(view_model.cycle_id)
        self.cycle_start_time = view_model.start_ts
        self.sim_entry_price = view_model.entry_mid
        self.sim_exit_price = view_model.exit_mid
        self.sim_last_mid = view_model.last_mid
        winner_side = view_model.winner or "—"
        loser_side = view_model.loser or "—"
        self.sim_condition = f"{winner_side} / {loser_side}"
        if self.state_machine.state == BotState.ENTERING:
            self.sim_total_raw_bps = None
            self.sim_total_net_bps = None
            self.sim_winner_raw_bps = None
            self.sim_winner_net_bps = None
            self.sim_loser_raw_bps = None
            self.sim_loser_net_bps = None
            self.sim_long_raw_bps = None
            self.sim_short_raw_bps = None
            self.sim_result = None
            self.sim_pnl_usdt = None
            self.sim_reason = None
            self.sim_ws_age_ms = None
            self.sim_tick_rate = None
            self.sim_impulse_bps = None
            self.sim_spread_bps = None
        if self.state_machine.state == BotState.COOLDOWN:
            self.cooldown_timer.start(self.strategy_params.cooldown_s * 1000)
        total_raw = None
        if view_model.long_raw_bps is not None and view_model.short_raw_bps is not None:
            total_raw = view_model.long_raw_bps + view_model.short_raw_bps
        self.sim_total_raw_bps = total_raw
        self.sim_total_net_bps = view_model.net_bps_total
        if view_model.winner in {"LONG", "SHORT"}:
            self.sim_winner_raw_bps = (
                view_model.long_raw_bps
                if view_model.winner == "LONG"
                else view_model.short_raw_bps
            )
            self.sim_winner_net_bps = (
                None
                if self.sim_winner_raw_bps is None
                else self.sim_winner_raw_bps - self.strategy_params.fee_total_bps
            )
            self.sim_loser_raw_bps = (
                view_model.short_raw_bps
                if view_model.winner == "LONG"
                else view_model.long_raw_bps
            )
            self.sim_loser_net_bps = (
                None
                if self.sim_loser_raw_bps is None
                else self.sim_loser_raw_bps - self.strategy_params.fee_total_bps
            )
        else:
            self.sim_winner_raw_bps = None
            self.sim_winner_net_bps = None
            self.sim_loser_raw_bps = None
            self.sim_loser_net_bps = None
        self.sim_long_raw_bps = view_model.long_raw_bps
        self.sim_short_raw_bps = view_model.short_raw_bps
        self.sim_result = view_model.result
        self.sim_pnl_usdt = view_model.pnl_usdt
        self.sim_reason = view_model.reason
        self.sim_ws_age_ms = view_model.ws_age_ms
        self.sim_tick_rate = view_model.tick_rate
        self.sim_impulse_bps = view_model.impulse_bps
        self.sim_spread_bps = view_model.spread_bps
        self.sim_detect_window_ticks = view_model.detect_window_ticks
        self.sim_detect_timeout_ms = view_model.detect_timeout_ms
        self.ws_age_ms = view_model.ws_age_ms
        self.http_age_ms = None
        self.effective_source = view_model.effective_source or "NONE"
        self.effective_age_ms = view_model.effective_age_ms
        self.data_stale = bool(view_model.data_stale)
        self.waiting_for_data = bool(view_model.waiting_for_data)
        if self.waiting_for_data:
            self.sim_reason = "WAIT_DATA"
        self._update_ui_state()

    def on_trade_row(self, payload: dict) -> None:
        if payload.get("phase") == "cycle_summary":
            self.sim_total_raw_bps = payload.get("raw_bps")
            self.sim_total_net_bps = payload.get("net_bps")
            self.sim_exit_price = payload.get("exit_price")
        self.add_trade_row(payload)

    def on_exposure_update(self, payload: dict) -> None:
        open_exposure = payload.get("open_exposure", False)
        if open_exposure:
            self.exposure_label.setText("ЕСТЬ ОТКРЫТАЯ ПОЗИЦИЯ")
            self.exposure_label.setStyleSheet("color: #e74c3c; font-weight: 600;")
        else:
            self.exposure_label.setText("позиция: нет")
            self.exposure_label.setStyleSheet("color: #8c8c8c;")

    def on_engine_log(self, message: dict) -> None:
        self.log_bus.log(
            message.get("category", "INFO"),
            message.get("level", "INFO"),
            message.get("message", ""),
            **message.get("fields", {}),
        )

    def _emit_heartbeat(self) -> None:
        self.request_ui_heartbeat.emit(time.monotonic())

    def _run_loop_check(self) -> None:
        if not self.run_mode:
            return
        if not self.settings_tab.live_enabled_checkbox.isChecked():
            return
        if self.connected and self.margin_permission_ok:
            self.request_attempt_entry.emit()

    def start_ws_thread(self) -> None:
        if self.ws_thread.isRunning():
            return
        self.ws_thread.start()

    def stop_ws_thread(self) -> None:
        if self.ws_thread.isRunning():
            self.ws_thread.stop()
            self.ws_thread.wait(2000)

    def _update_balance_from_spot_account(self, account: dict) -> None:
        balances = account.get("balances", [])
        usdt = next((item for item in balances if item.get("asset") == "USDT"), None)
        if usdt:
            free = float(usdt.get("free", 0.0))
            locked = float(usdt.get("locked", 0.0))
            self.spot_usdt_free = free
            self.spot_balance_label.setText(f"USDT (Spot): свободно={free:,.2f}")

    def _update_balance_from_margin_account(self, account: dict) -> None:
        assets = account.get("userAssets", [])
        usdt = next((item for item in assets if item.get("asset") == "USDT"), None)
        btc = next((item for item in assets if item.get("asset") == "BTC"), None)
        if usdt:
            self.margin_usdt_free = float(usdt.get("free", 0.0))
            self.margin_usdt_borrowed = float(usdt.get("borrowed", 0.0))
            self.margin_usdt_interest = float(usdt.get("interest", 0.0))
            self.margin_usdt_net = float(usdt.get("netAsset", 0.0))
            self.margin_balance_label.setText(
                "USDT (Маржа): "
                f"свободно={self.margin_usdt_free:,.2f} "
                f"долг={self.margin_usdt_borrowed:,.2f} "
                f"net={self.margin_usdt_net:,.2f}"
            )
        if btc:
            self.margin_btc_free = float(btc.get("free", 0.0))
            self.margin_btc_borrowed = float(btc.get("borrowed", 0.0))

    def _update_market_labels(self) -> None:
        self.mid_display.display(f"{self.market_tick.mid:.2f}")
        self.bid_value.setText(f"{self.market_tick.bid:.2f}")
        self.ask_value.setText(f"{self.market_tick.ask:.2f}")
        self.spread_value.setText(f"{self.market_tick.spread_bps:.2f}")
        if self.effective_age_ms is None:
            now = datetime.now(timezone.utc)
            age_ms = (now - self.market_tick.rx_time).total_seconds() * 1000
        else:
            age_ms = self.effective_age_ms
        self.tick_age_label.setText(f"Возраст тика: {age_ms:.0f} мс")
        self.card_tick_age_value.setText(f"{age_ms:.0f}")
        self.effective_source_value.setText(self.effective_source)
        self.last_update_label.setText(
            f"Последнее обновление: {self.market_tick.rx_time.strftime('%H:%M:%S.%f')[:-3]}"
        )

    def _update_sim_labels(self) -> None:
        winner = self.sim_condition
        self.sim_side_label.setText(f"победитель/лузер: {winner}")
        self.sim_notional_label.setText(
            f"номинал USD: {self.strategy_params.usd_notional:.2f}"
        )
        entry = "—" if not self.sim_entry_price else f"{self.sim_entry_price:,.2f}"
        last_mid = "—" if not self.sim_last_mid else f"{self.sim_last_mid:,.2f}"
        exit_mark = "—" if not self.sim_exit_price else f"{self.sim_exit_price:,.2f}"
        self.sim_entry_label.setText(f"вход_mid: {entry}")
        self.sim_last_label.setText(f"последний_mid: {last_mid}")
        self.sim_exit_label.setText(f"выход_mid: {exit_mark}")
        total_raw = (
            "—" if self.sim_total_raw_bps is None else f"{self.sim_total_raw_bps:.2f}"
        )
        total_net = (
            "—" if self.sim_total_net_bps is None else f"{self.sim_total_net_bps:.2f}"
        )
        winner_raw = (
            "—" if self.sim_winner_raw_bps is None else f"{self.sim_winner_raw_bps:.2f}"
        )
        winner_net = (
            "—" if self.sim_winner_net_bps is None else f"{self.sim_winner_net_bps:.2f}"
        )
        loser_raw = (
            "—" if self.sim_loser_raw_bps is None else f"{self.sim_loser_raw_bps:.2f}"
        )
        loser_net = (
            "—" if self.sim_loser_net_bps is None else f"{self.sim_loser_net_bps:.2f}"
        )
        long_raw = (
            "—" if self.sim_long_raw_bps is None else f"{self.sim_long_raw_bps:.2f}"
        )
        short_raw = (
            "—" if self.sim_short_raw_bps is None else f"{self.sim_short_raw_bps:.2f}"
        )
        reason = self.sim_reason or "—"
        ws_age = "—" if self.sim_ws_age_ms is None else f"{self.sim_ws_age_ms:.0f}"
        result = self.sim_result or "—"
        pnl = "—" if self.sim_pnl_usdt is None else f"{self.sim_pnl_usdt:.4f}"
        detect_window = (
            "—"
            if self.sim_detect_window_ticks is None
            else f"{self.sim_detect_window_ticks}"
        )
        detect_timeout = (
            "—" if self.sim_detect_timeout_ms is None else f"{self.sim_detect_timeout_ms}"
        )
        self.sim_reason_label.setText(f"reason: {reason}")
        self.sim_ws_age_label.setText(f"ws_age_ms: {ws_age}")
        self.sim_source_label.setText(f"source: {self.effective_source}")
        tick_rate = "—" if self.sim_tick_rate is None else f"{self.sim_tick_rate:.0f}"
        impulse = "—" if self.sim_impulse_bps is None else f"{self.sim_impulse_bps:.4f}"
        spread = "—" if self.sim_spread_bps is None else f"{self.sim_spread_bps:.2f}"
        self.sim_tick_label.setText(f"tick_rate: {tick_rate}")
        self.sim_impulse_label.setText(f"impulse_bps: {impulse}")
        self.sim_spread_label.setText(f"spread_bps: {spread}")
        self.sim_condition_label.setText(f"условие: {winner}")
        total_raw_label = "—" if total_raw == "—" else total_raw
        total_net_label = "—" if total_net == "—" else total_net
        self.raw_bps_label.setText(f"raw_bps всего: {total_raw_label}")
        self.net_bps_label.setText(f"net_bps всего: {total_net_label}")
        self.winner_raw_label.setText(f"raw_bps победителя: {winner_raw}")
        self.winner_net_label.setText(f"net_bps победителя: {winner_net}")
        self.loser_raw_label.setText(f"raw_bps лузера: {loser_raw}")
        self.loser_net_label.setText(f"net_bps лузера: {loser_net}")
        self.long_raw_label.setText(f"raw_bps long: {long_raw}")
        self.short_raw_label.setText(f"raw_bps short: {short_raw}")
        self.result_label.setText(f"result: {result}")
        self.pnl_label.setText(f"pnl_usdt: {pnl}")
        self.detect_window_label.setText(f"detect_ticks: {detect_window}")
        self.detect_timeout_label.setText(f"detect_timeout_ms: {detect_timeout}")

    def _update_state_labels(self) -> None:
        mode = "RUNNING" if self.run_mode else "IDLE"
        state_value = self.state_machine.state.value
        if self.run_mode and self.waiting_for_data:
            state_value = "WAIT_DATA"
        self.state_label.setText(
            f"Режим: {mode} | Состояние: {state_value}"
        )
        self.active_cycle_label.setText(
            f"активный цикл: {str(self.state_machine.active_cycle).lower()}"
        )
        self.cycle_id_label.setText(f"цикл_id: {self.state_machine.cycle_id}")
        if self.state_machine.active_cycle and self.cycle_start_time:
            start_str = self.cycle_start_time.strftime("%H:%M:%S")
            self.cycle_start_label.setText(f"старт: {start_str}")
            self.cycle_elapsed_label.setText(f"длительность_с: {self._elapsed_seconds():.1f}")
        else:
            self.cycle_start_label.setText("старт: —")
            self.cycle_elapsed_label.setText("длительность_с: —")

    def _update_ui_state(self) -> None:
        connection_text = "ПОДКЛЮЧЕНО" if self.connected else "ОТКЛЮЧЕНО"
        self.connection_label.setText(f"Связь: {connection_text}")
        self.account_connection_label.setText(f"Связь: {connection_text}")
        if self.connected:
            self.connection_label.setStyleSheet("color: #27ae60;")
            self.account_connection_label.setStyleSheet("color: #27ae60;")
        else:
            self.connection_label.setStyleSheet("color: #e74c3c;")
            self.account_connection_label.setStyleSheet("color: #e74c3c;")

        if self.effective_source == "WS":
            data_status = "WS"
            color = "color: #27ae60;"
        elif self.effective_source == "HTTP":
            data_status = "HTTP"
            color = "color: #f1c40f;"
        else:
            data_status = "NONE"
            color = "color: #e74c3c;"
        self.data_label.setText(f"Данные: {data_status}")
        self.data_label.setStyleSheet(color)
        self.account_data_label.setText(f"Данные: {data_status}")
        self.account_data_label.setStyleSheet(color)

        self.start_cycle_button.setEnabled(self._can_run() and not self.run_mode)
        self.stop_button.setEnabled(
            self.run_mode
            or self.state_machine.state
            in {
                BotState.ENTERING,
                BotState.DETECTING,
                BotState.CUTTING,
                BotState.RIDING,
                BotState.EXITING,
                BotState.CONTROLLED_FLATTEN,
                BotState.COOLDOWN,
            }
        )

        mode_text = {
            MarketDataMode.WS_ONLY: "WS only",
            MarketDataMode.HTTP_ONLY: "HTTP only",
            MarketDataMode.HYBRID: "Hybrid (WS→HTTP)",
        }.get(self.data_mode, "Hybrid (WS→HTTP)")
        self.price_source_label.setText(f"Режим: {mode_text}")
        self.fee_label.setText(f"комиссия bps: {self.strategy_params.fee_total_bps:.2f}")
        self.target_label.setText(f"цель net bps: {self.strategy_params.target_net_bps}")
        self.max_loss_label.setText(f"макс. убыток bps: {self.strategy_params.max_loss_bps}")
        self.max_spread_label.setText(f"макс. спред bps: {self.strategy_params.max_spread_bps}")
        self.min_volume_label.setText(f"мин. тик-рейт: {self.strategy_params.min_tick_rate}")
        self.cooldown_label.setText(f"cooldown сек: {self.strategy_params.cooldown_s}")
        self.permissions_label.setText(f"Права: {self.account_permissions}")
        if self.last_balance_update:
            self.balance_updated_label.setText(
                f"Обновлено: {self.last_balance_update.strftime('%H:%M:%S')}"
            )

        if not self.margin_permission_ok:
            self.settings_tab.live_enabled_checkbox.setChecked(False)
            self.settings_tab.live_enabled_checkbox.setEnabled(False)
            self.settings_tab.live_warning.setText(
                "Нет разрешения на маржинальную торговлю (Spot & Margin)."
            )
        else:
            self.settings_tab.live_enabled_checkbox.setEnabled(True)
            self.settings_tab.live_warning.setText("Отправляет реальные ордера")

        entry_allowed = self._can_run()
        self.entry_allowed_label.setText(
            f"готовность торговли: {str(entry_allowed).lower()}"
        )
        self.entry_allowed_label.setStyleSheet(
            "color: #27ae60;" if entry_allowed else "color: #e74c3c;"
        )
        self._update_state_labels()
        self._update_sim_labels()

    def on_log_entry(self, entry: dict) -> None:
        self.log_entries.append(entry)
        if not hasattr(self, "log_output"):
            return
        if self._log_entry_visible(entry):
            self.log_output.appendPlainText(self._format_log_line(entry))
        if entry.get("category") == "DEALS" and hasattr(self, "deals_log_output"):
            self.deals_log_output.appendPlainText(self._format_log_line(entry))

    def _format_log_line(self, entry: dict) -> str:
        ts = entry.get("timestamp")
        if isinstance(ts, datetime):
            ts_str = ts.strftime("%H:%M:%S.%f")[:-3]
        else:
            ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        category = entry.get("category", "INFO")
        level = entry.get("level", "INFO")
        message = entry.get("message", "")
        fields = entry.get("fields", {})
        lines = [f"{ts_str} | {category} | {level} | {message}"]
        for key, value in fields.items():
            lines.append(f"  {key}={value}")
        return "\n".join(lines)

    def _log_entry_visible(self, entry: dict) -> bool:
        if not hasattr(self, "log_filter_combo"):
            return True
        category_filter = self.log_filter_combo.currentText()
        category_map = {
            "ВСЕ": None,
            "Инфо": "INFO",
            "Ошибки": "ERROR",
            "Торговля": "TRADE",
            "Сделки": "DEALS",
        }
        selected_category = category_map.get(category_filter)
        if selected_category and entry.get("category") != selected_category:
            return False
        level_filter = self.log_level_combo.currentText()
        if level_filter != "ВСЕ" and entry.get("level") != level_filter:
            return False
        query = self.log_search_input.text().strip().lower()
        if not query:
            return True
        text = self._format_log_line(entry).lower()
        return query in text

    def _refresh_log_view(self) -> None:
        lines = []
        for entry in self.log_entries:
            if self._log_entry_visible(entry):
                lines.append(self._format_log_line(entry))
        self.log_output.setPlainText("\n".join(lines))

    def open_logs_folder(self) -> None:
        logs_path = Path("logs").resolve()
        QMessageBox.information(self, "Логи", f"Папка логов: {logs_path}")

    def save_logs_to_file(self) -> None:
        filename, _ = QFileDialog.getSaveFileName(
            self, "Сохранить лог", str(Path("logs").resolve()), "Text Files (*.txt)"
        )
        if not filename:
            return
        Path(filename).write_text(self.log_output.toPlainText(), encoding="utf-8")
        QMessageBox.information(self, "Логи", f"Сохранено: {filename}")

    def add_trade_row(self, payload: dict) -> None:
        row = self.trades_table.rowCount()
        self.trades_table.insertRow(row)
        ts = payload.get("ts")
        if isinstance(ts, datetime):
            ts_value = ts.strftime("%H:%M:%S")
        else:
            ts_value = datetime.now(timezone.utc).strftime("%H:%M:%S")
        values = [
            ts_value,
            str(payload.get("cycle_id", self.state_machine.cycle_id)),
            str(payload.get("phase", "")),
            str(payload.get("side", "")),
            f"{float(payload.get('qty', 0.0)):.6f}",
            f"{float(payload.get('entry_price', 0.0)):.2f}",
            f"{float(payload.get('exit_price', 0.0)):.2f}",
            f"{float(payload.get('raw_bps', 0.0)):.2f}",
            f"{float(payload.get('net_bps', 0.0)):.2f}",
            f"{float(payload.get('net_usd', 0.0)):.2f}",
            str(payload.get("duration_ms", 0)),
            str(payload.get("note", "")),
        ]
        for col, value in enumerate(values):
            self.trades_table.setItem(row, col, QTableWidgetItem(value))

    def add_test_trade(self) -> None:
        self.add_trade_row(
            {
                "cycle_id": self.state_machine.cycle_id,
                "phase": "debug",
                "side": "TEST",
                "qty": 0.001,
                "entry_price": 65000.0,
                "exit_price": 65010.0,
                "raw_bps": 1.5,
                "net_bps": 1.5 - self.strategy_params.fee_total_bps,
                "net_usd": 0.1,
                "duration_ms": 1000,
                "note": "debug",
            }
        )

    def _elapsed_seconds(self) -> float:
        if not self.cycle_start_time:
            return 0.0
        return (datetime.now(timezone.utc) - self.cycle_start_time).total_seconds()

    def _initialize_defaults(self) -> None:
        self._load_strategy_params()
        self._sync_params_to_form()
        self.source_mode_combo.setCurrentIndex(
            self.source_mode_combo.findData(self.data_mode.value)
        )
        self.request_set_data_mode.emit(self.data_mode.value)
        self._update_http_timer()
        self.cycle_start_time = None
        self.sim_entry_price = None
        self.sim_entry_qty = None
        self.sim_exit_price = None
        self.sim_total_raw_bps = None
        self.sim_total_net_bps = None

    def _load_strategy_params(self) -> None:
        params, symbol = self.strategy_store.load_strategy_params()
        self.strategy_params = params
        if symbol:
            index = self.symbol_combo.findText(symbol)
            if index >= 0:
                self.symbol_combo.setCurrentIndex(index)
        self.log_bus.log(
            "INFO",
            "INFO",
            "Trade params loaded",
            path="config/strategy_params.json",
            symbol=symbol,
        )

    def _sync_settings_from_form(self) -> None:
        self.connection_settings.api_key = self.settings_tab.api_key_input.text().strip()
        self.connection_settings.api_secret = self.settings_tab.api_secret_input.text().strip()
        self.connection_settings.mode = ConnectionMode.MARGIN
        self.connection_settings.leverage = int(self.settings_tab.leverage_combo.currentText()[0])
        self.connection_settings.save_local = self.settings_tab.save_checkbox.isChecked()
        self.connection_settings.live_enabled = (
            self.settings_tab.live_enabled_checkbox.isChecked()
        )

    def _load_settings_into_form(self) -> None:
        self.settings_tab.api_key_input.setText(self.connection_settings.api_key)
        self.settings_tab.api_secret_input.setText(self.connection_settings.api_secret)
        self.settings_tab.leverage_combo.setCurrentText(f"{self.connection_settings.leverage}x")
        self.settings_tab.save_checkbox.setChecked(self.connection_settings.save_local)
        self.settings_tab.live_enabled_checkbox.setChecked(self.connection_settings.live_enabled)

    def _can_run(self) -> bool:
        if not self.settings_tab.live_enabled_checkbox.isChecked():
            return False
        if not self.margin_permission_ok:
            return False
        if not self.connection_settings.api_key or not self.connection_settings.api_secret:
            return False
        return self.connected

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if not hasattr(self, "_initialized"):
            self._initialize_defaults()
            self._initialized = True

    def _auto_connect(self) -> None:
        self._sync_settings_from_form()
        if not self.connection_settings.api_key or not self.connection_settings.api_secret:
            self.connected = False
            self.state_machine.disconnect()
            self.account_permissions = "НЕТ КЛЮЧЕЙ"
            self.log_bus.log("INFO", "INFO", "Нет ключей API для REST")
            self.request_set_connection.emit({"api_key": "", "api_secret": ""})
            self.start_ws_thread()
            self._update_ui_state()
            return
        if self.connection_settings.save_local:
            self.settings_store.save(self.connection_settings)
        self.request_set_connection.emit(
            {
                "api_key": self.connection_settings.api_key,
                "api_secret": self.connection_settings.api_secret,
            }
        )
        self.request_validate.emit(False)
        self.start_ws_thread()


if __name__ == "__main__":
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()
