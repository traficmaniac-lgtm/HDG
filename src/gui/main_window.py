from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
import logging
import os
import threading
from typing import Optional

from datetime import datetime
import time
from collections import deque

import httpx
from PySide6.QtCore import Qt, QThread, QTimer, Signal, Slot
from PySide6.QtGui import QAction, QFontDatabase, QFontMetrics, QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMenu,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QTableView,
    QMessageBox,
    QSizePolicy,
)

from src.core.config_store import ApiCredentials, ConfigStore
from src.core.logger import INFO_EVENT_LEVEL, get_logger, resolve_level
from src.core.models import PriceState, Settings, SymbolProfile
from src.core.version import VERSION
from src.gui.api_settings_dialog import ApiSettingsDialog
from src.gui.trade_settings_dialog import TradeSettingsDialog
from src.services.binance_rest import BinanceRestClient
from src.services.http_price import HttpPriceService
from src.services.order_tracker import OrderTracker
from src.services.price_router import PriceRouter
from src.services.trade_executor import TradeExecutor
from src.services.ws_price import WsPriceWorker


class ElidedLabel(QLabel):
    def __init__(self, text: str = "", parent: Optional[QWidget] = None) -> None:
        super().__init__(text, parent)
        self._full_text = text

    def setText(self, text: str) -> None:  # noqa: N802 - Qt override
        self._full_text = text
        self._update_elide()

    def resizeEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().resizeEvent(event)
        self._update_elide()

    def _update_elide(self) -> None:
        metrics = QFontMetrics(self.font())
        elided = metrics.elidedText(
            self._full_text, Qt.TextElideMode.ElideRight, self.width()
        )
        super().setText(elided)


class MainWindow(QMainWindow):
    order_tracker_sync = Signal(list)
    order_tracker_stop = Signal()
    order_tracker_update_credentials = Signal(str, str)

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"Directional Hedge Scalper v{VERSION} — EURIUSDT Core")
        self.setMinimumSize(900, 600)

        self._connected = False
        self._ws_connected = False
        self._settings: Optional[Settings] = None
        self._router: Optional[PriceRouter] = None
        self._rest: Optional[BinanceRestClient] = None
        self._http_service: Optional[HttpPriceService] = None
        self._trade_executor: Optional[TradeExecutor] = None
        self._symbol_profile = SymbolProfile()
        self._auth_warning_shown = False
        self._api_credentials = ApiCredentials()
        self._config_store = ConfigStore()
        self._last_mid: Optional[float] = None
        self._orders_error_logged = False
        self._margin_checked = False
        self._margin_api_access = False
        self._borrow_allowed_by_api: Optional[bool] = None
        self._order_tracker: Optional[OrderTracker] = None
        self._order_tracker_thread: Optional[QThread] = None
        self._last_ws_status_log_ts = 0.0
        self._data_blind_active = False
        self._last_data_blind_log_ts = 0.0
        self._open_order_rows: dict[str, int] = {}
        self._fills_history: list[dict] = []
        self._fill_rows_by_event_key: dict[str, int] = {}
        self._cycle_id_counter = 0
        self._last_buy_fill_price: Optional[float] = None
        self._profile_info_text = "—"
        self._log_queue = deque()
        self._log_lock = threading.Lock()
        self._log_dropped_lines = 0
        self._last_log_message: Optional[str] = None
        self._last_log_ts = 0.0
        self._log_max_lines = 500
        self._log_max_buffer = 700
        self._log_flush_limit = 120
        self._auth_warning_pending = False
        self._logger = get_logger()
        self._verbose_log_last_ts: dict[str, float] = {}
        self._ui_log_last_ts: dict[str, float] = {}

        self._ui_timer = QTimer(self)
        self._ui_timer.timeout.connect(self._refresh_ui)
        self._http_timer = QTimer(self)
        self._http_timer.timeout.connect(self._poll_http)
        self._orders_timer = QTimer(self)
        self._orders_timer.timeout.connect(self._refresh_orders)
        self._watchdog_timer = QTimer(self)
        self._watchdog_timer.timeout.connect(self._watchdog_tick)
        self._log_flush_timer = QTimer(self)
        self._log_flush_timer.timeout.connect(self._flush_log_queue)

        self._ws_thread = None
        self._ws_worker = None
        self._rest_exec = ThreadPoolExecutor(max_workers=1)
        self._rest_future = None

        self._build_ui()
        self._load_api_state()
        self._update_status(False)
        self._log_flush_timer.start(200)

    def _build_ui(self) -> None:
        central = QWidget()
        root = QVBoxLayout(central)

        self._build_menu()

        fixed_font = QFontDatabase.systemFont(QFontDatabase.FixedFont)

        header = QFrame()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(12, 6, 12, 6)
        header_layout.setSpacing(8)
        header.setFixedHeight(46)
        self.summary_label = ElidedLabel(
            "EURIUSDT | SRC NONE | ages —/— | spread — | FSM —/— | pos — | "
            "pnl —/—/— | last_exit —"
        )
        self.summary_label.setFont(fixed_font)
        self.summary_label.setStyleSheet("font-weight: 600;")
        self.summary_label.setWordWrap(False)
        self.summary_label.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
        self.summary_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.connect_button = QPushButton("CONNECT")
        self.connect_button.clicked.connect(self._toggle_connection)
        self.status_label = QLabel("DISCONNECTED")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.status_label.setFixedWidth(120)
        self.status_label.setStyleSheet("color: #3ad07d; font-weight: 600;")
        self.start_long_button = QPushButton("START LONG")
        self.start_long_button.clicked.connect(self._start_trading_long)
        self.start_short_button = QPushButton("START SHORT")
        self.start_short_button.clicked.connect(self._start_trading_short)
        self.stop_button = QPushButton("STOP")
        self.stop_button.clicked.connect(self._stop_trading)
        self.settings_button = QPushButton("SETTINGS")
        self.settings_button.clicked.connect(self._open_trade_settings)
        self.connect_button.setFixedHeight(28)
        self.connect_button.setFixedWidth(130)
        self.start_long_button.setFixedHeight(28)
        self.start_long_button.setFixedWidth(130)
        self.start_short_button.setFixedHeight(28)
        self.start_short_button.setFixedWidth(130)
        self.stop_button.setFixedHeight(28)
        self.stop_button.setFixedWidth(110)
        self.settings_button.setFixedHeight(28)
        self.settings_button.setFixedWidth(120)
        self.start_long_button.setEnabled(False)
        self.start_short_button.setEnabled(False)
        self.stop_button.setEnabled(False)

        buttons_panel = QWidget()
        buttons_layout = QHBoxLayout(buttons_panel)
        buttons_layout.setContentsMargins(0, 0, 0, 0)
        buttons_layout.setSpacing(8)
        buttons_layout.addWidget(self.connect_button)
        buttons_layout.addWidget(self.start_long_button)
        buttons_layout.addWidget(self.start_short_button)
        buttons_layout.addWidget(self.stop_button)
        buttons_layout.addWidget(self.settings_button)

        header_layout.addWidget(self.summary_label)
        header_layout.addStretch(1)
        header_layout.addWidget(buttons_panel)
        header_layout.addWidget(self.status_label)
        root.addWidget(header)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setChildrenCollapsible(False)

        hud_widget = QWidget()
        hud_layout = QVBoxLayout(hud_widget)
        hud_layout.setSpacing(10)

        market_hud = QFrame()
        market_hud.setFixedHeight(70)
        market_hud.setStyleSheet(
            "QFrame { border: 1px solid #2b2b2b; border-radius: 8px; }"
        )
        market_layout = QVBoxLayout(market_hud)
        market_layout.setContentsMargins(10, 6, 10, 6)
        market_layout.setSpacing(2)
        self.market_primary_label = QLabel("Bid —  Ask —  Mid —  Spr —t")
        primary_font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        primary_font.setPointSize(primary_font.pointSize() + 3)
        self.market_primary_label.setFont(primary_font)
        self.market_primary_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.NoTextInteraction
        )
        self.market_primary_label.setAlignment(
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        )
        self.market_secondary_label = QLabel(
            "SRC — | ws False age — | http age — | blind — | reason —"
        )
        secondary_font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        secondary_font.setPointSize(secondary_font.pointSize() - 1)
        self.market_secondary_label.setFont(secondary_font)
        self.market_secondary_label.setStyleSheet("color: #9a9a9a;")
        self.market_secondary_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.NoTextInteraction
        )
        self.market_secondary_label.setAlignment(
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        )
        market_layout.addWidget(self.market_primary_label)
        market_layout.addWidget(self.market_secondary_label)

        health_group = QGroupBox("Data Health")
        health_group.setFixedHeight(88)
        health_layout = QGridLayout(health_group)
        health_layout.setContentsMargins(10, 10, 10, 10)
        health_layout.setHorizontalSpacing(8)
        health_layout.setVerticalSpacing(6)
        self.ws_connected_label = self._make_value_label("—", min_chars=8, fixed_width=180)
        self.data_blind_label = self._make_value_label("—", min_chars=8, fixed_width=180)
        self.switch_reason_label = self._make_value_label("—", min_chars=10, fixed_width=180)
        self._add_card_row(health_layout, 0, "ws_connected", self.ws_connected_label)
        self._add_card_row(health_layout, 1, "data_blind", self.data_blind_label)
        self._add_card_row(
            health_layout, 2, "effective_source_reason", self.switch_reason_label
        )

        entry_group = QGroupBox("Entry")
        entry_group.setFixedHeight(88)
        entry_layout = QGridLayout(entry_group)
        entry_layout.setContentsMargins(10, 10, 10, 10)
        entry_layout.setHorizontalSpacing(8)
        entry_layout.setVerticalSpacing(6)
        self.entry_price_label = self._make_value_label("—", min_chars=12, fixed_width=180)
        self.entry_age_label = self._make_value_label("—", min_chars=8, fixed_width=180)
        self.entry_reprice_reason_label = self._make_value_label(
            "—", min_chars=12, fixed_width=180
        )
        self._add_card_row(entry_layout, 0, "entry_price", self.entry_price_label)
        self._add_card_row(entry_layout, 1, "entry_age_ms", self.entry_age_label)
        self._add_card_row(
            entry_layout, 2, "last_entry_reprice_reason", self.entry_reprice_reason_label
        )

        exit_group = QGroupBox("Exit")
        exit_group.setFixedHeight(110)
        exit_layout = QGridLayout(exit_group)
        exit_layout.setContentsMargins(10, 10, 10, 10)
        exit_layout.setHorizontalSpacing(8)
        exit_layout.setVerticalSpacing(6)
        self.tp_price_label = self._make_value_label("—", min_chars=12, fixed_width=180)
        self.sl_price_label = self._make_value_label("—", min_chars=12, fixed_width=180)
        self.exit_intent_label = self._make_value_label("—", min_chars=10, fixed_width=180)
        self.exit_policy_label = self._make_value_label("—", min_chars=10, fixed_width=180)
        self.exit_age_label = self._make_value_label("—", min_chars=8, fixed_width=180)
        self._add_card_row(exit_layout, 0, "tp_trigger_price", self.tp_price_label)
        self._add_card_row(exit_layout, 1, "sl_trigger_price", self.sl_price_label)
        self._add_card_row(exit_layout, 2, "exit_intent", self.exit_intent_label)
        self._add_card_row(exit_layout, 3, "exit_policy", self.exit_policy_label)
        self._add_card_row(exit_layout, 4, "exit_age_ms", self.exit_age_label)

        timers_group = QGroupBox("Timers")
        timers_group.setFixedHeight(66)
        timers_layout = QGridLayout(timers_group)
        timers_layout.setContentsMargins(10, 10, 10, 10)
        timers_layout.setHorizontalSpacing(8)
        timers_layout.setVerticalSpacing(6)
        self.cycles_target_label = self._make_value_label("—", min_chars=6, fixed_width=180)
        self._add_card_row(timers_layout, 0, "cycles_target", self.cycles_target_label)

        hud_layout.addWidget(market_hud)
        hud_layout.addWidget(health_group)
        hud_layout.addWidget(entry_group)
        hud_layout.addWidget(exit_group)
        hud_layout.addWidget(timers_group)
        hud_layout.addStretch(1)

        orders_tabs = QTabWidget()
        orders_tabs.setTabPosition(QTabWidget.TabPosition.North)

        open_tab = QWidget()
        open_layout = QVBoxLayout(open_tab)
        self.orders_model = QStandardItemModel(0, 8, self)
        self.orders_model.setHorizontalHeaderLabels(
            [
                "time",
                "side",
                "price",
                "qty",
                "status",
                "age_ms",
                "dist_ticks_to_fill",
                "tag",
            ]
        )
        self.orders_table = QTableView()
        self.orders_table.setModel(self.orders_model)
        self.orders_table.verticalHeader().setVisible(False)
        self.orders_table.setAlternatingRowColors(True)
        self.orders_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.orders_table.setWordWrap(False)
        self.orders_table.setFont(fixed_font)
        open_header = self.orders_table.horizontalHeader()
        open_header.setStretchLastSection(False)
        open_header.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        column_widths = [90, 55, 110, 110, 90, 80, 120, 260]
        for idx, width in enumerate(column_widths):
            self.orders_table.setColumnWidth(idx, width)
        open_layout.addWidget(self.orders_table, stretch=1)
        orders_tabs.addTab(open_tab, "OPEN")

        fills_tab = QWidget()
        fills_layout = QVBoxLayout(fills_tab)
        self.fills_model = QStandardItemModel(0, 10, self)
        self.fills_model.setHorizontalHeaderLabels(
            [
                "time",
                "side",
                "avg_fill_price",
                "qty",
                "status",
                "pnl_quote",
                "pnl_bps",
                "unreal_est",
                "exit_reason",
                "cycle_id",
            ]
        )
        self.fills_table = QTableView()
        self.fills_table.setModel(self.fills_model)
        self.fills_table.verticalHeader().setVisible(False)
        self.fills_table.setAlternatingRowColors(True)
        self.fills_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.fills_table.setWordWrap(False)
        self.fills_table.setFont(fixed_font)
        fills_header = self.fills_table.horizontalHeader()
        fills_header.setStretchLastSection(False)
        fills_header.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        fills_widths = [90, 55, 120, 110, 80, 110, 90, 110, 120, 90]
        for idx, width in enumerate(fills_widths):
            self.fills_table.setColumnWidth(idx, width)
        fills_layout.addWidget(self.fills_table, stretch=1)
        orders_tabs.addTab(fills_tab, "FILLS")

        splitter.addWidget(hud_widget)
        splitter.addWidget(orders_tabs)
        hud_widget.setMinimumWidth(420)
        hud_widget.setMaximumWidth(460)
        hud_widget.setFixedWidth(440)
        orders_tabs.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        splitter.setSizes([440, 1000])
        root.addWidget(splitter, stretch=1)

        scoreboard = QFrame()
        scoreboard_layout = QHBoxLayout(scoreboard)
        scoreboard_layout.setContentsMargins(8, 4, 8, 6)
        scoreboard_layout.setSpacing(18)
        self.wins_value_label = self._make_score_value_label("0")
        self.losses_value_label = self._make_score_value_label("0")
        self.winrate_value_label = self._make_score_value_label("0.0%")
        self.pnl_session_value_label = self._make_score_value_label("—", min_chars=10)
        self.avg_pnl_value_label = self._make_score_value_label("—", min_chars=10)
        scoreboard_layout.addWidget(self._make_score_item("WINS", self.wins_value_label))
        scoreboard_layout.addWidget(self._make_score_item("LOSSES", self.losses_value_label))
        scoreboard_layout.addWidget(self._make_score_item("WINRATE", self.winrate_value_label))
        scoreboard_layout.addWidget(
            self._make_score_item("PNL_SESSION", self.pnl_session_value_label)
        )
        scoreboard_layout.addWidget(
            self._make_score_item("AVG_PNL_PER_CYCLE", self.avg_pnl_value_label)
        )
        scoreboard_layout.addStretch(1)
        root.addWidget(scoreboard)

        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.document().setMaximumBlockCount(self._log_max_lines)
        self._log_dialog = QDialog(self)
        self._log_dialog.setWindowTitle("Logs")
        log_layout = QVBoxLayout(self._log_dialog)
        log_layout.addWidget(self.log)
        self._log_dialog.resize(900, 480)

        self.setCentralWidget(central)

    def _build_menu(self) -> None:
        menu = QMenu("Меню", self)
        self.menuBar().addMenu(menu)

        api_action = QAction("Настройки API...", self)
        api_action.triggered.connect(self._open_api_settings)
        menu.addAction(api_action)

        profile_action = QAction("Инфо профиля", self)
        profile_action.triggered.connect(self._open_profile_info)
        menu.addAction(profile_action)

        log_view_action = QAction("Показать лог", self)
        log_view_action.triggered.connect(self._open_log_dialog)
        menu.addAction(log_view_action)

        save_log_action = QAction("Сохранить лог", self)
        save_log_action.triggered.connect(self._save_log)
        menu.addAction(save_log_action)

        clear_log_action = QAction("Очистить лог", self)
        clear_log_action.triggered.connect(self._clear_log)
        menu.addAction(clear_log_action)

        auto_calc_action = QAction("Авто-расчёт параметров торговли", self)
        auto_calc_action.setEnabled(False)
        menu.addAction(auto_calc_action)

        menu.addSeparator()

        exit_action = QAction("Выход", self)
        exit_action.triggered.connect(self.close)
        menu.addAction(exit_action)

    def _make_value_label(
        self,
        text: str = "—",
        min_chars: int = 8,
        align: Qt.AlignmentFlag = Qt.AlignmentFlag.AlignRight,
        fixed_width: Optional[int] = None,
    ) -> QLabel:
        label = QLabel(text)
        font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        label.setFont(font)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
        label.setAlignment(align | Qt.AlignmentFlag.AlignVCenter)
        metrics = QFontMetrics(font)
        if fixed_width is not None:
            label.setFixedWidth(fixed_width)
        else:
            label.setMinimumWidth(metrics.horizontalAdvance("0" * min_chars))
        return label

    @staticmethod
    def _make_card_label(text: str) -> QLabel:
        label = QLabel(text)
        label.setStyleSheet("color: #8c8c8c;")
        label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        return label

    def _add_card_row(self, layout: QGridLayout, row: int, title: str, value: QLabel) -> None:
        label = self._make_card_label(title)
        layout.addWidget(label, row, 0)
        layout.addWidget(value, row, 1)

    @staticmethod
    def _elide_text(text: str, table: QTableView, column: int) -> str:
        metrics = QFontMetrics(table.font())
        width = table.columnWidth(column) - 12
        return metrics.elidedText(text, Qt.TextElideMode.ElideRight, max(0, width))

    def _make_big_value_label(self, text: str = "—", min_chars: int = 10) -> QLabel:
        label = QLabel(text)
        font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        font.setPointSize(font.pointSize() + 6)
        label.setFont(font)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
        label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        metrics = QFontMetrics(font)
        label.setMinimumWidth(metrics.horizontalAdvance("0" * min_chars))
        return label

    def _make_score_value_label(self, text: str = "—", min_chars: int = 6) -> QLabel:
        label = QLabel(text)
        font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        font.setPointSize(font.pointSize() + 2)
        label.setFont(font)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        metrics = QFontMetrics(font)
        label.setMinimumWidth(metrics.horizontalAdvance("0" * min_chars))
        return label

    @staticmethod
    def _make_score_item(title: str, value_label: QLabel) -> QWidget:
        container = QFrame()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(4, 2, 4, 2)
        title_label = QLabel(title)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet("color: #8c8c8c; font-size: 10px;")
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        return container

    def _open_profile_info(self) -> None:
        QMessageBox.information(self, "Symbol Info", self._profile_info_text or "—")

    def _open_log_dialog(self) -> None:
        if self._log_dialog:
            self._log_dialog.show()
            self._log_dialog.raise_()

    @Slot()
    def _toggle_connection(self) -> None:
        if self._connected:
            self.disconnect()
        else:
            self.connect()

    def connect(self) -> None:
        if self._connected:
            return
        try:
            self._settings = self._load_settings()
        except Exception as exc:
            self._append_log(f"Failed to load settings: {exc}")
            return

        self._append_log("Loaded settings")
        env = self._load_env()

        self._router = PriceRouter(self._settings)
        api_credentials = self._resolve_api_credentials(env)
        self._rest = BinanceRestClient(
            api_key=api_credentials.key,
            api_secret=api_credentials.secret,
        )
        self._http_service = HttpPriceService()

        try:
            server_time = self._rest.get_server_time()
            self._append_log(f"Server time synced: {server_time.get('serverTime')}")
            exchange_info = self._rest.get_exchange_info(self._settings.symbol)
            self._symbol_profile = self._parse_symbol_profile(exchange_info)
            self._render_symbol_profile(self._symbol_profile)
            if self._router:
                self._router.set_tick_size(self._symbol_profile.tick_size)
        except Exception as exc:
            self._append_log(f"REST init error: {exc}")

        self._margin_checked = False
        self._margin_api_access = False
        self._borrow_allowed_by_api = None
        if self._rest and self._has_valid_api_credentials(api_credentials):
            self._check_margin_permissions()

        if (
            self._rest
            and self._router
            and self._settings
            and self._has_valid_api_credentials(api_credentials)
        ):
            self._trade_executor = TradeExecutor(
                rest=self._rest,
                router=self._router,
                settings=self._settings,
                profile=self._symbol_profile,
                logger=self._append_log,
            )
            self._trade_executor.set_margin_capabilities(
                self._margin_api_access, self._borrow_allowed_by_api
            )
            tracker_rest = BinanceRestClient(
                api_key=api_credentials.key,
                api_secret=api_credentials.secret,
            )
            self._order_tracker = OrderTracker(
                rest=tracker_rest,
                symbol=self._settings.symbol,
                poll_ms=self._settings.order_poll_ms,
                logger=self._append_log,
                owns_rest=True,
            )
            self._order_tracker_thread = QThread(self)
            self._order_tracker.moveToThread(self._order_tracker_thread)
            self._order_tracker_thread.started.connect(self._order_tracker.start)
            self._order_tracker_thread.finished.connect(self._order_tracker.deleteLater)
            self.order_tracker_sync.connect(
                self._order_tracker.sync_active_orders, Qt.ConnectionType.QueuedConnection
            )
            self.order_tracker_stop.connect(
                self._order_tracker.stop, Qt.ConnectionType.QueuedConnection
            )
            self.order_tracker_update_credentials.connect(
                self._order_tracker.update_api_credentials,
                Qt.ConnectionType.QueuedConnection,
            )
            self._order_tracker_thread.start()
            self._order_tracker.order_filled.connect(self._on_order_filled)
            self._order_tracker.order_partial.connect(self._on_order_partial)
            self._order_tracker.order_done.connect(self._on_order_done)
            self._update_trading_controls()
        else:
            self.start_long_button.setEnabled(False)
            self.start_short_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            if not self._has_valid_api_credentials(api_credentials):
                self._append_log("[API] missing, trading disabled")

        self._start_ws()
        self._http_timer.start(self._settings.http_poll_ms)
        self._ui_timer.start(self._settings.ui_refresh_ms)
        self._orders_timer.start(1000)
        self._watchdog_timer.start(self._settings.watchdog_poll_ms)

        self._connected = True
        self._update_status(True)
        self._append_log("CONNECT: data services started")

    def disconnect(self) -> None:
        if not self._connected:
            return

        if self._trade_executor:
            self._trade_executor.stop_run_by_user(reason="disconnect")

        self._ui_timer.stop()
        self._http_timer.stop()
        self._orders_timer.stop()
        self._watchdog_timer.stop()
        if self._order_tracker:
            self.order_tracker_stop.emit()
            self._order_tracker = None
        if self._order_tracker_thread:
            self._order_tracker_thread.quit()
            self._order_tracker_thread.wait(2000)
            self._order_tracker_thread = None
        if self._rest_future is not None:
            self._rest_future.cancel()
            self._rest_future = None

        if self._ws_worker is not None:
            self._ws_worker.stop()
        if self._ws_thread is not None:
            self._ws_thread.quit()
            self._ws_thread.wait(2000)

        if self._rest:
            self._rest.close()
        if self._http_service:
            self._http_service.close()

        self._trade_executor = None
        self.start_long_button.setEnabled(False)
        self.start_short_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self._orders_model_clear()
        self._fills_model_clear()
        self._set_hud_defaults()

        self._connected = False
        self._ws_connected = False
        self._margin_checked = False
        self._margin_api_access = False
        self._borrow_allowed_by_api = None
        self._update_status(False)
        self._append_log("DISCONNECT: data services stopped")

    def _start_ws(self) -> None:
        from PySide6.QtCore import QThread

        if not self._settings:
            return
        self._ws_thread = QThread()
        self._ws_worker = WsPriceWorker(
            self._settings.symbol,
            reconnect_dedup_ms=self._settings.ws_reconnect_dedup_ms,
        )
        self._ws_worker.moveToThread(self._ws_thread)
        self._ws_thread.started.connect(self._ws_worker.run)
        self._ws_worker.finished.connect(self._ws_thread.quit)
        self._ws_worker.finished.connect(self._ws_worker.deleteLater)
        self._ws_thread.finished.connect(self._ws_thread.deleteLater)
        self._ws_worker.tick.connect(self._on_ws_tick)
        self._ws_worker.status.connect(self._on_ws_status)
        self._ws_worker.issue.connect(self._on_ws_issue)
        self._ws_worker.log.connect(self._append_log)
        self._ws_thread.start()

    @Slot(float, float)
    def _on_ws_tick(self, bid: float, ask: float) -> None:
        if self._router:
            self._router.update_ws(bid, ask)
        if self._trade_executor:
            self._trade_executor.note_progress("ws_tick")

    @Slot(bool)
    def _on_ws_status(self, status: bool) -> None:
        if status == self._ws_connected:
            return
        self._ws_connected = status
        if self._router:
            self._router.set_ws_connected(status)
        now = time.monotonic()
        throttle_s = (
            self._settings.ws_log_throttle_ms / 1000.0 if self._settings else 5.0
        )
        if now - self._last_ws_status_log_ts >= throttle_s:
            self._last_ws_status_log_ts = now
            self._append_log(f"WS connected: {status}")

    @Slot(str)
    def _on_ws_issue(self, reason: str) -> None:
        if self._router:
            self._router.record_ws_issue(reason)

    @Slot(int, str, float, float, "qint64")
    def _on_order_filled(
        self, order_id: int, side: str, price: float, qty: float, ts_ms: int
    ) -> None:
        if self._trade_executor:
            self._trade_executor.handle_order_filled(
                order_id=order_id,
                side=side,
                price=price,
                qty=qty,
                ts_ms=ts_ms,
            )
        self._register_fill(order_id, side, price, qty, ts_ms)
        self._refresh_orders()

    @Slot(int, str, float, float, "qint64")
    def _on_order_partial(
        self, order_id: int, side: str, cum_qty: float, avg_price: float, ts_ms: int
    ) -> None:
        if self._trade_executor:
            self._trade_executor.handle_order_partial(
                order_id=order_id,
                side=side,
                cum_qty=cum_qty,
                avg_price=avg_price,
                ts_ms=ts_ms,
            )
        self._refresh_orders()

    @Slot(int, str, "qint64")
    def _on_order_done(self, order_id: int, status: str, ts_ms: int) -> None:
        if self._trade_executor:
            self._trade_executor.handle_order_done(order_id, status, ts_ms)
        self._refresh_orders()

    @Slot()
    def _poll_http(self) -> None:
        if not self._settings or not self._http_service:
            return
        if self._router and not self._settings.position_guard_http:
            state_label = (
                self._trade_executor.get_state_label() if self._trade_executor else "—"
            )
            in_position = state_label in {
                "POS_OPEN",
                "ENTRY_WORKING",
                "EXIT_TP_WORKING",
                "EXIT_SL_WORKING",
                "RECOVERY",
                "RECONCILE",
                "SAFE_STOP",
            }
            if not in_position and not self._router.is_ws_stale():
                return
        try:
            payload = self._http_service.fetch_book_ticker(self._settings.symbol)
            bid = float(payload.get("bidPrice", 0.0))
            ask = float(payload.get("askPrice", 0.0))
            if self._router:
                self._router.update_http(bid, ask)
            if self._trade_executor:
                self._trade_executor.note_progress("http_tick")
        except Exception as exc:
            self._append_log(f"HTTP ticker error: {exc}")

    def _watchdog_tick(self) -> None:
        if not self._trade_executor:
            return
        if self._rest_future is not None and self._rest_future.done():
            try:
                snapshot = self._rest_future.result(timeout=0)
            except Exception as exc:
                self._append_log(f"[WD] reconcile failed: {exc}")
                self._trade_executor.mark_reconcile_failed(str(exc))
            else:
                if snapshot is None:
                    self._append_log("[WD] reconcile failed: empty snapshot")
                    self._trade_executor.mark_reconcile_failed("snapshot_missing")
                else:
                    self._trade_executor.apply_reconcile_snapshot(snapshot)
                    self._append_log("[WD] reconcile applied")
            self._rest_future = None
        if self._rest_future is None:
            if self._trade_executor.watchdog_tick():
                self._rest_future = self._rest_exec.submit(
                    self._trade_executor.collect_reconcile_snapshot
                )
                self._append_log("[WD] reconcile submitted")

    def _refresh_ui(self) -> None:
        if not self._router:
            return
        price_state, health_state = self._router.build_price_state()
        self._last_mid = price_state.mid
        for log_message in self._router.consume_logs():
            if isinstance(log_message, tuple):
                message, level = log_message
                self._append_log(message, level=level)
            else:
                self._append_log(log_message)

        tick_size = self._symbol_profile.tick_size
        spread_ticks = self._calculate_spread_ticks(
            price_state.bid, price_state.ask, tick_size
        )
        bid_label = self._fmt_price(price_state.bid, width=9)
        ask_label = self._fmt_price(price_state.ask, width=9)
        mid_label = self._fmt_price(price_state.mid, width=9)
        spread_label = self._fmt_int(spread_ticks, width=3)
        self.market_primary_label.setText(
            f"Bid {bid_label}  Ask {ask_label}  Mid {mid_label}  Spr {spread_label}t"
        )
        ws_age = self._fmt_int(health_state.ws_age_ms, width=6)
        http_age = self._fmt_int(health_state.http_age_ms, width=6)
        self.market_secondary_label.setText(
            f"SRC {price_state.source} | ws {health_state.ws_connected} age {ws_age} | "
            f"http age {http_age} | blind {price_state.data_blind} | "
            f"reason {health_state.last_switch_reason or '—'}"
        )

        self.ws_connected_label.setText(str(health_state.ws_connected))
        self.data_blind_label.setText(str(price_state.data_blind))
        self.switch_reason_label.setText(health_state.last_switch_reason or "—")

        state_label = "—"
        last_action = "—"
        if self._trade_executor:
            state_label = self._trade_executor.get_state_label()
            last_action = self._trade_executor.last_action

        orders_snapshot = (
            self._trade_executor.get_orders_snapshot() if self._trade_executor else []
        )
        entry_side = self._trade_executor.get_entry_side() if self._trade_executor else "BUY"
        entry_order = self._find_active_order(orders_snapshot, entry_side)
        entry_price = entry_order.get("price") if entry_order else None
        if entry_price is None and self._trade_executor:
            entry_price = self._trade_executor.get_buy_price()
        entry_age_ms = self._entry_age_ms(entry_order)
        self.entry_price_label.setText(self._fmt_price(entry_price, width=12))
        self.entry_age_label.setText(self._fmt_int(entry_age_ms, width=8))
        self.entry_reprice_reason_label.setText("—")

        self._update_exit_status(price_state.mid)

        exit_intent = self._trade_executor.exit_intent if self._trade_executor else None
        exit_policy = "—"
        if exit_intent and self._trade_executor:
            exit_policy, _ = self._trade_executor.resolve_exit_policy(exit_intent)
        self.exit_intent_label.setText(exit_intent or "—")
        self.exit_policy_label.setText(exit_policy or "—")
        self.exit_age_label.setText(self._fmt_int(self._exit_age_ms(), width=8))

        cycles_target = None
        if self._trade_executor:
            cycles_target = self._trade_executor.cycles_target
        elif self._settings:
            cycles_target = self._settings.cycle_count
        self.cycles_target_label.setText(self._fmt_int(cycles_target, width=6))

        self._update_summary_bar(price_state, health_state, spread_ticks, state_label, last_action)
        self._update_scoreboard()
        self._log_data_blind_state(price_state, state_label)
        self._update_trading_controls(price_state)
        if self._trade_executor and self._trade_executor.process_cycle_flow():
            self._refresh_orders()

    def _update_status(self, connected: bool) -> None:
        if connected:
            self.status_label.setText("CONNECTED")
            self.status_label.setStyleSheet("color: #3ad07d; font-weight: 600;")
            self.connect_button.setText("DISCONNECT")
        else:
            self.status_label.setText("DISCONNECTED")
            self.status_label.setStyleSheet("color: #ff5f57; font-weight: 600;")
            self.connect_button.setText("CONNECT")

    def _update_trading_controls(self, price_state: Optional[object] = None) -> None:
        if price_state is not None:
            self._last_mid = price_state.mid
            if self._trade_executor:
                self._trade_executor.evaluate_exit_conditions()
        can_trade = (
            self._connected
            and self._trade_executor is not None
            and self._has_valid_api_credentials(self._api_credentials)
        )
        state_label = self._trade_executor.get_state_label() if self._trade_executor else "—"
        run_active = self._trade_executor.run_active if self._trade_executor else False
        can_start = (
            can_trade and self._margin_api_access and state_label == "IDLE" and not run_active
        )
        self.start_long_button.setEnabled(can_start)
        self.start_short_button.setEnabled(can_start)
        self.stop_button.setEnabled(can_trade)

    def _check_margin_permissions(self) -> None:
        if not self._rest:
            return
        try:
            self._rest.get_margin_account()
            self._margin_api_access = True
            self._margin_checked = True
            borrow_allowed = self._probe_borrow_access()
            self._borrow_allowed_by_api = borrow_allowed
            borrow_status = "ok" if borrow_allowed else "fail"
            self._append_log(f"[MARGIN_CHECK] ok borrow={borrow_status}")
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response else "?"
            code = None
            msg = None
            path = "?"
            try:
                payload = exc.response.json() if exc.response else {}
                code = payload.get("code")
                msg = payload.get("msg")
            except Exception:
                pass
            if exc.request and exc.request.url:
                path = exc.request.url.path
            self._margin_api_access = False
            self._margin_checked = True
            self._borrow_allowed_by_api = None
            self._append_log(
                f"[MARGIN_CHECK] fail http={status} code={code} msg={msg} path={path}"
            )
        except Exception as exc:
            self._margin_api_access = False
            self._margin_checked = True
            self._borrow_allowed_by_api = None
            self._append_log(f"[MARGIN_CHECK] error: {exc}")
        if self._trade_executor:
            self._trade_executor.set_margin_capabilities(
                self._margin_api_access, self._borrow_allowed_by_api
            )

    @Slot()
    def _start_trading(self, direction: str) -> None:
        if not self._trade_executor:
            return
        if not self._margin_api_access:
            self._append_log("[TRADE] blocked: margin_not_authorized")
            self._trade_executor.last_action = "margin_not_authorized"
            return
        placed = self._trade_executor.start_cycle_run(direction=direction)
        if placed:
            self._refresh_orders()

    @Slot()
    def _start_trading_long(self) -> None:
        self._start_trading(direction="LONG")

    @Slot()
    def _start_trading_short(self) -> None:
        self._start_trading(direction="SHORT")

    @Slot()
    def _close_position(self) -> None:
        if not self._trade_executor:
            return
        self._trade_executor.close_position()
        self._refresh_orders()

    @Slot()
    def _stop_trading(self) -> None:
        if not self._trade_executor:
            return
        self._trade_executor.stop_run_by_user()
        self._refresh_orders()

    @Slot()
    def _save_log(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Log", "trade_log.txt", "Text Files (*.txt)"
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(self.log.toPlainText())
            self._append_log(f"[LOG] saved path={path}")
        except Exception as exc:
            self._append_log(f"Failed to save log: {exc}")

    @Slot()
    def _clear_log(self) -> None:
        with self._log_lock:
            self._log_queue.clear()
            self._log_dropped_lines = 0
        self._last_log_message = None
        self._last_log_ts = 0.0
        self._verbose_log_last_ts.clear()
        self._ui_log_last_ts.clear()
        self.log.clear()

    def _append_log(self, message: str, level: str = "INFO", key: Optional[str] = None) -> None:
        if self._should_warn_invalid_api(message):
            self._auth_warning_pending = True
        log_level = resolve_level(level)
        is_event = self._is_event_log(message)
        if is_event:
            log_level = INFO_EVENT_LEVEL
        self._logger.log(log_level, message)
        if not self._should_emit_ui_log(message, log_level, is_event):
            return
        now = time.monotonic()
        if self._last_log_message == message and now - self._last_log_ts < 0.5:
            return
        log_key = key or self._derive_log_key(message)
        last_ui_ts = self._ui_log_last_ts.get(log_key)
        if last_ui_ts is not None and now - last_ui_ts < 0.5:
            return
        self._ui_log_last_ts[log_key] = now
        self._last_log_message = message
        self._last_log_ts = now
        with self._log_lock:
            overflow = len(self._log_queue) + 1 - self._log_max_buffer
            if overflow > 0:
                for _ in range(overflow):
                    self._log_queue.popleft()
                self._log_dropped_lines += overflow
            self._log_queue.append(message)

    def _should_emit_ui_log(self, message: str, level: int, is_event: bool) -> bool:
        if is_event:
            return True
        if level >= logging.WARNING:
            return True
        if level == logging.INFO:
            return self._is_major_info(message)
        return False

    @staticmethod
    def _is_major_info(message: str) -> bool:
        tags = (
            "[API]",
            "[CONNECT]",
            "[DISCONNECT]",
            "[MARGIN_CHECK]",
            "[TRADE]",
            "[WD]",
            "[LOG]",
        )
        return message.startswith(tags)

    def _allow_verbose_log(self, key: Optional[str], message: str) -> bool:
        if not self._settings or not self._settings.verbose_ui_log:
            return False
        log_key = key or self._derive_log_key(message)
        now = time.monotonic()
        last_ts = self._verbose_log_last_ts.get(log_key)
        if last_ts is not None and now - last_ts < 0.5:
            return False
        self._verbose_log_last_ts[log_key] = now
        return True

    @staticmethod
    def _derive_log_key(message: str) -> str:
        if message.startswith("["):
            end = message.find("]")
            if end != -1:
                return message[: end + 1]
        return message.split(" ", 1)[0]

    @staticmethod
    def _is_event_log(message: str) -> bool:
        tags = (
            "[STATE]",
            "[ENTRY]",
            "[FILL]",
            "[EXIT]",
            "[RECOVER]",
            "[ERROR]",
            "[START]",
            "[STOP]",
            "[CYCLE]",
            "[LOG]",
        )
        if message.startswith("[ENTRY]"):
            return any(token in message for token in ("placed", "repriced", "replaced"))
        if message.startswith("[EXIT]"):
            return "maker" in message or "cross" in message
        lowered = message.lower()
        if "error" in lowered or lowered.startswith("failed"):
            return True
        return message.startswith(tags)

    def _flush_log_queue(self) -> None:
        batch: list[str] = []
        with self._log_lock:
            if self._log_dropped_lines:
                dropped = self._log_dropped_lines
                self._log_dropped_lines = 0
                batch.append(f"[LOG] dropped {dropped} lines (buffer overflow)")
            while self._log_queue and len(batch) < self._log_flush_limit:
                batch.append(self._log_queue.popleft())
        if batch:
            self.log.setUpdatesEnabled(False)
            try:
                self.log.appendPlainText("\n".join(batch))
            finally:
                self.log.setUpdatesEnabled(True)
        if self._auth_warning_pending:
            self._auth_warning_pending = False
            self._show_auth_warning_once()

    def _should_warn_invalid_api(self, message: str) -> bool:
        if self._auth_warning_shown:
            return False
        lowered = message.lower()
        return "invalid api key format (-2014)" in lowered or "code=-2014" in lowered

    def _show_auth_warning_once(self) -> None:
        if self._auth_warning_shown:
            return
        self._auth_warning_shown = True
        QMessageBox.warning(
            self,
            "API ключ недействителен",
            "[AUTH] invalid api key format (-2014). "
            "Check API settings in меню -> Настройки API.",
        )

    def _load_settings(self) -> Settings:
        payload = self._config_store.load_settings()
        order_quote = self._bounded_float(payload.get("order_quote", 50.0), 0.0, 1e9, 50.0)
        max_budget = self._bounded_float(payload.get("max_budget", 1000.0), 0.0, 1e9, 1000.0)
        if max_budget < order_quote:
            max_budget = order_quote
        budget_reserve = self._bounded_float(
            payload.get("budget_reserve", 20.0), 0.0, max_budget, 20.0
        )
        return Settings(
            symbol=payload.get("symbol", "EURIUSDT"),
            ws_fresh_ms=int(payload.get("ws_fresh_ms", 700)),
            ws_stale_ms=int(payload.get("ws_stale_ms", 1500)),
            http_fresh_ms=int(payload.get("http_fresh_ms", 1500)),
            http_poll_ms=int(payload.get("http_poll_ms", payload.get("http_interval_ms", 350))),
            ui_refresh_ms=int(payload.get("ui_refresh_ms", 100)),
            ws_log_throttle_ms=int(payload.get("ws_log_throttle_ms", 5000)),
            ws_reconnect_dedup_ms=int(payload.get("ws_reconnect_dedup_ms", 10000)),
            order_poll_ms=int(payload.get("order_poll_ms", 1500)),
            ws_switch_hysteresis_ms=int(
                payload.get(
                    "ws_switch_hysteresis_ms", payload.get("source_switch_hysteresis_ms", 1000)
                )
            ),
            min_source_hold_ms=self._bounded_int(
                payload.get("min_source_hold_ms", 3000), 0, 60000, 3000
            ),
            ws_stable_required_ms=self._bounded_int(
                payload.get(
                    "ws_stable_required_ms",
                    payload.get("ws_switch_hysteresis_ms", 1500),
                ),
                0,
                20000,
                1500,
            ),
            ws_stale_grace_ms=self._bounded_int(
                payload.get("ws_stale_grace_ms", 3000), 0, 60000, 3000
            ),
            entry_max_age_ms=self._bounded_int(
                payload.get("entry_max_age_ms", 800), 200, 5000, 800
            ),
            exit_max_age_ms=self._bounded_int(
                payload.get("exit_max_age_ms", 2500), 500, 10000, 2500
            ),
            tp_max_age_ms=self._bounded_int(
                payload.get("tp_max_age_ms", 400), 100, 2000, 400
            ),
            settlement_grace_ms=self._bounded_int(
                payload.get("settlement_grace_ms", 400), 100, 2000, 400
            ),
            repricing_cooldown_ms=self._bounded_int(
                payload.get("repricing_cooldown_ms", 75), 10, 2000, 75
            ),
            max_wait_price_ms=self._bounded_int(
                payload.get("max_wait_price_ms", 5000), 1000, 30000, 5000
            ),
            price_wait_log_every_ms=self._bounded_int(
                payload.get("price_wait_log_every_ms", 1000), 250, 10000, 1000
            ),
            tp_cross_after_ms=self._bounded_int(
                payload.get("tp_cross_after_ms", 900), 200, 5000, 900
            ),
            inflight_deadline_ms=self._bounded_int(
                payload.get("inflight_deadline_ms", 2500), 500, 10000, 2500
            ),
            watchdog_poll_ms=self._bounded_int(
                payload.get("watchdog_poll_ms", 250), 200, 1000, 250
            ),
            max_state_stuck_ms=self._bounded_int(
                payload.get("max_state_stuck_ms", 8000), 2000, 120000, 8000
            ),
            max_no_progress_ms=self._bounded_int(
                payload.get("max_no_progress_ms", 5000), 1000, 60000, 5000
            ),
            max_reconcile_retries=self._bounded_int(
                payload.get("max_reconcile_retries", 3), 1, 10, 3
            ),
            sell_refresh_grace_ms=self._bounded_int(
                payload.get("sell_refresh_grace_ms", 400), 100, 3000, 400
            ),
            epsilon_qty=self._bounded_float(
                payload.get("epsilon_qty", 1e-6), 1e-9, 1e-2, 1e-6
            ),
            position_guard_http=bool(payload.get("position_guard_http", True)),
            entry_mode=str(payload.get("entry_mode", "NORMAL")).upper(),
            account_mode=str(payload.get("account_mode", "CROSS_MARGIN")),
            leverage_hint=int(
                payload.get("leverage_hint", payload.get("max_leverage_hint", 3))
            ),
            entry_reprice_min_ticks=self._bounded_int(
                payload.get(
                    "reprice_min_tick_moves",
                    payload.get("entry_reprice_min_ticks", 1),
                ),
                1,
                10,
                1,
            ),
            entry_reprice_cooldown_ms=self._bounded_int(
                payload.get(
                    "reprice_cooldown_ms",
                    payload.get("entry_reprice_cooldown_ms", 1200),
                ),
                100,
                5000,
                1200,
            ),
            entry_reprice_require_stable_source=bool(
                payload.get("require_stable_source_for_reprice", True)
            ),
            entry_reprice_stable_source_grace_ms=self._bounded_int(
                payload.get("stable_source_grace_ms", 3000), 0, 20000, 3000
            ),
            entry_reprice_min_consecutive_fresh_reads=self._bounded_int(
                payload.get("min_consecutive_fresh_reads", 2), 1, 10, 2
            ),
            take_profit_ticks=int(payload.get("take_profit_ticks", 1)),
            stop_loss_ticks=int(payload.get("stop_loss_ticks", 2)),
            order_type=str(payload.get("order_type", "LIMIT")).upper(),
            exit_order_type=str(payload.get("exit_order_type", "LIMIT")).upper(),
            max_buy_retries=self._bounded_int(
                payload.get("max_buy_retries", 3), 0, 10, 3
            ),
            allow_borrow=bool(payload.get("allow_borrow", True)),
            side_effect_type=str(payload.get("side_effect_type", "AUTO_BORROW_REPAY")).upper(),
            margin_isolated=bool(payload.get("margin_isolated", False)),
            auto_exit_enabled=bool(
                payload.get("auto_exit_enabled", payload.get("auto_close", True))
            ),
            max_sell_retries=int(payload.get("max_sell_retries", 3)),
            max_wait_sell_ms=self._bounded_int(
                payload.get("max_wait_sell_ms", 15000), 1000, 120000, 15000
            ),
            allow_force_close=bool(payload.get("allow_force_close", False)),
            cycle_count=self._bounded_int(payload.get("cycle_count", 1), 1, 1000, 1),
            order_quote=order_quote,
            max_budget=max_budget,
            budget_reserve=budget_reserve,
            verbose_ui_log=bool(payload.get("verbose_ui_log", False)),
        )

    def _log_data_blind_state(self, price_state: PriceState, state_label: str) -> None:
        now = time.monotonic()
        if price_state.data_blind and price_state.from_cache:
            if not self._data_blind_active or now - self._last_data_blind_log_ts >= 5.0:
                self._data_blind_active = True
                self._last_data_blind_log_ts = now
                age_label = (
                    str(price_state.cache_age_ms)
                    if price_state.cache_age_ms is not None
                    else "?"
                )
                self._append_log(
                    f"[DATA] mid_from_cache age_ms={age_label} state={state_label}"
                )
        elif price_state.data_blind:
            if not self._data_blind_active or now - self._last_data_blind_log_ts >= 5.0:
                self._data_blind_active = True
                self._last_data_blind_log_ts = now
                self._append_log(
                    f"[DATA_BLIND] no_cache state={state_label} action=freeze_exits"
                )
        else:
            self._data_blind_active = False

    def _load_api_state(self) -> None:
        env = self._load_env()
        creds = self._resolve_api_credentials(env)
        self._api_credentials = ApiCredentials(
            key=self._sanitize_api_value(creds.key),
            secret=self._sanitize_api_value(creds.secret),
        )
        if not self._has_valid_api_credentials(self._api_credentials):
            self._append_log("[API] missing, trading disabled")

    def _resolve_api_credentials(self, env: dict) -> ApiCredentials:
        env_key = self._sanitize_api_value(env.get("BINANCE_API_KEY", ""))
        env_secret = self._sanitize_api_value(env.get("BINANCE_API_SECRET", ""))
        if self._is_valid_api_value(env_key) and self._is_valid_api_value(env_secret):
            return ApiCredentials(key=env_key, secret=env_secret)
        local_creds = self._config_store.load_api_credentials()
        return ApiCredentials(
            key=self._sanitize_api_value(local_creds.key),
            secret=self._sanitize_api_value(local_creds.secret),
        )

    def _has_valid_api_credentials(self, creds: ApiCredentials) -> bool:
        return self._is_valid_api_value(creds.key) and self._is_valid_api_value(
            creds.secret
        )

    @staticmethod
    def _is_valid_api_value(value: str) -> bool:
        return len(value) > 20 and " " not in value

    @staticmethod
    def _sanitize_api_value(value: str) -> str:
        return value.strip().strip("'\"")

    @staticmethod
    def _bounded_int(value: object, min_value: int, max_value: int, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return max(min_value, min(max_value, parsed))

    @staticmethod
    def _bounded_float(
        value: object, min_value: float, max_value: float, default: float
    ) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        return max(min_value, min(max_value, parsed))

    def _open_api_settings(self) -> None:
        dialog = ApiSettingsDialog(self, store=self._config_store)
        dialog.saved.connect(self._on_api_saved)
        dialog.exec()

    def _open_trade_settings(self) -> None:
        dialog = TradeSettingsDialog(
            self,
            store=self._config_store,
            settings=self._settings,
        )
        dialog.saved.connect(self._on_trade_settings_saved)
        dialog.exec()

    def _on_trade_settings_saved(
        self,
        order_quote: float,
        max_budget: float,
        budget_reserve: float,
        cycle_count: int,
        take_profit_ticks: int,
        stop_loss_ticks: int,
        entry_max_age_ms: int,
        exit_max_age_ms: int,
        http_fresh_ms: int,
        allow_borrow: bool,
        auto_exit_enabled: bool,
        verbose_ui_log: bool,
    ) -> None:
        if self._settings:
            self._settings.order_quote = order_quote
            self._settings.max_budget = max_budget
            self._settings.budget_reserve = budget_reserve
            self._settings.cycle_count = cycle_count
            self._settings.take_profit_ticks = take_profit_ticks
            self._settings.stop_loss_ticks = stop_loss_ticks
            self._settings.entry_max_age_ms = self._bounded_int(
                entry_max_age_ms, 200, 5000, 800
            )
            self._settings.exit_max_age_ms = self._bounded_int(
                exit_max_age_ms, 500, 10000, 2500
            )
            self._settings.http_fresh_ms = self._bounded_int(
                http_fresh_ms, 200, 10000, 1500
            )
            self._settings.allow_borrow = allow_borrow
            self._settings.auto_exit_enabled = auto_exit_enabled
            self._settings.verbose_ui_log = verbose_ui_log
        self._append_log("[SETTINGS] saved")

    def _on_api_saved(self, key: str, secret: str) -> None:
        self._api_credentials = ApiCredentials(key=key, secret=secret)
        if self._rest:
            self._rest.api_key = key
            self._rest.api_secret = secret
        if self._order_tracker:
            self.order_tracker_update_credentials.emit(key, secret)
        self._append_log("[API] saved ok")
        self._configure_trading_state()
        if self._connected and self._rest:
            self._check_margin_permissions()

    def _configure_trading_state(self) -> None:
        if not self._connected or not self._settings or not self._router or not self._rest:
            return
        if not self._has_valid_api_credentials(self._api_credentials):
            self._trade_executor = None
            self.start_long_button.setEnabled(False)
            self.start_short_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            self._append_log("[API] missing, trading disabled")
            return
        if not self._trade_executor:
            self._trade_executor = TradeExecutor(
                rest=self._rest,
                router=self._router,
                settings=self._settings,
                profile=self._symbol_profile,
                logger=self._append_log,
            )
        self._update_trading_controls()
        if self._trade_executor:
            self._trade_executor.set_margin_capabilities(
                self._margin_api_access, self._borrow_allowed_by_api
            )

    def _probe_borrow_access(self) -> bool:
        if not self._settings or not self._rest:
            return False
        base_asset, _ = self._split_symbol(self._settings.symbol)
        try:
            self._rest.probe_margin_borrow_access(base_asset)
            return True
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response else "?"
            code = None
            try:
                payload = exc.response.json() if exc.response else {}
                code = payload.get("code")
            except Exception:
                pass
            if status == 401 or code == -1002:
                return False
            return True
        except Exception:
            return True

    @staticmethod
    def _split_symbol(symbol: str) -> tuple[str, str]:
        if symbol.endswith("USDT"):
            return symbol[:-4], "USDT"
        if symbol.endswith("BUSD"):
            return symbol[:-4], "BUSD"
        if symbol.endswith("USDC"):
            return symbol[:-4], "USDC"
        return symbol[:-3], symbol[-3:]

    def _load_env(self) -> dict:
        env = dict(os.environ)
        env_path = Path(__file__).resolve().parents[2] / "config" / ".env"
        if not env_path.exists():
            return env
        if find_spec("dotenv") is None:
            self._append_log("python-dotenv is not installed; skipping .env load.")
            return env
        dotenv = import_module("dotenv")
        file_env = {
            key: value or ""
            for key, value in dotenv.dotenv_values(env_path).items()
        }
        for key, value in file_env.items():
            env.setdefault(key, value)
        return env

    def _update_exit_status(self, mid: Optional[float]) -> None:
        buy_price = None
        tp_price = None
        sl_price = None
        tp_ready = False
        sl_ready = False
        tick_size = self._symbol_profile.tick_size if self._symbol_profile else None
        if self._trade_executor and self._settings:
            buy_price = self._trade_executor.get_buy_price()
            if buy_price is not None and tick_size:
                tp_price = buy_price + self._settings.take_profit_ticks * tick_size
                sl_price = buy_price - self._settings.stop_loss_ticks * tick_size
                if mid is not None and mid >= tp_price:
                    tp_ready = True
                if mid is not None and mid <= sl_price:
                    sl_ready = True

        self.tp_price_label.setText(self._fmt_price(tp_price, width=12))
        self.sl_price_label.setText(self._fmt_price(sl_price, width=12))
        if tp_ready:
            self.tp_price_label.setStyleSheet("color: #3ad07d; font-weight: 600;")
        else:
            self.tp_price_label.setStyleSheet("")
        if sl_ready:
            self.sl_price_label.setStyleSheet("color: #ff5f57; font-weight: 600;")
        else:
            self.sl_price_label.setStyleSheet("")

    def _parse_symbol_profile(self, exchange_info: dict) -> SymbolProfile:
        symbols = exchange_info.get("symbols", [])
        if not symbols:
            return SymbolProfile()
        entry = symbols[0]
        filters = {item.get("filterType"): item for item in entry.get("filters", [])}
        lot = filters.get("LOT_SIZE", {})
        price = filters.get("PRICE_FILTER", {})
        notional = filters.get("MIN_NOTIONAL", {})
        min_notional = self._safe_float(notional.get("minNotional"))
        if min_notional is None:
            notional = filters.get("NOTIONAL", {})
            min_notional = self._safe_float(notional.get("minNotional"))
        return SymbolProfile(
            tick_size=self._safe_float(price.get("tickSize")),
            step_size=self._safe_float(lot.get("stepSize")),
            min_qty=self._safe_float(lot.get("minQty")),
            min_notional=min_notional,
        )

    def _render_symbol_profile(self, profile: SymbolProfile) -> None:
        self._profile_info_text = (
            "Symbol Profile\n"
            f"tickSize: {self._fmt_price(profile.tick_size)}\n"
            f"stepSize: {self._fmt_price(profile.step_size)}\n"
            f"minQty: {self._fmt_price(profile.min_qty)}\n"
            f"minNotional: {self._fmt_price(profile.min_notional)}"
        )

    def _orders_model_clear(self) -> None:
        self.orders_model.removeRows(0, self.orders_model.rowCount())
        self._open_order_rows.clear()

    def _fills_model_clear(self) -> None:
        self.fills_model.removeRows(0, self.fills_model.rowCount())
        self._fill_rows_by_event_key.clear()
        self._fills_history.clear()
        self._cycle_id_counter = 0
        self._last_buy_fill_price = None

    def _refresh_orders(self) -> None:
        if not self._connected or not self._rest or not self._settings:
            return
        open_orders = []
        orders_error = False
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except Exception as exc:
            if not self._orders_error_logged:
                self._append_log(f"[ORDERS] fetch failed: {exc}")
                self._orders_error_logged = True
            orders_error = True

        if not orders_error:
            self._orders_error_logged = False
        price_state = None
        if self._router:
            price_state, _ = self._router.build_price_state()
        if self._trade_executor:
            self._trade_executor.sync_open_orders(open_orders)
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        orders = (
            self._trade_executor.get_orders_snapshot()
            if self._trade_executor
            else []
        )
        if self._order_tracker:
            self.order_tracker_sync.emit(orders)
        self._update_open_orders_table(orders, now_ms, price_state)

    def _update_open_orders_table(
        self,
        orders: list[dict],
        now_ms: int,
        price_state: Optional[PriceState],
    ) -> None:
        seen_keys: set[str] = set()
        bid = price_state.bid if price_state else None
        ask = price_state.ask if price_state else None
        for order in orders:
            key = self._order_key(order)
            seen_keys.add(key)
            row = self._open_order_rows.get(key)
            order_time = order.get("created_ts")
            age_ms = None
            if isinstance(order_time, int):
                age_ms = max(0, now_ms - order_time)
            qty = order.get("qty")
            price = order.get("price")
            side = str(order.get("side", "—")).upper()
            dist_ticks = self._dist_ticks_to_fill(side, price, bid, ask)
            display_time = self._format_time(order_time)
            row_values = [
                display_time,
                side or "—",
                self._fmt_price(price, width=12),
                self._fmt_qty(qty, width=8),
                str(order.get("status", "—")),
                self._fmt_int(age_ms, width=8),
                self._fmt_int(dist_ticks, width=6),
                str(order.get("clientOrderId", "—")),
            ]
            if row is None:
                items = [QStandardItem(value) for value in row_values]
                for idx, item in enumerate(items):
                    if idx == 7:
                        item.setTextAlignment(
                            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                        )
                        item.setText(self._elide_text(item.text(), self.orders_table, idx))
                        item.setToolTip(row_values[idx])
                    else:
                        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                items[0].setData(key, Qt.ItemDataRole.UserRole)
                self.orders_model.appendRow(items)
                self._open_order_rows[key] = self.orders_model.rowCount() - 1
            else:
                for col, value in enumerate(row_values):
                    item = self.orders_model.item(row, col)
                    if not item:
                        continue
                    if col == 7:
                        elided = self._elide_text(value, self.orders_table, col)
                        if item.text() != elided:
                            item.setText(elided)
                            item.setToolTip(value)
                    elif item.text() != value:
                        item.setText(value)
                first_item = self.orders_model.item(row, 0)
                if first_item and first_item.data(Qt.ItemDataRole.UserRole) != key:
                    first_item.setData(key, Qt.ItemDataRole.UserRole)
        rows_to_remove = [
            row for key, row in self._open_order_rows.items() if key not in seen_keys
        ]
        for row in sorted(rows_to_remove, reverse=True):
            self.orders_model.removeRow(row)
        if rows_to_remove:
            self._rebuild_open_order_index()

    def _rebuild_open_order_index(self) -> None:
        self._open_order_rows.clear()
        for row in range(self.orders_model.rowCount()):
            item = self.orders_model.item(row, 0)
            if item is None:
                continue
            key = item.data(Qt.ItemDataRole.UserRole)
            if isinstance(key, str):
                self._open_order_rows[key] = row

    @staticmethod
    def _order_key(order: dict) -> str:
        order_id = order.get("orderId")
        client_id = order.get("clientOrderId")
        created_ts = order.get("created_ts")
        return str(order_id or client_id or created_ts or id(order))

    def _dist_ticks_to_fill(
        self,
        side: str,
        price: Optional[float],
        bid: Optional[float],
        ask: Optional[float],
    ) -> Optional[int]:
        tick_size = self._symbol_profile.tick_size
        if not tick_size or price is None:
            return None
        if side == "BUY":
            if bid is None:
                return None
            return max(0, round((price - bid) / tick_size))
        if side == "SELL":
            if ask is None:
                return None
            return max(0, round((ask - price) / tick_size))
        return None

    @staticmethod
    def _calculate_spread_ticks(
        bid: Optional[float],
        ask: Optional[float],
        tick_size: Optional[float],
    ) -> Optional[int]:
        if bid is None or ask is None or not tick_size:
            return None
        return max(0, int((ask - bid) / tick_size))

    @staticmethod
    def _find_active_order(orders: list[dict], side: str) -> Optional[dict]:
        for order in orders:
            if order.get("side") == side and order.get("status") != "FILLED":
                return order
        return next((order for order in orders if order.get("side") == side), None)

    def _entry_age_ms(self, entry_order: Optional[dict]) -> Optional[int]:
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        if entry_order:
            order_time = entry_order.get("created_ts")
            if isinstance(order_time, int):
                return max(0, now_ms - order_time)
        if self._trade_executor:
            elapsed = self._trade_executor._entry_attempt_elapsed_ms()
            if elapsed is not None:
                return elapsed
        return None

    def _exit_age_ms(self) -> Optional[int]:
        if not self._trade_executor or self._trade_executor.exit_intent_set_ts is None:
            return None
        return int(
            (time.monotonic() - self._trade_executor.exit_intent_set_ts) * 1000.0
        )

    def _update_summary_bar(
        self,
        price_state: PriceState,
        health_state: object,
        spread_ticks: Optional[int],
        state_label: str,
        last_action: str,
    ) -> None:
        symbol = self._settings.symbol if self._settings else "EURIUSDT"
        ws_age = self._fmt_int(health_state.ws_age_ms, width=5)
        http_age = self._fmt_int(health_state.http_age_ms, width=5)
        spread_label = self._fmt_int(spread_ticks, width=4)
        position_qty = None
        buy_price = None
        if self._trade_executor:
            if self._trade_executor.position is not None:
                position_qty = self._trade_executor.position.get("qty")
            buy_price = self._trade_executor.get_buy_price()
        pos_label = "—"
        if position_qty is not None:
            pos_label = (
                f"{self._fmt_qty(position_qty, width=7)}@"
                f"{self._fmt_price(buy_price, width=10)}"
            )
        pnl_unreal = (
            self._trade_executor.get_unrealized_pnl(price_state.mid)
            if self._trade_executor
            else None
        )
        pnl_cycle = self._trade_executor.pnl_cycle if self._trade_executor else None
        pnl_session = self._trade_executor.pnl_session if self._trade_executor else None
        last_exit = self._trade_executor.last_exit_reason if self._trade_executor else None
        self.summary_label.setText(
            f"{symbol} | SRC {price_state.source} | ages {ws_age}/{http_age} | "
            f"spread {spread_label} | FSM {state_label}/{last_action} | "
            f"pos {pos_label} | pnl "
            f"{self._fmt_pnl(pnl_unreal, width=9)}/"
            f"{self._fmt_pnl(pnl_cycle, width=9)}/"
            f"{self._fmt_pnl(pnl_session, width=9)} | "
            f"last_exit {last_exit or '—'}"
        )

    def _update_scoreboard(self) -> None:
        realized = [
            entry["pnl_quote"]
            for entry in self._fills_history
            if entry.get("pnl_quote") is not None
        ]
        wins = sum(1 for pnl in realized if pnl > 0)
        losses = sum(1 for pnl in realized if pnl < 0)
        total = wins + losses
        winrate = (wins / total) * 100.0 if total else 0.0
        avg_pnl = sum(realized) / len(realized) if realized else None
        pnl_session = self._trade_executor.pnl_session if self._trade_executor else None
        self.wins_value_label.setText(str(wins))
        self.losses_value_label.setText(str(losses))
        self.winrate_value_label.setText(f"{winrate:>5.1f}%")
        self.pnl_session_value_label.setText(self._fmt_pnl(pnl_session, width=10))
        self.avg_pnl_value_label.setText(self._fmt_pnl(avg_pnl, width=10))

    def _set_hud_defaults(self) -> None:
        self.market_primary_label.setText("Bid —  Ask —  Mid —  Spr —t")
        self.market_secondary_label.setText(
            "SRC — | ws False age — | http age — | blind — | reason —"
        )
        self.ws_connected_label.setText("—")
        self.data_blind_label.setText("—")
        self.switch_reason_label.setText("—")
        self.entry_price_label.setText("—")
        self.entry_age_label.setText("—")
        self.entry_reprice_reason_label.setText("—")
        self.tp_price_label.setText("—")
        self.sl_price_label.setText("—")
        self.exit_intent_label.setText("—")
        self.exit_policy_label.setText("—")
        self.exit_age_label.setText("—")
        self.cycles_target_label.setText("—")
        self.summary_label.setText(
            "EURIUSDT | SRC NONE | ages —/— | spread — | FSM —/— | pos — | "
            "pnl —/—/— | last_exit —"
        )
        self.wins_value_label.setText("0")
        self.losses_value_label.setText("0")
        self.winrate_value_label.setText("0.0%")
        self.pnl_session_value_label.setText("—")
        self.avg_pnl_value_label.setText("—")

    def _register_fill(
        self, order_id: int, side: str, price: float, qty: float, ts_ms: int
    ) -> None:
        side_upper = str(side).upper()
        status = "FILLED"
        event_key = self._fill_event_key(order_id, side_upper, status, qty, price, ts_ms)
        if event_key in self._fill_rows_by_event_key:
            return
        pnl_quote = None
        pnl_bps = None
        exit_reason = "—"
        cycle_id = "—"
        unreal_est = None
        buy_price = None
        if side_upper == "BUY":
            self._last_buy_fill_price = price
            if self._last_mid is not None and qty:
                unreal_est = (self._last_mid - price) * qty
        elif side_upper == "SELL":
            self._cycle_id_counter += 1
            cycle_id = str(self._cycle_id_counter)
            if self._trade_executor:
                pnl_quote = self._trade_executor.pnl_cycle
                exit_reason = self._trade_executor.last_exit_reason or "—"
                buy_price = self._trade_executor.get_buy_price()
            if pnl_quote is None:
                if buy_price is None:
                    buy_price = self._last_buy_fill_price
                if buy_price is not None and qty:
                    pnl_quote = (price - buy_price) * qty
            if pnl_quote is not None and buy_price and qty:
                notion = buy_price * qty
                if notion > 0:
                    pnl_bps = (pnl_quote / notion) * 10000
        entry = {
            "time": self._format_time(ts_ms),
            "side": side_upper,
            "status": status,
            "avg_fill_price": price,
            "qty": qty,
            "pnl_quote": pnl_quote,
            "pnl_bps": pnl_bps,
            "unreal_est": unreal_est,
            "exit_reason": exit_reason,
            "cycle_id": cycle_id,
            "event_key": event_key,
        }
        self._fills_history.insert(0, entry)
        self._append_fill_row(entry)
        if len(self._fills_history) > 200:
            removed = self._fills_history.pop()
            self._remove_fill_row_by_key(removed.get("event_key"))

    def _append_fill_row(self, entry: dict) -> None:
        row = self._fill_row_values(entry)
        items = [QStandardItem(value) for value in row]
        for idx, item in enumerate(items):
            if idx in {8, 9}:
                item.setTextAlignment(
                    Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                )
                item.setText(self._elide_text(item.text(), self.fills_table, idx))
                item.setToolTip(row[idx])
            else:
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        self._shift_fill_row_indices(1)
        self.fills_model.insertRow(0, items)
        event_key = entry.get("event_key")
        if isinstance(event_key, str):
            self._fill_rows_by_event_key[event_key] = 0

    def _fill_row_values(self, entry: dict) -> list[str]:
        return [
            entry.get("time", "—"),
            entry.get("side", "—"),
            self._fmt_price(entry.get("avg_fill_price"), width=12),
            self._fmt_qty(entry.get("qty"), width=8),
            str(entry.get("status", "—")),
            self._fmt_pnl(entry.get("pnl_quote"), width=12),
            self._fmt_pnl(entry.get("pnl_bps"), width=8),
            self._fmt_pnl(entry.get("unreal_est"), width=12),
            str(entry.get("exit_reason", "—")),
            str(entry.get("cycle_id", "—")),
        ]

    def _shift_fill_row_indices(self, delta: int) -> None:
        if not delta:
            return
        for key in list(self._fill_rows_by_event_key.keys()):
            self._fill_rows_by_event_key[key] += delta

    def _remove_fill_row_by_key(self, event_key: Optional[str]) -> None:
        if not event_key:
            return
        row = self._fill_rows_by_event_key.pop(event_key, None)
        if row is None:
            return
        self.fills_model.removeRow(row)
        for key, idx in list(self._fill_rows_by_event_key.items()):
            if idx > row:
                self._fill_rows_by_event_key[key] = idx - 1

    @staticmethod
    def _fill_event_key(
        order_id: Optional[int],
        side: str,
        status: str,
        cum_qty: float,
        avg_price: float,
        ts_ms: int,
    ) -> str:
        if order_id is not None:
            return f"{order_id}:{side}:{status}:{cum_qty}:{avg_price}"
        return f"{ts_ms}:{side}:{status}:{cum_qty}:{avg_price}"

    @staticmethod
    def _format_time(value: Optional[int]) -> str:
        if not value:
            return "—"
        try:
            return datetime.fromtimestamp(value / 1000).strftime("%H:%M:%S")
        except (OSError, ValueError):
            return "—"

    @staticmethod
    def _short_order_id(value: Optional[int]) -> str:
        if value is None:
            return "—"
        return str(value)[-6:]

    @staticmethod
    def _fmt_qty(value: Optional[float], width: Optional[int] = None) -> str:
        if value is None:
            return "—"
        if width is not None:
            return f"{value:>{width}.4f}"
        return f"{value:.4f}"

    @staticmethod
    def _fmt_pnl(value: Optional[float], width: Optional[int] = None) -> str:
        if value is None:
            return "—"
        if width is not None:
            return f"{value:>{width}.4f}"
        return f"{value:.4f}"

    @staticmethod
    def _fmt_price(value: Optional[float], width: Optional[int] = None) -> str:
        if value is None:
            return "—"
        if width is not None:
            return f"{value:>{width}.5f}"
        return f"{value:.5f}"

    @staticmethod
    def _fmt_int(value: Optional[int], width: Optional[int] = None) -> str:
        if value is None:
            return "—"
        if width is not None:
            return f"{value:>{width}d}"
        return str(value)

    @staticmethod
    def _safe_float(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None
