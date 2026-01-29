from __future__ import annotations

from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from typing import Optional

from datetime import datetime
import time

import httpx
from PySide6.QtCore import Qt, QTimer, Slot
from PySide6.QtGui import QAction, QFontDatabase, QFontMetrics, QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QFrame,
    QFormLayout,
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
)

from src.core.config_store import ApiCredentials, ConfigStore
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


class MainWindow(QMainWindow):
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
        self._last_ws_status_log_ts = 0.0
        self._data_blind_active = False
        self._ttl_expired_logged = False
        self._last_data_blind_log_ts = 0.0
        self._last_ttl_expired_log_ts = 0.0
        self._open_order_rows: dict[str, int] = {}
        self._fills_history: list[dict] = []
        self._fill_seen_keys: set[tuple[object, str, int]] = set()
        self._profile_info_text = "—"

        self._ui_timer = QTimer(self)
        self._ui_timer.timeout.connect(self._refresh_ui)
        self._http_timer = QTimer(self)
        self._http_timer.timeout.connect(self._poll_http)
        self._orders_timer = QTimer(self)
        self._orders_timer.timeout.connect(self._refresh_orders)

        self._ws_thread = None
        self._ws_worker = None

        self._build_ui()
        self._load_api_state()
        self._update_status(False)

    def _build_ui(self) -> None:
        central = QWidget()
        root = QVBoxLayout(central)

        self._build_menu()

        fixed_font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        fixed_metrics = QFontMetrics(fixed_font)

        header = QFrame()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(8, 6, 8, 6)
        header_layout.setSpacing(8)
        self.summary_label = QLabel(
            "EURIUSDT | SRC NONE | ages —/— | spread — | FSM —/— | pos — | "
            "pnl —/—/— | last_exit —"
        )
        self.summary_label.setFont(fixed_font)
        self.summary_label.setStyleSheet("font-weight: 600;")
        self.summary_label.setWordWrap(False)
        self.summary_label.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
        self.summary_label.setMinimumWidth(fixed_metrics.horizontalAdvance("0" * 140))
        self.connect_button = QPushButton("CONNECT")
        self.connect_button.clicked.connect(self._toggle_connection)
        self.status_label = QLabel("DISCONNECTED")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setFixedWidth(140)
        self.status_label.setStyleSheet("color: #ff5f57; font-weight: 600;")
        self.start_button = QPushButton("START")
        self.start_button.clicked.connect(self._start_trading)
        self.stop_button = QPushButton("STOP")
        self.stop_button.clicked.connect(self._stop_trading)
        self.settings_button = QPushButton("SETTINGS")
        self.settings_button.clicked.connect(self._open_trade_settings)
        for button in (
            self.connect_button,
            self.start_button,
            self.stop_button,
            self.settings_button,
        ):
            button.setFixedHeight(28)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(False)

        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(6)
        buttons_layout.addWidget(self.connect_button)
        buttons_layout.addWidget(self.start_button)
        buttons_layout.addWidget(self.stop_button)
        buttons_layout.addWidget(self.settings_button)

        header_layout.addWidget(self.summary_label)
        header_layout.addStretch(1)
        header_layout.addLayout(buttons_layout)
        header_layout.addStretch(1)
        header_layout.addWidget(self.status_label)
        root.addWidget(header)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setChildrenCollapsible(False)

        hud_widget = QWidget()
        hud_layout = QVBoxLayout(hud_widget)
        hud_layout.setSpacing(10)

        market_group = QGroupBox("Market & Decision")
        market_layout = QGridLayout(market_group)
        market_layout.setHorizontalSpacing(10)
        market_layout.setVerticalSpacing(6)
        self.bid_value_label = self._make_big_value_label("—", min_chars=12)
        self.ask_value_label = self._make_big_value_label("—", min_chars=12)
        self.mid_value_label = self._make_big_value_label("—", min_chars=12)
        self.spread_value_label = self._make_big_value_label("—", min_chars=6)
        market_layout.addWidget(QLabel("Bid"), 0, 0)
        market_layout.addWidget(self.bid_value_label, 0, 1)
        market_layout.addWidget(QLabel("Ask"), 1, 0)
        market_layout.addWidget(self.ask_value_label, 1, 1)
        market_layout.addWidget(QLabel("Mid"), 2, 0)
        market_layout.addWidget(self.mid_value_label, 2, 1)
        market_layout.addWidget(QLabel("Spread (ticks)"), 3, 0)
        market_layout.addWidget(self.spread_value_label, 3, 1)

        health_group = QGroupBox("Data Health")
        health_layout = QFormLayout(health_group)
        health_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        self.ws_connected_label = self._make_value_label("—", min_chars=8)
        self.data_blind_label = self._make_value_label("—", min_chars=8)
        self.switch_reason_label = self._make_value_label("—", min_chars=10)
        health_layout.addRow("ws_connected", self.ws_connected_label)
        health_layout.addRow("data_blind", self.data_blind_label)
        health_layout.addRow("effective_source_reason", self.switch_reason_label)

        entry_group = QGroupBox("Entry")
        entry_layout = QFormLayout(entry_group)
        self.entry_price_label = self._make_value_label("—", min_chars=12)
        self.entry_offset_label = self._make_value_label("—", min_chars=6)
        self.entry_age_label = self._make_value_label("—", min_chars=8)
        self.entry_reprice_reason_label = self._make_value_label("—", min_chars=12)
        entry_layout.addRow("entry_price", self.entry_price_label)
        entry_layout.addRow("entry_offset_ticks", self.entry_offset_label)
        entry_layout.addRow("entry_age_ms", self.entry_age_label)
        entry_layout.addRow("last_entry_reprice_reason", self.entry_reprice_reason_label)

        exit_group = QGroupBox("Exit")
        exit_layout = QFormLayout(exit_group)
        self.tp_price_label = self._make_value_label("—", min_chars=12)
        self.sl_price_label = self._make_value_label("—", min_chars=12)
        self.exit_intent_label = self._make_value_label("—", min_chars=10)
        self.exit_policy_label = self._make_value_label("—", min_chars=10)
        self.exit_age_label = self._make_value_label("—", min_chars=8)
        exit_layout.addRow("tp_trigger_price", self.tp_price_label)
        exit_layout.addRow("sl_trigger_price", self.sl_price_label)
        exit_layout.addRow("exit_intent", self.exit_intent_label)
        exit_layout.addRow("exit_policy", self.exit_policy_label)
        exit_layout.addRow("exit_age_ms", self.exit_age_label)

        timers_group = QGroupBox("Timers")
        timers_layout = QFormLayout(timers_group)
        self.max_entry_total_label = self._make_value_label("—", min_chars=8)
        self.max_exit_total_label = self._make_value_label("—", min_chars=8)
        self.cycles_target_label = self._make_value_label("—", min_chars=6)
        timers_layout.addRow("max_entry_total_ms", self.max_entry_total_label)
        timers_layout.addRow("max_exit_total_ms", self.max_exit_total_label)
        timers_layout.addRow("cycles_target", self.cycles_target_label)

        hud_layout.addWidget(market_group)
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
        column_widths = [
            fixed_metrics.horizontalAdvance("0" * 8),
            fixed_metrics.horizontalAdvance("0" * 5),
            fixed_metrics.horizontalAdvance("0" * 12),
            fixed_metrics.horizontalAdvance("0" * 8),
            fixed_metrics.horizontalAdvance("0" * 8),
            fixed_metrics.horizontalAdvance("0" * 8),
            fixed_metrics.horizontalAdvance("0" * 10),
            fixed_metrics.horizontalAdvance("0" * 12),
        ]
        for idx, width in enumerate(column_widths):
            self.orders_table.setColumnWidth(idx, width + 12)
        open_layout.addWidget(self.orders_table, stretch=1)
        orders_tabs.addTab(open_tab, "OPEN")

        fills_tab = QWidget()
        fills_layout = QVBoxLayout(fills_tab)
        self.fills_model = QStandardItemModel(0, 8, self)
        self.fills_model.setHorizontalHeaderLabels(
            [
                "time",
                "side",
                "avg_fill_price",
                "qty",
                "pnl_quote",
                "pnl_bps",
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
        fills_widths = [
            fixed_metrics.horizontalAdvance("0" * 8),
            fixed_metrics.horizontalAdvance("0" * 5),
            fixed_metrics.horizontalAdvance("0" * 12),
            fixed_metrics.horizontalAdvance("0" * 8),
            fixed_metrics.horizontalAdvance("0" * 12),
            fixed_metrics.horizontalAdvance("0" * 8),
            fixed_metrics.horizontalAdvance("0" * 10),
            fixed_metrics.horizontalAdvance("0" * 8),
        ]
        for idx, width in enumerate(fills_widths):
            self.fills_table.setColumnWidth(idx, width + 12)
        fills_layout.addWidget(self.fills_table, stretch=1)
        orders_tabs.addTab(fills_tab, "FILLS")

        splitter.addWidget(hud_widget)
        splitter.addWidget(orders_tabs)
        splitter.setSizes([450, 550])
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
        self.log.setMaximumBlockCount(500)
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
    ) -> QLabel:
        label = QLabel(text)
        font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        label.setFont(font)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
        label.setAlignment(align | Qt.AlignmentFlag.AlignVCenter)
        metrics = QFontMetrics(font)
        label.setMinimumWidth(metrics.horizontalAdvance("0" * min_chars))
        return label

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
            self._order_tracker = OrderTracker(
                rest=self._rest,
                symbol=self._settings.symbol,
                poll_ms=self._settings.order_poll_ms,
                logger=self._append_log,
                parent=self,
            )
            self._order_tracker.order_filled.connect(self._on_order_filled)
            self._order_tracker.order_partial.connect(self._on_order_partial)
            self._order_tracker.order_done.connect(self._on_order_done)
            self._order_tracker.start()
            self._update_trading_controls()
        else:
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            if not self._has_valid_api_credentials(api_credentials):
                self._append_log("[API] missing, trading disabled")

        self._start_ws()
        self._http_timer.start(self._settings.http_poll_ms)
        self._ui_timer.start(self._settings.ui_refresh_ms)
        self._orders_timer.start(1000)

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
        if self._order_tracker:
            self._order_tracker.stop()
            self._order_tracker = None

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
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self._orders_model_clear()
        self._fills_model_clear()
        self._fills_history.clear()
        self._fill_seen_keys.clear()
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
            in_position = state_label in {"POSITION_OPEN", "WAIT_SELL"}
            if not in_position and not self._router.is_ws_stale():
                return
        try:
            payload = self._http_service.fetch_book_ticker(self._settings.symbol)
            bid = float(payload.get("bidPrice", 0.0))
            ask = float(payload.get("askPrice", 0.0))
            if self._router:
                self._router.update_http(bid, ask)
        except Exception as exc:
            self._append_log(f"HTTP ticker error: {exc}")

    def _refresh_ui(self) -> None:
        if not self._router:
            return
        price_state, health_state = self._router.build_price_state()
        self._last_mid = price_state.mid
        for log_message in self._router.consume_logs():
            self._append_log(log_message)

        tick_size = self._symbol_profile.tick_size
        spread_ticks = self._calculate_spread_ticks(
            price_state.bid, price_state.ask, tick_size
        )
        self.bid_value_label.setText(self._fmt_price(price_state.bid, width=12))
        self.ask_value_label.setText(self._fmt_price(price_state.ask, width=12))
        self.mid_value_label.setText(self._fmt_price(price_state.mid, width=12))
        self.spread_value_label.setText(self._fmt_int(spread_ticks, width=6))

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
        entry_order = self._find_active_order(orders_snapshot, "BUY")
        entry_price = entry_order.get("price") if entry_order else None
        if entry_price is None and self._trade_executor:
            entry_price = self._trade_executor.get_buy_price()
        entry_age_ms = self._entry_age_ms(entry_order)
        entry_offset = (
            self._settings.entry_offset_ticks if self._settings else None
        )
        self.entry_price_label.setText(self._fmt_price(entry_price, width=12))
        self.entry_offset_label.setText(self._fmt_int(entry_offset, width=6))
        self.entry_age_label.setText(self._fmt_int(entry_age_ms, width=8))
        self.entry_reprice_reason_label.setText("—")

        self._update_exit_status(price_state.mid)

        exit_intent = self._trade_executor.exit_intent if self._trade_executor else None
        exit_policy = "—"
        if exit_intent:
            exit_policy, _ = TradeExecutor._resolve_exit_policy(exit_intent)
        self.exit_intent_label.setText(exit_intent or "—")
        self.exit_policy_label.setText(exit_policy or "—")
        self.exit_age_label.setText(self._fmt_int(self._exit_age_ms(), width=8))

        self.max_entry_total_label.setText(
            self._fmt_int(
                getattr(self._settings, "max_entry_total_ms", None), width=8
            )
        )
        self.max_exit_total_label.setText(
            self._fmt_int(getattr(self._settings, "max_exit_total_ms", None), width=8)
        )
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
        self.start_button.setEnabled(
            can_trade and self._margin_api_access and state_label == "IDLE"
        )
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
    def _start_trading(self) -> None:
        if not self._trade_executor:
            return
        if not self._margin_api_access:
            self._append_log("[TRADE] blocked: margin_not_authorized")
            self._trade_executor.last_action = "margin_not_authorized"
            return
        placed = self._trade_executor.start_cycle_run()
        if placed:
            self._refresh_orders()

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
        self.log.clear()

    def _append_log(self, message: str) -> None:
        self.log.appendPlainText(message)
        if self._should_warn_invalid_api(message):
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
            good_quote_ttl_ms=int(payload.get("good_quote_ttl_ms", 3000)),
            mid_fresh_ms=self._bounded_int(payload.get("mid_fresh_ms", 800), 200, 5000, 800),
            max_wait_price_ms=self._bounded_int(
                payload.get("max_wait_price_ms", 5000), 1000, 30000, 5000
            ),
            price_wait_log_every_ms=self._bounded_int(
                payload.get("price_wait_log_every_ms", 1000), 250, 10000, 1000
            ),
            position_guard_http=bool(payload.get("position_guard_http", True)),
            entry_mode=str(payload.get("entry_mode", "NORMAL")).upper(),
            aggressive_offset_ticks=self._bounded_int(
                payload.get("aggressive_offset_ticks", 0), 0, 2, 0
            ),
            max_entry_total_ms=self._bounded_int(
                payload.get("max_entry_total_ms", 30000), 1000, 120000, 30000
            ),
            account_mode=str(payload.get("account_mode", "CROSS_MARGIN")),
            leverage_hint=int(
                payload.get("leverage_hint", payload.get("max_leverage_hint", 3))
            ),
            offset_ticks=int(payload.get("offset_ticks", payload.get("test_tick_offset", 1))),
            entry_offset_ticks=int(payload.get("entry_offset_ticks", payload.get("offset_ticks", 1))),
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
            exit_offset_ticks=int(payload.get("exit_offset_ticks", 1)),
            sl_offset_ticks=int(payload.get("sl_offset_ticks", 0)),
            buy_ttl_ms=self._bounded_int(payload.get("buy_ttl_ms", 2500), 500, 20000, 2500),
            max_buy_retries=self._bounded_int(
                payload.get("max_buy_retries", 3), 0, 10, 3
            ),
            allow_borrow=bool(payload.get("allow_borrow", True)),
            side_effect_type=str(payload.get("side_effect_type", "AUTO_BORROW_REPAY")).upper(),
            margin_isolated=bool(payload.get("margin_isolated", False)),
            auto_exit_enabled=bool(
                payload.get("auto_exit_enabled", payload.get("auto_close", True))
            ),
            sell_ttl_ms=int(payload.get("sell_ttl_ms", 8000)),
            max_sell_retries=int(payload.get("max_sell_retries", 3)),
            max_sl_ttl_retries=self._bounded_int(
                payload.get("max_sl_ttl_retries", 2), 0, 20, 2
            ),
            force_close_on_ttl=bool(payload.get("force_close_on_ttl", True)),
            max_wait_sell_ms=self._bounded_int(
                payload.get("max_wait_sell_ms", 15000), 1000, 120000, 15000
            ),
            max_exit_total_ms=self._bounded_int(
                payload.get("max_exit_total_ms", 120000), 1000, 300000, 120000
            ),
            allow_force_close=bool(payload.get("allow_force_close", False)),
            cycle_count=self._bounded_int(payload.get("cycle_count", 1), 1, 1000, 1),
            order_quote=order_quote,
            max_budget=max_budget,
            budget_reserve=budget_reserve,
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
            self._ttl_expired_logged = False
        elif price_state.data_blind and price_state.ttl_expired:
            if not self._ttl_expired_logged or now - self._last_ttl_expired_log_ts >= 5.0:
                self._ttl_expired_logged = True
                self._last_ttl_expired_log_ts = now
                self._append_log(
                    f"[DATA_BLIND] ttl_expired state={state_label} action=freeze_exits"
                )
        else:
            self._data_blind_active = False

    def _load_api_state(self) -> None:
        creds = self._config_store.load_api_credentials()
        self._api_credentials = ApiCredentials(
            key=self._sanitize_api_value(creds.key),
            secret=self._sanitize_api_value(creds.secret),
        )
        if not self._has_valid_api_credentials(self._api_credentials):
            self._append_log("[API] missing, trading disabled")

    def _resolve_api_credentials(self, env: dict) -> ApiCredentials:
        if self._has_valid_api_credentials(self._api_credentials):
            return self._api_credentials
        env_key = self._sanitize_api_value(env.get("BINANCE_KEY", ""))
        env_secret = self._sanitize_api_value(env.get("BINANCE_SECRET", ""))
        return ApiCredentials(key=env_key, secret=env_secret)

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
        entry_offset_ticks: int,
        exit_offset_ticks: int,
        mid_fresh_ms: int,
        http_fresh_ms: int,
        max_entry_total_ms: int,
        allow_borrow: bool,
        auto_exit_enabled: bool,
    ) -> None:
        if self._settings:
            self._settings.order_quote = order_quote
            self._settings.max_budget = max_budget
            self._settings.budget_reserve = budget_reserve
            self._settings.cycle_count = cycle_count
            self._settings.take_profit_ticks = take_profit_ticks
            self._settings.stop_loss_ticks = stop_loss_ticks
            self._settings.entry_offset_ticks = self._bounded_int(
                entry_offset_ticks, 0, 1000, 1
            )
            self._settings.offset_ticks = self._settings.entry_offset_ticks
            self._settings.exit_offset_ticks = exit_offset_ticks
            self._settings.mid_fresh_ms = self._bounded_int(
                mid_fresh_ms, 200, 5000, 800
            )
            self._settings.http_fresh_ms = self._bounded_int(
                http_fresh_ms, 200, 10000, 1500
            )
            self._settings.max_entry_total_ms = self._bounded_int(
                max_entry_total_ms, 1000, 120000, 30000
            )
            self._settings.allow_borrow = allow_borrow
            self._settings.auto_exit_enabled = auto_exit_enabled
        self._append_log("[SETTINGS] saved")

    def _on_api_saved(self, key: str, secret: str) -> None:
        self._api_credentials = ApiCredentials(key=key, secret=secret)
        if self._rest:
            self._rest.api_key = key
            self._rest.api_secret = secret
        self._append_log("[API] saved ok")
        self._configure_trading_state()
        if self._connected and self._rest:
            self._check_margin_permissions()

    def _configure_trading_state(self) -> None:
        if not self._connected or not self._settings or not self._router or not self._rest:
            return
        if not self._has_valid_api_credentials(self._api_credentials):
            self._trade_executor = None
            self.start_button.setEnabled(False)
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
        env_path = Path(__file__).resolve().parents[2] / "config" / ".env"
        if not env_path.exists():
            return {}
        if find_spec("dotenv") is None:
            self._append_log("python-dotenv is not installed; skipping .env load.")
            return {}
        dotenv = import_module("dotenv")
        return {
            key: value or ""
            for key, value in dotenv.dotenv_values(env_path).items()
        }

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
            self._order_tracker.sync_active_orders(orders)
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
                for item in items:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                items[0].setData(key, Qt.ItemDataRole.UserRole)
                self.orders_model.appendRow(items)
                self._open_order_rows[key] = self.orders_model.rowCount() - 1
            else:
                for col, value in enumerate(row_values):
                    item = self.orders_model.item(row, col)
                    if item and item.text() != value:
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
            return max(0, int((price - bid) / tick_size))
        if side == "SELL":
            if ask is None:
                return None
            return max(0, int((price - ask) / tick_size))
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
        self.bid_value_label.setText("—")
        self.ask_value_label.setText("—")
        self.mid_value_label.setText("—")
        self.spread_value_label.setText("—")
        self.ws_connected_label.setText("—")
        self.data_blind_label.setText("—")
        self.switch_reason_label.setText("—")
        self.entry_price_label.setText("—")
        self.entry_offset_label.setText("—")
        self.entry_age_label.setText("—")
        self.entry_reprice_reason_label.setText("—")
        self.tp_price_label.setText("—")
        self.sl_price_label.setText("—")
        self.exit_intent_label.setText("—")
        self.exit_policy_label.setText("—")
        self.exit_age_label.setText("—")
        self.max_entry_total_label.setText("—")
        self.max_exit_total_label.setText("—")
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
        key = (order_id, side, ts_ms)
        if key in self._fill_seen_keys:
            return
        self._fill_seen_keys.add(key)
        side_upper = str(side).upper()
        pnl_quote = None
        pnl_bps = None
        exit_reason = "—"
        if self._trade_executor and side_upper == "SELL":
            pnl_quote = self._trade_executor.pnl_cycle
            exit_reason = self._trade_executor.last_exit_reason or "—"
            buy_price = self._trade_executor.get_buy_price()
            if pnl_quote is not None and buy_price and qty:
                notion = buy_price * qty
                if notion > 0:
                    pnl_bps = (pnl_quote / notion) * 10000
        entry = {
            "time": self._format_time(ts_ms),
            "side": side_upper,
            "avg_fill_price": price,
            "qty": qty,
            "pnl_quote": pnl_quote,
            "pnl_bps": pnl_bps,
            "exit_reason": exit_reason,
            "cycle_id": "—",
        }
        self._fills_history.insert(0, entry)
        self._append_fill_row(entry)
        if len(self._fills_history) > 200:
            self._fills_history.pop()
            if self.fills_model.rowCount() > 200:
                self.fills_model.removeRow(self.fills_model.rowCount() - 1)

    def _append_fill_row(self, entry: dict) -> None:
        row = [
            entry.get("time", "—"),
            entry.get("side", "—"),
            self._fmt_price(entry.get("avg_fill_price"), width=12),
            self._fmt_qty(entry.get("qty"), width=8),
            self._fmt_pnl(entry.get("pnl_quote"), width=12),
            self._fmt_pnl(entry.get("pnl_bps"), width=8),
            str(entry.get("exit_reason", "—")),
            str(entry.get("cycle_id", "—")),
        ]
        items = [QStandardItem(value) for value in row]
        for item in items:
            item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        self.fills_model.insertRow(0, items)

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
