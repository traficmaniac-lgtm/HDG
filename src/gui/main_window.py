from __future__ import annotations

from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from typing import Optional

from datetime import datetime
import time

import httpx
from PySide6.QtCore import Qt, QTimer, Slot
from PySide6.QtGui import QAction, QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QFileDialog,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMenu,
    QPlainTextEdit,
    QPushButton,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QTableView,
    QMessageBox,
)

from src.core.config_store import ApiCredentials, ConfigStore
from src.core.models import Settings, SymbolProfile
from src.core.version import VERSION
from src.gui.api_settings_dialog import ApiSettingsDialog
from src.gui.trade_settings_dialog import TradeSettingsDialog
from src.services.binance_rest import BinanceRestClient
from src.services.http_price import HttpPriceService
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
        self._last_age_update_ts = 0.0

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

        header = QFrame()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(8, 6, 8, 6)
        header_layout.setSpacing(8)
        self.summary_label = QLabel(
            "EURIUSDT | SRC: NONE | MARGIN_AUTH: — | BORROW: — | last_action: — | orders: —"
        )
        self.summary_label.setStyleSheet("font-weight: 600;")
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
        self.settings_button = QPushButton("⚙ Настройки")
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

        tabs = QTabWidget()

        summary_frame = QGroupBox("Сводка")
        summary_layout = QHBoxLayout(summary_frame)
        summary_layout.setSpacing(16)

        market_column = QVBoxLayout()
        market_title = QLabel("Market")
        market_title.setStyleSheet("font-weight: 600;")
        self.mid_label = QLabel("Mid: —")
        self.bid_label = QLabel("Bid: —")
        self.ask_label = QLabel("Ask: —")
        self.age_label = QLabel("Age: — ms")
        market_column.addWidget(market_title)
        market_column.addWidget(self.mid_label)
        market_column.addWidget(self.bid_label)
        market_column.addWidget(self.ask_label)
        market_column.addWidget(self.age_label)

        health_column = QVBoxLayout()
        health_title = QLabel("Health")
        health_title.setStyleSheet("font-weight: 600;")
        self.ws_connected_label = QLabel("ws_connected: False")
        self.ws_age_label = QLabel("ws_age_ms: —")
        self.http_age_label = QLabel("http_age_ms: —")
        self.switch_reason_label = QLabel("last_switch_reason: —")
        health_column.addWidget(health_title)
        health_column.addWidget(self.ws_connected_label)
        health_column.addWidget(self.ws_age_label)
        health_column.addWidget(self.http_age_label)
        health_column.addWidget(self.switch_reason_label)

        profile_column = QVBoxLayout()
        profile_title = QLabel("Profile")
        profile_title.setStyleSheet("font-weight: 600;")
        self.tick_label = QLabel("tickSize: —")
        self.step_label = QLabel("stepSize: —")
        self.min_qty_label = QLabel("minQty: —")
        self.min_notional_label = QLabel("minNotional: —")
        profile_column.addWidget(profile_title)
        profile_column.addWidget(self.tick_label)
        profile_column.addWidget(self.step_label)
        profile_column.addWidget(self.min_qty_label)
        profile_column.addWidget(self.min_notional_label)

        summary_layout.addLayout(market_column)
        summary_layout.addLayout(health_column)
        summary_layout.addLayout(profile_column)

        terminal_tab = QWidget()
        terminal_layout = QVBoxLayout(terminal_tab)
        terminal_layout.setSpacing(8)
        terminal_layout.addWidget(summary_frame)

        orders_box = QGroupBox("Ордера")
        orders_layout = QVBoxLayout(orders_box)
        pnl_layout = QHBoxLayout()
        self.pnl_unrealized_label = QLabel("Unrealized (est): —")
        self.pnl_cycle_label = QLabel("PnL за цикл: —")
        self.pnl_session_label = QLabel("Session PnL: —")
        pnl_layout.addWidget(self.pnl_unrealized_label)
        pnl_layout.addSpacing(12)
        pnl_layout.addWidget(self.pnl_cycle_label)
        pnl_layout.addSpacing(12)
        pnl_layout.addWidget(self.pnl_session_label)
        pnl_layout.addStretch(1)
        orders_layout.addLayout(pnl_layout)

        self.orders_model = QStandardItemModel(0, 9, self)
        self.orders_model.setHorizontalHeaderLabels(
            [
                "time",
                "orderId",
                "side",
                "price",
                "qty",
                "status",
                "age_ms",
                "pnl_est",
                "tag",
            ]
        )
        self.orders_table = QTableView()
        self.orders_table.setModel(self.orders_model)
        self.orders_table.verticalHeader().setVisible(False)
        self.orders_table.setAlternatingRowColors(True)
        self.orders_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        header = self.orders_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        orders_layout.addWidget(self.orders_table, stretch=1)
        terminal_layout.addWidget(orders_box, stretch=1)
        tabs.addTab(terminal_tab, "Terminal")

        logs_tab = QWidget()
        logs_layout = QVBoxLayout(logs_tab)
        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumBlockCount(500)
        logs_layout.addWidget(self.log, stretch=1)

        log_actions = QHBoxLayout()
        self.clear_log_button = QPushButton("Clear")
        self.clear_log_button.clicked.connect(self._clear_log)
        log_actions.addStretch(1)
        log_actions.addWidget(self.clear_log_button)
        logs_layout.addLayout(log_actions)
        tabs.addTab(logs_tab, "Logs")

        root.addWidget(tabs, stretch=1)

        self.setCentralWidget(central)

    def _build_menu(self) -> None:
        menu = QMenu("Меню", self)
        self.menuBar().addMenu(menu)

        api_action = QAction("Настройки API...", self)
        api_action.triggered.connect(self._open_api_settings)
        menu.addAction(api_action)

        save_log_action = QAction("Сохранить лог", self)
        save_log_action.triggered.connect(self._save_log)
        menu.addAction(save_log_action)

        auto_calc_action = QAction("Авто-расчёт параметров торговли", self)
        auto_calc_action.setEnabled(False)
        menu.addAction(auto_calc_action)

        menu.addSeparator()

        exit_action = QAction("Выход", self)
        exit_action.triggered.connect(self.close)
        menu.addAction(exit_action)

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
            self._update_trading_controls()
        else:
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            if not self._has_valid_api_credentials(api_credentials):
                self._append_log("[API] missing, trading disabled")

        self._start_ws()
        self._http_timer.start(self._settings.http_interval_ms)
        self._ui_timer.start(self._settings.ui_refresh_ms)
        self._orders_timer.start(1000)

        self._connected = True
        self._update_status(True)
        self._append_log("CONNECT: data services started")

    def disconnect(self) -> None:
        if not self._connected:
            return

        self._ui_timer.stop()
        self._http_timer.stop()
        self._orders_timer.stop()

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
        self.pnl_unrealized_label.setText("Unrealized (est): —")
        self.pnl_cycle_label.setText("PnL за цикл: —")
        self.pnl_session_label.setText("Session PnL: —")

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
        self._ws_worker = WsPriceWorker(self._settings.symbol)
        self._ws_worker.moveToThread(self._ws_thread)
        self._ws_thread.started.connect(self._ws_worker.run)
        self._ws_worker.finished.connect(self._ws_thread.quit)
        self._ws_worker.finished.connect(self._ws_worker.deleteLater)
        self._ws_thread.finished.connect(self._ws_thread.deleteLater)
        self._ws_worker.tick.connect(self._on_ws_tick)
        self._ws_worker.status.connect(self._on_ws_status)
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
        self._append_log(f"WS connected: {status}")

    @Slot()
    def _poll_http(self) -> None:
        if not self._settings or not self._http_service:
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
        health_state.ws_connected = self._ws_connected
        self._last_mid = price_state.mid

        self.mid_label.setText(f"Mid: {self._fmt_price(price_state.mid)}")
        self.bid_label.setText(f"Bid: {self._fmt_price(price_state.bid)}")
        self.ask_label.setText(f"Ask: {self._fmt_price(price_state.ask)}")
        last_action = "—"
        orders_count = "—"
        if self._trade_executor:
            last_action = self._trade_executor.last_action
            orders_count = str(self._trade_executor.orders_count)
        margin_auth = "OK" if self._margin_api_access else "FAIL"
        borrow_status = "—"
        if self._borrow_allowed_by_api is not None:
            borrow_status = "OK" if self._borrow_allowed_by_api else "FAIL"
        self.summary_label.setText(
            f"{self._settings.symbol if self._settings else 'EURIUSDT'} | "
            f"SRC: {price_state.source} | "
            f"MARGIN_AUTH: {margin_auth} | BORROW: {borrow_status} | "
            f"last_action: {last_action} | orders: {orders_count}"
        )
        now = time.monotonic()
        if now - self._last_age_update_ts >= 0.25:
            self._last_age_update_ts = now
            self.age_label.setText(
                f"Age: {self._fmt_int(price_state.mid_age_ms)} ms"
            )

        self.ws_connected_label.setText(f"ws_connected: {health_state.ws_connected}")
        self.ws_age_label.setText(f"ws_age_ms: {self._fmt_int(health_state.ws_age_ms)}")
        self.http_age_label.setText(
            f"http_age_ms: {self._fmt_int(health_state.http_age_ms)}"
        )
        self.switch_reason_label.setText(
            f"last_switch_reason: {health_state.last_switch_reason or '—'}"
        )
        self._update_trading_controls(price_state)

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
        can_trade = (
            self._connected
            and self._trade_executor is not None
            and self._has_valid_api_credentials(self._api_credentials)
        )
        self.start_button.setEnabled(can_trade and self._margin_api_access)
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
        placed = self._trade_executor.place_test_orders_margin()
        if placed:
            self._refresh_orders()

    @Slot()
    def _stop_trading(self) -> None:
        if not self._trade_executor:
            return
        self._trade_executor.cancel_test_orders_margin()
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
        return Settings(
            symbol=payload.get("symbol", "EURIUSDT"),
            ws_fresh_ms=int(payload.get("ws_fresh_ms", 700)),
            http_fresh_ms=int(payload.get("http_fresh_ms", 1500)),
            http_interval_ms=int(payload.get("http_interval_ms", 1000)),
            ui_refresh_ms=int(payload.get("ui_refresh_ms", 100)),
            account_mode=str(payload.get("account_mode", "CROSS_MARGIN")),
            leverage_hint=int(
                payload.get("leverage_hint", payload.get("max_leverage_hint", 3))
            ),
            nominal_usd=float(payload.get("nominal_usd", payload.get("test_notional_usd", 10.0))),
            offset_ticks=int(payload.get("offset_ticks", payload.get("test_tick_offset", 1))),
            order_type=str(payload.get("order_type", "LIMIT")).upper(),
            allow_borrow=bool(payload.get("allow_borrow", True)),
            side_effect_type=str(payload.get("side_effect_type", "AUTO_BORROW_REPAY")).upper(),
            margin_isolated=bool(payload.get("margin_isolated", False)),
        )

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
        notional: float,
        tick_offset: int,
        order_type: str,
        allow_borrow: bool,
        side_effect_type: str,
    ) -> None:
        if self._settings:
            self._settings.nominal_usd = notional
            self._settings.offset_ticks = tick_offset
            self._settings.order_type = order_type
            self._settings.allow_borrow = allow_borrow
            self._settings.side_effect_type = side_effect_type
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
        self.tick_label.setText(f"tickSize: {self._fmt_price(profile.tick_size)}")
        self.step_label.setText(f"stepSize: {self._fmt_price(profile.step_size)}")
        self.min_qty_label.setText(f"minQty: {self._fmt_price(profile.min_qty)}")
        self.min_notional_label.setText(
            f"minNotional: {self._fmt_price(profile.min_notional)}"
        )

    def _orders_model_clear(self) -> None:
        self.orders_model.removeRows(0, self.orders_model.rowCount())

    def _refresh_orders(self) -> None:
        if not self._connected or not self._rest or not self._settings:
            return
        try:
            open_orders = self._rest.get_margin_open_orders(self._settings.symbol)
        except Exception as exc:
            if not self._orders_error_logged:
                self._append_log(f"[ORDERS] fetch failed: {exc}")
                self._orders_error_logged = True
            return

        self._orders_error_logged = False
        self._orders_model_clear()
        mid = self._last_mid
        total_pnl = 0.0
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        for order in open_orders:
            order_time = order.get("time")
            age_ms = None
            if isinstance(order_time, int):
                age_ms = max(0, now_ms - order_time)
            qty = self._safe_float(order.get("origQty"))
            price = self._safe_float(order.get("price"))
            side = str(order.get("side", "—")).upper()
            pnl_est = None
            if mid is not None and qty is not None and price is not None:
                if side == "BUY":
                    pnl_est = (mid - price) * qty
                elif side == "SELL":
                    pnl_est = (price - mid) * qty
            if pnl_est is not None:
                total_pnl += pnl_est
            display_time = self._format_time(order_time)
            row = [
                display_time,
                self._short_order_id(order.get("orderId")),
                side or "—",
                self._fmt_price(price),
                self._fmt_qty(qty),
                str(order.get("status", "—")),
                self._fmt_int(age_ms),
                self._fmt_pnl(pnl_est),
                str(order.get("clientOrderId", "—")),
            ]
            items = [QStandardItem(value) for value in row]
            for item in items:
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.orders_model.appendRow(items)

        if open_orders:
            self.pnl_unrealized_label.setText(
                f"Unrealized (est): {self._fmt_pnl(total_pnl)}"
            )
        else:
            self.pnl_unrealized_label.setText("Unrealized (est): —")

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
    def _fmt_qty(value: Optional[float]) -> str:
        if value is None:
            return "—"
        return f"{value:.4f}"

    @staticmethod
    def _fmt_pnl(value: Optional[float]) -> str:
        if value is None:
            return "—"
        return f"{value:.4f}"

    @staticmethod
    def _fmt_price(value: Optional[float]) -> str:
        if value is None:
            return "—"
        return f"{value:.5f}"

    @staticmethod
    def _fmt_int(value: Optional[int]) -> str:
        if value is None:
            return "—"
        return str(value)

    @staticmethod
    def _safe_float(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None
