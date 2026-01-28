from __future__ import annotations

import json
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, QTimer, Slot
from PySide6.QtWidgets import (
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPlainTextEdit,
    QPushButton,
    QFileDialog,
    QVBoxLayout,
    QWidget,
)

from src.core.models import Settings, SymbolProfile
from src.core.version import VERSION
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
        self._last_trade_action = "—"
        self._orders_count = 0

        self._ui_timer = QTimer(self)
        self._ui_timer.timeout.connect(self._refresh_ui)
        self._http_timer = QTimer(self)
        self._http_timer.timeout.connect(self._poll_http)

        self._ws_thread = None
        self._ws_worker = None

        self._build_ui()
        self._update_status(False)

    def _build_ui(self) -> None:
        central = QWidget()
        root = QVBoxLayout(central)

        header = QFrame()
        header_layout = QHBoxLayout(header)
        self.title_label = QLabel(f"Directional Hedge Scalper v{VERSION} — EURIUSDT Core")
        self.title_label.setStyleSheet("font-weight: 600; font-size: 16px;")
        self.connect_button = QPushButton("CONNECT")
        self.connect_button.clicked.connect(self._toggle_connection)
        self.status_label = QLabel("DISCONNECTED")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setFixedWidth(140)
        self.status_label.setStyleSheet("color: #ff5f57; font-weight: 600;")
        header_layout.addWidget(self.title_label)
        header_layout.addStretch(1)
        header_layout.addWidget(self.connect_button)
        header_layout.addWidget(self.status_label)
        root.addWidget(header)

        blocks = QHBoxLayout()
        root.addLayout(blocks)

        self.market_box = QGroupBox("Market (EURIUSDT)")
        market_layout = QVBoxLayout(self.market_box)
        self.mid_label = QLabel("Mid: —")
        self.bid_label = QLabel("Bid: —")
        self.ask_label = QLabel("Ask: —")
        self.age_label = QLabel("Age (ms): —")
        self.source_label = QLabel("Source: NONE")
        market_layout.addWidget(self.mid_label)
        market_layout.addWidget(self.bid_label)
        market_layout.addWidget(self.ask_label)
        market_layout.addWidget(self.age_label)
        market_layout.addWidget(self.source_label)
        blocks.addWidget(self.market_box)

        self.symbol_box = QGroupBox("Symbol Profile")
        symbol_layout = QVBoxLayout(self.symbol_box)
        self.tick_label = QLabel("tickSize: —")
        self.step_label = QLabel("stepSize: —")
        self.min_qty_label = QLabel("minQty: —")
        self.min_notional_label = QLabel("minNotional: —")
        symbol_layout.addWidget(self.tick_label)
        symbol_layout.addWidget(self.step_label)
        symbol_layout.addWidget(self.min_qty_label)
        symbol_layout.addWidget(self.min_notional_label)
        blocks.addWidget(self.symbol_box)

        self.health_box = QGroupBox("Health")
        health_layout = QVBoxLayout(self.health_box)
        self.ws_connected_label = QLabel("ws_connected: False")
        self.ws_age_label = QLabel("ws_age_ms: —")
        self.http_age_label = QLabel("http_age_ms: —")
        self.switch_reason_label = QLabel("last_switch_reason: —")
        health_layout.addWidget(self.ws_connected_label)
        health_layout.addWidget(self.ws_age_label)
        health_layout.addWidget(self.http_age_label)
        health_layout.addWidget(self.switch_reason_label)
        blocks.addWidget(self.health_box)

        self.trade_box = QGroupBox("Trading (TEST)")
        trade_layout = QVBoxLayout(self.trade_box)
        self.start_button = QPushButton("START TRADING (TEST)")
        self.start_button.clicked.connect(self._start_trading)
        self.stop_button = QPushButton("STOP")
        self.stop_button.clicked.connect(self._stop_trading)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self.orders_label = QLabel("orders: 0")
        self.last_action_label = QLabel("last_action: —")
        trade_layout.addWidget(self.start_button)
        trade_layout.addWidget(self.stop_button)
        trade_layout.addWidget(self.orders_label)
        trade_layout.addWidget(self.last_action_label)
        blocks.addWidget(self.trade_box)

        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumBlockCount(500)
        root.addWidget(self.log, stretch=1)

        log_actions = QHBoxLayout()
        self.save_log_button = QPushButton("Save Log")
        self.save_log_button.clicked.connect(self._save_log)
        log_actions.addStretch(1)
        log_actions.addWidget(self.save_log_button)
        root.addLayout(log_actions)

        self.setCentralWidget(central)

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

        env = self._load_env()
        self._append_log("Loaded settings and .env")

        self._router = PriceRouter(self._settings)
        self._rest = BinanceRestClient(
            api_key=env.get("BINANCE_KEY", ""),
            api_secret=env.get("BINANCE_SECRET", ""),
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

        if self._rest and self._router and self._settings:
            self._trade_executor = TradeExecutor(
                rest=self._rest,
                router=self._router,
                settings=self._settings,
                profile=self._symbol_profile,
                logger=self._append_log,
            )
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(True)

        self._start_ws()
        self._http_timer.start(self._settings.http_interval_ms)
        self._ui_timer.start(self._settings.ui_refresh_ms)

        self._connected = True
        self._update_status(True)
        self._append_log("CONNECT: data services started")

    def disconnect(self) -> None:
        if not self._connected:
            return

        self._ui_timer.stop()
        self._http_timer.stop()

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
        self._orders_count = 0
        self._last_trade_action = "—"
        self._update_trade_labels()

        self._connected = False
        self._ws_connected = False
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

        self.mid_label.setText(f"Mid: {self._fmt_price(price_state.mid)}")
        self.bid_label.setText(f"Bid: {self._fmt_price(price_state.bid)}")
        self.ask_label.setText(f"Ask: {self._fmt_price(price_state.ask)}")
        self.age_label.setText(
            f"Age (ms): {self._fmt_int(price_state.mid_age_ms)}"
        )
        self.source_label.setText(f"Source: {price_state.source}")

        self.ws_connected_label.setText(f"ws_connected: {health_state.ws_connected}")
        self.ws_age_label.setText(f"ws_age_ms: {self._fmt_int(health_state.ws_age_ms)}")
        self.http_age_label.setText(
            f"http_age_ms: {self._fmt_int(health_state.http_age_ms)}"
        )
        self.switch_reason_label.setText(
            f"last_switch_reason: {health_state.last_switch_reason or '—'}"
        )

    def _update_trade_labels(self) -> None:
        self.orders_label.setText(f"orders: {self._orders_count}")
        self.last_action_label.setText(f"last_action: {self._last_trade_action}")

    def _update_status(self, connected: bool) -> None:
        if connected:
            self.status_label.setText("CONNECTED")
            self.status_label.setStyleSheet("color: #3ad07d; font-weight: 600;")
            self.connect_button.setText("DISCONNECT")
        else:
            self.status_label.setText("DISCONNECTED")
            self.status_label.setStyleSheet("color: #ff5f57; font-weight: 600;")
            self.connect_button.setText("CONNECT")

    @Slot()
    def _start_trading(self) -> None:
        if not self._trade_executor:
            return
        placed = self._trade_executor.place_test_orders_margin()
        if placed:
            self._orders_count = placed
            self._last_trade_action = "placed"
            self._update_trade_labels()

    @Slot()
    def _stop_trading(self) -> None:
        if not self._trade_executor:
            return
        self._trade_executor.cancel_test_orders_margin()
        self._orders_count = 0
        self._last_trade_action = "cancelled"
        self._update_trade_labels()

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
            self._append_log(f"Log saved: {path}")
        except Exception as exc:
            self._append_log(f"Failed to save log: {exc}")

    def _append_log(self, message: str) -> None:
        self.log.appendPlainText(message)

    def _load_settings(self) -> Settings:
        settings_path = Path(__file__).resolve().parents[2] / "config" / "settings.json"
        with settings_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return Settings(
            symbol=payload.get("symbol", "EURIUSDT"),
            ws_fresh_ms=int(payload.get("ws_fresh_ms", 700)),
            http_fresh_ms=int(payload.get("http_fresh_ms", 1500)),
            http_interval_ms=int(payload.get("http_interval_ms", 1000)),
            ui_refresh_ms=int(payload.get("ui_refresh_ms", 100)),
            account_mode=str(payload.get("account_mode", "CROSS_MARGIN")),
            max_leverage_hint=int(payload.get("max_leverage_hint", 3)),
            test_notional_usd=float(payload.get("test_notional_usd", 10.0)),
            test_tick_offset=int(payload.get("test_tick_offset", 1)),
            margin_isolated=bool(payload.get("margin_isolated", False)),
        )

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
        return SymbolProfile(
            tick_size=self._safe_float(price.get("tickSize")),
            step_size=self._safe_float(lot.get("stepSize")),
            min_qty=self._safe_float(lot.get("minQty")),
            min_notional=self._safe_float(notional.get("minNotional")),
        )

    def _render_symbol_profile(self, profile: SymbolProfile) -> None:
        self.tick_label.setText(f"tickSize: {self._fmt_price(profile.tick_size)}")
        self.step_label.setText(f"stepSize: {self._fmt_price(profile.step_size)}")
        self.min_qty_label.setText(f"minQty: {self._fmt_price(profile.min_qty)}")
        self.min_notional_label.setText(
            f"minNotional: {self._fmt_price(profile.min_notional)}"
        )

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
