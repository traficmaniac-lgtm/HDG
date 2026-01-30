from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from src.core.config_store import ConfigStore
from src.core.models import Settings


class TradeSettingsDialog(QDialog):
    saved = Signal(
        float,
        float,
        float,
        int,
        int,
        int,
        int,
        int,
        int,
        bool,
        bool,
        bool,
    )

    def __init__(
        self, parent=None, store: ConfigStore | None = None, settings: Settings | None = None
    ) -> None:
        super().__init__(parent)
        self._store = store or ConfigStore()
        self._settings = settings
        self.setWindowTitle("Параметры торговли")
        self.setMinimumWidth(360)
        self.setFixedHeight(430)

        layout = QVBoxLayout(self)
        tabs = QTabWidget()
        basic_tab = QWidget()
        advanced_tab = QWidget()
        tabs.addTab(basic_tab, "Основное")
        tabs.addTab(advanced_tab, "Продвинутое")
        tabs.setCurrentIndex(0)

        basic_form = QFormLayout(basic_tab)
        basic_form.setVerticalSpacing(8)

        self.order_quote_input = QDoubleSpinBox()
        self.order_quote_input.setRange(0.0, 1_000_000_000.0)
        self.order_quote_input.setDecimals(2)
        self.order_quote_input.setSingleStep(1.0)
        self.order_quote_input.setValue(
            float(getattr(settings, "order_quote", 50.0) or 0.0) if settings else 50.0
        )
        basic_form.addRow("Размер ордера (USDT)", self.order_quote_input)

        self.max_budget_input = QDoubleSpinBox()
        self.max_budget_input.setRange(0.0, 1_000_000_000.0)
        self.max_budget_input.setDecimals(2)
        self.max_budget_input.setSingleStep(10.0)
        self.max_budget_input.setValue(
            float(getattr(settings, "max_budget", 1000.0) or 0.0) if settings else 1000.0
        )
        basic_form.addRow("Макс. бюджет (USDT)", self.max_budget_input)

        self.budget_reserve_input = QDoubleSpinBox()
        self.budget_reserve_input.setRange(0.0, 1_000_000_000.0)
        self.budget_reserve_input.setDecimals(2)
        self.budget_reserve_input.setSingleStep(1.0)
        self.budget_reserve_input.setValue(
            float(getattr(settings, "budget_reserve", 20.0) or 0.0) if settings else 20.0
        )
        basic_form.addRow("Резерв (USDT)", self.budget_reserve_input)

        self.cycle_count_input = QSpinBox()
        self.cycle_count_input.setRange(1, 1000)
        self.cycle_count_input.setValue(
            int(getattr(settings, "cycle_count", 1) or 1) if settings else 1
        )
        basic_form.addRow("Кол-во циклов", self.cycle_count_input)

        self.take_profit_input = QSpinBox()
        self.take_profit_input.setRange(1, 1000)
        self.take_profit_input.setValue(
            int(getattr(settings, "take_profit_ticks", 1) or 1) if settings else 1
        )
        basic_form.addRow("TP (тики)", self.take_profit_input)

        self.stop_loss_input = QSpinBox()
        self.stop_loss_input.setRange(1, 1000)
        self.stop_loss_input.setValue(
            int(getattr(settings, "stop_loss_ticks", 2) or 2) if settings else 2
        )
        basic_form.addRow("SL (тики)", self.stop_loss_input)

        self.allow_borrow_input = QCheckBox("Разрешить заём (borrow)")
        self.allow_borrow_input.setChecked(bool(getattr(settings, "allow_borrow", True)))
        basic_form.addRow("", self.allow_borrow_input)

        self.auto_exit_input = QCheckBox("Авто-выход по TP/SL")
        self.auto_exit_input.setChecked(
            bool(
                getattr(settings, "auto_exit_enabled", getattr(settings, "auto_close", True))
            )
        )
        basic_form.addRow("", self.auto_exit_input)

        advanced_form = QFormLayout(advanced_tab)
        advanced_form.setVerticalSpacing(8)

        self.entry_max_age_input = QSpinBox()
        self.entry_max_age_input.setRange(200, 5000)
        self.entry_max_age_input.setSingleStep(50)
        self.entry_max_age_input.setValue(
            int(getattr(settings, "entry_max_age_ms", 800) or 800) if settings else 800
        )
        advanced_form.addRow("Entry max age (ms)", self.entry_max_age_input)

        self.exit_max_age_input = QSpinBox()
        self.exit_max_age_input.setRange(500, 10000)
        self.exit_max_age_input.setSingleStep(100)
        self.exit_max_age_input.setValue(
            int(getattr(settings, "exit_max_age_ms", 2500) or 2500) if settings else 2500
        )
        advanced_form.addRow("Exit max age (ms)", self.exit_max_age_input)

        self.http_fresh_input = QSpinBox()
        self.http_fresh_input.setRange(200, 10000)
        self.http_fresh_input.setSingleStep(100)
        self.http_fresh_input.setValue(
            int(getattr(settings, "http_fresh_ms", 1500) or 1500) if settings else 1500
        )
        advanced_form.addRow("HTTP fresh (ms)", self.http_fresh_input)

        self.verbose_ui_log_input = QCheckBox("Verbose UI log")
        self.verbose_ui_log_input.setChecked(
            bool(getattr(settings, "verbose_ui_log", False))
        )
        advanced_form.addRow("", self.verbose_ui_log_input)

        layout.addWidget(tabs)

        buttons_layout = QHBoxLayout()
        buttons_layout.addStretch(1)
        save_button = QPushButton("Сохранить")
        cancel_button = QPushButton("Отмена")
        save_button.clicked.connect(self._on_save)
        cancel_button.clicked.connect(self.reject)
        buttons_layout.addWidget(save_button)
        buttons_layout.addWidget(cancel_button)
        layout.addLayout(buttons_layout)

    def _on_save(self) -> None:
        payload = self._store.normalize_settings(self._store.load_settings())
        payload.pop("nominal_usd", None)
        payload.pop("budget_mode_enabled", None)
        payload.pop("budget_quote", None)
        payload.pop("usage_pct", None)
        payload.pop("min_quote_reserve", None)
        payload["order_quote"] = float(self.order_quote_input.value())
        payload["max_budget"] = float(self.max_budget_input.value())
        payload["budget_reserve"] = float(self.budget_reserve_input.value())
        payload["take_profit_ticks"] = int(self.take_profit_input.value())
        payload["stop_loss_ticks"] = int(self.stop_loss_input.value())
        payload["cycle_count"] = int(self.cycle_count_input.value())
        payload["entry_max_age_ms"] = int(self.entry_max_age_input.value())
        payload["exit_max_age_ms"] = int(self.exit_max_age_input.value())
        payload["http_fresh_ms"] = int(self.http_fresh_input.value())
        payload["allow_borrow"] = bool(self.allow_borrow_input.isChecked())
        payload["auto_exit_enabled"] = bool(self.auto_exit_input.isChecked())
        payload["verbose_ui_log"] = bool(self.verbose_ui_log_input.isChecked())
        payload = self._store.normalize_settings(payload)
        self._store.save_settings(payload)
        self.saved.emit(
            float(self.order_quote_input.value()),
            float(self.max_budget_input.value()),
            float(self.budget_reserve_input.value()),
            int(self.cycle_count_input.value()),
            int(self.take_profit_input.value()),
            int(self.stop_loss_input.value()),
            int(self.entry_max_age_input.value()),
            int(self.exit_max_age_input.value()),
            int(self.http_fresh_input.value()),
            bool(self.allow_borrow_input.isChecked()),
            bool(self.auto_exit_input.isChecked()),
            bool(self.verbose_ui_log_input.isChecked()),
        )
        self.accept()
