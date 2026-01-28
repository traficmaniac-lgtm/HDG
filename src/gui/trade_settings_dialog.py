from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
)

from src.core.config_store import ConfigStore
from src.core.models import Settings


class TradeSettingsDialog(QDialog):
    saved = Signal(
        float, int, int, int, int, str, int, int, int, str, bool, str, bool, float, float
    )

    def __init__(
        self, parent=None, store: ConfigStore | None = None, settings: Settings | None = None
    ) -> None:
        super().__init__(parent)
        self._store = store or ConfigStore()
        self._settings = settings
        self.setWindowTitle("Параметры торговли")
        self.setMinimumWidth(360)

        layout = QVBoxLayout(self)
        form = QFormLayout()

        order_header = QLabel("ОРДЕР")
        order_header.setStyleSheet("font-weight: bold;")
        form.addRow(order_header)

        self.order_quote_input = QDoubleSpinBox()
        self.order_quote_input.setRange(0.0, 1_000_000_000.0)
        self.order_quote_input.setDecimals(2)
        self.order_quote_input.setSingleStep(1.0)
        self.order_quote_input.setValue(
            float(getattr(settings, "order_quote", 50.0) or 0.0) if settings else 50.0
        )
        form.addRow("Размер ордера (USDT)", self.order_quote_input)

        self.buy_ttl_input = QSpinBox()
        self.buy_ttl_input.setRange(500, 20000)
        self.buy_ttl_input.setSingleStep(100)
        self.buy_ttl_input.setValue(
            int(getattr(settings, "buy_ttl_ms", 2500) or 2500) if settings else 2500
        )
        form.addRow("TTL входа BUY (мс)", self.buy_ttl_input)

        self.buy_retry_input = QSpinBox()
        self.buy_retry_input.setRange(0, 10)
        self.buy_retry_input.setValue(
            int(getattr(settings, "max_buy_retries", 3) or 0) if settings else 3
        )
        form.addRow("Повторы входа BUY", self.buy_retry_input)

        self.order_type_input = QComboBox()
        self.order_type_input.addItems(["LIMIT", "MARKET"])
        current_order_type = str(getattr(settings, "order_type", "LIMIT") or "LIMIT").upper()
        index = self.order_type_input.findText(current_order_type)
        if index >= 0:
            self.order_type_input.setCurrentIndex(index)
        form.addRow("Тип ордера", self.order_type_input)

        capital_header = QLabel("КАПИТАЛ")
        capital_header.setStyleSheet("font-weight: bold;")
        form.addRow(capital_header)

        self.max_budget_input = QDoubleSpinBox()
        self.max_budget_input.setRange(0.0, 1_000_000_000.0)
        self.max_budget_input.setDecimals(2)
        self.max_budget_input.setSingleStep(10.0)
        self.max_budget_input.setValue(
            float(getattr(settings, "max_budget", 1000.0) or 0.0) if settings else 1000.0
        )
        form.addRow("Максимальный бюджет (USDT)", self.max_budget_input)

        self.budget_reserve_input = QDoubleSpinBox()
        self.budget_reserve_input.setRange(0.0, 1_000_000_000.0)
        self.budget_reserve_input.setDecimals(2)
        self.budget_reserve_input.setSingleStep(1.0)
        self.budget_reserve_input.setValue(
            float(getattr(settings, "budget_reserve", 20.0) or 0.0) if settings else 20.0
        )
        form.addRow("Резерв (USDT)", self.budget_reserve_input)

        self.tick_offset_input = QSpinBox()
        self.tick_offset_input.setRange(0, 1000)
        self.tick_offset_input.setValue(
            int(getattr(settings, "offset_ticks", 1) or 0) if settings else 1
        )
        form.addRow("Смещение в тиках", self.tick_offset_input)

        self.take_profit_input = QSpinBox()
        self.take_profit_input.setRange(1, 1000)
        self.take_profit_input.setValue(
            int(getattr(settings, "take_profit_ticks", 1) or 1) if settings else 1
        )
        form.addRow("Take-profit (тики)", self.take_profit_input)

        self.stop_loss_input = QSpinBox()
        self.stop_loss_input.setRange(1, 1000)
        self.stop_loss_input.setValue(
            int(getattr(settings, "stop_loss_ticks", 2) or 2) if settings else 2
        )
        form.addRow("Stop-loss (тики)", self.stop_loss_input)

        self.cycle_count_input = QSpinBox()
        self.cycle_count_input.setRange(1, 1000)
        self.cycle_count_input.setValue(
            int(getattr(settings, "cycle_count", 1) or 1) if settings else 1
        )
        form.addRow("Количество циклов", self.cycle_count_input)

        self.exit_order_type_input = QComboBox()
        self.exit_order_type_input.addItems(["LIMIT", "MARKET"])
        current_exit_order_type = str(
            getattr(settings, "exit_order_type", "LIMIT") or "LIMIT"
        ).upper()
        index = self.exit_order_type_input.findText(current_exit_order_type)
        if index >= 0:
            self.exit_order_type_input.setCurrentIndex(index)
        form.addRow("Тип выхода", self.exit_order_type_input)

        self.exit_offset_input = QSpinBox()
        self.exit_offset_input.setRange(0, 1000)
        self.exit_offset_input.setValue(
            int(getattr(settings, "exit_offset_ticks", 1) or 1) if settings else 1
        )
        form.addRow("Смещение выхода (тики)", self.exit_offset_input)

        self.allow_borrow_input = QCheckBox("Разрешить заём (borrow)")
        self.allow_borrow_input.setChecked(bool(getattr(settings, "allow_borrow", True)))
        form.addRow("", self.allow_borrow_input)

        self.auto_exit_input = QCheckBox("Авто-выход по TP/SL")
        self.auto_exit_input.setChecked(
            bool(
                getattr(settings, "auto_exit_enabled", getattr(settings, "auto_close", True))
            )
        )
        form.addRow("", self.auto_exit_input)

        self.side_effect_input = QComboBox()
        self.side_effect_input.addItems(["AUTO_BORROW_REPAY", "MARGIN_BUY", "NONE"])
        current_side_effect = str(
            getattr(settings, "side_effect_type", "AUTO_BORROW_REPAY") or "AUTO_BORROW_REPAY"
        ).upper()
        index = self.side_effect_input.findText(current_side_effect)
        if index >= 0:
            self.side_effect_input.setCurrentIndex(index)
        form.addRow("SideEffectType", self.side_effect_input)

        mode_layout = QHBoxLayout()
        mode_label = QLabel("CROSS MARGIN")
        mode_hint = QLabel(f"x{getattr(settings, 'leverage_hint', 3)} (hint)")
        mode_layout.addWidget(mode_label)
        mode_layout.addSpacing(6)
        mode_layout.addWidget(mode_hint)
        mode_layout.addStretch(1)
        form.addRow("Режим", mode_layout)

        self.hedge_profile = QComboBox()
        self.hedge_profile.addItem("Default")
        self.hedge_profile.setEnabled(False)
        form.addRow("Хедж-профиль", self.hedge_profile)

        layout.addLayout(form)

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
        payload = self._store.load_settings()
        payload.pop("nominal_usd", None)
        payload.pop("budget_mode_enabled", None)
        payload.pop("budget_quote", None)
        payload.pop("usage_pct", None)
        payload.pop("min_quote_reserve", None)
        payload["order_quote"] = float(self.order_quote_input.value())
        payload["max_budget"] = float(self.max_budget_input.value())
        payload["budget_reserve"] = float(self.budget_reserve_input.value())
        payload["offset_ticks"] = int(self.tick_offset_input.value())
        payload["take_profit_ticks"] = int(self.take_profit_input.value())
        payload["stop_loss_ticks"] = int(self.stop_loss_input.value())
        payload["cycle_count"] = int(self.cycle_count_input.value())
        payload["order_type"] = str(self.order_type_input.currentText()).upper()
        payload["buy_ttl_ms"] = int(self.buy_ttl_input.value())
        payload["max_buy_retries"] = int(self.buy_retry_input.value())
        payload["exit_order_type"] = str(self.exit_order_type_input.currentText()).upper()
        payload["exit_offset_ticks"] = int(self.exit_offset_input.value())
        payload["allow_borrow"] = bool(self.allow_borrow_input.isChecked())
        payload["side_effect_type"] = str(self.side_effect_input.currentText()).upper()
        payload["auto_exit_enabled"] = bool(self.auto_exit_input.isChecked())
        self._store.save_settings(payload)
        self.saved.emit(
            float(self.order_quote_input.value()),
            int(self.tick_offset_input.value()),
            int(self.take_profit_input.value()),
            int(self.stop_loss_input.value()),
            int(self.cycle_count_input.value()),
            str(self.order_type_input.currentText()).upper(),
            int(self.buy_ttl_input.value()),
            int(self.buy_retry_input.value()),
            int(self.exit_offset_input.value()),
            str(self.exit_order_type_input.currentText()).upper(),
            bool(self.allow_borrow_input.isChecked()),
            str(self.side_effect_input.currentText()).upper(),
            bool(self.auto_exit_input.isChecked()),
            float(self.max_budget_input.value()),
            float(self.budget_reserve_input.value()),
        )
        self.accept()
