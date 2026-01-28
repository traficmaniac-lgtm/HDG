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
    saved = Signal(float, int, str, bool, str, bool)

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

        self.notional_input = QDoubleSpinBox()
        self.notional_input.setRange(0.0, 1000000.0)
        self.notional_input.setDecimals(2)
        self.notional_input.setSingleStep(1.0)
        self.notional_input.setValue(
            float(getattr(settings, "nominal_usd", 10.0) or 0.0)
            if settings
            else 10.0
        )
        form.addRow("Номинал (USD)", self.notional_input)

        self.tick_offset_input = QSpinBox()
        self.tick_offset_input.setRange(0, 1000)
        self.tick_offset_input.setValue(
            int(getattr(settings, "offset_ticks", 1) or 0) if settings else 1
        )
        form.addRow("Смещение в тиках", self.tick_offset_input)

        self.order_type_input = QComboBox()
        self.order_type_input.addItems(["LIMIT", "MARKET"])
        current_order_type = str(getattr(settings, "order_type", "LIMIT") or "LIMIT").upper()
        index = self.order_type_input.findText(current_order_type)
        if index >= 0:
            self.order_type_input.setCurrentIndex(index)
        form.addRow("Тип ордера", self.order_type_input)

        self.allow_borrow_input = QCheckBox("Разрешить заём (borrow)")
        self.allow_borrow_input.setChecked(bool(getattr(settings, "allow_borrow", True)))
        form.addRow("", self.allow_borrow_input)

        self.auto_close_input = QCheckBox("Авто-закрытие после BUY")
        self.auto_close_input.setChecked(bool(getattr(settings, "auto_close", False)))
        form.addRow("", self.auto_close_input)

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
        payload["nominal_usd"] = float(self.notional_input.value())
        payload["offset_ticks"] = int(self.tick_offset_input.value())
        payload["order_type"] = str(self.order_type_input.currentText()).upper()
        payload["allow_borrow"] = bool(self.allow_borrow_input.isChecked())
        payload["side_effect_type"] = str(self.side_effect_input.currentText()).upper()
        payload["auto_close"] = bool(self.auto_close_input.isChecked())
        self._store.save_settings(payload)
        self.saved.emit(
            float(self.notional_input.value()),
            int(self.tick_offset_input.value()),
            str(self.order_type_input.currentText()).upper(),
            bool(self.allow_borrow_input.isChecked()),
            str(self.side_effect_input.currentText()).upper(),
            bool(self.auto_close_input.isChecked()),
        )
        self.accept()
