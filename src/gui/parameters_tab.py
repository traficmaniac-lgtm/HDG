from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


class ParametersTab(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)

        self.order_mode_combo = QComboBox()
        self.order_mode_combo.addItem("Рынок", "market")
        self.order_mode_combo.addItem("Агрессивный лимит", "aggressive_limit")

        self.usd_notional_spin = QDoubleSpinBox()
        self.usd_notional_spin.setRange(1.0, 1000.0)
        self.usd_notional_spin.setDecimals(2)

        self.leverage_max_spin = QSpinBox()
        self.leverage_max_spin.setRange(1, 9)

        self.slip_bps_spin = QDoubleSpinBox()
        self.slip_bps_spin.setRange(0.0, 50.0)
        self.slip_bps_spin.setDecimals(1)
        self.slip_bps_spin.setSingleStep(0.1)

        self.max_loss_spin = QSpinBox()
        self.max_loss_spin.setRange(1, 50)

        self.fee_spin = QDoubleSpinBox()
        self.fee_spin.setRange(0.0, 50.0)
        self.fee_spin.setDecimals(2)

        self.target_net_spin = QSpinBox()
        self.target_net_spin.setRange(1, 50)

        self.max_spread_spin = QDoubleSpinBox()
        self.max_spread_spin.setRange(0.1, 200.0)
        self.max_spread_spin.setDecimals(2)

        self.min_tick_rate_spin = QSpinBox()
        self.min_tick_rate_spin.setRange(1, 200)

        self.detect_timeout_spin = QSpinBox()
        self.detect_timeout_spin.setRange(100, 5000)
        self.detect_timeout_spin.setSingleStep(50)

        self.impulse_min_spin = QDoubleSpinBox()
        self.impulse_min_spin.setRange(0.0, 50.0)
        self.impulse_min_spin.setDecimals(1)
        self.impulse_min_spin.setSingleStep(0.1)

        self.winner_threshold_spin = QDoubleSpinBox()
        self.winner_threshold_spin.setRange(0.0, 50.0)
        self.winner_threshold_spin.setDecimals(1)
        self.winner_threshold_spin.setSingleStep(0.1)

        self.emergency_stop_spin = QSpinBox()
        self.emergency_stop_spin.setRange(1, 200)

        self.cooldown_spin = QSpinBox()
        self.cooldown_spin.setRange(0, 60)

        self.direction_window_combo = QComboBox()
        self.direction_window_combo.addItems([str(value) for value in range(1, 21)])

        self.burst_volume_spin = QDoubleSpinBox()
        self.burst_volume_spin.setRange(0.0, 1_000_000.0)
        self.burst_volume_spin.setDecimals(2)

        self.auto_loop_checkbox = QCheckBox("Авто-цикл")

        form.addRow("Режим ордера", self.order_mode_combo)
        form.addRow("Номинал USD", self.usd_notional_spin)
        form.addRow("Плечо max", self.leverage_max_spin)
        form.addRow("Slip bps", self.slip_bps_spin)
        form.addRow("Макс. убыток (bps)", self.max_loss_spin)
        form.addRow("Комиссия всего (bps)", self.fee_spin)
        form.addRow("Цель net (bps)", self.target_net_spin)
        form.addRow("Макс. спред (bps)", self.max_spread_spin)
        form.addRow("Мин. тик-рейт (в сек)", self.min_tick_rate_spin)
        form.addRow("Таймаут детекта (мс)", self.detect_timeout_spin)
        form.addRow("Импульс min (bps)", self.impulse_min_spin)
        form.addRow("Порог победителя (bps)", self.winner_threshold_spin)
        form.addRow("Аварийный стоп (bps)", self.emergency_stop_spin)
        form.addRow("Cooldown (сек)", self.cooldown_spin)
        form.addRow("Окно детекта (тик)", self.direction_window_combo)
        form.addRow("Фильтр объема", self.burst_volume_spin)
        form.addRow("", self.auto_loop_checkbox)

        layout.addLayout(form)

        btn_layout = QHBoxLayout()
        self.apply_params_button = QPushButton("ПРИМЕНИТЬ")
        self.reset_params_button = QPushButton("СБРОСИТЬ")
        btn_layout.addStretch(1)
        btn_layout.addWidget(self.apply_params_button)
        btn_layout.addWidget(self.reset_params_button)
        layout.addLayout(btn_layout)
        layout.addStretch(1)
