from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)


class SettingsTab(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        mode_layout = QHBoxLayout()
        mode_layout.addWidget(QLabel("Режим"))
        self.mode_label = QLabel("Кросс-маржа")
        self.mode_label.setStyleSheet("font-weight: 600;")
        mode_layout.addWidget(self.mode_label)
        mode_layout.addStretch(1)

        leverage_layout = QHBoxLayout()
        self.leverage_combo = QComboBox()
        self.leverage_combo.addItems(["1x", "2x", "3x"])
        leverage_layout.addWidget(QLabel("Плечо (цель): 1x–3x (через займ)"))
        leverage_layout.addWidget(self.leverage_combo)
        leverage_layout.addStretch(1)

        api_form = QFormLayout()
        api_form.setLabelAlignment(Qt.AlignRight)
        self.api_key_input = QLineEdit()
        self.api_key_input.setPlaceholderText("API ключ")
        self.api_key_input.setMinimumWidth(360)
        self.api_key_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.api_secret_input = QLineEdit()
        self.api_secret_input.setPlaceholderText("API секрет")
        self.api_secret_input.setEchoMode(QLineEdit.Password)
        self.api_secret_input.setMinimumWidth(360)
        self.api_secret_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        api_form.addRow("API ключ", self.api_key_input)
        api_form.addRow("API секрет", self.api_secret_input)

        self.save_checkbox = QCheckBox("Сохранять локально")
        self.save_checkbox.setChecked(True)

        self.local_note = QLabel("Хранится только локально")
        self.local_note.setStyleSheet("color: #8c8c8c;")

        self.live_enabled_checkbox = QCheckBox("РЕАЛЬНАЯ ТОРГОВЛЯ ВКЛ.")
        self.live_enabled_checkbox.setChecked(False)
        self.live_warning = QLabel("Отправляет реальные ордера")
        self.live_warning.setStyleSheet("color: #e74c3c; font-size: 11px;")

        connect_layout = QHBoxLayout()
        self.connect_button = QPushButton("ПОДКЛЮЧИТЬ")
        self.disconnect_button = QPushButton("ОТКЛЮЧИТЬ")
        connect_layout.addWidget(self.connect_button)
        connect_layout.addWidget(self.disconnect_button)
        connect_layout.addStretch(1)

        action_layout = QHBoxLayout()
        self.save_button = QPushButton("СОХРАНИТЬ")
        self.test_button = QPushButton("ПРОВЕРИТЬ СВЯЗЬ")
        self.clear_button = QPushButton("ОЧИСТИТЬ")
        action_layout.addWidget(self.save_button)
        action_layout.addWidget(self.test_button)
        action_layout.addWidget(self.clear_button)
        action_layout.addStretch(1)

        layout.addLayout(mode_layout)
        layout.addLayout(leverage_layout)
        layout.addLayout(api_form)
        layout.addWidget(self.save_checkbox)
        layout.addWidget(self.local_note)
        layout.addWidget(self.live_enabled_checkbox)
        layout.addWidget(self.live_warning)
        layout.addLayout(action_layout)
        layout.addLayout(connect_layout)
        layout.addStretch(1)
