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
    QRadioButton,
    QVBoxLayout,
    QWidget,
)


class SettingsTab(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        mode_layout = QHBoxLayout()
        self.mode_margin = QRadioButton("Spot Margin")
        self.mode_futures = QRadioButton("Futures")
        self.mode_margin.setChecked(True)
        mode_layout.addWidget(self.mode_margin)
        mode_layout.addWidget(self.mode_futures)
        mode_layout.addStretch(1)

        leverage_layout = QHBoxLayout()
        self.leverage_combo = QComboBox()
        self.leverage_combo.addItems(["1x", "2x", "3x"])
        self.leverage_combo.setEnabled(False)
        leverage_layout.addWidget(QLabel("Leverage"))
        leverage_layout.addWidget(self.leverage_combo)
        leverage_layout.addStretch(1)

        api_form = QFormLayout()
        api_form.setLabelAlignment(Qt.AlignRight)
        self.api_key_input = QLineEdit()
        self.api_key_input.setPlaceholderText("API Key")
        self.api_secret_input = QLineEdit()
        self.api_secret_input.setPlaceholderText("API Secret")
        self.api_secret_input.setEchoMode(QLineEdit.Password)
        api_form.addRow("API Key", self.api_key_input)
        api_form.addRow("API Secret", self.api_secret_input)

        self.save_checkbox = QCheckBox("Сохранять локально")
        self.save_checkbox.setChecked(True)

        self.local_note = QLabel("Хранится только локально")
        self.local_note.setStyleSheet("color: #8c8c8c;")

        connect_layout = QHBoxLayout()
        self.connect_button = QPushButton("CONNECT")
        self.disconnect_button = QPushButton("DISCONNECT")
        connect_layout.addWidget(self.connect_button)
        connect_layout.addWidget(self.disconnect_button)
        connect_layout.addStretch(1)

        action_layout = QHBoxLayout()
        self.save_button = QPushButton("SAVE")
        self.test_button = QPushButton("TEST CONNECTION")
        self.clear_button = QPushButton("CLEAR")
        action_layout.addWidget(self.save_button)
        action_layout.addWidget(self.test_button)
        action_layout.addWidget(self.clear_button)
        action_layout.addStretch(1)

        layout.addLayout(mode_layout)
        layout.addLayout(leverage_layout)
        layout.addLayout(api_form)
        layout.addWidget(self.save_checkbox)
        layout.addWidget(self.local_note)
        layout.addLayout(action_layout)
        layout.addLayout(connect_layout)
        layout.addStretch(1)
