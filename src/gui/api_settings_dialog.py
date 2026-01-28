from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from src.core.config_store import ApiCredentials, ConfigStore


class ApiSettingsDialog(QDialog):
    saved = Signal(str, str)

    def __init__(self, parent=None, store: ConfigStore | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Настройки API")
        self.setModal(True)
        self._store = store or ConfigStore()

        self._key_edit = QLineEdit()
        self._secret_edit = QLineEdit()
        self._secret_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self._toggle_secret_button = QPushButton("Показать")
        self._toggle_secret_button.setCheckable(True)
        self._toggle_secret_button.toggled.connect(self._toggle_secret)

        self._save_button = QPushButton("Сохранить")
        self._save_button.clicked.connect(self._on_save)
        self._cancel_button = QPushButton("Отмена")
        self._cancel_button.clicked.connect(self.reject)

        form = QFormLayout()
        form.addRow(QLabel("API Key"), self._key_edit)
        secret_row = QHBoxLayout()
        secret_row.addWidget(self._secret_edit, stretch=1)
        secret_row.addWidget(self._toggle_secret_button)
        form.addRow(QLabel("API Secret"), secret_row)

        buttons = QHBoxLayout()
        buttons.addStretch(1)
        buttons.addWidget(self._save_button)
        buttons.addWidget(self._cancel_button)

        root = QVBoxLayout(self)
        root.addLayout(form)
        root.addLayout(buttons)

        self._load_initial()

    def _load_initial(self) -> None:
        creds = self._store.load_api_credentials()
        self._key_edit.setText(creds.key)
        self._secret_edit.setText(creds.secret)

    def _toggle_secret(self, checked: bool) -> None:
        if checked:
            self._secret_edit.setEchoMode(QLineEdit.EchoMode.Normal)
            self._toggle_secret_button.setText("Скрыть")
        else:
            self._secret_edit.setEchoMode(QLineEdit.EchoMode.Password)
            self._toggle_secret_button.setText("Показать")

    def _on_save(self) -> None:
        key = self._sanitize(self._key_edit.text())
        secret = self._sanitize(self._secret_edit.text())

        if not self._is_valid(key) or not self._is_valid(secret):
            QMessageBox.warning(
                self,
                "Некорректный ключ",
                "API Key/Secret должны быть длиннее 20 символов и без пробелов.",
            )
            return

        self._store.save_api_credentials(key, secret)
        self.saved.emit(key, secret)
        self.accept()

    @staticmethod
    def _sanitize(value: str) -> str:
        value = value.strip()
        value = value.strip("'\"")
        return value

    @staticmethod
    def _is_valid(value: str) -> bool:
        if len(value) <= 20:
            return False
        return " " not in value

