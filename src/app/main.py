import sys

from PySide6.QtWidgets import QApplication

from src.core.config_store import ConfigStore
from src.core.logger import configure_log_session
from src.core.version import VERSION
from src.gui.main_window import MainWindow


def main() -> int:
    config_store = ConfigStore()
    settings = config_store.load_settings()
    symbol = settings.get("symbol", "EURIUSDT")
    mode = settings.get("entry_mode", "NORMAL")
    configure_log_session(VERSION, symbol, mode)
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
