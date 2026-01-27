from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QObject, Signal


LOG_FORMAT = "%(asctime)s.%(msecs)03d | %(levelname)s | %(module)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class LogEntry:
    timestamp: datetime
    category: str
    level: str
    message: str
    fields: dict[str, object]


class QtLogEmitter(QObject):
    message = Signal(str)


class LogBus(QObject):
    entry = Signal(dict)

    def __init__(self, logger: logging.Logger) -> None:
        super().__init__()
        self._logger = logger

    def log(self, category: str, level: str, message: str, **fields: object) -> None:
        normalized_level = level.upper()
        entry = LogEntry(
            timestamp=datetime.now(timezone.utc),
            category=category.upper(),
            level=normalized_level,
            message=message,
            fields=fields,
        )
        self._logger.log(self._level_to_number(normalized_level), self._format_entry(entry))
        self.entry.emit(
            {
                "timestamp": entry.timestamp,
                "category": entry.category,
                "level": entry.level,
                "message": entry.message,
                "fields": entry.fields,
            }
        )

    @staticmethod
    def _level_to_number(level: str) -> int:
        return {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARN": logging.WARNING,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }.get(level.upper(), logging.INFO)

    @staticmethod
    def _format_entry(entry: LogEntry) -> str:
        fields = " ".join(f"{key}={value}" for key, value in entry.fields.items())
        if fields:
            return f"{entry.category} | {entry.message} | {fields}"
        return f"{entry.category} | {entry.message}"


class QtLogHandler(logging.Handler):
    def __init__(self, emitter: QtLogEmitter) -> None:
        super().__init__()
        self.emitter = emitter

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        self.emitter.message.emit(msg)


def setup_logger(log_dir: Path, emitter: Optional[QtLogEmitter] = None) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("dhs")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    file_handler = RotatingFileHandler(
        log_dir / "bot.log", maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if emitter is not None:
        qt_handler = QtLogHandler(emitter)
        qt_handler.setFormatter(formatter)
        logger.addHandler(qt_handler)

    logger.propagate = False
    return logger
