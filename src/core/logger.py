from __future__ import annotations

import gzip
import json
import logging
import logging.handlers
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

INFO_EVENT_LEVEL = logging.INFO + 5
logging.addLevelName(INFO_EVENT_LEVEL, "INFO_EVENT")


_SESSION_DIR: Optional[Path] = None
_SESSION_CONTEXT: dict[str, str] = {
    "version": "unknown",
    "symbol": "UNKNOWN",
    "mode": "UNKNOWN",
}


def configure_log_session(version: str, symbol: str, mode: str) -> Path:
    _SESSION_CONTEXT.update(
        {"version": str(version), "symbol": str(symbol), "mode": str(mode)}
    )
    return _ensure_session_dir()


def _log_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    path = root / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _session_root() -> Path:
    root = _log_dir()
    path = root / "sessions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _ensure_session_dir() -> Path:
    global _SESSION_DIR
    if _SESSION_DIR is not None:
        return _SESSION_DIR
    session_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"
    session_dir = _session_root() / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    session_meta = {
        "version": _SESSION_CONTEXT["version"],
        "symbol": _SESSION_CONTEXT["symbol"],
        "mode": _SESSION_CONTEXT["mode"],
        "start_time": datetime.now().isoformat(timespec="seconds"),
    }
    with (session_dir / "session.json").open("w", encoding="utf-8") as handle:
        json.dump(session_meta, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    _SESSION_DIR = session_dir
    return session_dir


def _gzip_namer(name: str) -> str:
    return f"{name}.gz"


def _gzip_rotator(source: str, dest: str) -> None:
    with open(source, "rb") as source_handle, gzip.open(dest, "wb") as dest_handle:
        shutil.copyfileobj(source_handle, dest_handle)
    os.remove(source)


class BufferedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    def __init__(self, *args, flush_interval: int = 50, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._flush_interval = max(1, flush_interval)
        self._emit_count = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if self.stream is None:
                self.stream = self._open()
            if self.shouldRollover(record):
                self.doRollover()
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self._emit_count += 1
            if self._emit_count >= self._flush_interval:
                self._emit_count = 0
                self.flush()
        except Exception:
            self.handleError(record)


def get_logger() -> logging.Logger:
    logger = logging.getLogger("hdg")
    if getattr(logger, "_hdg_configured", False):
        return logger
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    log_dir = _ensure_session_dir()
    app_handler = BufferedRotatingFileHandler(
        log_dir / "app.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
        delay=True,
        flush_interval=50,
    )
    app_handler.setLevel(logging.INFO)
    app_handler.namer = _gzip_namer
    app_handler.rotator = _gzip_rotator
    debug_handler = BufferedRotatingFileHandler(
        log_dir / "debug.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
        delay=True,
        flush_interval=100,
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.namer = _gzip_namer
    debug_handler.rotator = _gzip_rotator
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    app_handler.setFormatter(formatter)
    debug_handler.setFormatter(formatter)
    logger.addHandler(app_handler)
    logger.addHandler(debug_handler)
    logger._hdg_configured = True
    return logger


def resolve_level(level: str) -> int:
    level_name = level.upper()
    if level_name == "INFO_EVENT":
        return INFO_EVENT_LEVEL
    return logging._nameToLevel.get(level_name, logging.INFO)
