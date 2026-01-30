from __future__ import annotations

import logging
from pathlib import Path

INFO_EVENT_LEVEL = logging.INFO + 5
logging.addLevelName(INFO_EVENT_LEVEL, "INFO_EVENT")


def _log_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    path = root / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_logger() -> logging.Logger:
    logger = logging.getLogger("hdg")
    if getattr(logger, "_hdg_configured", False):
        return logger
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    log_dir = _log_dir()
    app_handler = logging.FileHandler(log_dir / "app.log", encoding="utf-8")
    app_handler.setLevel(logging.INFO)
    debug_handler = logging.FileHandler(log_dir / "debug.log", encoding="utf-8")
    debug_handler.setLevel(logging.DEBUG)
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
