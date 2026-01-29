from __future__ import annotations

import json
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ApiCredentials:
    key: str = ""
    secret: str = ""


class ConfigStore:
    def __init__(self, settings_path: Optional[Path] = None) -> None:
        self._settings_path = settings_path or (
            Path(__file__).resolve().parents[2] / "config" / "settings.json"
        )

    @property
    def settings_path(self) -> Path:
        return self._settings_path

    def normalize_settings(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            return {}
        cleaned = dict(payload)
        deprecated_keys = {
            "entry_offset_ticks",
            "exit_offset_ticks",
            "offset_ticks",
            "test_tick_offset",
            "aggressive_offset_ticks",
            "sl_offset_ticks",
            "max_entry_total_ms",
            "buy_ttl_ms",
            "sell_ttl_ms",
        }
        for key in deprecated_keys:
            cleaned.pop(key, None)
        for key in list(cleaned.keys()):
            key_lower = key.lower()
            if "ttl" in key_lower or "offset" in key_lower:
                cleaned.pop(key, None)
        return cleaned

    def load_settings(self) -> dict:
        try:
            with self._settings_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
                return self.normalize_settings(payload)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            self._backup_corrupt_settings()
            return {}

    def _backup_corrupt_settings(self) -> None:
        if not self._settings_path.exists():
            return
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = self._settings_path.with_name(
            f"{self._settings_path.stem}.invalid-{timestamp}{self._settings_path.suffix}"
        )
        try:
            content = self._settings_path.read_text(encoding="utf-8", errors="replace")
            backup_path.write_text(content, encoding="utf-8")
        except OSError:
            return

    def save_settings(self, payload: dict) -> None:
        payload = self.normalize_settings(payload)
        with self._settings_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")

    def load_api_credentials(self) -> ApiCredentials:
        payload = self.load_settings()
        api_payload = payload.get("api", {}) if isinstance(payload, dict) else {}
        return ApiCredentials(
            key=str(api_payload.get("key", "") or ""),
            secret=str(api_payload.get("secret", "") or ""),
        )

    def save_api_credentials(self, key: str, secret: str) -> None:
        payload = self.load_settings()
        payload["api"] = {"key": key, "secret": secret}
        payload.setdefault("api_storage", {"source": "settings.json"})
        self.save_settings(payload)
