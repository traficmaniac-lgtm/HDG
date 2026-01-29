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
        self._local_settings_path = self._settings_path.with_name("settings.local.json")

    @property
    def settings_path(self) -> Path:
        return self._settings_path

    def normalize_settings(self, payload: dict) -> dict:
        cleaned, removed_keys = self._sanitize_settings(payload)
        self._log_removed_keys(removed_keys)
        return cleaned

    def _sanitize_settings(self, payload: dict) -> tuple[dict, list[str]]:
        if not isinstance(payload, dict):
            return {}, []
        removed_keys: list[str] = []

        def scrub(obj: dict, prefix: str = "") -> dict:
            cleaned: dict = {}
            for key, value in obj.items():
                key_str = str(key)
                key_lower = key_str.lower()
                path_key = f"{prefix}.{key_str}" if prefix else key_str
                if self._is_deprecated_key(key_lower):
                    removed_keys.append(path_key)
                    continue
                if isinstance(value, dict):
                    cleaned[key] = scrub(value, path_key)
                else:
                    cleaned[key] = value
            return cleaned

        return scrub(payload), removed_keys

    @staticmethod
    def _is_deprecated_key(key_lower: str) -> bool:
        if "ttl" in key_lower or "offset" in key_lower:
            return True
        if key_lower in {"max_exit_total_ms", "max_entry_total_ms"}:
            return True
        if key_lower.startswith("max_") and key_lower.endswith("_total_ms"):
            return True
        return False

    @staticmethod
    def _log_removed_keys(removed_keys: list[str]) -> None:
        for key in removed_keys:
            print(f"[MIGRATE_DROP] key={key}")

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
        payload = self.load_local_settings()
        api_payload = payload.get("api", {}) if isinstance(payload, dict) else {}
        return ApiCredentials(
            key=str(api_payload.get("key", "") or ""),
            secret=str(api_payload.get("secret", "") or ""),
        )

    def save_api_credentials(self, key: str, secret: str) -> None:
        payload = self.load_local_settings()
        payload["api"] = {"key": key, "secret": secret}
        payload.setdefault("api_storage", {"source": "settings.local.json"})
        self.save_local_settings(payload)

    def load_local_settings(self) -> dict:
        try:
            with self._local_settings_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
                return self.normalize_settings(payload)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            return {}

    def save_local_settings(self, payload: dict) -> None:
        payload = self.normalize_settings(payload)
        with self._local_settings_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
