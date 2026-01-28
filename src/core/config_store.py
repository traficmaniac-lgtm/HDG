from __future__ import annotations

import json
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

    def load_settings(self) -> dict:
        with self._settings_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save_settings(self, payload: dict) -> None:
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
