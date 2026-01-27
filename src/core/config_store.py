from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from src.core.models import ConnectionMode, ConnectionSettings, StrategyParams


class SettingsStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or Path("config/settings.json")
        self.env_path = Path(".env.local")

    def load(self) -> ConnectionSettings:
        if self.path.exists():
            return self._load_from_json(self.path)
        if self.env_path.exists():
            return self._load_from_env(self.env_path)
        return ConnectionSettings()

    def save(self, settings: ConnectionSettings) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(settings)
        payload["mode"] = settings.mode.value
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _load_from_json(self, path: Path) -> ConnectionSettings:
        data = json.loads(path.read_text(encoding="utf-8"))
        return self._settings_from_payload(data)

    def _load_from_env(self, path: Path) -> ConnectionSettings:
        data: dict[str, Any] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip() or "=" not in line:
                continue
            key, value = line.split("=", 1)
            data[key.strip()] = value.strip()
        payload = {
            "api_key": data.get("BINANCE_API_KEY", ""),
            "api_secret": data.get("BINANCE_API_SECRET", ""),
            "mode": data.get("BINANCE_MODE", ConnectionMode.MARGIN.value),
            "leverage": int(data.get("BINANCE_LEVERAGE", "1")),
            "save_local": True,
            "live_enabled": data.get("BINANCE_LIVE_ENABLED", "false").lower() == "true",
        }
        return self._settings_from_payload(payload)

    def _settings_from_payload(self, payload: dict[str, Any]) -> ConnectionSettings:
        mode_raw = str(payload.get("mode", ConnectionMode.MARGIN.value))
        if mode_raw in ConnectionMode._value2member_map_:
            mode = ConnectionMode(mode_raw)
        else:
            mode = ConnectionMode.MARGIN
        return ConnectionSettings(
            api_key=str(payload.get("api_key", "")),
            api_secret=str(payload.get("api_secret", "")),
            mode=mode,
            leverage=int(payload.get("leverage", 1)),
            save_local=bool(payload.get("save_local", True)),
            live_enabled=bool(payload.get("live_enabled", False)),
        )


class StrategyParamsStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or Path("config/strategy_params.json")

    def load_strategy_params(self) -> tuple[StrategyParams, str]:
        if not self.path.exists():
            return StrategyParams(), "BTCUSDT"
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if "strategy_params" in data:
            params = StrategyParams(**data.get("strategy_params", {}))
            symbol = str(data.get("symbol", "BTCUSDT"))
            return params, symbol
        symbol = str(data.get("last_symbol", "BTCUSDT"))
        default_payload = data.get("default", {})
        by_symbol = data.get("by_symbol", {})
        symbol_payload = by_symbol.get(symbol, default_payload)
        params = StrategyParams(**symbol_payload)
        return params, symbol

    def save_strategy_params(self, params: StrategyParams, symbol: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existing: dict[str, Any] = {}
        if self.path.exists():
            existing = json.loads(self.path.read_text(encoding="utf-8"))
        by_symbol = existing.get("by_symbol", {})
        by_symbol[symbol] = asdict(params)
        payload = {
            "default": asdict(params),
            "by_symbol": by_symbol,
            "last_symbol": symbol,
        }
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
