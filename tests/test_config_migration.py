from __future__ import annotations

import json

from src.core.config_store import ConfigStore


def test_sanitize_removes_deprecated_keys(tmp_path) -> None:
    store = ConfigStore(settings_path=tmp_path / "settings.json")
    payload = {
        "buy_ttl_ms": 100,
        "entry_offset_ticks": 2,
        "max_exit_total_ms": 120000,
        "max_custom_total_ms": 5000,
        "nested": {
            "sell_ttl_ms": 200,
            "sl_offset_ticks": 1,
            "max_entry_total_ms": 3000,
            "keep_me": 7,
        },
        "keep_root": True,
    }

    cleaned = store.normalize_settings(payload)

    assert "buy_ttl_ms" not in cleaned
    assert "entry_offset_ticks" not in cleaned
    assert "max_exit_total_ms" not in cleaned
    assert "max_custom_total_ms" not in cleaned
    assert "keep_root" in cleaned
    assert "nested" in cleaned
    assert "sell_ttl_ms" not in cleaned["nested"]
    assert "sl_offset_ticks" not in cleaned["nested"]
    assert "max_entry_total_ms" not in cleaned["nested"]
    assert cleaned["nested"]["keep_me"] == 7


def test_save_strips_deprecated_keys(tmp_path) -> None:
    store = ConfigStore(settings_path=tmp_path / "settings.json")
    payload = {
        "good_quote_ttl_ms": 3000,
        "offset_ticks": 1,
        "max_exit_total_ms": 120000,
        "keep": "ok",
    }
    store.save_settings(payload)

    content = (tmp_path / "settings.json").read_text(encoding="utf-8")
    assert "ttl" not in content.lower()
    assert "offset" not in content.lower()
    assert "max_exit_total_ms" not in content.lower()
    loaded = json.loads(content)
    assert loaded == {"keep": "ok"}
