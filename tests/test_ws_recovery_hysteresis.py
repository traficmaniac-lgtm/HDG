from __future__ import annotations

from src.services import market_data as market_data_module
from src.services.market_data import MarketDataService


def _set_time(monkeypatch, now_ms: float) -> None:
    monkeypatch.setattr(market_data_module.time, "monotonic", lambda: now_ms / 1000)


def _make_ws_tick(symbol: str, rx_time_ms: float) -> dict:
    return {
        "symbol": symbol,
        "bid": 1.0,
        "ask": 1.1,
        "mid": 1.05,
        "spread_bps": 100.0,
        "bid_raw": 1.0,
        "ask_raw": 1.1,
        "mid_raw": 1.05,
        "spread_bps_raw": 100.0,
        "rx_time_ms": rx_time_ms,
        "source": "WS",
    }


def _make_http_tick(symbol: str, rx_time_ms: float) -> dict:
    return {
        "symbol": symbol,
        "bid": 1.0,
        "ask": 1.1,
        "mid": 1.05,
        "spread_bps": 100.0,
        "rx_time_ms": rx_time_ms,
        "source": "HTTP",
    }


def test_ws_recovery_hysteresis(monkeypatch) -> None:
    service = MarketDataService()
    symbol = "EURIUSDT"

    now_ms = 0.0
    _set_time(monkeypatch, now_ms)
    service.set_ws_connected(True)

    for offset in [1000, 1100, 1200, 1300, 1400]:
        now_ms = float(offset)
        _set_time(monkeypatch, now_ms)
        service.update_tick(_make_ws_tick(symbol, now_ms))

    assert service.get_snapshot().effective_source == "WS"

    now_ms = 2500.0
    _set_time(monkeypatch, now_ms)
    service.update_tick(_make_http_tick(symbol, now_ms))
    assert service.get_snapshot().effective_source == "HTTP"

    for offset in [3000, 3100, 3200, 3300, 3400]:
        now_ms = float(offset)
        _set_time(monkeypatch, now_ms)
        service.update_tick(_make_ws_tick(symbol, now_ms))
    assert service.get_snapshot().effective_source == "HTTP"

    for offset in [5600, 5700, 5800, 5900, 6000]:
        now_ms = float(offset)
        _set_time(monkeypatch, now_ms)
        service.update_tick(_make_ws_tick(symbol, now_ms))

    assert service.get_snapshot().effective_source == "WS"
