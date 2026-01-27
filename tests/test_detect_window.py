from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from src.core.models import StrategyParams, SymbolFilters
from src.core.state_machine import BotState, BotStateMachine
from src.engine.directional_cycle import DirectionalCycle
from src.services.market_data import MarketDataService


class DummyExecution:
    symbol = "BTCUSDT"

    def get_margin_account(self) -> dict[str, Any]:
        return {"userAssets": [{"asset": "USDT", "free": "10000", "borrowed": "0"}]}

    def get_spot_account(self) -> dict[str, Any]:
        return {"balances": [{"asset": "USDT", "free": "10000"}]}

    def get_open_orders(self) -> list[dict[str, Any]]:
        return []


def _make_cycle(strategy: StrategyParams, market_data: MarketDataService) -> DirectionalCycle:
    return DirectionalCycle(
        execution=DummyExecution(),
        state_machine=BotStateMachine(),
        strategy=strategy,
        symbol_filters=SymbolFilters(),
        market_data=market_data,
        emit_log=lambda *args, **kwargs: None,
        emit_trade_row=lambda payload: None,
        emit_exposure=lambda exposure: None,
        on_not_authorized=lambda context: None,
    )


def _send_ws_tick(
    cycle: DirectionalCycle,
    market_data: MarketDataService,
    bid_raw: Decimal,
    ask_raw: Decimal,
    rx_time_ms: float,
) -> None:
    mid_raw = (bid_raw + ask_raw) / Decimal("2")
    spread_bps_raw = (ask_raw - bid_raw) / mid_raw * Decimal("10000")
    payload = {
        "bid": float(bid_raw),
        "ask": float(ask_raw),
        "mid": float(mid_raw),
        "spread_bps": float(spread_bps_raw),
        "bid_raw": bid_raw,
        "ask_raw": ask_raw,
        "mid_raw": mid_raw,
        "spread_bps_raw": spread_bps_raw,
        "rx_time_ms": rx_time_ms,
        "source": "WS",
    }
    market_data.update_tick(payload)
    cycle.update_tick(payload)


def test_detect_winner_long_from_rising_mid() -> None:
    market_data = MarketDataService()
    market_data.set_ws_connected(True)
    strategy = StrategyParams(
        min_tick_rate=1,
        winner_threshold_bps=0.1,
        detect_window_ticks=5,
        detect_timeout_ms=10_000,
    )
    cycle = _make_cycle(strategy, market_data)
    cycle._entry_mid = Decimal("100.0")
    cycle._detect_start_ts = time.monotonic()
    cycle._state_machine.state = BotState.DETECTING

    base_ms = time.monotonic() * 1000
    for idx in range(5):
        bid = Decimal("100.00") + Decimal(idx) * Decimal("0.01")
        ask = bid + Decimal("0.02")
        _send_ws_tick(cycle, market_data, bid, ask, base_ms + idx * 100)

    assert cycle._winner_side == "LONG"


def test_detect_buffer_accumulates_window_ticks() -> None:
    market_data = MarketDataService()
    market_data.set_ws_connected(True)
    strategy = StrategyParams(min_tick_rate=1, detect_window_ticks=5, detect_timeout_ms=10_000)
    cycle = _make_cycle(strategy, market_data)
    cycle._entry_mid = Decimal("100.0")
    cycle._detect_start_ts = time.monotonic()
    cycle._state_machine.state = BotState.DETECTING

    base_ms = time.monotonic() * 1000
    for idx in range(5):
        bid = Decimal("100.00") + Decimal(idx) * Decimal("0.005")
        ask = bid + Decimal("0.01")
        _send_ws_tick(cycle, market_data, bid, ask, base_ms + idx * 100)

    assert len(cycle._detect_mid_buffer) == cycle._detect_window_ticks
    mid_first = cycle._detect_mid_buffer[0]
    mid_last = cycle._detect_mid_buffer[-1]
    delta_bps = float((mid_last - mid_first) / cycle._entry_mid * Decimal("10000"))
    assert delta_bps != 0.0
