from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from src.core.models import StrategyParams, SymbolFilters
from src.core.state_machine import BotStateMachine
from src.engine.directional_cycle import DirectionalCycle
from src.services.market_data import MarketDataService


class DummyExecution:
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


def test_impulse_bps_from_raw_mid() -> None:
    market_data = MarketDataService()
    market_data.set_ws_connected(True)
    strategy = StrategyParams(min_tick_rate=1)
    cycle = _make_cycle(strategy, market_data)

    base_ms = time.monotonic() * 1000
    _send_ws_tick(cycle, market_data, Decimal("60000.0"), Decimal("60000.1"), base_ms)
    _send_ws_tick(cycle, market_data, Decimal("60000.1"), Decimal("60000.2"), base_ms + 100)

    metrics = cycle._compute_entry_metrics()
    assert metrics["impulse_ready"] is True
    assert metrics["impulse_bps"] > 0


def test_tick_rate_counts_ws_messages() -> None:
    market_data = MarketDataService()
    market_data.set_ws_connected(True)
    strategy = StrategyParams(min_tick_rate=1)
    cycle = _make_cycle(strategy, market_data)

    base_ms = time.monotonic() * 1000
    for idx in range(5):
        _send_ws_tick(
            cycle,
            market_data,
            Decimal("60000.0"),
            Decimal("60000.1"),
            base_ms + idx * 150,
        )

    metrics = cycle._compute_entry_metrics()
    assert metrics["tick_rate"] == 5


def test_impulse_degrades_after_grace() -> None:
    market_data = MarketDataService()
    market_data.set_ws_connected(True)
    strategy = StrategyParams(
        min_tick_rate=1,
        max_spread_bps=100.0,
        use_impulse_filter=True,
        impulse_min_bps=10.0,
        impulse_grace_ms=500,
    )
    cycle = _make_cycle(strategy, market_data)
    cycle._armed_ts = time.monotonic() - 1.0

    base_ms = time.monotonic() * 1000
    _send_ws_tick(cycle, market_data, Decimal("60000.0"), Decimal("60000.1"), base_ms)

    assert cycle._entry_filter_reason() is None
