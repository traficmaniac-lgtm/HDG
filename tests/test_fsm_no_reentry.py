from __future__ import annotations

from src.core.trade_engine import TradeEngine
from src.core.state_machine import BotState
from src.services.market_data import MarketDataService


def test_reentry_blocked_in_riding(monkeypatch) -> None:
    engine = TradeEngine(market_data=MarketDataService())
    blocked_logs: list[str] = []
    called = {"attempt": 0}

    def fake_log(_category: str, _level: str, message: str, **_fields: object) -> None:
        blocked_logs.append(message)

    def fake_attempt_entry() -> None:
        called["attempt"] += 1

    monkeypatch.setattr(engine, "_emit_log", fake_log)
    monkeypatch.setattr(engine._cycle, "attempt_entry", fake_attempt_entry)

    engine._connected = True
    engine._margin_permission_ok = True
    engine._auto_loop = True
    engine._stop_requested = False
    engine._state_machine.active_cycle = True
    engine._state_machine.state = BotState.RIDING
    engine._state_machine.cycle_id = 3

    engine.attempt_entry()

    assert called["attempt"] == 0
    assert "REENTRY_BLOCKED" in blocked_logs

    engine.close()
