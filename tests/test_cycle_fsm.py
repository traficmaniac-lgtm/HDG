from __future__ import annotations

from typing import Any, Optional

from src.core.models import StrategyParams, SymbolFilters
from src.core.state_machine import BotState, BotStateMachine
from src.engine.directional_cycle import DirectionalCycle
from src.services.market_data import MarketDataService


class DummyExecution:
    def __init__(self) -> None:
        self.symbol = "BTCUSDT"
        self.orders: list[dict[str, Any]] = []
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[int] = None

    def get_open_orders(self) -> list[dict[str, Any]]:
        return []

    def get_margin_account(self) -> dict[str, Any]:
        return {
            "userAssets": [
                {"asset": "BTC", "free": "0", "borrowed": "0", "netAsset": "0"},
                {"asset": "USDT", "free": "0", "borrowed": "0", "netAsset": "0"},
            ]
        }

    def get_spot_account(self) -> dict[str, Any]:
        return {"balances": [{"asset": "USDT", "free": "0"}]}

    def place_order(
        self,
        side: str,
        quantity: float,
        order_mode: str,
        price: Optional[float] = None,
        side_effect_type: Optional[str] = None,
        time_in_force: Optional[str] = None,
        is_isolated: bool = False,
        client_order_id: Optional[str] = None,
    ) -> dict[str, Any]:
        order = {
            "orderId": len(self.orders) + 1,
            "side": side,
            "status": "FILLED",
            "executedQty": quantity,
            "price": price or 0.0,
            "avgPrice": price or 0.0,
            "clientOrderId": client_order_id,
        }
        self.orders.append(order)
        return order

    def wait_for_fill(
        self, order_id: Optional[int], timeout_s: float = 3.0
    ) -> Optional[tuple[float, float, dict[str, Any]]]:
        order = next((item for item in self.orders if item["orderId"] == order_id), None)
        if not order:
            return None
        return float(order.get("avgPrice", 0.0)), float(order.get("executedQty", 0.0)), order

    def cancel_open_orders(self) -> None:
        return None

    def cancel_order(self, order_id: int) -> Optional[dict[str, Any]]:
        return {"orderId": order_id, "status": "CANCELED"}

    def get_order(self, order_id: int) -> Optional[dict[str, Any]]:
        return next((item for item in self.orders if item["orderId"] == order_id), None)

    def repay_asset(self, asset: str, amount: float) -> Optional[dict[str, Any]]:
        return {"asset": asset, "amount": amount}


def build_cycle(state_machine: BotStateMachine) -> DirectionalCycle:
    market_data = MarketDataService()
    filters = SymbolFilters(step_size=0.000001, min_qty=0.000001, tick_size=0.01)
    logs: list[dict[str, Any]] = []

    def emit_log(*_args: Any, **_kwargs: Any) -> None:
        logs.append({"args": _args, "kwargs": _kwargs})

    cycle = DirectionalCycle(
        execution=DummyExecution(),
        state_machine=state_machine,
        strategy=StrategyParams(),
        symbol_filters=filters,
        market_data=market_data,
        emit_log=emit_log,
        emit_trade_row=lambda *_args, **_kwargs: None,
        emit_exposure=lambda *_args, **_kwargs: None,
        on_not_authorized=lambda *_args, **_kwargs: None,
    )
    return cycle


def test_single_flight_blocks_reentry() -> None:
    state_machine = BotStateMachine()
    state_machine.state = BotState.ARMED
    state_machine.active_cycle = True
    cycle = build_cycle(state_machine)

    cycle.attempt_entry()

    assert state_machine.cycle_id == 0
    assert state_machine.state == BotState.ARMED


def test_armed_to_entering_once() -> None:
    state_machine = BotStateMachine()

    assert state_machine.start_cycle() is False

    state_machine.state = BotState.ARMED
    assert state_machine.start_cycle() is True
    assert state_machine.state == BotState.ENTERING
    assert state_machine.active_cycle is True
    assert state_machine.cycle_id == 1
    assert state_machine.start_cycle() is False


def test_cycle_id_tagging() -> None:
    state_machine = BotStateMachine()
    state_machine.cycle_id = 12
    cycle = build_cycle(state_machine)

    assert cycle._build_client_order_id("LONG", "ENTRY") == "DHS-BTCUSDT-12-LONG-ENTRY"


def test_cooldown_requires_flat(monkeypatch) -> None:
    state_machine = BotStateMachine()
    state_machine.state = BotState.COOLDOWN
    state_machine.active_cycle = True
    cycle = build_cycle(state_machine)
    called = {"flatten": False}

    def fake_flatten(reason: str) -> bool:
        called["flatten"] = True
        return False

    monkeypatch.setattr(cycle, "_is_flat", lambda force=False: False)
    monkeypatch.setattr(cycle, "_controlled_flatten", fake_flatten)

    cycle.end_cooldown(auto_resume=True)

    assert called["flatten"] is True
    assert state_machine.state == BotState.COOLDOWN
    assert state_machine.active_cycle is True
