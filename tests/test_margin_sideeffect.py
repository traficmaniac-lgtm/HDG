from __future__ import annotations

from typing import Any, Optional

from src.exchange.binance_margin import BinanceMarginExecution


class DummyMarginClient:
    def __init__(self) -> None:
        self.last_payload: Optional[dict[str, Any]] = None
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[int] = None

    def place_order_margin(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.last_payload = payload
        return {"orderId": 123, **payload}


def test_margin_order_defaults_side_effect_type() -> None:
    client = DummyMarginClient()
    execution = BinanceMarginExecution(client, symbol="BTCUSDT")

    execution.place_order("SELL", 0.1, "market")

    assert client.last_payload is not None
    assert client.last_payload["sideEffectType"] == "AUTO_BORROW_REPAY"


def test_margin_order_preserves_valid_side_effect_type() -> None:
    client = DummyMarginClient()
    execution = BinanceMarginExecution(client, symbol="BTCUSDT")

    execution.place_order("BUY", 0.2, "market", side_effect_type="AUTO_REPAY")

    assert client.last_payload is not None
    assert client.last_payload["sideEffectType"] == "AUTO_REPAY"
