from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from src.core.models import StrategyParams, SymbolFilters
from src.core.state_machine import BotState, BotStateMachine
from src.engine.directional_cycle import DirectionalCycle


@dataclass
class TickState:
    mid: float
    bid: float
    ask: float


class FakeExecution:
    def __init__(self, price_provider) -> None:
        self._price_provider = price_provider
        self._orders: dict[int, dict[str, Any]] = {}
        self._next_id = 1
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[int] = None
        self._net_btc = 0.0

    def get_margin_account(self) -> dict[str, Any]:
        btc_free = max(self._net_btc, 0.0)
        btc_borrowed = max(-self._net_btc, 0.0)
        return {
            "userAssets": [
                {
                    "asset": "BTC",
                    "free": f"{btc_free:.6f}",
                    "borrowed": f"{btc_borrowed:.6f}",
                    "netAsset": f"{self._net_btc:.6f}",
                },
                {
                    "asset": "USDT",
                    "free": "0.000000",
                    "borrowed": "0.000000",
                    "netAsset": "0.000000",
                },
            ]
        }

    def get_spot_account(self) -> dict[str, Any]:
        return {
            "balances": [
                {
                    "asset": "USDT",
                    "free": "1000.000000",
                    "locked": "0.000000",
                }
            ]
        }

    def get_open_orders(self) -> list[dict[str, Any]]:
        return []

    def cancel_open_orders(self) -> None:
        return None

    def cancel_order(self, order_id: int) -> None:
        self._orders.pop(order_id, None)

    def repay_asset(self, asset: str, amount: float) -> None:
        if asset == "BTC":
            self._net_btc += amount

    def place_order(
        self,
        side: str,
        quantity: float,
        order_mode: str,
        price: Optional[float] = None,
        side_effect_type: Optional[str] = None,
        time_in_force: Optional[str] = None,
        is_isolated: bool = False,
    ) -> dict[str, Any]:
        order_id = self._next_id
        self._next_id += 1
        fill_price = price or self._price_provider()
        if side == "BUY":
            self._net_btc += quantity
        else:
            self._net_btc -= quantity
        order = {
            "orderId": order_id,
            "status": "FILLED",
            "executedQty": f"{quantity:.6f}",
            "cummulativeQuoteQty": f"{quantity * fill_price:.8f}",
            "avgPrice": f"{fill_price:.2f}",
            "side": side,
        }
        self._orders[order_id] = order
        return order

    def get_order(self, order_id: int) -> Optional[dict[str, Any]]:
        return self._orders.get(order_id)

    def wait_for_fill(self, order_id: Optional[int], timeout_s: float = 3.0):
        if not order_id:
            return None
        order = self._orders.get(order_id)
        if not order:
            return None
        avg_price = float(order.get("avgPrice", 0.0))
        executed_qty = float(order.get("executedQty", 0.0))
        return avg_price, executed_qty, order


def log_line(category: str, level: str, message: str, **fields: Any) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    line = f"{ts} | {category} | {level} | {message}"
    print(line)
    for key, value in fields.items():
        print(f"  {key}={value}")


def trade_row(payload: dict[str, Any]) -> None:
    ts = payload.get("ts")
    ts_str = ts.strftime("%H:%M:%S.%f")[:-3] if isinstance(ts, datetime) else "--"
    print(f"{ts_str} | DEALS | INFO | trade_row {payload}")


def run_cycle(
    cycle: DirectionalCycle,
    tick_state: TickState,
    success: bool,
    state_machine: BotStateMachine,
) -> None:
    base_mid = tick_state.mid
    idx = 0
    while idx < 20:
        drift = 0.0
        if success:
            drift = (idx + 1) * 0.00015 * base_mid
        elif idx == 0:
            drift = 0.00003 * base_mid
        tick_state.mid = base_mid + drift
        tick_state.bid = tick_state.mid - 1
        tick_state.ask = tick_state.mid + 1
        cycle.update_tick(
            {
                "bid": tick_state.bid,
                "ask": tick_state.ask,
                "mid": tick_state.mid,
                "spread_bps": 0.2,
                "event_time": datetime.now(timezone.utc),
                "rx_time": datetime.now(timezone.utc),
                "rx_time_ms": time.monotonic() * 1000,
                "source": "WS",
            }
        )
        cycle.attempt_entry()
        if state_machine.state == BotState.COOLDOWN:
            break
        time.sleep(0.05)
        idx += 1
    if not success and state_machine.state != BotState.COOLDOWN:
        time.sleep(0.3)
        cycle.attempt_entry()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycles", type=int, default=100)
    args = parser.parse_args()

    tick_state = TickState(mid=50000.0, bid=49999.0, ask=50001.0)
    execution = FakeExecution(lambda: tick_state.mid)
    state_machine = BotStateMachine()
    state_machine.arm()
    strategy = StrategyParams(
        order_mode="market",
        usd_notional=20.0,
        fee_total_bps=2.0,
        target_net_bps=4,
        max_spread_bps=5.0,
        min_tick_rate=1,
        detect_timeout_ms=200,
        impulse_min_bps=0.1,
        winner_threshold_bps=0.5,
        emergency_stop_bps=10,
        cooldown_s=2,
        detect_window_ticks=1,
        auto_loop=True,
    )
    filters = SymbolFilters(step_size=0.000001, min_qty=0.000001, min_notional=5.0, tick_size=0.1)
    cycle = DirectionalCycle(
        execution=execution,
        state_machine=state_machine,
        strategy=strategy,
        symbol_filters=filters,
        emit_log=log_line,
        emit_trade_row=trade_row,
        emit_exposure=lambda _: None,
        on_not_authorized=lambda _: None,
    )

    cycle.arm()
    for idx in range(args.cycles):
        success = idx % 2 == 0
        run_cycle(cycle, tick_state, success=success, state_machine=state_machine)
        cycle.end_cooldown(auto_resume=True)
        cycle.arm()


if __name__ == "__main__":
    main()
