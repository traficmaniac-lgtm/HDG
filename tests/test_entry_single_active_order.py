from __future__ import annotations

import time

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services.trade_executor import TradeExecutor, TradeState


class DummyRest:
    def __init__(self) -> None:
        self.status_by_id: dict[int, str] = {}

    def get_margin_order(self, symbol: str, order_id: int) -> dict:
        return {"symbol": symbol, "orderId": order_id, "status": self.status_by_id.get(order_id, "CANCELED")}


class SequenceRouter:
    def __init__(self, bid: float, tick: float) -> None:
        self.bid = bid
        self.tick = tick

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        ask = self.bid + self.tick
        mid = (self.bid + ask) / 2.0
        return (
            PriceState(
                bid=self.bid,
                ask=ask,
                mid=mid,
                source="HTTP",
                mid_age_ms=10,
                data_blind=False,
            ),
            HealthState(ws_connected=False, ws_age_ms=99999, http_age_ms=10),
        )

    def get_ws_issue_counts(self) -> tuple[int, int]:
        return (0, 0)


def make_settings() -> Settings:
    return Settings(
        symbol="TESTUSDT",
        ws_fresh_ms=250,
        ws_stale_ms=1000,
        http_fresh_ms=500,
        http_poll_ms=500,
        ui_refresh_ms=250,
        ws_log_throttle_ms=1000,
        ws_reconnect_dedup_ms=1000,
        order_poll_ms=500,
        ws_switch_hysteresis_ms=500,
        min_source_hold_ms=500,
        ws_stable_required_ms=1000,
        ws_stale_grace_ms=500,
        entry_max_age_ms=1000,
        exit_max_age_ms=2500,
        tp_max_age_ms=400,
        settlement_grace_ms=400,
        repricing_cooldown_ms=75,
        max_wait_price_ms=2000,
        price_wait_log_every_ms=500,
        tp_cross_after_ms=900,
        inflight_deadline_ms=2500,
        watchdog_poll_ms=250,
        max_state_stuck_ms=8000,
        max_no_progress_ms=5000,
        max_reconcile_retries=3,
        sell_refresh_grace_ms=400,
        epsilon_qty=1e-6,
        position_guard_http=False,
        entry_mode="FOLLOW_TOP",
        account_mode="MARGIN",
        leverage_hint=1,
        entry_reprice_min_ticks=1,
        entry_reprice_cooldown_ms=0,
        entry_reprice_require_stable_source=False,
        entry_reprice_stable_source_grace_ms=0,
        entry_reprice_min_consecutive_fresh_reads=1,
        take_profit_ticks=1,
        stop_loss_ticks=1,
        order_type="LIMIT",
        exit_order_type="LIMIT",
        max_buy_retries=0,
        allow_borrow=False,
        side_effect_type="AUTO_REPAY",
        margin_isolated=True,
        auto_exit_enabled=True,
        max_sell_retries=0,
        max_wait_sell_ms=0,
        allow_force_close=False,
        cycle_count=1,
        order_quote=10.0,
        max_budget=100.0,
        budget_reserve=0.0,
    )


def test_entry_single_active_order() -> None:
    router = SequenceRouter(bid=1.2000, tick=0.0001)
    rest = DummyRest()
    profile = SymbolProfile(tick_size=0.0001, step_size=0.0001, min_qty=0.0001, min_notional=0.0)
    executor = TradeExecutor(
        rest=rest,
        router=router,
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg, **_kwargs: None,
    )
    executor.state = TradeState.STATE_WAIT_BUY
    executor.active_test_orders = [
        {
            "orderId": 101,
            "side": "BUY",
            "price": 1.2000,
            "bid_at_place": 1.2000,
            "qty": 10.0,
            "cum_qty": 0.0,
            "avg_fill_price": 0.0,
            "last_fill_price": None,
            "created_ts": 0,
            "updated_ts": 0,
            "status": "NEW",
            "tag": executor.TAG,
            "clientOrderId": "test",
            "cycle_id": 1,
        }
    ]
    executor.entry_active_order_id = 101
    executor.entry_active_price = 1.2000
    executor._entry_last_ref_bid = 1.2000

    actions: list[str] = []
    next_order_id = 101

    def fake_cancel(symbol: str, order_id: int, is_isolated: str, tag: str) -> bool:
        actions.append(f"cancel:{order_id}")
        rest.status_by_id[order_id] = "CANCELED"
        return True

    def fake_place(*_args, **_kwargs) -> dict:
        nonlocal next_order_id
        next_order_id += 1
        actions.append(f"place:{next_order_id}")
        return {"orderId": next_order_id, "clientOrderId": f"client-{next_order_id}"}

    executor._cancel_margin_order = fake_cancel  # type: ignore[assignment]
    executor._place_margin_order = fake_place  # type: ignore[assignment]

    for idx in range(3):
        router.bid = 1.2000 + (idx + 1) * profile.tick_size
        executor._entry_reprice_last_ts = time.monotonic() - 10.0
        current_order_id = executor.active_test_orders[0]["orderId"]
        executor._handle_entry_follow_top({current_order_id: {"status": "NEW"}})
        assert len(executor.active_test_orders) == 1
        assert executor.active_test_orders[0]["orderId"] != current_order_id

    assert actions == [
        "cancel:101",
        "place:102",
        "cancel:102",
        "place:103",
        "cancel:103",
        "place:104",
    ]
