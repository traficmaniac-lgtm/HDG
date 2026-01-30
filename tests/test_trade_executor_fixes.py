from __future__ import annotations

import time

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services import trade_executor as trade_executor_module
from src.services.trade_executor import TradeExecutor, TradeState


class DummyRest:
    pass


class DummyRouter:
    def __init__(self, price_state: PriceState, health_state: HealthState) -> None:
        self._price_state = price_state
        self._health_state = health_state

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        return self._price_state, self._health_state

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
        tp_cross_after_ms=10,
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
        entry_reprice_cooldown_ms=1200,
        entry_reprice_require_stable_source=True,
        entry_reprice_stable_source_grace_ms=3000,
        entry_reprice_min_consecutive_fresh_reads=1,
        take_profit_ticks=2,
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


def make_executor(router: DummyRouter) -> TradeExecutor:
    profile = SymbolProfile(
        tick_size=0.0001,
        step_size=0.0001,
        min_qty=0.0001,
        min_notional=0.0,
    )
    return TradeExecutor(
        rest=DummyRest(),
        router=router,
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )


def test_tp_cross_not_before_armed() -> None:
    price_state = PriceState(
        bid=1.1988,
        ask=1.1989,
        mid=1.19885,
        source="HTTP",
        mid_age_ms=10,
        data_blind=False,
    )
    health_state = HealthState(ws_connected=False, ws_age_ms=99999, http_age_ms=10)
    executor = make_executor(DummyRouter(price_state, health_state))

    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.1988,
            "qty": 1.0,
            "cum_qty": 1.0,
            "avg_fill_price": 1.1988,
            "status": "FILLED",
        }
    ]
    executor._entry_avg_price = 1.1988
    executor._executed_qty_total = 1.0
    executor._remaining_qty = 1.0
    executor.position = {"buy_price": 1.1988, "qty": 1.0, "opened_ts": 0, "partial": True}
    executor.exit_intent = "TP"
    executor._tp_exit_phase = "MAKER"
    executor._tp_maker_started_ts = time.monotonic() - 0.5

    placed: list[str] = []

    def fake_place_sell_order(*_args, **kwargs) -> int:
        placed.append(str(kwargs.get("reason")))
        return 1

    executor._place_sell_order = fake_place_sell_order  # type: ignore[assignment]

    executor._ensure_exit_orders(price_state, health_state)

    assert executor._tp_exit_phase == "MAKER"
    assert placed == ["TP_MAKER"]


def test_reconcile_clamp_blocks_insane_totals() -> None:
    price_state = PriceState(
        bid=1.0,
        ask=1.0,
        mid=1.0,
        source="HTTP",
        mid_age_ms=10,
        data_blind=False,
    )
    health_state = HealthState(ws_connected=False, ws_age_ms=99999, http_age_ms=10)
    executor = make_executor(DummyRouter(price_state, health_state))
    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.0,
            "qty": 286.2,
            "cum_qty": 0.0,
            "avg_fill_price": 0.0,
            "status": "NEW",
        }
    ]
    executor.entry_active_order_id = 1
    now_ms = int(time.time() * 1000)
    executor._current_cycle_id = 1
    executor._cycle_start_ts_ms = now_ms
    executor._cycle_order_ids = {1}

    snapshot = {
        "open_orders": [],
        "trades": [
            {
                "qty": 5769.0,
                "price": 1.0,
                "side": "BUY",
                "isBuyer": True,
                "orderId": 1,
                "time": now_ms,
            },
        ],
        "balance": {"total": 0.0, "free": 0.0, "locked": 0.0},
    }

    executor.apply_reconcile_snapshot(snapshot)

    assert executor._executed_qty_total != 5769.0
    assert executor._reconcile_invalid_totals is True
    assert executor._safe_stop_issued is False
    assert executor.state != TradeState.STATE_SAFE_STOP


def test_entry_reprice_on_1_tick_when_http_fresh() -> None:
    price_state = PriceState(
        bid=1.1983,
        ask=1.1984,
        mid=1.19835,
        source="HTTP",
        mid_age_ms=2000,
        data_blind=False,
    )
    health_state = HealthState(ws_connected=False, ws_age_ms=99999, http_age_ms=200)
    executor = make_executor(DummyRouter(price_state, health_state))
    executor.state = TradeState.STATE_ENTRY_WORKING
    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.1982,
            "qty": 10.0,
            "cum_qty": 0.0,
            "avg_fill_price": 0.0,
            "status": "NEW",
            "tag": executor.TAG,
            "clientOrderId": "test",
            "cycle_id": 1,
        }
    ]
    executor.entry_active_order_id = 1
    executor.entry_active_price = 1.1982
    executor._entry_last_ref_bid = 1.1982

    executor._cancel_margin_order = lambda *args, **kwargs: True  # type: ignore[assignment]
    executor._get_margin_order_snapshot = (  # type: ignore[assignment]
        lambda _order_id: {"status": "CANCELED"}
    )

    def fake_place_margin_order(*_args, **_kwargs) -> dict:
        return {"orderId": 2, "clientOrderId": "test-2"}

    executor._place_margin_order = fake_place_margin_order  # type: ignore[assignment]

    executor._handle_entry_follow_top({1: {"orderId": 1, "status": "NEW"}})

    assert executor.entry_active_order_id == 2
    assert abs(executor.entry_active_price - 1.1983) <= 1e-9


def test_entry_not_blocked_by_data_blind() -> None:
    price_state = PriceState(
        bid=1.1001,
        ask=1.1002,
        mid=1.10015,
        source="HTTP",
        mid_age_ms=2000,
        data_blind=True,
    )
    health_state = HealthState(ws_connected=False, ws_age_ms=None, http_age_ms=200)
    executor = make_executor(DummyRouter(price_state, health_state))
    executor.state = TradeState.STATE_ENTRY_WORKING
    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.1000,
            "qty": 10.0,
            "cum_qty": 0.0,
            "avg_fill_price": 0.0,
            "status": "NEW",
            "tag": executor.TAG,
            "clientOrderId": "test",
            "cycle_id": 1,
        }
    ]
    executor.entry_active_order_id = 1
    executor.entry_active_price = 1.1000
    executor._entry_last_ref_bid = 1.1000

    executor._cancel_margin_order = lambda *args, **kwargs: True  # type: ignore[assignment]
    executor._get_margin_order_snapshot = (  # type: ignore[assignment]
        lambda _order_id: {"status": "CANCELED"}
    )

    def fake_place_margin_order(*_args, **_kwargs) -> dict:
        return {"orderId": 2, "clientOrderId": "test-2"}

    executor._place_margin_order = fake_place_margin_order  # type: ignore[assignment]

    executor._handle_entry_follow_top({1: {"orderId": 1, "status": "NEW"}})

    assert executor.entry_active_order_id == 2


def test_entry_wait_deadline_triggers_cancel(monkeypatch) -> None:
    price_state = PriceState(
        bid=1.2000,
        ask=1.2001,
        mid=1.20005,
        source="HTTP",
        mid_age_ms=10,
        data_blind=False,
    )
    health_state = HealthState(ws_connected=False, ws_age_ms=None, http_age_ms=10)
    executor = make_executor(DummyRouter(price_state, health_state))
    executor.state = TradeState.STATE_ENTRY_WORKING
    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.2000,
            "qty": 10.0,
            "cum_qty": 0.0,
            "avg_fill_price": 0.0,
            "status": "NEW",
            "tag": executor.TAG,
            "clientOrderId": "test",
            "cycle_id": 1,
        }
    ]
    executor.entry_active_order_id = 1

    now_s = 20.0
    monkeypatch.setattr(trade_executor_module.time, "monotonic", lambda: now_s)
    executor._wait_state_kind = "ENTRY_WAIT"
    executor._wait_state_enter_ts_ms = int((now_s * 1000) - 13000)
    executor._wait_state_deadline_ms = executor.ENTRY_WAIT_DEADLINE_MS
    executor._last_progress_ts = now_s - 13.0

    cancelled: list[int] = []

    def fake_cancel(*_args, **_kwargs) -> bool:
        cancelled.append(1)
        return True

    executor._cancel_margin_order = fake_cancel  # type: ignore[assignment]

    assert executor._check_wait_deadline(now_s) is True
    assert executor.entry_cancel_pending is True
    assert cancelled == [1]
