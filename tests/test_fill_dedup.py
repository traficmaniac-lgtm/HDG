from __future__ import annotations

import time

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.core.order_registry import OrderMeta, OrderRole
from src.services.trade_executor import TradeExecutor


class DummyRest:
    pass


class DummyRouter:
    def build_price_state(self) -> tuple[PriceState, HealthState]:
        return (
            PriceState(bid=1.0, ask=1.01, mid=1.005, source="TEST", mid_age_ms=10),
            HealthState(ws_connected=True, ws_age_ms=10, http_age_ms=10),
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
        entry_mode="NORMAL",
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


def make_executor() -> TradeExecutor:
    profile = SymbolProfile(tick_size=0.0001, step_size=0.0001, min_qty=0.0001, min_notional=0.0)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg, **_kwargs: None,
    )
    executor._place_sell_order = lambda *_args, **_kwargs: 0  # type: ignore[assignment]
    return executor


def _register_order(
    executor: TradeExecutor,
    order_id: int,
    cycle_id: int,
    role: OrderRole,
    side: str,
) -> None:
    now = time.time()
    executor._order_registry.register_new(
        OrderMeta(
            client_id=f"client-{order_id}",
            order_id=order_id,
            symbol="TESTUSDT",
            cycle_id=cycle_id,
            direction="LONG",
            role=role,
            side=side,
            price=1.0,
            orig_qty=1.0,
            created_ts=now,
            last_update_ts=now,
            status="NEW",
        )
    )


def test_double_fill_same_cum_qty_applied_once() -> None:
    executor = make_executor()
    cycle_id = executor.ledger.start_cycle("TESTUSDT", "LONG", 0.0, 0.0)
    executor._current_cycle_id = cycle_id
    _register_order(executor, 101, cycle_id, OrderRole.ENTRY, "BUY")

    executor.handle_order_partial(101, "BUY", 1.0, 1.0, 1000)
    executor.handle_order_partial(101, "BUY", 1.0, 1.0, 2000)

    cycle = executor.ledger.get_cycle(cycle_id)
    assert cycle is not None
    assert cycle.executed_qty == 1.0


def test_exit_fill_duplicate_does_not_double_close() -> None:
    executor = make_executor()
    cycle_id = executor.ledger.start_cycle("TESTUSDT", "LONG", 0.0, 0.0)
    executor._current_cycle_id = cycle_id
    _register_order(executor, 201, cycle_id, OrderRole.ENTRY, "BUY")
    _register_order(executor, 202, cycle_id, OrderRole.EXIT_TP, "SELL")

    executor.handle_order_partial(201, "BUY", 1.0, 1.0, 1000)
    executor.handle_order_partial(202, "SELL", 1.0, 1.01, 1100)
    executor.handle_order_partial(202, "SELL", 1.0, 1.01, 1200)

    cycle = executor.ledger.get_cycle(cycle_id)
    assert cycle is not None
    assert cycle.closed_qty == 1.0


def test_remaining_qty_never_negative() -> None:
    executor = make_executor()
    cycle_id = executor.ledger.start_cycle("TESTUSDT", "LONG", 0.0, 0.0)
    executor._current_cycle_id = cycle_id
    _register_order(executor, 301, cycle_id, OrderRole.ENTRY, "BUY")
    _register_order(executor, 302, cycle_id, OrderRole.EXIT_TP, "SELL")

    executor.handle_order_partial(301, "BUY", 1.0, 1.0, 1000)
    executor.handle_order_partial(302, "SELL", 2.0, 1.01, 1100)

    cycle = executor.ledger.get_cycle(cycle_id)
    assert cycle is not None
    assert cycle.remaining_qty >= 0.0
