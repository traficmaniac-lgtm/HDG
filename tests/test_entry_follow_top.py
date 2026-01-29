from __future__ import annotations

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services.trade_executor import TradeExecutor


class DummyRest:
    pass


class DummyRouter:
    def build_price_state(self) -> tuple[PriceState, HealthState]:
        return PriceState(), HealthState()

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
        mid_fresh_ms=1000,
        max_wait_price_ms=2000,
        price_wait_log_every_ms=500,
        position_guard_http=False,
        entry_mode="FOLLOW_TOP",
        account_mode="MARGIN",
        leverage_hint=1,
        entry_reprice_min_ticks=1,
        entry_reprice_cooldown_ms=1200,
        entry_reprice_require_stable_source=True,
        entry_reprice_stable_source_grace_ms=3000,
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


def make_executor() -> TradeExecutor:
    profile = SymbolProfile(tick_size=0.0001, step_size=0.0001, min_qty=0.0001, min_notional=0.0)
    return TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )


def test_entry_ref_movement() -> None:
    executor = make_executor()
    executor._entry_last_ref_bid = 1.19430

    moved_ticks, can_reprice = executor._entry_ref_movement(1.19430, 0.0001, 1000)

    assert moved_ticks == 0
    assert can_reprice is False
    assert executor._entry_last_ref_bid == 1.19430

    moved_ticks, can_reprice = executor._entry_ref_movement(1.19440, 0.0001, 2000)

    assert moved_ticks == 1
    assert can_reprice is True
    assert executor._entry_last_ref_bid == 1.19440


def test_partial_fill_triggers_sell() -> None:
    executor = make_executor()
    placed: list[float] = []

    def fake_place_sell_order(*_args, **_kwargs) -> int:
        placed.append(executor._resolve_remaining_qty_raw())
        return 1

    executor._place_sell_order = fake_place_sell_order  # type: ignore[assignment]
    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.20000,
            "qty": 20.0,
            "cum_qty": 0.0,
            "avg_fill_price": 0.0,
            "created_ts": 0,
            "updated_ts": 0,
            "status": "NEW",
            "tag": executor.TAG,
            "clientOrderId": "test",
            "cycle_id": 1,
        }
    ]

    executor.handle_order_partial(order_id=1, side="BUY", cum_qty=10.0, avg_price=1.20000, ts_ms=1000)
    executor.handle_order_partial(order_id=1, side="BUY", cum_qty=15.0, avg_price=1.20000, ts_ms=2000)

    assert placed == [10.0, 15.0]
