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
    return TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )


def test_fill_dedup_by_order_id_and_cum_qty() -> None:
    executor = make_executor()

    first = executor._should_process_fill(10, "BUY", 5.0, 1.2, 1)
    second = executor._should_process_fill(10, "BUY", 5.0, 1.3, 1)

    assert first is True
    assert second is False
