from __future__ import annotations

import time

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services.trade_executor import TradeExecutor, TradeState


class DummyRest:
    pass


class DummyRouter:
    def __init__(self, data_blind: bool = False) -> None:
        self._data_blind = data_blind

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        return PriceState(data_blind=self._data_blind), HealthState()

    def get_ws_issue_counts(self) -> tuple[int, int]:
        return (0, 0)


def make_settings(cycle_count: int = 2) -> Settings:
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
        cycle_count=cycle_count,
        order_quote=10.0,
        max_budget=100.0,
        budget_reserve=0.0,
    )


def make_executor(logs: list[str], data_blind: bool = False, cycle_count: int = 2) -> TradeExecutor:
    profile = SymbolProfile(tick_size=0.0001, step_size=0.0001, min_qty=0.0001, min_notional=0.0)
    return TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(data_blind=data_blind),
        settings=make_settings(cycle_count=cycle_count),
        profile=profile,
        logger=logs.append,
    )


def test_cycle_completion_auto_next_schedules_idle() -> None:
    logs: list[str] = []
    executor = make_executor(logs)
    executor.run_active = True
    executor.cycles_target = 2
    executor.cycles_done = 0
    executor.state = TradeState.STATE_POS_OPEN
    executor._cycle_started_ts = time.monotonic() - 1.0

    executor._finalize_cycle_close(
        reason="TP",
        pnl=1.0,
        qty=1.0,
        buy_price=1.0,
        cycle_id=1,
    )

    assert executor.state == TradeState.STATE_IDLE
    assert executor.cycles_done == 1
    assert executor._next_cycle_ready_ts is not None
    assert any("[CYCLE] done -> auto_next state=IDLE" in entry for entry in logs)


def test_trade_state_has_no_next_cycle() -> None:
    assert "NEXT_CYCLE" not in {state.value for state in TradeState}
    assert not hasattr(TradeState, "STATE_NEXT_CYCLE")
