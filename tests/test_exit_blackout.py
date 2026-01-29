from __future__ import annotations

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services.trade_executor import TradeExecutor, TradeState


class DummyRest:
    pass


class HttpFreshRouter:
    def __init__(self, mid: float, bid: float, ask: float) -> None:
        self._mid = mid
        self._bid = bid
        self._ask = ask

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        return (
            PriceState(
                bid=self._bid,
                ask=self._ask,
                mid=self._mid,
                source="HTTP",
                mid_age_ms=500,
                data_blind=False,
            ),
            HealthState(ws_connected=False, ws_age_ms=99999, http_age_ms=500),
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


def test_exit_not_frozen_when_http_fresh() -> None:
    profile = SymbolProfile(tick_size=0.001, step_size=0.001, min_qty=0.001, min_notional=0.0)
    buy_price = 1.000
    mid = 1.001
    router = HttpFreshRouter(mid=mid, bid=mid - 0.001, ask=mid + 0.001)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=router,
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )
    executor.state = TradeState.STATE_POSITION_OPEN
    executor.position = {"buy_price": buy_price, "qty": 1.0, "opened_ts": 0, "partial": False}

    placed: list[str] = []

    def fake_place_sell(*_args, **_kwargs) -> int:
        placed.append("sell")
        return 1

    executor._place_sell_order = fake_place_sell  # type: ignore[assignment]

    executor.evaluate_exit_conditions()

    assert executor._exits_frozen is False
    assert placed == ["sell"]
