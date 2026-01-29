from __future__ import annotations

import time

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services.trade_executor import TradeExecutor, TradeState


class DummyRest:
    def get_margin_account(self) -> dict:
        return {"userAssets": [{"asset": "TEST", "free": "0", "locked": "0"}]}


class DummyRouter:
    def __init__(self, bid: float, ask: float, source: str = "HTTP") -> None:
        self._bid = bid
        self._ask = ask
        self._source = source

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        mid = (self._bid + self._ask) / 2.0
        return (
            PriceState(
                bid=self._bid,
                ask=self._ask,
                mid=mid,
                source=self._source,
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
        tp_cross_after_ms=10,
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


def test_tp_cross_not_before_armed() -> None:
    profile = SymbolProfile(tick_size=0.0001, step_size=0.0001, min_qty=0.0001, min_notional=0.0)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(bid=1.0000, ask=1.0001),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )

    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.0000,
            "qty": 1.0,
            "cum_qty": 1.0,
            "avg_fill_price": 1.0000,
            "status": "FILLED",
        }
    ]
    executor._entry_avg_price = 1.0000
    executor._executed_qty_total = 1.0
    executor._remaining_qty = 1.0
    executor.position = {"buy_price": 1.0000, "qty": 1.0, "opened_ts": 0, "partial": False}
    executor.exit_intent = "TP"
    executor._tp_exit_phase = "MAKER"
    executor._tp_maker_started_ts = time.monotonic() - 0.1

    placed: list[str] = []

    def fake_place_sell_order(*_args, **kwargs) -> int:
        placed.append(str(kwargs.get("reason")))
        return 1

    executor._place_sell_order = fake_place_sell_order  # type: ignore[assignment]

    price_state, health_state = executor._router.build_price_state()
    executor._ensure_exit_orders(price_state, health_state)

    assert executor._tp_exit_phase == "MAKER"
    assert placed == ["TP_MAKER"]


def test_reconcile_filters_by_cycle_order_ids() -> None:
    profile = SymbolProfile(tick_size=0.01, step_size=0.01, min_qty=0.01, min_notional=0.0)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(bid=1.0, ask=1.01),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )
    executor._cycle_order_ids = {101}
    executor._cycle_start_ts_ms = int(time.time() * 1000)
    executor.position = {"buy_price": 1.0, "qty": 1.0, "opened_ts": 0, "partial": True, "initial_qty": 1.0}

    snapshot = {
        "open_orders": [],
        "balance": None,
        "trades": [
            {"orderId": 202, "qty": 5.0, "price": 1.0, "side": "BUY"},
            {"orderId": 101, "qty": 0.5, "price": 1.0, "side": "BUY"},
        ],
    }

    executor.apply_reconcile_snapshot(snapshot)

    assert executor._executed_qty_total == 0.5


def test_invalid_totals_do_not_apply_ledger_and_trigger_fallback() -> None:
    profile = SymbolProfile(tick_size=0.01, step_size=0.01, min_qty=0.01, min_notional=0.0)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(bid=1.0, ask=1.01),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )
    executor._cycle_order_ids = {101}
    executor._cycle_start_ts_ms = int(time.time() * 1000)
    executor.position = {"buy_price": 1.0, "qty": 1.0, "opened_ts": 0, "partial": True, "initial_qty": 1.0}

    snapshot = {
        "open_orders": [],
        "balance": None,
        "trades": [
            {"orderId": 101, "qty": 5.0, "price": 1.0, "side": "BUY"},
        ],
    }

    executor.apply_reconcile_snapshot(snapshot)

    assert executor._reconcile_invalid_totals is True
    assert executor._executed_qty_total == 0.0


def test_wait_deadline_triggers_recover() -> None:
    profile = SymbolProfile(tick_size=0.01, step_size=0.01, min_qty=0.01, min_notional=0.0)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(bid=1.0, ask=1.01),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )
    executor.state = TradeState.STATE_ENTRY_WORKING
    executor._wait_state_kind = "ENTRY_WAIT"
    executor._wait_state_enter_ts_ms = int(time.monotonic() * 1000) - 50
    executor._wait_state_deadline_ms = 1

    executor.collect_reconcile_snapshot = lambda: {  # type: ignore[assignment]
        "open_orders": [],
        "trades": [],
        "balance": {"asset": "TEST", "free": 0.0, "locked": 0.0, "total": 0.0},
    }

    assert executor._check_wait_deadline(time.monotonic()) is True
    assert executor._last_recover_reason == "deadline"


def test_recover_places_exit_when_position_open_and_no_exit() -> None:
    profile = SymbolProfile(tick_size=0.01, step_size=0.01, min_qty=0.01, min_notional=0.0)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(bid=1.0, ask=1.01),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )
    executor.position = {"buy_price": 1.0, "qty": 1.0, "opened_ts": 0, "partial": False, "initial_qty": 1.0}
    executor._remaining_qty = 1.0

    executor.collect_reconcile_snapshot = lambda: {  # type: ignore[assignment]
        "open_orders": [],
        "trades": [],
        "balance": {"asset": "TEST", "free": 1.0, "locked": 0.0, "total": 1.0},
    }

    placed: list[str] = []

    def fake_place_sell_order(*_args, **kwargs) -> int:
        placed.append(str(kwargs.get("reason")))
        return 1

    executor._place_sell_order = fake_place_sell_order  # type: ignore[assignment]

    assert executor._recover_stuck(reason="deadline") is True
    assert placed == ["RECOVERY"]
