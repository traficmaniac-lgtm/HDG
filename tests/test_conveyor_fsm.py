from __future__ import annotations

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.core.trade_ledger import DealStatus, TradeLedger
from src.services.trade_executor import TradeExecutor


class DummyRest:
    def __init__(self) -> None:
        self.place_calls = 0

    def create_margin_order(self, params: dict) -> dict:
        self.place_calls += 1
        return {"orderId": self.place_calls, "clientOrderId": params.get("newClientOrderId")}


class DummyRouter:
    def build_price_state(self) -> tuple[PriceState, HealthState]:
        return (
            PriceState(bid=100.0, ask=101.0, mid=100.5, mid_age_ms=0, data_blind=False),
            HealthState(ws_connected=True, ws_age_ms=0, http_age_ms=0, last_switch_reason="ws"),
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
    profile = SymbolProfile(tick_size=0.1, step_size=0.1, min_qty=0.1, min_notional=0.0)
    return TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg, **_kwargs: None,
    )


def test_tp_sl_math_long_short() -> None:
    executor = make_executor()

    tp_long, sl_long = executor._compute_tp_sl_prices(entry_avg=100.0)

    assert tp_long is not None and tp_long > 100.0
    assert sl_long is not None and sl_long < 100.0

    executor._direction = "SHORT"
    tp_short, sl_short = executor._compute_tp_sl_prices(entry_avg=100.0)

    assert tp_short is not None and tp_short < 100.0
    assert sl_short is not None and sl_short > 100.0


def test_fill_dedupe_skips_duplicates() -> None:
    executor = make_executor()

    first = executor._should_apply_fill_dedup(1, 1.23456, 100.12, 1_700_000_000_000, None)
    second = executor._should_apply_fill_dedup(1, 1.23456, 100.12, 1_700_000_000_100, None)

    assert first is True
    assert second is False


def test_tp_idempotent_prevents_duplicates() -> None:
    executor = make_executor()
    rest = executor._rest
    executor._direction = "LONG"
    executor._current_cycle_id = 1
    executor._executed_qty_total = 1.0
    executor._remaining_qty = 1.0
    executor._entry_avg_price = 100.0
    executor.position = {
        "buy_price": 100.0,
        "qty": 1.0,
        "opened_ts": 0,
        "partial": False,
        "initial_qty": 1.0,
        "side": "LONG",
    }

    tp_key = executor._build_tp_key(1, "LONG", "SELL", 1.0, 101.0)
    executor._active_tp_keys.add(tp_key)

    placed = executor._place_sell_order(
        reason="TP_MAKER",
        price_override=101.0,
        exit_intent="TP",
        qty_override=1.0,
        ref_bid=100.8,
        ref_ask=101.0,
    )

    assert placed == 0
    assert isinstance(rest, DummyRest)
    assert rest.place_calls == 0


def test_deal_close_when_remaining_zero() -> None:
    ledger = TradeLedger()
    cycle_id = ledger.start_cycle(
        symbol="TESTUSDT",
        direction="LONG",
        entry_qty=0.0,
        entry_avg=100.0,
    )
    ledger.on_entry_fill(cycle_id, 1.0, 100.0)
    ledger.on_exit_fill(cycle_id, 1.0, 101.0)

    cycle = ledger.get_cycle(cycle_id)
    assert cycle is not None
    assert cycle.remaining_qty == 0.0

    ledger.close_cycle(cycle_id, notes="TP")
    deal = ledger.deals_by_cycle_id.get(cycle_id)
    assert deal is not None
    assert deal.status == DealStatus.CLOSED


def test_reconcile_restore_exchange_qty() -> None:
    executor = make_executor()
    snapshot = {
        "open_orders": [],
        "balance": {"total": 1.5, "borrowed": 0.0, "net": 1.5},
    }

    executor.apply_reconcile_snapshot(snapshot)

    cycle = executor.ledger.active_cycle
    assert cycle is not None
    assert cycle.remaining_qty == 1.5
    assert "RECOVERED_FROM_EXCHANGE" in (cycle.notes or "")
