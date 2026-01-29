from __future__ import annotations

import time

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services.trade_executor import TradeExecutor


class DummyRest:
    pass


class DummyRouter:
    def __init__(self, bid: float, ask: float) -> None:
        self._bid = bid
        self._ask = ask

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        mid = (self._bid + self._ask) / 2.0
        return (
            PriceState(
                bid=self._bid,
                ask=self._ask,
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
        tp_cross_after_ms=10,
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
        cycle_count=1,
        order_quote=10.0,
        max_budget=100.0,
        budget_reserve=0.0,
    )


def test_tp_cross_after_timeout() -> None:
    profile = SymbolProfile(tick_size=0.0001, step_size=0.0001, min_qty=0.0001, min_notional=0.0)
    executor = TradeExecutor(
        rest=DummyRest(),
        router=DummyRouter(bid=1.1969, ask=1.1970),
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )

    executor.active_test_orders = [
        {
            "orderId": 1,
            "side": "BUY",
            "price": 1.1967,
            "qty": 10.0,
            "cum_qty": 1.0,
            "avg_fill_price": 1.1967,
            "status": "PARTIALLY_FILLED",
        }
    ]
    executor._entry_avg_price = 1.1967
    executor._executed_qty_total = 1.0
    executor._remaining_qty = 1.0
    executor.position = {"buy_price": 1.1967, "qty": 1.0, "opened_ts": 0, "partial": True}
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

    assert executor._tp_exit_phase == "CROSS"
    assert placed == ["TP_CROSS"]
