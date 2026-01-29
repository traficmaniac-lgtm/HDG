from __future__ import annotations

import time

from src.core.models import HealthState, PriceState, Settings, SymbolProfile
from src.services.trade_executor import TradeExecutor, TradeState


class DummyRest:
    def __init__(
        self,
        open_orders: list[dict] | None = None,
        trades: list[dict] | None = None,
        base_free: float = 0.0,
        base_locked: float = 0.0,
    ) -> None:
        self._open_orders = open_orders or []
        self._trades = trades or []
        self._base_free = base_free
        self._base_locked = base_locked
        self._next_order_id = 100
        self.orders: list[dict] = []

    def get_margin_open_orders(self, _symbol: str) -> list[dict]:
        return list(self._open_orders)

    def get_margin_my_trades(self, _symbol: str, limit: int = 50) -> list[dict]:
        return list(self._trades[:limit])

    def get_margin_account(self) -> dict:
        return {
            "userAssets": [
                {"asset": "TEST", "free": str(self._base_free), "locked": str(self._base_locked)},
                {"asset": "USDT", "free": "0", "locked": "0"},
            ]
        }

    def create_margin_order(self, params: dict) -> dict:
        self._next_order_id += 1
        order = {
            "orderId": self._next_order_id,
            "clientOrderId": params.get("newClientOrderId", ""),
            "side": params.get("side"),
            "price": params.get("price", "0"),
            "status": "NEW",
        }
        self.orders.append(order)
        self._open_orders.append(order)
        return order

    def cancel_margin_order(self, params: dict) -> None:
        order_id = params.get("orderId")
        self._open_orders = [
            order for order in self._open_orders if order.get("orderId") != order_id
        ]


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

    def get_mid_snapshot(self, _max_age_ms: int) -> tuple[bool, float, int, str, str]:
        mid = (self._bid + self._ask) / 2.0
        return True, mid, 5, self._source, "fresh"

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
        max_state_stuck_ms=50,
        max_no_progress_ms=50,
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


def test_deadline_triggers_recover() -> None:
    router = DummyRouter(bid=1.0, ask=1.1)
    rest = DummyRest()
    profile = SymbolProfile(tick_size=0.01, step_size=0.01, min_qty=0.01, min_notional=0.0)
    executor = TradeExecutor(
        rest=rest,
        router=router,
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )
    executor.state = TradeState.STATE_ENTRY_WORKING
    executor._wait_state_kind = "ENTRY_WAIT"
    executor._wait_state_enter_ts_ms = int(time.monotonic() * 1000) - 50
    executor._wait_state_deadline_ms = 1

    triggered = executor.watchdog_tick()

    assert triggered is True
    assert executor._last_recover_reason == "deadline"
    assert executor._last_recover_from == TradeState.STATE_ENTRY_WORKING


def test_recover_places_cross_if_position_open_and_no_exit() -> None:
    now_ms = int(time.time() * 1000)
    trades = [{"price": "1.0", "qty": "1.0", "isBuyer": True, "time": now_ms, "orderId": 701}]
    router = DummyRouter(bid=100.0, ask=101.0)
    rest = DummyRest(open_orders=[], trades=trades, base_free=1.0, base_locked=0.0)
    profile = SymbolProfile(tick_size=0.01, step_size=0.01, min_qty=0.01, min_notional=0.0)
    executor = TradeExecutor(
        rest=rest,
        router=router,
        settings=make_settings(),
        profile=profile,
        logger=lambda _msg: None,
    )
    executor._current_cycle_id = 1
    executor._cycle_start_ts_ms = now_ms - 500
    executor._cycle_order_ids = {701}
    executor.exit_intent = "TP"

    placed = executor._recover_stuck(reason="deadline")

    assert placed is True
    assert rest.orders
    assert rest.orders[0]["side"] == "SELL"
    assert executor.sell_active_order_id is not None
