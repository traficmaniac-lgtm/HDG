from __future__ import annotations

from src.core.models import Settings
from src.services import price_router as price_router_module
from src.services.price_router import PriceRouter


def _set_time(monkeypatch, now_ms: float) -> None:
    monkeypatch.setattr(price_router_module.time, "monotonic", lambda: now_ms / 1000)


def _make_settings(**overrides: object) -> Settings:
    base = dict(
        symbol="EURIUSDT",
        ws_fresh_ms=700,
        ws_stale_ms=500,
        http_fresh_ms=500,
        http_poll_ms=350,
        ui_refresh_ms=100,
        ws_log_throttle_ms=5000,
        ws_reconnect_dedup_ms=10000,
        order_poll_ms=1500,
        ws_switch_hysteresis_ms=1000,
        min_source_hold_ms=3000,
        ws_stable_required_ms=1500,
        ws_stale_grace_ms=0,
        entry_max_age_ms=800,
        exit_max_age_ms=2500,
        max_wait_price_ms=5000,
        price_wait_log_every_ms=1000,
        position_guard_http=True,
        entry_mode="NORMAL",
        account_mode="CROSS_MARGIN",
        leverage_hint=3,
        entry_reprice_min_ticks=1,
        entry_reprice_cooldown_ms=1200,
        entry_reprice_require_stable_source=True,
        entry_reprice_stable_source_grace_ms=3000,
        entry_reprice_min_consecutive_fresh_reads=2,
        take_profit_ticks=1,
        stop_loss_ticks=2,
        order_type="LIMIT",
        exit_order_type="LIMIT",
        max_buy_retries=3,
        allow_borrow=True,
        side_effect_type="AUTO_BORROW_REPAY",
        margin_isolated=False,
        auto_exit_enabled=True,
        max_sell_retries=3,
        max_wait_sell_ms=15000,
        allow_force_close=False,
        cycle_count=1,
        order_quote=50.0,
        max_budget=1000.0,
        budget_reserve=20.0,
    )
    base.update(overrides)
    return Settings(**base)


def test_source_hold_hysteresis(monkeypatch) -> None:
    settings = _make_settings()
    router = PriceRouter(settings)
    router.set_tick_size(0.0001)

    for now_ms in [0.0, 500.0, 1000.0, 1500.0]:
        _set_time(monkeypatch, now_ms)
        router.update_ws(1.0, 1.1)
        router.get_best_quote()

    _set_time(monkeypatch, 1500.0)
    assert router.get_best_quote()[3] == "WS"

    _set_time(monkeypatch, 2100.0)
    router.update_http(1.0, 1.1)
    assert router.get_best_quote()[3] == "HTTP"

    for now_ms in [3600.0, 4200.0, 4800.0]:
        _set_time(monkeypatch, now_ms)
        router.update_ws(1.0, 1.1)
        router.get_best_quote()

    _set_time(monkeypatch, 4800.0)
    assert router.get_best_quote()[3] == "HTTP"

    _set_time(monkeypatch, 5200.0)
    router.update_ws(1.0, 1.1)
    assert router.get_best_quote()[3] == "WS"
