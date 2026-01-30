from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Settings:
    symbol: str
    ws_fresh_ms: int
    ws_stale_ms: int
    http_fresh_ms: int
    http_poll_ms: int
    ui_refresh_ms: int
    ws_log_throttle_ms: int
    ws_reconnect_dedup_ms: int
    order_poll_ms: int
    ws_switch_hysteresis_ms: int
    min_source_hold_ms: int
    ws_stable_required_ms: int
    ws_stale_grace_ms: int
    entry_max_age_ms: int
    exit_max_age_ms: int
    tp_max_age_ms: int
    settlement_grace_ms: int
    repricing_cooldown_ms: int
    max_wait_price_ms: int
    price_wait_log_every_ms: int
    tp_cross_after_ms: int
    inflight_deadline_ms: int
    watchdog_poll_ms: int
    max_state_stuck_ms: int
    max_no_progress_ms: int
    max_reconcile_retries: int
    sell_refresh_grace_ms: int
    epsilon_qty: float
    position_guard_http: bool
    entry_mode: str
    account_mode: str
    leverage_hint: int
    entry_reprice_min_ticks: int
    entry_reprice_cooldown_ms: int
    entry_reprice_require_stable_source: bool
    entry_reprice_stable_source_grace_ms: int
    entry_reprice_min_consecutive_fresh_reads: int
    take_profit_ticks: int
    stop_loss_ticks: int
    order_type: str
    exit_order_type: str
    max_buy_retries: int
    allow_borrow: bool
    side_effect_type: str
    margin_isolated: bool
    auto_exit_enabled: bool
    max_sell_retries: int
    max_wait_sell_ms: int
    allow_force_close: bool
    cycle_count: int
    order_quote: float
    max_budget: float
    budget_reserve: float
    verbose_ui_log: bool = False
    tp_missing_watchdog_ms: int = 1500
    idle_guard_ms: int = 6000


@dataclass
class SymbolProfile:
    tick_size: Optional[float] = None
    step_size: Optional[float] = None
    min_qty: Optional[float] = None
    min_notional: Optional[float] = None


@dataclass
class PriceState:
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    source: str = "NONE"
    mid_age_ms: Optional[int] = None
    data_blind: bool = False
    from_cache: bool = False
    cache_age_ms: Optional[int] = None


@dataclass
class HealthState:
    ws_connected: bool = False
    ws_age_ms: Optional[int] = None
    http_age_ms: Optional[int] = None
    last_switch_reason: str = ""
