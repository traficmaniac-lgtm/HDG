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
    good_quote_ttl_ms: int
    position_guard_http: bool
    account_mode: str
    leverage_hint: int
    offset_ticks: int
    entry_offset_ticks: int
    take_profit_ticks: int
    stop_loss_ticks: int
    order_type: str
    exit_order_type: str
    exit_offset_ticks: int
    buy_ttl_ms: int
    max_buy_retries: int
    allow_borrow: bool
    side_effect_type: str
    margin_isolated: bool
    auto_exit_enabled: bool
    sell_ttl_ms: int
    max_sell_retries: int
    force_close_on_ttl: bool
    cycle_count: int
    order_quote: float
    max_budget: float
    budget_reserve: float


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
    ttl_expired: bool = False


@dataclass
class HealthState:
    ws_connected: bool = False
    ws_age_ms: Optional[int] = None
    http_age_ms: Optional[int] = None
    last_switch_reason: str = ""
