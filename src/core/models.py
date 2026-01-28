from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Settings:
    symbol: str
    ws_fresh_ms: int
    http_fresh_ms: int
    http_interval_ms: int
    ui_refresh_ms: int
    account_mode: str
    leverage_hint: int
    nominal_usd: float
    offset_ticks: int
    order_type: str
    allow_borrow: bool
    side_effect_type: str
    margin_isolated: bool


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


@dataclass
class HealthState:
    ws_connected: bool = False
    ws_age_ms: Optional[int] = None
    http_age_ms: Optional[int] = None
    last_switch_reason: str = ""
