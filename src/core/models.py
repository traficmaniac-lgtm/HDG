from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional


class ConnectionMode(str, Enum):
    MARGIN = "MARGIN"
    FUTURES = "FUTURES"


class MarketDataMode(str, Enum):
    WS_ONLY = "WS_ONLY"
    HTTP_ONLY = "HTTP_ONLY"
    HYBRID = "HYBRID"


@dataclass
class ConnectionSettings:
    api_key: str = ""
    api_secret: str = ""
    mode: ConnectionMode = ConnectionMode.MARGIN
    leverage: int = 1
    save_local: bool = True
    live_enabled: bool = False


@dataclass
class MarketTick:
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    spread_bps: float = 0.0
    bid_raw: Decimal = Decimal("0")
    ask_raw: Decimal = Decimal("0")
    mid_raw: Decimal = Decimal("0")
    event_time: datetime = field(default_factory=datetime.utcnow)
    rx_time: datetime = field(default_factory=datetime.utcnow)


@dataclass
class MarketSnapshot:
    last_ws_tick_ms: Optional[float] = None
    last_http_tick_ms: Optional[float] = None
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    effective_source: str = "NONE"
    ws_age_ms: float = 9999.0
    http_age_ms: float = 9999.0
    effective_age_ms: float = 9999.0
    data_stale: bool = True


@dataclass
class StrategyParams:
    order_mode: str = "market"
    sim_side: str = "BUY"
    usd_notional: float = 20.0
    leverage_max: int = 3
    slip_bps: float = 2.0
    max_loss_bps: int = 5
    fee_total_bps: float = 7.0
    target_net_bps: int = 10
    max_spread_bps: float = 8.0
    min_tick_rate: int = 5
    detect_timeout_ms: int = 6000
    use_impulse_filter: bool = True
    impulse_min_bps: float = 1.0
    impulse_grace_ms: int = 2000
    impulse_degrade_mode: str = "DISABLE_AFTER_GRACE"
    winner_threshold_bps: float = 0.2
    emergency_stop_bps: int = 10
    cooldown_s: int = 3
    detect_window_ticks: int = 8
    allow_no_winner_flatten: bool = True
    no_winner_policy: str = "NO_LOSS"
    burst_volume_threshold: float = 0.0
    auto_loop: bool = False
    max_cycles: int = 10


@dataclass
class CycleStatus:
    state: str = "IDLE"
    active_cycle: bool = False
    cycle_id: int = 0
    start_time: Optional[datetime] = None
    entry_mid: Optional[float] = None
    detect_mid: Optional[float] = None
    exit_mid: Optional[float] = None
    winner_side: str = "—"
    loser_side: str = "—"


@dataclass
class CycleTelemetry:
    cycle_id: int = 0
    state: str = "IDLE"
    active_cycle: bool = False
    inflight_entry: bool = False
    inflight_exit: bool = False
    open_orders_count: int = 0
    pos_long_qty: float = 0.0
    pos_short_qty: float = 0.0
    last_action: Optional[str] = None
    started_at: Optional[datetime] = None
    duration_s: Optional[float] = None
    nominal_usd: float = 0.0
    fee_total_bps: float = 0.0
    target_net_bps: float = 0.0
    max_loss_bps: float = 0.0
    max_spread_bps: float = 0.0
    min_tick_rate: float = 0.0
    cooldown_s: float = 0.0
    detect_window_ticks: int = 0
    detect_timeout_ms: int = 0
    tick_rate: Optional[float] = None
    impulse_bps: Optional[float] = None
    spread_bps: Optional[float] = None
    ws_age_ms: Optional[float] = None
    effective_source: Optional[str] = None
    last_mid: Optional[float] = None
    entry_mid: Optional[float] = None
    exit_mid: Optional[float] = None
    winner_side: Optional[str] = None
    loser_side: Optional[str] = None
    long_raw_bps: Optional[float] = None
    short_raw_bps: Optional[float] = None
    winner_raw_bps: Optional[float] = None
    loser_raw_bps: Optional[float] = None
    winner_net_bps: Optional[float] = None
    loser_net_bps: Optional[float] = None
    total_raw_bps: Optional[float] = None
    total_net_bps: Optional[float] = None
    result: Optional[str] = None
    pnl_usdt: Optional[float] = None
    condition: Optional[str] = None
    reason: Optional[str] = None
    last_error: Optional[str] = None


@dataclass
class CycleViewModel:
    state: str
    cycle_id: int
    active_cycle: bool
    inflight_entry: bool
    inflight_exit: bool
    open_orders_count: int
    pos_long_qty: float
    pos_short_qty: float
    last_action: Optional[str]
    start_ts: Optional[datetime]
    duration_s: Optional[float]
    entry_mid: Optional[float]
    last_mid: Optional[float]
    exit_mid: Optional[float]
    impulse_bps: Optional[float]
    tick_rate: Optional[float]
    spread_bps: Optional[float]
    ws_age_ms: Optional[float]
    effective_source: Optional[str]
    effective_age_ms: Optional[float]
    long_raw_bps: Optional[float]
    short_raw_bps: Optional[float]
    winner: Optional[str]
    loser: Optional[str]
    reason: Optional[str]
    target_net_bps: float
    fee_bps: float
    max_loss_bps: float
    detect_window_ticks: int
    detect_timeout_ms: int
    cooldown_s: float
    result: Optional[str]
    pnl_usdt: Optional[float]
    net_bps_total: Optional[float]
    data_stale: bool
    waiting_for_data: bool


@dataclass
class SymbolFilters:
    step_size: float = 0.0
    min_qty: float = 0.0
    min_notional: float = 0.0
    tick_size: float = 0.0


@dataclass
class TradeRecord:
    ts: datetime
    cycle_id: int
    side: str
    entry_price: float
    exit_price: float
    raw_bps: float
    fees_bps: float
    net_bps: float
    net_usd: float
    duration_s: float
    note: str = ""
