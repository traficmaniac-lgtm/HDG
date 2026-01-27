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
    detect_timeout_ms: int = 800
    use_impulse_filter: bool = True
    impulse_min_bps: float = 1.0
    impulse_grace_ms: int = 2000
    impulse_degrade_mode: str = "DISABLE_AFTER_GRACE"
    winner_threshold_bps: float = 1.0
    emergency_stop_bps: int = 10
    cooldown_s: int = 3
    detect_window_ticks: int = 2
    burst_volume_threshold: float = 0.0
    auto_loop: bool = False
    max_cycles: int = 0


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
