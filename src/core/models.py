from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class ConnectionMode(str, Enum):
    MARGIN = "MARGIN"
    FUTURES = "FUTURES"


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
    event_time: datetime = field(default_factory=datetime.utcnow)
    rx_time: datetime = field(default_factory=datetime.utcnow)


@dataclass
class StrategyParams:
    order_mode: str = "market"
    sim_side: str = "BUY"
    usd_notional: float = 20.0
    max_loss_bps: int = 5
    fee_total_bps: float = 7.0
    target_net_bps: int = 10
    max_spread_bps: float = 8.0
    cooldown_s: int = 3
    direction_detect_window_ticks: int = 2
    burst_volume_threshold: float = 0.0
    auto_loop: bool = False


@dataclass
class CycleStatus:
    state: str = "IDLE"
    active_cycle: bool = False
    cycle_id: int = 0
    start_time: Optional[datetime] = None
    entry_price_long: Optional[float] = None
    entry_price_short: Optional[float] = None
    raw_bps_long: Optional[float] = None
    raw_bps_short: Optional[float] = None
    winner_side: str = "—"
    loser_side: str = "—"


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
