from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional


class CycleStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    ERROR = "ERROR"


@dataclass
class TradeCycle:
    cycle_id: int
    symbol: str
    direction: str
    started_ts: float
    closed_ts: Optional[float]
    entry_qty: float
    entry_avg: float
    exit_qty: float
    exit_avg: float
    fees_quote: float
    realized_pnl_quote: float
    unrealized_pnl_quote: float
    win: Optional[bool]
    notes: str
    status: CycleStatus = CycleStatus.OPEN


class TradeLedger:
    def __init__(self, logger: Optional[Callable[[str], None]] = None) -> None:
        self._next_id = 1
        self.cycles: list[TradeCycle] = []
        self.active_cycle: Optional[TradeCycle] = None
        self._logger = logger
        self._last_unreal_log_ts = 0.0

    def start_cycle(
        self,
        symbol: str,
        direction: str,
        entry_qty: float,
        entry_avg: float,
        notes: str = "",
    ) -> int:
        if self.active_cycle and self.active_cycle.status == CycleStatus.OPEN:
            return self.active_cycle.cycle_id
        cycle_id = self._next_id
        self._next_id += 1
        cycle = TradeCycle(
            cycle_id=cycle_id,
            symbol=symbol,
            direction=direction,
            started_ts=time.time(),
            closed_ts=None,
            entry_qty=entry_qty,
            entry_avg=entry_avg,
            exit_qty=0.0,
            exit_avg=0.0,
            fees_quote=0.0,
            realized_pnl_quote=0.0,
            unrealized_pnl_quote=0.0,
            win=None,
            notes=notes,
        )
        self.cycles.append(cycle)
        self.active_cycle = cycle
        if self._logger:
            self._logger(f"[LEDGER_OPEN] id={cycle_id} dir={direction}")
        return cycle_id

    def on_entry_fill(self, qty: float, price: float) -> None:
        cycle = self.active_cycle
        if not cycle or cycle.status != CycleStatus.OPEN or qty <= 0:
            return
        new_qty = cycle.entry_qty + qty
        if new_qty <= 0:
            return
        cycle.entry_avg = ((cycle.entry_avg * cycle.entry_qty) + (price * qty)) / new_qty
        cycle.entry_qty = new_qty

    def on_exit_fill(self, qty: float, price: float) -> None:
        cycle = self.active_cycle
        if not cycle or cycle.status != CycleStatus.OPEN or qty <= 0:
            return
        new_qty = cycle.exit_qty + qty
        if new_qty <= 0:
            return
        cycle.exit_avg = ((cycle.exit_avg * cycle.exit_qty) + (price * qty)) / new_qty
        cycle.exit_qty = new_qty

    def close_cycle(self, notes: str = "") -> None:
        cycle = self.active_cycle
        if not cycle or cycle.status != CycleStatus.OPEN:
            return
        realized = 0.0
        if cycle.exit_qty > 0:
            if cycle.direction == "SHORT":
                realized = (cycle.entry_avg - cycle.exit_avg) * cycle.exit_qty
            else:
                realized = (cycle.exit_avg - cycle.entry_avg) * cycle.exit_qty
        realized -= cycle.fees_quote
        cycle.realized_pnl_quote = realized
        cycle.unrealized_pnl_quote = 0.0
        cycle.win = None if cycle.exit_qty <= 0 else realized > 0
        cycle.status = CycleStatus.CLOSED
        cycle.closed_ts = time.time()
        if notes:
            if cycle.notes:
                cycle.notes = f"{cycle.notes},{notes}"
            else:
                cycle.notes = notes
        if self._logger:
            self._logger(
                f"[LEDGER_CLOSE] id={cycle.cycle_id} pnl={realized:.8f} "
                f"win={cycle.win} notes={cycle.notes or '—'}"
            )
        self.active_cycle = None

    def mark_error(self, notes: str = "") -> None:
        cycle = self.active_cycle
        if not cycle or cycle.status != CycleStatus.OPEN:
            return
        cycle.status = CycleStatus.ERROR
        if notes:
            if cycle.notes:
                cycle.notes = f"{cycle.notes},{notes}"
            else:
                cycle.notes = notes

    def update_unrealized(self, mark_price: float, mark_side: str) -> None:
        cycle = self.active_cycle
        if not cycle or cycle.status != CycleStatus.OPEN:
            return
        if cycle.entry_qty <= 0:
            return
        if cycle.direction == "SHORT":
            unreal = (cycle.entry_avg - mark_price) * cycle.entry_qty
        else:
            unreal = (mark_price - cycle.entry_avg) * cycle.entry_qty
        unreal -= cycle.fees_quote
        cycle.unrealized_pnl_quote = unreal
        now = time.monotonic()
        if self._logger and (now - self._last_unreal_log_ts) >= 2.0:
            self._last_unreal_log_ts = now
            self._logger(
                f"[LEDGER_UNREAL] id={cycle.cycle_id} unreal={unreal:.8f} side={mark_side}"
            )

    def get_cycle_rows(self, limit: int = 200) -> list[dict]:
        rows: list[dict] = []
        for cycle in reversed(self.cycles):
            if len(rows) >= limit:
                break
            qty = cycle.entry_qty if cycle.entry_qty else cycle.exit_qty
            unreal = cycle.unrealized_pnl_quote if cycle.status == CycleStatus.OPEN else None
            exit_avg = cycle.exit_avg if cycle.status == CycleStatus.CLOSED else None
            rows.append(
                {
                    "cycle_id": cycle.cycle_id,
                    "direction": cycle.direction,
                    "entry_avg": cycle.entry_avg,
                    "exit_avg": exit_avg,
                    "qty": qty,
                    "status": cycle.status.value,
                    "unreal_pnl": unreal,
                    "realized_pnl": cycle.realized_pnl_quote,
                    "win": cycle.win,
                    "notes": cycle.notes,
                }
            )
        return rows
