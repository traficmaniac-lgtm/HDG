from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from enum import Enum
from typing import Callable, Optional


class CycleStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    ERROR = "ERROR"


class DealStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


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
    executed_qty: float
    closed_qty: float
    remaining_qty: float
    fees_quote: float
    realized_pnl_quote: float
    unrealized_pnl_quote: float
    win: Optional[bool]
    notes: str
    status: CycleStatus = CycleStatus.OPEN


@dataclass
class Deal:
    deal_id: int
    direction: str
    entry_avg: float
    exit_avg: float
    qty: float
    status: DealStatus
    realized_pnl: float
    unrealized_pnl: float
    opened_ts: float
    closed_ts: Optional[float]
    duration_ms: Optional[int]
    entry_order_ids: list[int]
    exit_order_ids: list[int]
    cycle_id: int


class TradeLedger:
    def __init__(self, logger: Optional[Callable[[str], None]] = None) -> None:
        self._next_id = 1
        self._next_deal_id = 1
        self.cycles: list[TradeCycle] = []
        self.cycles_by_id: dict[int, TradeCycle] = {}
        self.active_cycle: Optional[TradeCycle] = None
        self.deals: list[Deal] = []
        self.deals_by_cycle_id: dict[int, Deal] = {}
        self.active_deal: Optional[Deal] = None
        self._logger = logger
        self._last_unreal_log_ts = 0.0
        self._last_qty_snapshot: dict[int, tuple[float, float, float]] = {}

    @staticmethod
    def _round_qty(value: float) -> float:
        return float(Decimal(value).quantize(Decimal("0.000000000001"), rounding=ROUND_FLOOR))

    def _update_remaining_qty(self, cycle: TradeCycle) -> None:
        if cycle.closed_qty > cycle.executed_qty:
            cycle.closed_qty = cycle.executed_qty
        remaining = cycle.executed_qty - cycle.closed_qty
        cycle.remaining_qty = max(self._round_qty(remaining), 0.0)

    def _log_cycle_qty(self, cycle: TradeCycle) -> None:
        snapshot = (cycle.executed_qty, cycle.closed_qty, cycle.remaining_qty)
        last = self._last_qty_snapshot.get(cycle.cycle_id)
        if last == snapshot:
            return
        self._last_qty_snapshot[cycle.cycle_id] = snapshot
        if self._logger:
            self._logger(
                "[CYCLE_QTY] "
                f"id={cycle.cycle_id} exec={cycle.executed_qty:.8f} "
                f"closed={cycle.closed_qty:.8f} rem={cycle.remaining_qty:.8f}"
            )

    def _log_warn(self, message: str) -> None:
        if self._logger:
            self._logger(f"[DEAL_WARN] {message}")

    def _get_deal_for_cycle(self, cycle: TradeCycle) -> Optional[Deal]:
        return self.deals_by_cycle_id.get(cycle.cycle_id)

    def _create_deal(self, cycle: TradeCycle) -> Optional[Deal]:
        if cycle.executed_qty <= 0:
            return None
        if cycle.entry_avg <= 0:
            self._log_warn(
                f"deal_skip cycle_id={cycle.cycle_id} entry_avg={cycle.entry_avg:.8f}"
            )
            return None
        deal_id = self._next_deal_id
        self._next_deal_id += 1
        deal = Deal(
            deal_id=deal_id,
            direction=cycle.direction,
            entry_avg=cycle.entry_avg,
            exit_avg=0.0,
            qty=cycle.executed_qty,
            status=DealStatus.OPEN,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            opened_ts=time.time(),
            closed_ts=None,
            duration_ms=None,
            entry_order_ids=[],
            exit_order_ids=[],
            cycle_id=cycle.cycle_id,
        )
        self.deals.append(deal)
        self.deals_by_cycle_id[cycle.cycle_id] = deal
        self.active_deal = deal
        if self._logger:
            self._logger(
                f"[DEAL_OPEN] id={deal.deal_id} dir={deal.direction} "
                f"entry_avg={deal.entry_avg:.8f} exec_qty={deal.qty:.8f}"
            )
        return deal

    def _update_deal_from_cycle(self, deal: Deal, cycle: TradeCycle) -> None:
        deal.entry_avg = cycle.entry_avg
        deal.qty = cycle.executed_qty
        deal.unrealized_pnl = cycle.unrealized_pnl_quote

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
            executed_qty=0.0,
            closed_qty=0.0,
            remaining_qty=0.0,
            fees_quote=0.0,
            realized_pnl_quote=0.0,
            unrealized_pnl_quote=0.0,
            win=None,
            notes=notes,
        )
        self.cycles.append(cycle)
        self.cycles_by_id[cycle_id] = cycle
        self.active_cycle = cycle
        if self._logger:
            self._logger(f"[LEDGER_OPEN] id={cycle_id} dir={direction}")
        return cycle_id

    def _resolve_cycle(self, cycle_id: Optional[int]) -> Optional[TradeCycle]:
        if cycle_id is not None:
            return self.cycles_by_id.get(cycle_id)
        return self.active_cycle

    def get_cycle(self, cycle_id: Optional[int]) -> Optional[TradeCycle]:
        return self._resolve_cycle(cycle_id)

    def on_entry_fill(self, cycle_id: Optional[int], qty: float, price: float) -> None:
        cycle = self._resolve_cycle(cycle_id)
        if not cycle or cycle.status != CycleStatus.OPEN or qty <= 0:
            return
        new_qty = cycle.executed_qty + qty
        if new_qty <= 0:
            return
        cycle.entry_avg = ((cycle.entry_avg * cycle.executed_qty) + (price * qty)) / new_qty
        cycle.executed_qty = new_qty
        cycle.entry_qty = new_qty
        self._update_remaining_qty(cycle)
        self._log_cycle_qty(cycle)
        deal = self._get_deal_for_cycle(cycle)
        if not deal:
            deal = self._create_deal(cycle)
        elif deal.status == DealStatus.OPEN:
            self._update_deal_from_cycle(deal, cycle)

    def on_exit_fill(self, cycle_id: Optional[int], qty: float, price: float) -> None:
        cycle = self._resolve_cycle(cycle_id)
        if not cycle or cycle.status != CycleStatus.OPEN or qty <= 0:
            return
        new_qty = cycle.closed_qty + qty
        if new_qty <= 0:
            return
        cycle.exit_avg = ((cycle.exit_avg * cycle.closed_qty) + (price * qty)) / new_qty
        cycle.closed_qty = new_qty
        cycle.exit_qty = new_qty
        self._update_remaining_qty(cycle)
        self._log_cycle_qty(cycle)
        deal = self._get_deal_for_cycle(cycle)
        if deal and deal.status == DealStatus.OPEN:
            deal.exit_avg = cycle.exit_avg

    def close_cycle(self, cycle_id: Optional[int], notes: str = "") -> None:
        cycle = self._resolve_cycle(cycle_id)
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
        self._update_remaining_qty(cycle)
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
        if self.active_cycle == cycle:
            self.active_cycle = None
        deal = self._get_deal_for_cycle(cycle)
        if not deal:
            if cycle.executed_qty <= 0:
                self._log_warn(
                    f"close_without_exec cycle_id={cycle.cycle_id} exec_qty={cycle.executed_qty:.8f}"
                )
            return
        if cycle.entry_avg <= 0:
            self._log_warn(
                f"close_with_entry_avg_zero cycle_id={cycle.cycle_id} entry_avg={cycle.entry_avg:.8f}"
            )
            return
        if deal.status == DealStatus.CLOSED:
            return
        if cycle.remaining_qty > 0:
            self._log_warn(
                f"close_with_remaining cycle_id={cycle.cycle_id} remaining={cycle.remaining_qty:.8f}"
            )
        deal.exit_avg = cycle.exit_avg
        deal.status = DealStatus.CLOSED
        deal.realized_pnl = cycle.realized_pnl_quote
        deal.unrealized_pnl = 0.0
        deal.qty = cycle.executed_qty
        deal.closed_ts = cycle.closed_ts
        deal.duration_ms = (
            int((deal.closed_ts - deal.opened_ts) * 1000.0) if deal.closed_ts else None
        )
        if self._logger:
            self._logger(
                f"[DEAL_CLOSE] id={deal.deal_id} dir={deal.direction} "
                f"entry_avg={deal.entry_avg:.8f} exit_avg={deal.exit_avg:.8f} "
                f"qty={deal.qty:.8f} realized={deal.realized_pnl:.8f}"
            )
        if self.active_deal == deal:
            self.active_deal = None

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
        if cycle.remaining_qty <= 0:
            return
        if cycle.direction == "SHORT":
            unreal = (cycle.entry_avg - mark_price) * cycle.remaining_qty
        else:
            unreal = (mark_price - cycle.entry_avg) * cycle.remaining_qty
        unreal -= cycle.fees_quote
        cycle.unrealized_pnl_quote = unreal
        now = time.monotonic()
        if self._logger and (now - self._last_unreal_log_ts) >= 2.0:
            self._last_unreal_log_ts = now
            self._logger(
                f"[LEDGER_UNREAL] id={cycle.cycle_id} unreal={unreal:.8f} side={mark_side}"
            )
        deal = self._get_deal_for_cycle(cycle)
        if deal and deal.status == DealStatus.OPEN:
            deal.unrealized_pnl = unreal

    def get_cycle_rows(self, limit: int = 200) -> list[dict]:
        rows: list[dict] = []
        for cycle in reversed(self.cycles):
            if len(rows) >= limit:
                break
            unreal = cycle.unrealized_pnl_quote if cycle.status == CycleStatus.OPEN else None
            exit_avg = cycle.exit_avg if cycle.status == CycleStatus.CLOSED else None
            rows.append(
                {
                    "time_open": cycle.started_ts,
                    "cycle_id": cycle.cycle_id,
                    "direction": cycle.direction,
                    "entry_avg": cycle.entry_avg,
                    "exit_avg": exit_avg,
                    "executed_qty": cycle.executed_qty,
                    "closed_qty": cycle.closed_qty,
                    "remaining_qty": cycle.remaining_qty,
                    "status": cycle.status.value,
                    "unreal_pnl": unreal,
                    "realized_pnl": cycle.realized_pnl_quote,
                }
            )
        return rows

    def get_open_deal(self) -> Optional[Deal]:
        deal = self.active_deal
        if not deal or deal.status != DealStatus.OPEN:
            return None
        if deal.qty <= 0:
            return None
        if deal.entry_avg <= 0:
            self._log_warn(
                f"display_entry_avg_zero deal_id={deal.deal_id} entry_avg={deal.entry_avg:.8f}"
            )
            return None
        return deal

    def list_closed_deals(self, limit: int = 200) -> list[Deal]:
        deals: list[Deal] = []
        for deal in reversed(self.deals):
            if len(deals) >= limit:
                break
            if deal.status != DealStatus.CLOSED:
                continue
            if deal.qty <= 0 or deal.entry_avg <= 0:
                self._log_warn(
                    f"display_skip deal_id={deal.deal_id} entry_avg={deal.entry_avg:.8f} qty={deal.qty:.8f}"
                )
                continue
            deals.append(deal)
        return deals
