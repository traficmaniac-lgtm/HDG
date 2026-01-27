from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Optional


@dataclass
class OrderBook:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_update: Optional[datetime] = None

    def apply_snapshot(self, bids: Iterable[Iterable[str]], asks: Iterable[Iterable[str]]) -> None:
        self.bids = {float(price): float(size) for price, size in bids}
        self.asks = {float(price): float(size) for price, size in asks}
        self.last_update = datetime.now(timezone.utc)

    def apply_delta(self, bids: Iterable[Iterable[str]], asks: Iterable[Iterable[str]]) -> None:
        for price, size in bids:
            self._set_level(self.bids, float(price), float(size))
        for price, size in asks:
            self._set_level(self.asks, float(price), float(size))
        self.last_update = datetime.now(timezone.utc)

    def _set_level(self, book: dict[float, float], price: float, size: float) -> None:
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size

    def is_ready(self) -> bool:
        return bool(self.bids) and bool(self.asks)

    def best_bid(self) -> Optional[float]:
        if not self.bids:
            return None
        return max(self.bids)

    def best_ask(self) -> Optional[float]:
        if not self.asks:
            return None
        return min(self.asks)

    def vwap_for_notional(self, side: str, notional: float) -> Optional[tuple[float, float]]:
        if notional <= 0:
            return None
        levels = self._sorted_levels(side)
        remaining = notional
        total_qty = 0.0
        total_notional = 0.0
        for price, qty in levels:
            level_notional = price * qty
            take = min(level_notional, remaining)
            if take <= 0:
                continue
            total_notional += take
            total_qty += take / price
            remaining -= take
            if remaining <= 0:
                break
        if total_qty <= 0 or remaining > 0:
            return None
        return total_notional / total_qty, total_qty

    def vwap_for_qty(self, side: str, qty: float) -> Optional[float]:
        if qty <= 0:
            return None
        levels = self._sorted_levels(side)
        remaining = qty
        total_qty = 0.0
        total_notional = 0.0
        for price, size in levels:
            take = min(size, remaining)
            if take <= 0:
                continue
            total_notional += take * price
            total_qty += take
            remaining -= take
            if remaining <= 0:
                break
        if total_qty <= 0 or remaining > 0:
            return None
        return total_notional / total_qty

    def _sorted_levels(self, side: str) -> list[tuple[float, float]]:
        if side.upper() == "BUY":
            return sorted(self.asks.items(), key=lambda item: item[0])
        return sorted(self.bids.items(), key=lambda item: item[0], reverse=True)
