from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class OrderRole(Enum):
    ENTRY = "ENTRY"
    EXIT_TP = "EXIT_TP"
    EXIT_CROSS = "EXIT_CROSS"
    CANCEL_ONLY = "CANCEL_ONLY"


@dataclass
class OrderMeta:
    client_id: str
    order_id: Optional[int]
    symbol: str
    cycle_id: int
    direction: str
    role: OrderRole
    side: str
    price: float
    orig_qty: float
    created_ts: float
    last_update_ts: float
    status: str


class OrderRegistry:
    def __init__(self) -> None:
        self.by_client: dict[str, OrderMeta] = {}
        self.by_order_id: dict[int, str] = {}

    def register_new(self, meta: OrderMeta) -> None:
        if meta.client_id in self.by_client:
            raise ValueError(f"client_id already registered: {meta.client_id}")
        self.by_client[meta.client_id] = meta
        if isinstance(meta.order_id, int):
            self.by_order_id[meta.order_id] = meta.client_id

    def get_meta_by_client_id(self, client_id: str) -> Optional[OrderMeta]:
        return self.by_client.get(client_id)

    def bind_order_id(self, client_id: str, order_id: int) -> None:
        meta = self.by_client.get(client_id)
        if meta is None:
            return
        meta.order_id = order_id
        meta.last_update_ts = time.time()
        self.by_order_id[order_id] = client_id

    def update_status(
        self,
        client_id: str,
        status: str,
        filled_qty_delta: float = 0,
    ) -> None:
        meta = self.by_client.get(client_id)
        if meta is None:
            return
        meta.status = status
        meta.last_update_ts = time.time()

    def get_meta_by_order_id(self, order_id: int) -> Optional[OrderMeta]:
        client_id = self.by_order_id.get(order_id)
        if not client_id:
            return None
        return self.by_client.get(client_id)

    def get_meta_by_role_cycle_direction(
        self,
        cycle_id: int,
        role: OrderRole,
        direction: str,
    ) -> Optional[OrderMeta]:
        for meta in self.by_client.values():
            if meta.cycle_id != cycle_id:
                continue
            if meta.role != role:
                continue
            if meta.direction != direction:
                continue
            return meta
        return None

    def get_active_for_role(self, cycle_id: int, role: OrderRole) -> Optional[OrderMeta]:
        active_status = {"NEW", "WORKING", "PARTIAL"}
        for meta in self.by_client.values():
            if meta.cycle_id != cycle_id:
                continue
            if meta.role != role:
                continue
            if meta.status in active_status:
                return meta
        return None

    def mark_replaced(
        self,
        old_client_id: str,
        new_client_id: str,
        new_order_id: Optional[int],
    ) -> None:
        now = time.time()
        old_meta = self.by_client.get(old_client_id)
        if old_meta is not None:
            old_meta.status = "CANCELED"
            old_meta.last_update_ts = now
        new_meta = self.by_client.get(new_client_id)
        if new_meta is not None:
            new_meta.last_update_ts = now
            new_meta.status = "WORKING"
            if isinstance(new_order_id, int):
                new_meta.order_id = new_order_id
                self.by_order_id[new_order_id] = new_client_id

    def prune_old(self, max_age_s: float = 3600) -> None:
        now = time.time()
        stale_clients = [
            client_id
            for client_id, meta in self.by_client.items()
            if now - meta.last_update_ts > max_age_s
        ]
        for client_id in stale_clients:
            meta = self.by_client.pop(client_id, None)
            if meta and isinstance(meta.order_id, int):
                self.by_order_id.pop(meta.order_id, None)
