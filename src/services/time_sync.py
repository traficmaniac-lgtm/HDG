from __future__ import annotations

import time
from typing import Optional

import httpx


class TimeSync:
    def __init__(self, client: httpx.Client, base_url: str) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._offset_ms = 0
        self._last_sync: Optional[float] = None

    @property
    def offset_ms(self) -> int:
        return self._offset_ms

    def timestamp_ms(self) -> int:
        return int(time.time() * 1000) + self._offset_ms

    def sync(self) -> bool:
        try:
            resp = self._client.get(f"{self._base_url}/api/v3/time")
            resp.raise_for_status()
            payload = resp.json()
            server_time = int(payload.get("serverTime", 0))
            local_time = int(time.time() * 1000)
            if server_time > 0:
                self._offset_ms = server_time - local_time
                self._last_sync = time.monotonic()
                return True
        except Exception:
            return False
        return False
