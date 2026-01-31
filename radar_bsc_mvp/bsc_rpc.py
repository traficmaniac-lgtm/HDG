import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


@dataclass
class LogBatch:
    logs: List[Dict[str, object]]
    capped: bool
    error: Optional[str]


class BscRpcClient:
    def __init__(self, rpc_url: str, session: Optional[requests.Session] = None) -> None:
        self.rpc_url = rpc_url
        self.session = session or requests.Session()

    def _rpc_call(self, method: str, params: List[object]) -> Dict[str, object]:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        response = self.session.post(self.rpc_url, json=payload, timeout=25)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(data["error"])
        return data

    def get_latest_block(self) -> int:
        data = self._rpc_call("eth_blockNumber", [])
        return int(data["result"], 16)

    def get_block(self, block_number: int) -> Dict[str, object]:
        data = self._rpc_call("eth_getBlockByNumber", [hex(block_number), False])
        return data.get("result") or {}

    def get_logs(self, address: str, from_block: int, to_block: int) -> List[Dict[str, object]]:
        params = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": address,
            "topics": [TRANSFER_TOPIC],
        }
        data = self._rpc_call("eth_getLogs", [params])
        return data.get("result", [])

    def get_logs_chunked(
        self,
        address: str,
        ranges: Iterable[tuple[int, int]],
        max_logs: int,
        status_callback,
    ) -> LogBatch:
        collected: List[Dict[str, object]] = []
        capped = False
        error: Optional[str] = None

        for from_block, to_block in ranges:
            try:
                logs = self.get_logs(address, from_block, to_block)
            except Exception as exc:  # noqa: BLE001
                error = str(exc)
                break
            if logs:
                collected.extend(logs)
            if len(collected) >= max_logs:
                collected = collected[:max_logs]
                capped = True
                break
            status_callback(f"Logs {from_block}-{to_block}: {len(logs)}")
            time.sleep(0.08)
        return LogBatch(logs=collected, capped=capped, error=error)
