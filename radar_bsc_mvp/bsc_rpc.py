import time
from typing import Dict, List, Optional

import requests

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
DECIMALS_SELECTOR = "0x313ce567"


class BscRpcClient:
    def __init__(self, rpc_url: str, session: Optional[requests.Session] = None) -> None:
        self.rpc_url = rpc_url
        self.session = session or requests.Session()
        self._decimals_cache: Dict[str, Optional[int]] = {}

    def _rpc_call(self, method: str, params: List[object]) -> Dict[str, object]:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        response = self.session.post(self.rpc_url, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(data["error"])
        return data

    def get_latest_block(self) -> int:
        data = self._rpc_call("eth_blockNumber", [])
        return int(data["result"], 16)

    def get_logs(self, address: str, from_block: int, to_block: int) -> List[Dict[str, object]]:
        params = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": address,
            "topics": [TRANSFER_TOPIC],
        }
        data = self._rpc_call("eth_getLogs", [params])
        return data.get("result", [])

    def get_decimals(self, address: str) -> Optional[int]:
        key = address.lower()
        if key in self._decimals_cache:
            return self._decimals_cache[key]
        call_params = {"to": address, "data": DECIMALS_SELECTOR}
        try:
            data = self._rpc_call("eth_call", [call_params, "latest"])
            raw = data.get("result")
            if not raw:
                self._decimals_cache[key] = None
                return None
            decimals = int(raw, 16)
            if decimals < 0 or decimals > 36:
                decimals = None
        except Exception:  # noqa: BLE001
            decimals = None
        self._decimals_cache[key] = decimals
        time.sleep(0.05)
        return decimals
