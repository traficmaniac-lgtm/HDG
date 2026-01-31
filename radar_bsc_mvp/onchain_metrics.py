import math
import time
from typing import Callable, Dict, List, Optional

import pandas as pd

from bsc_rpc import BscRpcClient

BLOCK_TIME_SECONDS = 3.0


def enrich_candidates(
    candidates: pd.DataFrame,
    rpc_client: BscRpcClient,
    lookback_minutes: int,
    chunk_blocks: int,
    max_logs_per_token: int,
    status_callback: Callable[[str], None],
) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    latest_block = rpc_client.get_latest_block()
    blocks_back = int((lookback_minutes * 60) / BLOCK_TIME_SECONDS)
    start_block = max(0, latest_block - blocks_back)

    enriched_rows: List[Dict[str, object]] = []
    for _, row in candidates.iterrows():
        token_address = row.get("baseAddress")
        token_symbol = row.get("baseSymbol")
        if not token_address:
            continue

        status_callback(f"Onchain {token_symbol}...")
        tx_count = 0
        transfer_volume = 0
        unique_senders = set()
        unique_receivers = set()
        raw_amounts: List[int] = []
        on_error = ""
        logs_capped = False

        try:
            chunk_count = math.ceil((latest_block - start_block + 1) / chunk_blocks)
            logs_seen = 0
            for chunk_index in range(chunk_count):
                chunk_start = start_block + chunk_index * chunk_blocks
                chunk_end = min(latest_block, chunk_start + chunk_blocks - 1)
                logs = rpc_client.get_logs(token_address, chunk_start, chunk_end)
                for log in logs:
                    topics = log.get("topics", [])
                    if len(topics) < 3:
                        continue
                    tx_count += 1
                    logs_seen += 1
                    if logs_seen >= max_logs_per_token:
                        logs_capped = True
                        break
                    try:
                        amount = int(log.get("data", "0x0"), 16)
                        transfer_volume += amount
                        raw_amounts.append(amount)
                    except ValueError:
                        pass
                    sender = f"0x{topics[1][-40:]}".lower()
                    receiver = f"0x{topics[2][-40:]}".lower()
                    unique_senders.add(sender)
                    unique_receivers.add(receiver)
                if logs_capped:
                    break
                time.sleep(0.08)
        except Exception as exc:  # noqa: BLE001
            on_error = str(exc)

        decimals = rpc_client.get_decimals(token_address)
        on_volume = None
        on_whale_count = None
        if decimals is not None:
            try:
                on_volume = transfer_volume / (10**decimals)
            except ZeroDivisionError:
                on_volume = None
            if on_volume is not None and raw_amounts:
                whale_threshold = 10_000
                divisor = 10**decimals if decimals else 1
                on_whale_count = sum(
                    1 for amount in raw_amounts if (amount / divisor) >= whale_threshold
                )

        enriched_rows.append(
            {
                **row.to_dict(),
                "on_tx_count": tx_count,
                "on_volume_raw": transfer_volume,
                "on_volume": on_volume,
                "on_unique_senders": len(unique_senders),
                "on_unique_receivers": len(unique_receivers),
                "on_whale_count": on_whale_count,
                "on_from_block": start_block,
                "on_to_block": latest_block,
                "on_logs_capped": logs_capped,
                "on_error": on_error,
            }
        )

    enriched = pd.DataFrame(enriched_rows)
    return enriched.reset_index(drop=True)
