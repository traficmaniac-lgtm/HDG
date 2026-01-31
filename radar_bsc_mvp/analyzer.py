import math
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Tuple

import pandas as pd

from bsc_rpc import BscRpcClient
from dexscreener_client import DexScreenerClient, build_universe

BLOCK_TIME_SECONDS = 3.0


@dataclass
class EnrichConfig:
    lookback_minutes: int
    chunk_blocks: int
    max_logs_per_token: int
    whale_threshold_raw: int


def build_universe_frame(
    client: DexScreenerClient,
    min_liq_usd: float,
    min_vol_1h_usd: float,
    target: int,
    exclude_stable_pairs: bool,
    status_callback: Callable[[str], None],
) -> pd.DataFrame:
    queries = [
        "usdt bsc",
        "wbnb bsc",
        "busd bsc",
        "pancakeswap bsc",
        "meme bsc",
        "ai bsc",
        "trending bsc",
    ]
    pairs = build_universe(
        client,
        queries=queries,
        min_liq_usd=min_liq_usd,
        min_vol_1h_usd=min_vol_1h_usd,
        exclude_stable_pairs=exclude_stable_pairs,
        target=target,
        status_callback=status_callback,
    )
    data = [pair.to_dict() for pair in pairs]
    frame = pd.DataFrame(data)
    if frame.empty:
        return frame
    frame = frame.sort_values("volume1h", ascending=False)
    return frame.reset_index(drop=True)


def select_candidates(
    universe: pd.DataFrame,
    top_n: int,
    min_liq_usd: float,
    min_vol_1h_usd: float,
) -> pd.DataFrame:
    if universe.empty:
        return universe
    filtered = universe.copy()
    filtered = filtered[filtered["liquidityUsd"] >= min_liq_usd]
    filtered = filtered[filtered["volume1h"] >= min_vol_1h_usd]
    filtered = filtered.sort_values("volume1h", ascending=False)
    return filtered.head(top_n).reset_index(drop=True)


def _build_ranges(start_block: int, end_block: int, chunk_blocks: int) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    cursor = start_block
    while cursor <= end_block:
        chunk_end = min(end_block, cursor + chunk_blocks - 1)
        ranges.append((cursor, chunk_end))
        cursor = chunk_end + 1
    return ranges


def _bucket_index(block_number: int, start_block: int, blocks_per_bucket: int) -> int:
    if blocks_per_bucket <= 0:
        return 0
    return max(0, int((block_number - start_block) / blocks_per_bucket))


def enrich_candidates(
    candidates: pd.DataFrame,
    rpc_client: BscRpcClient,
    config: EnrichConfig,
    status_callback: Callable[[str], None],
) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    latest_block = rpc_client.get_latest_block()
    blocks_back = int((config.lookback_minutes * 60) / BLOCK_TIME_SECONDS)
    start_block = max(0, latest_block - blocks_back)
    blocks_per_bucket = max(1, int(60 / BLOCK_TIME_SECONDS))
    bucket_count = max(1, int(math.ceil((latest_block - start_block + 1) / blocks_per_bucket)))

    enriched_rows: List[Dict[str, object]] = []

    for _, row in candidates.iterrows():
        token_address = row.get("baseAddress")
        token_symbol = row.get("baseSymbol")
        if not token_address:
            continue

        status_callback(f"Onchain {token_symbol}...")
        transfer_count = 0
        whale_transfers_count = 0
        whale_transfers_sum_raw = 0
        unique_senders: set[str] = set()
        unique_receivers: set[str] = set()
        on_error = ""
        logs_capped = False

        transfer_buckets = [0 for _ in range(bucket_count)]
        whale_buckets = [0 for _ in range(bucket_count)]
        whale_sum_buckets = [0 for _ in range(bucket_count)]

        ranges = _build_ranges(start_block, latest_block, config.chunk_blocks)
        batch = rpc_client.get_logs_chunked(
            token_address,
            ranges,
            config.max_logs_per_token,
            status_callback,
        )
        if batch.error:
            on_error = batch.error
            status_callback(f"RPC error for {token_symbol}: {batch.error}")
        if batch.capped:
            logs_capped = True

        for log in batch.logs:
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            transfer_count += 1
            sender = f"0x{topics[1][-40:]}".lower()
            receiver = f"0x{topics[2][-40:]}".lower()
            unique_senders.add(sender)
            unique_receivers.add(receiver)

            try:
                amount = int(log.get("data", "0x0"), 16)
            except ValueError:
                amount = 0

            block_number = int(log.get("blockNumber", "0x0"), 16)
            bucket_idx = _bucket_index(block_number, start_block, blocks_per_bucket)
            if bucket_idx >= bucket_count:
                bucket_idx = bucket_count - 1
            transfer_buckets[bucket_idx] += 1

            if amount >= config.whale_threshold_raw:
                whale_transfers_count += 1
                whale_transfers_sum_raw += amount
                whale_buckets[bucket_idx] += 1
                whale_sum_buckets[bucket_idx] += amount

        enriched_rows.append(
            {
                **row.to_dict(),
                "on_transfer_count": transfer_count,
                "on_unique_senders": len(unique_senders),
                "on_unique_receivers": len(unique_receivers),
                "on_whale_transfers_count": whale_transfers_count,
                "on_whale_transfers_sum_raw": whale_transfers_sum_raw,
                "on_from_block": start_block,
                "on_to_block": latest_block,
                "on_logs_capped": logs_capped,
                "on_error": on_error,
                "on_transfer_buckets": transfer_buckets,
                "on_whale_buckets": whale_buckets,
                "on_whale_sum_buckets": whale_sum_buckets,
            }
        )
        time.sleep(0.1)

    enriched = pd.DataFrame(enriched_rows)
    return enriched.reset_index(drop=True)


def _percentile(series: pd.Series, pct: float) -> float:
    if series.empty:
        return 0.0
    return float(series.quantile(pct))


def apply_signals(df: pd.DataFrame, lookback_minutes: int) -> pd.DataFrame:
    if df.empty:
        return df

    results = df.copy()
    results["tx_rate"] = results["on_transfer_count"] / max(lookback_minutes, 1)
    results["whale_rate"] = results["on_whale_transfers_count"] / max(lookback_minutes, 1)

    tx_p75 = _percentile(results["tx_rate"], 0.75)
    tx_p90 = _percentile(results["tx_rate"], 0.9)
    whale_p75 = _percentile(results["whale_rate"], 0.75)
    whale_p90 = _percentile(results["whale_rate"], 0.9)
    vol_p75 = _percentile(results["volume1h"], 0.75)

    scores: List[int] = []
    labels: List[str] = []
    reasons: List[str] = []

    for _, row in results.iterrows():
        score = 50
        reason_parts: List[str] = []

        tx_rate = row["tx_rate"]
        whale_rate = row["whale_rate"]
        volume = row["volume1h"]
        liquidity = row["liquidityUsd"]
        chg_1h = row["chg1h"]
        txns_1h = row.get("txns1h", 0) or 0

        if tx_rate >= tx_p90:
            score += 20
            reason_parts.append("transfer spike")
        elif tx_rate >= tx_p75:
            score += 10
            reason_parts.append("transfer above avg")

        if whale_rate >= whale_p90:
            score += 15
            reason_parts.append("whale spike")
        elif whale_rate >= whale_p75:
            score += 7
            reason_parts.append("whale activity")

        if volume >= vol_p75:
            score += 10
            reason_parts.append("high dex volume")

        if chg_1h >= 10:
            score += 5
            reason_parts.append("price up")
        elif chg_1h <= -10:
            score -= 5
            reason_parts.append("price down")

        if liquidity < 50_000:
            score -= 12
            reason_parts.append("low liquidity")

        if txns_1h < 5 and tx_rate > tx_p75:
            score -= 10
            reason_parts.append("low dex txns vs transfers")

        score = max(0, min(100, score))

        if score >= 75:
            label = "HOT"
        elif score >= 60:
            label = "WARM"
        elif score >= 40:
            label = "NEUTRAL"
        else:
            label = "RISKY"

        scores.append(score)
        labels.append(label)
        reasons.append(", ".join(reason_parts) if reason_parts else "no strong signal")

    results["signal_score"] = scores
    results["signal_label"] = labels
    results["signal_reason"] = reasons
    results = results.sort_values("signal_score", ascending=False)
    return results.reset_index(drop=True)
