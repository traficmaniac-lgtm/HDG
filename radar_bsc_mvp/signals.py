from typing import List, Tuple

import numpy as np
import pandas as pd


def select_candidates(universe: pd.DataFrame, top_n: int) -> pd.DataFrame:
    if universe.empty:
        return universe

    candidates = universe.copy()
    candidates["score_pre"] = (
        np.log1p(candidates["volume1h"]) * 0.5
        + np.log1p(candidates["volume5m"]) * 0.3
        + candidates["chg1h"].abs() * 1.5
        - (candidates["liquidityUsd"] < 80_000).astype(float) * 2.0
    )
    candidates = candidates.sort_values("score_pre", ascending=False).head(top_n)
    return candidates.reset_index(drop=True)


def _percentile(series: pd.Series, pct: float) -> float:
    if series.empty:
        return 0.0
    return float(series.quantile(pct))


def apply_signals(df: pd.DataFrame, lookback_minutes: int) -> pd.DataFrame:
    if df.empty:
        return df

    results = df.copy()
    results["tx_rate"] = results["on_tx_count"] / max(lookback_minutes, 1)
    results["on_volume_usd_est"] = results.apply(
        lambda row: (row["on_volume"] or 0) * (row["priceUsd"] or 0), axis=1
    )
    results["onchain_to_dex_ratio"] = results["on_volume_usd_est"] / (
        results["volume1h"] + 1
    )

    tx_p90 = _percentile(results["tx_rate"], 0.9)
    ratio_p90 = _percentile(results["onchain_to_dex_ratio"], 0.9)

    signals: List[str] = []
    reasons: List[str] = []
    scores: List[float] = []

    for _, row in results.iterrows():
        reason_parts: List[str] = []
        signal_labels: List[str] = []

        tx_rate = row["tx_rate"]
        ratio = row["onchain_to_dex_ratio"]
        chg1h = row["chg1h"]
        liquidity = row["liquidityUsd"]

        tx_spike = tx_rate >= tx_p90 and tx_rate > 0
        vol_spike = ratio >= max(0.6, ratio_p90) and ratio > 0
        price_move = abs(chg1h) >= 8
        risk = liquidity < 80_000 and chg1h > 15

        if tx_spike:
            signal_labels.append("TX_SPIKE")
            reason_parts.append(f"TX_SPIKE: {tx_rate:.2f} tx/min (P90 {tx_p90:.2f})")
        if vol_spike:
            signal_labels.append("VOL_SPIKE")
            reason_parts.append(
                f"VOL_SPIKE: onchain≈${row['on_volume_usd_est']:.0f} vs dex1h≈${row['volume1h']:.0f}"
            )
        if price_move:
            signal_labels.append("PRICE_MOVE")
            reason_parts.append(f"PRICE_MOVE: {chg1h:.1f}%")
        if risk:
            signal_labels.append("RISK")
            reason_parts.append("RISK: low liquidity + fast pump")

        hot = (tx_spike and vol_spike) or (vol_spike and price_move)
        if hot:
            signal_labels.append("HOT")
            reason_parts.append("HOT: combined spikes")

        if "RISK" in signal_labels:
            signal = "RISK"
        elif "HOT" in signal_labels:
            signal = "HOT"
        elif "VOL_SPIKE" in signal_labels:
            signal = "VOL_SPIKE"
        elif "TX_SPIKE" in signal_labels:
            signal = "TX_SPIKE"
        elif "PRICE_MOVE" in signal_labels:
            signal = "PRICE_MOVE"
        else:
            signal = "NONE"

        base_score = row.get("score_pre", 0) or 0
        score = float(base_score)
        if signal == "HOT":
            score += 8
        elif signal in {"VOL_SPIKE", "TX_SPIKE"}:
            score += 4
        elif signal == "PRICE_MOVE":
            score += 2
        if signal == "RISK":
            score -= 5

        signals.append(signal)
        reasons.append("; ".join(reason_parts) if reason_parts else "No strong signals")
        scores.append(score)

    results["signal"] = signals
    results["signal_reason"] = reasons
    results["score_final"] = scores
    results = results.sort_values("score_final", ascending=False)
    return results.reset_index(drop=True)
