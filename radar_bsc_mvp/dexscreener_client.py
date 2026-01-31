import time
from typing import Callable, Dict, Iterable, List, Optional, Set

import pandas as pd
import requests

DEXSCREENER_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs"


def _to_float(value: Optional[object]) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_stable(symbol: str) -> bool:
    stable_tokens = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDD", "FDUSD"}
    return symbol.upper() in stable_tokens


def _parse_pair(pair: Dict[str, object]) -> Dict[str, object]:
    liquidity = pair.get("liquidity", {}) or {}
    volume = pair.get("volume", {}) or {}
    price_change = pair.get("priceChange", {}) or {}
    base_token = pair.get("baseToken", {}) or {}
    quote_token = pair.get("quoteToken", {}) or {}

    return {
        "pairAddress": pair.get("pairAddress"),
        "dexId": pair.get("dexId"),
        "url": pair.get("url"),
        "baseSymbol": base_token.get("symbol"),
        "baseAddress": base_token.get("address"),
        "quoteSymbol": quote_token.get("symbol"),
        "quoteAddress": quote_token.get("address"),
        "priceUsd": _to_float(pair.get("priceUsd")),
        "liquidityUsd": _to_float(liquidity.get("usd")),
        "volume5m": _to_float(volume.get("m5")),
        "volume1h": _to_float(volume.get("h1")),
        "volume24h": _to_float(volume.get("h24")),
        "chg5m": _to_float(price_change.get("m5")),
        "chg1h": _to_float(price_change.get("h1")),
        "chg24h": _to_float(price_change.get("h24")),
        "fdv": _to_float(pair.get("fdv")),
        "pairCreatedAt": pair.get("pairCreatedAt"),
    }


def _fetch_pairs(session: requests.Session) -> List[Dict[str, object]]:
    response = session.get(DEXSCREENER_PAIRS_URL, timeout=20)
    response.raise_for_status()
    payload = response.json()
    return payload.get("pairs", [])


def build_universe(
    session: requests.Session,
    min_liq_usd: float,
    min_vol_1h_usd: float,
    target: int,
    exclude_stable_pairs: bool,
    status_callback: Callable[[str], None],
    max_rounds: int = 8,
    pause_s: float = 0.4,
) -> pd.DataFrame:
    unique_pairs: Dict[str, Dict[str, object]] = {}
    attempts = 0
    while len(unique_pairs) < target and attempts < max_rounds:
        attempts += 1
        status_callback(f"DexScreener fetch {attempts}/{max_rounds}...")
        retries = 0
        while retries < 3:
            try:
                pairs = _fetch_pairs(session)
                break
            except Exception as exc:  # noqa: BLE001
                retries += 1
                if retries >= 3:
                    raise RuntimeError(f"DexScreener error: {exc}") from exc
                time.sleep(0.6)
        for pair in pairs:
            if pair.get("chainId") != "bsc":
                continue
            parsed = _parse_pair(pair)
            pair_address = parsed.get("pairAddress")
            if not pair_address:
                continue
            if parsed["liquidityUsd"] < min_liq_usd:
                continue
            if parsed["volume1h"] < min_vol_1h_usd:
                continue
            if exclude_stable_pairs:
                base_symbol = parsed.get("baseSymbol") or ""
                quote_symbol = parsed.get("quoteSymbol") or ""
                if _is_stable(base_symbol) and _is_stable(quote_symbol):
                    continue
            unique_pairs[pair_address] = parsed
        if len(unique_pairs) >= target:
            break
        time.sleep(pause_s)

    universe = pd.DataFrame(list(unique_pairs.values()))
    if universe.empty:
        return universe
    universe = universe.sort_values("volume1h", ascending=False)
    return universe.reset_index(drop=True)
