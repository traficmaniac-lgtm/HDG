import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests


@dataclass
class DexPair:
    pair_address: str
    dex_id: str
    chain_id: str
    url: str
    base_symbol: str
    base_address: str
    quote_symbol: str
    quote_address: str
    price_usd: float
    liquidity_usd: float
    volume_5m: float
    volume_1h: float
    volume_24h: float
    txns_5m: int
    txns_1h: int
    txns_24h: int
    chg_5m: float
    chg_1h: float
    chg_24h: float
    fdv: float
    pair_created_at: Optional[int]

    def to_dict(self) -> Dict[str, object]:
        return {
            "pairAddress": self.pair_address,
            "dexId": self.dex_id,
            "chainId": self.chain_id,
            "url": self.url,
            "baseSymbol": self.base_symbol,
            "baseAddress": self.base_address,
            "quoteSymbol": self.quote_symbol,
            "quoteAddress": self.quote_address,
            "priceUsd": self.price_usd,
            "liquidityUsd": self.liquidity_usd,
            "volume5m": self.volume_5m,
            "volume1h": self.volume_1h,
            "volume24h": self.volume_24h,
            "txns5m": self.txns_5m,
            "txns1h": self.txns_1h,
            "txns24h": self.txns_24h,
            "chg5m": self.chg_5m,
            "chg1h": self.chg_1h,
            "chg24h": self.chg_24h,
            "fdv": self.fdv,
            "pairCreatedAt": self.pair_created_at,
        }


def _to_float(value: Optional[object]) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Optional[object]) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _is_stable(symbol: str) -> bool:
    stable_tokens = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDD", "FDUSD"}
    return symbol.upper() in stable_tokens


def _parse_txns(txns: object) -> Dict[str, int]:
    txns = txns or {}
    def _sum_txns(entry: object) -> int:
        entry = entry or {}
        return _to_int(entry.get("buys")) + _to_int(entry.get("sells"))

    return {
        "m5": _sum_txns(txns.get("m5")),
        "h1": _sum_txns(txns.get("h1")),
        "h24": _sum_txns(txns.get("h24")),
    }


def _parse_pair(pair: Dict[str, object]) -> DexPair:
    liquidity = pair.get("liquidity", {}) or {}
    volume = pair.get("volume", {}) or {}
    price_change = pair.get("priceChange", {}) or {}
    base_token = pair.get("baseToken", {}) or {}
    quote_token = pair.get("quoteToken", {}) or {}
    txns = _parse_txns(pair.get("txns"))

    return DexPair(
        pair_address=str(pair.get("pairAddress") or ""),
        dex_id=str(pair.get("dexId") or ""),
        chain_id=str(pair.get("chainId") or ""),
        url=str(pair.get("url") or ""),
        base_symbol=str(base_token.get("symbol") or ""),
        base_address=str(base_token.get("address") or ""),
        quote_symbol=str(quote_token.get("symbol") or ""),
        quote_address=str(quote_token.get("address") or ""),
        price_usd=_to_float(pair.get("priceUsd")),
        liquidity_usd=_to_float(liquidity.get("usd")),
        volume_5m=_to_float(volume.get("m5")),
        volume_1h=_to_float(volume.get("h1")),
        volume_24h=_to_float(volume.get("h24")),
        txns_5m=_to_int(txns.get("m5")),
        txns_1h=_to_int(txns.get("h1")),
        txns_24h=_to_int(txns.get("h24")),
        chg_5m=_to_float(price_change.get("m5")),
        chg_1h=_to_float(price_change.get("h1")),
        chg_24h=_to_float(price_change.get("h24")),
        fdv=_to_float(pair.get("fdv")),
        pair_created_at=pair.get("pairCreatedAt"),
    )


class DexScreenerClient:
    def __init__(self, base_url: str, session: Optional[requests.Session] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()

    def search(self, query: str) -> List[Dict[str, object]]:
        url = f"{self.base_url}/latest/dex/search"
        response = self.session.get(url, params={"q": query}, timeout=25)
        response.raise_for_status()
        payload = response.json()
        return payload.get("pairs", [])

    def pair_by_address(self, chain_id: str, pair_address: str) -> List[Dict[str, object]]:
        url = f"{self.base_url}/latest/dex/pairs/{chain_id}/{pair_address}"
        response = self.session.get(url, timeout=25)
        response.raise_for_status()
        payload = response.json()
        return payload.get("pairs", [])

    def token_pairs(self, chain_id: str, token_address: str) -> List[Dict[str, object]]:
        url = f"{self.base_url}/token-pairs/v1/{chain_id}/{token_address}"
        response = self.session.get(url, timeout=25)
        response.raise_for_status()
        payload = response.json()
        return payload.get("pairs", [])


def build_universe(
    client: DexScreenerClient,
    queries: Iterable[str],
    min_liq_usd: float,
    min_vol_1h_usd: float,
    exclude_stable_pairs: bool,
    target: int,
    status_callback,
    pause_s: float = 0.4,
) -> List[DexPair]:
    unique_pairs: Dict[str, DexPair] = {}

    for query in queries:
        status_callback(f"DexScreener search: {query}")
        for attempt in range(3):
            try:
                raw_pairs = client.search(query)
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == 2:
                    raise RuntimeError(f"DexScreener error: {exc}") from exc
                time.sleep(0.6)
        for raw in raw_pairs:
            if raw.get("chainId") != "bsc":
                continue
            parsed = _parse_pair(raw)
            if not parsed.pair_address:
                continue
            if parsed.liquidity_usd < min_liq_usd:
                continue
            if parsed.volume_1h < min_vol_1h_usd:
                continue
            if exclude_stable_pairs:
                if _is_stable(parsed.base_symbol) and _is_stable(parsed.quote_symbol):
                    continue
            unique_pairs[parsed.pair_address] = parsed
        if len(unique_pairs) >= target:
            break
        time.sleep(pause_s)

    return list(unique_pairs.values())
