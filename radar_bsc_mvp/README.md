# BSC Radar MVP (v0.3.0)

Desktop PySide6 app that builds a rolling DexScreener universe, selects candidates, enriches with BSC onchain Transfer logs, and generates simple signals with charts.

## Install

```bash
pip install PySide6 requests pandas matplotlib python-dateutil
```

## Run (PowerShell)

```powershell
./run.ps1
```

## Typical workflow

1. **Build Universe** (pulls DexScreener `/latest/dex/pairs`, filters BSC + liquidity/volume).
2. **Select Candidates** (Top-N composite score).
3. **Enrich Onchain** (BSC RPC `eth_getLogs` Transfer events).
4. **Save CSV** or **Save JSONL**.

## Notes

- Universe uses rolling polling to avoid 404 and collect 300–600 unique BSC pairs.
- If RPC rate limits, reduce **Top N** and/or **Lookback minutes**.
- Signals are simple: TX_SPIKE, VOL_SPIKE, PRICE_MOVE, HOT, RISK.
