# BSC Radar MVP v0.2.0 (DexScreener + BSC RPC)

Desktop GUI app that builds a DexScreener universe, selects candidates, enriches with BSC onchain Transfer logs, and plots signals.

## Key features

- Correct DexScreener endpoints (`/latest/dex/search`, `/latest/dex/pairs/{chain}/{pair}`)
- Separate DexScreener base URL + BSC RPC URL inputs
- Simple signal scoring (HOT/WARM/NEUTRAL/RISKY) with color tags
- Saves CSV/JSONL to `radar_bsc_mvp/output/` with timestamps

## Windows запуск

```powershell
cd $env:USERPROFILE\Desktop\HDG\radar_bsc_mvp
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

## Typical workflow

1. **Build Universe** (DexScreener search по запросам: usdt bsc, wbnb bsc, busd bsc, pancakeswap bsc, meme bsc, ai bsc, trending bsc)
2. **Select Candidates** (фильтр по min liquidity / min volume + Top N)
3. **Enrich Onchain** (BSC RPC `eth_getLogs` Transfer события)
4. **Plot Selected** (графики для выбранной строки)
5. **Save CSV / Save JSONL** (в `output/`)

## Notes

- DexScreener отдаёт snapshot-данные; график цены рисуется в snapshot-режиме.
- Если RPC начинает ограничивать, уменьшите Top N, Lookback minutes, или Max logs per token.
