# BSC Radar MVP

`radar_bsc_mvp.py` is a minimal desktop GUI that builds a BSC token universe from DexScreener, selects top candidates, and enriches them with onchain Transfer activity from the public BSC RPC.

## How to run

1. Ensure dependencies are available: `python`, `requests`, `pandas`, and `PySide6`.
2. From the repository root, launch:

```bash
python radar_bsc_mvp/radar_bsc_mvp.py
```

## Workflow (button order)

1. **Build Universe** — pulls BSC pairs from DexScreener and filters by liquidity/volume.
2. **Select Candidates** — ranks the universe and picks the Top-N tokens.
3. **Enrich Onchain** — fetches Transfer logs from BSC RPC for the last 120 minutes.
4. **Save CSV** — saves the enriched table to disk.
