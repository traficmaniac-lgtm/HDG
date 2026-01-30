# REPORT

- 0.7.29.2: Filter reconcile trades to the active cycle, gate watchdog on ENTRY, relax HTTP entry freshness, add TP maker/cross logs, and rate-limit WS summaries.
- 0.7.29.3: Deterministic ENTRY follow-top plus strict 2-phase EXIT (maker→cross), watchdog only for real stalls.
- 0.7.29.7: Hardened reconcile/recover against phantom fills, enforced entry reprice cooldown/min-tick gating, added explicit ENTRY/EXIT markers, and expanded invariant tests + checklist.
- 0.7.29.8: Fix freeze-after-start: data_blind semantics + entry deadlines + progress watchdog.
- 0.7.29.9: Fixed UI freeze via buffered log flush + throttled noisy logs.
