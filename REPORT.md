# REPORT

- 0.7.29.2: Filter reconcile trades to the active cycle, gate watchdog on ENTRY, relax HTTP entry freshness, add TP maker/cross logs, and rate-limit WS summaries.
- 0.7.29.3: Deterministic ENTRY follow-top plus strict 2-phase EXIT (maker→cross), watchdog only for real stalls.
- 0.7.29.7: Hardened reconcile/recover against phantom fills, enforced entry reprice cooldown/min-tick gating, added explicit ENTRY/EXIT markers, and expanded invariant tests + checklist.
- 0.7.29.8: Fix freeze-after-start: data_blind semantics + entry deadlines + progress watchdog.
- 0.7.29.9: Fixed UI freeze via buffered log flush + throttled noisy logs.
- 0.7.30.0: Added dual start buttons (LONG/SHORT). Shared settings. LONG unchanged.
- 0.7.30.1: UI log is event-only, debug spam moved to file, buffered flush.
- 0.7.30.2: Fix cross-close pricing, isolate timers, recover respects active exit. Reduce emergency market losses.
- 0.7.30.3: Fix FSM deadlock: real progress only, hard deadlines, stop interrupt.
- 0.7.30.4: Enforced data-blind trade gating, deterministic reconcile recovery, exit timeout cross-retry with SL-only emergency, entry ownership cleanup, and lighter UI log buffering.
  - Tests:
    - A) Start LONG and SHORT; set effective_source=NONE or data_blind=True → bot must HOLD (no order placement).
    - B) Fill ENTRY and transition to POS_OPEN/EXIT → confirm no ENTRY_CANCEL_TIMEOUT logs.
    - C) Simulate sharp price jump → UI stays responsive; reconcile recovers without SAFE_STOP loop.
    - D) Force exit timeout without SL breach → verify no emergency market; cross attempts occur; market only after SL breach.
    - E) Check UI log: responsive, last 500 lines retained, INFO_EVENT/major INFO only; debug spam in logs/debug.log.
