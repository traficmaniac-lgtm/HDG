v0.7.28
- Fixed cycle FSM auto-looping to return to IDLE and schedule the next cycle after completion.
- Removed the NEXT_CYCLE state and added auto-next logging for block reasons.
- Added tests for trade executor auto-next cycle scheduling and state coverage.

v0.7.26
- Fixed entry follow-top ref tracking with integer tick comparisons and state/reprice diagnostics.
- Guaranteed SELL placement/replacement on BUY fills (partial or full) with explicit BUY_FILL logs.
- Added enriched PLACE_SELL logging with price-source health details.
- Added unit tests for entry ref movement and partial-fill sell triggering.
