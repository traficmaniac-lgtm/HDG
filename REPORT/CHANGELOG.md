v0.7.26
- Fixed entry follow-top ref tracking with integer tick comparisons and state/reprice diagnostics.
- Guaranteed SELL placement/replacement on BUY fills (partial or full) with explicit BUY_FILL logs.
- Added enriched PLACE_SELL logging with price-source health details.
- Added unit tests for entry ref movement and partial-fill sell triggering.
