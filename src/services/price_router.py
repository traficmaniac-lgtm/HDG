from __future__ import annotations

import time
from typing import Optional

from src.core.models import HealthState, PriceState, Settings


class PriceRouter:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ws_bid: Optional[float] = None
        self._ws_ask: Optional[float] = None
        self._http_bid: Optional[float] = None
        self._http_ask: Optional[float] = None
        self._last_ws_ts: Optional[float] = None
        self._last_http_ts: Optional[float] = None
        self._stable_bid: Optional[float] = None
        self._stable_ask: Optional[float] = None
        self._stable_mid: Optional[float] = None
        self._stable_ts: Optional[float] = None
        self._stable_source = "NONE"
        self._last_source = "NONE"
        self._last_switch_reason = ""
        self._source_hold_until_ts: Optional[float] = None
        self._last_hold_remaining_ms: Optional[int] = None
        self._ws_fresh_streak = 0
        self._ws_stable_since: Optional[float] = None
        self._ws_connected = False
        self._ws_no_first_tick_count = 0
        self._ws_stale_ticks_count = 0
        self._last_good_bid: Optional[float] = None
        self._last_good_ask: Optional[float] = None
        self._last_good_mid: Optional[float] = None
        self._last_good_ts: Optional[float] = None
        self._last_good_source: str = "NONE"
        self._tick_size: Optional[float] = None
        self._log_queue: list[str] = []
        self._last_source_log_ts = 0.0
        self._last_effective_source = "NONE"
        self._last_summary_log_ts = 0.0
        self._last_summary_payload: Optional[tuple[object, ...]] = None

    def update_ws(self, bid: float, ask: float) -> None:
        self._ws_bid = bid
        self._ws_ask = ask
        now = time.monotonic()
        if self._last_ws_ts is None:
            self._ws_fresh_streak = 1
            self._ws_stable_since = now
        else:
            gap_ms = (now - self._last_ws_ts) * 1000.0
            if gap_ms > self._settings.ws_fresh_ms:
                self._ws_fresh_streak = 1
                self._ws_stable_since = now
            else:
                self._ws_fresh_streak += 1
                if self._ws_stable_since is None:
                    self._ws_stable_since = now
        self._last_ws_ts = now

    def update_http(self, bid: float, ask: float) -> None:
        self._http_bid = bid
        self._http_ask = ask
        self._last_http_ts = time.monotonic()

    def set_tick_size(self, tick_size: Optional[float]) -> None:
        self._tick_size = tick_size

    def set_ws_connected(self, value: bool) -> None:
        self._ws_connected = value

    def record_ws_issue(self, reason: str) -> None:
        reason_lower = reason.lower()
        if "no first tick" in reason_lower:
            self._ws_no_first_tick_count += 1
        elif "stale ticks" in reason_lower:
            self._ws_stale_ticks_count += 1

    def get_ws_issue_counts(self) -> tuple[int, int]:
        return self._ws_no_first_tick_count, self._ws_stale_ticks_count

    def _is_source_valid(
        self,
        source: str,
        ws_age_ms: Optional[int],
        http_age_ms: Optional[int],
    ) -> bool:
        if source == "WS":
            if self._ws_bid is None or self._ws_ask is None:
                return False
            if ws_age_ms is None:
                return False
            grace_ms = int(getattr(self._settings, "ws_stale_grace_ms", 0) or 0)
            return ws_age_ms <= self._settings.ws_stale_ms + grace_ms
        if source == "HTTP":
            if self._http_bid is None or self._http_ask is None:
                return False
            if http_age_ms is None:
                return False
            grace_ms = int(getattr(self._settings, "ws_stale_grace_ms", 0) or 0)
            return http_age_ms <= self._settings.http_fresh_ms + grace_ms
        return False

    def consume_logs(self) -> list[str]:
        if not self._log_queue:
            return []
        logs = list(self._log_queue)
        self._log_queue.clear()
        return logs

    def is_ws_stale(self) -> bool:
        if self._last_ws_ts is None:
            return True
        age_ms = (time.monotonic() - self._last_ws_ts) * 1000
        return age_ms > self._settings.ws_stale_ms

    def get_best_quote(self) -> tuple[
        Optional[float],
        Optional[float],
        Optional[float],
        str,
        Optional[int],
        Optional[int],
        Optional[int],
        bool,
        bool,
        Optional[int],
    ]:
        now = time.monotonic()
        ws_age_ms = (
            int((now - self._last_ws_ts) * 1000) if self._last_ws_ts is not None else None
        )
        http_age_ms = (
            int((now - self._last_http_ts) * 1000)
            if self._last_http_ts is not None
            else None
        )
        ws_fresh = ws_age_ms is not None and ws_age_ms <= self._settings.ws_fresh_ms
        http_fresh = http_age_ms is not None and http_age_ms <= self._settings.http_fresh_ms
        http_blind_ms = int(
            getattr(
                self._settings,
                "http_blind_ms",
                max(self._settings.http_fresh_ms * 2, 1500),
            )
        )
        ws_blind_ms = int(
            getattr(
                self._settings,
                "ws_blind_ms",
                max(self._settings.ws_stale_ms * 2, self._settings.ws_fresh_ms),
            )
        )
        http_blind = http_age_ms is None or http_age_ms > http_blind_ms
        ws_blind = ws_age_ms is None or ws_age_ms > ws_blind_ms
        data_blind = http_blind and ws_blind

        stable_ms = (
            int((now - self._ws_stable_since) * 1000)
            if self._ws_stable_since is not None
            else 0
        )
        ws_stable_required_ms = int(
            getattr(
                self._settings,
                "ws_stable_required_ms",
                self._settings.ws_switch_hysteresis_ms,
            )
        )
        ws_acceptable = ws_fresh and stable_ms >= ws_stable_required_ms

        hold_remaining_ms = 0
        min_hold_ms = int(getattr(self._settings, "min_source_hold_ms", 0) or 0)
        if self._source_hold_until_ts is not None:
            hold_remaining_ms = max(
                0, int((self._source_hold_until_ts - now) * 1000)
            )

        effective_source = self._stable_source
        current_has_quote = False
        if effective_source == "WS":
            current_has_quote = self._ws_bid is not None and self._ws_ask is not None
        elif effective_source == "HTTP":
            current_has_quote = self._http_bid is not None and self._http_ask is not None
        force_break_hold = (
            (effective_source == "WS" and not ws_acceptable) or not current_has_quote
        )
        if (
            effective_source in {"WS", "HTTP"}
            and hold_remaining_ms > 0
            and not force_break_hold
        ):
            self._last_switch_reason = "hold"
        else:
            if effective_source == "WS":
                if not ws_acceptable:
                    http_has_quote = self._http_bid is not None and self._http_ask is not None
                    if http_has_quote:
                        effective_source = "HTTP"
                        self._last_switch_reason = "stale"
                    else:
                        effective_source = "NONE"
                        self._last_switch_reason = "no_fresh_data"
                else:
                    self._last_switch_reason = "ws_fresh"
            elif effective_source == "HTTP":
                if ws_acceptable:
                    effective_source = "WS"
                    self._last_switch_reason = "stable"
                elif http_fresh:
                    self._last_switch_reason = "http_fresh"
                else:
                    effective_source = "NONE"
                    self._last_switch_reason = "no_fresh_data"
            else:
                if ws_acceptable:
                    effective_source = "WS"
                    self._last_switch_reason = "stable"
                elif http_fresh:
                    effective_source = "HTTP"
                    self._last_switch_reason = "http_fresh"
                else:
                    effective_source = "NONE"
                    self._last_switch_reason = "no_fresh_data"

            if effective_source != self._stable_source and effective_source != "NONE":
                from_source = self._stable_source
                if min_hold_ms > 0:
                    self._source_hold_until_ts = now + (min_hold_ms / 1000.0)
                else:
                    self._source_hold_until_ts = None
                hold_remaining_ms = (
                    max(0, int((self._source_hold_until_ts - now) * 1000))
                    if self._source_hold_until_ts is not None
                    else 0
                )
                if from_source in {"WS", "HTTP"}:
                    if from_source == "WS" and effective_source == "HTTP":
                        reason = "stale"
                    elif from_source == "HTTP" and effective_source == "WS":
                        reason = "stable"
                    else:
                        reason = self._last_switch_reason
                    self._log_queue.append(
                        "[SOURCE_LATCH] "
                        f"from={from_source} to={effective_source} reason={reason} "
                        f"hold_ms={min_hold_ms}"
                    )
            elif not current_has_quote:
                self._source_hold_until_ts = None
                hold_remaining_ms = 0
        http_has_quote = self._http_bid is not None and self._http_ask is not None
        if http_fresh and http_has_quote and effective_source == "NONE":
            effective_source = "HTTP"
            self._last_switch_reason = "http_fresh"
        self._last_hold_remaining_ms = hold_remaining_ms

        bid = None
        ask = None
        quote_source = "NONE"
        if effective_source == "WS" and ws_fresh:
            bid = self._ws_bid
            ask = self._ws_ask
            quote_source = "WS"
        elif http_fresh:
            bid = self._http_bid
            ask = self._http_ask
            quote_source = "HTTP"

        if bid is not None and ask is not None and bid > ask:
            self._log_queue.append(
                "[DATA] quote_inverted "
                f"bid={bid:.8f} ask={ask:.8f} src={quote_source}"
            )
            bid, ask = min(bid, ask), max(bid, ask)

        mid = (bid + ask) / 2.0 if bid is not None and ask is not None else None
        if mid is not None and self._tick_size:
            mid = round(mid / self._tick_size) * self._tick_size
        if mid is not None:
            self._last_good_bid = bid
            self._last_good_ask = ask
            self._last_good_mid = mid
            self._last_good_ts = now
            self._last_good_source = quote_source

        if data_blind:
            effective_source = "NONE"
            quote_source = "NONE"
        from_cache = False
        cache_age_ms = None
        if mid is None:
            if self._last_good_ts is not None:
                cache_age_ms = int((now - self._last_good_ts) * 1000)
                bid = self._last_good_bid
                ask = self._last_good_ask
                mid = self._last_good_mid
                quote_source = self._last_good_source
                from_cache = True
        if data_blind:
            quote_source = "NONE"
        if mid is not None:
            min_interval_s = 0.2
            should_update = (
                self._stable_ts is None or (now - self._stable_ts) >= min_interval_s
            )
            if should_update:
                self._stable_bid = bid
                self._stable_ask = ask
                self._stable_mid = mid
                self._stable_ts = self._last_good_ts if from_cache else now
                self._stable_source = quote_source if quote_source != "NONE" else effective_source
        elif from_cache:
            self._stable_bid = bid
            self._stable_ask = ask
            self._stable_mid = mid
            self._stable_ts = self._last_good_ts
            self._stable_source = quote_source if quote_source != "NONE" else effective_source
        else:
            self._stable_bid = None
            self._stable_ask = None
            self._stable_mid = None
            self._stable_ts = None
            self._stable_source = "NONE"

        age_ref = self._stable_ts
        if from_cache and self._last_good_ts is not None:
            age_ref = self._last_good_ts
        mid_age_ms = int((now - age_ref) * 1000) if age_ref is not None else None
        chosen_source = quote_source if quote_source != "NONE" else self._stable_source
        return (
            self._stable_bid,
            self._stable_ask,
            self._stable_mid,
            chosen_source,
            ws_age_ms,
            http_age_ms,
            mid_age_ms,
            data_blind,
            from_cache,
            cache_age_ms,
        )

    def build_price_state(self) -> tuple[PriceState, HealthState]:
        (
            bid,
            ask,
            mid,
            source,
            ws_age_ms,
            http_age_ms,
            mid_age_ms,
            data_blind,
            from_cache,
            cache_age_ms,
        ) = self.get_best_quote()

        if source != self._last_source:
            self._last_source = source

        now = time.monotonic()
        if source != self._last_effective_source:
            if now - self._last_source_log_ts >= 5.0:
                self._last_source_log_ts = now
                ws_age_label = ws_age_ms if ws_age_ms is not None else "?"
                hold_label = (
                    str(self._last_hold_remaining_ms)
                    if self._last_hold_remaining_ms is not None
                    else "?"
                )
                self._log_queue.append(
                    "[DATA] "
                    f"chosen_source={source} last_source={self._last_effective_source} "
                    f"hold_remaining_ms={hold_label} "
                    f"reason={self._last_switch_reason} ws_age={ws_age_label}"
                )
            self._last_effective_source = source

        summary_payload = (
            source,
            data_blind,
            self._ws_connected,
            ws_age_ms,
            http_age_ms,
        )
        summary_due = now - self._last_summary_log_ts >= 1.5
        if summary_due or summary_payload != self._last_summary_payload:
            self._last_summary_log_ts = now
            self._last_summary_payload = summary_payload
            ws_age_label = ws_age_ms if ws_age_ms is not None else "?"
            http_age_label = http_age_ms if http_age_ms is not None else "?"
            ws_state = "up" if self._ws_connected else "down"
            self._log_queue.append(
                "[DATA_SUMMARY] "
                f"ws_state={ws_state} last_reason={self._last_switch_reason} "
                f"ws_age_ms={ws_age_label} http_age_ms={http_age_label} "
                f"effective_source={source} data_blind={data_blind}"
            )

        price_state = PriceState(
            bid=bid,
            ask=ask,
            mid=mid,
            source=source,
            mid_age_ms=mid_age_ms,
            data_blind=data_blind,
            from_cache=from_cache,
            cache_age_ms=cache_age_ms,
        )
        health_state = HealthState(
            ws_connected=self._ws_connected,
            ws_age_ms=ws_age_ms,
            http_age_ms=http_age_ms,
            last_switch_reason=self._last_switch_reason,
        )
        return price_state, health_state

    def get_http_snapshot(
        self, required_fresh_ms: int
    ) -> tuple[bool, Optional[float], Optional[float], Optional[float], Optional[int], str]:
        if self._last_http_ts is None:
            return False, self._http_bid, self._http_ask, None, None, "no_http"
        age_ms = int((time.monotonic() - self._last_http_ts) * 1000.0)
        bid = self._http_bid
        ask = self._http_ask
        if bid is None or ask is None:
            return False, bid, ask, None, age_ms, "no_quote"
        if age_ms > required_fresh_ms:
            return False, bid, ask, None, age_ms, "stale"
        mid = (bid + ask) / 2.0
        if self._tick_size:
            mid = round(mid / self._tick_size) * self._tick_size
        return True, bid, ask, mid, age_ms, "fresh"

    def get_mid_snapshot(
        self, required_fresh_ms: int
    ) -> tuple[bool, Optional[float], Optional[int], str, str]:
        price_state, _ = self.build_price_state()
        mid = price_state.mid
        age_ms = price_state.mid_age_ms
        source = price_state.source
        if price_state.data_blind:
            return False, mid, age_ms, source, "data_blind"
        if mid is None:
            return False, None, age_ms, source, "no_mid"
        if age_ms is None:
            return False, mid, None, source, "no_age"
        if age_ms > required_fresh_ms:
            return False, mid, age_ms, source, "stale"
        return True, mid, age_ms, source, "fresh"
