from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc(value: str) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def hour_bucket(ts: datetime | None = None) -> str:
    now = (ts or utc_now()).astimezone(timezone.utc)
    return now.strftime("%Y-%m-%dT%H")


@dataclass
class EventWatchState:
    event_id: str
    first_seen_utc: str
    last_seen_utc: str
    total_attempts: int
    hard_failure_streak: int
    provider_429_streak: int
    provider_530_streak: int
    last_failure_reason: str


@dataclass
class GlobalWatchState:
    hour_bucket_utc: str
    holds_this_hour: int


class Watchdog:
    def __init__(self, state_path: Path, enabled: bool, settings: dict[str, Any] | Any) -> None:
        self.state_path = Path(state_path).resolve()
        self.enabled = bool(enabled)
        self.settings = settings if isinstance(settings, dict) else {
            k: getattr(settings, k)
            for k in dir(settings)
            if (not k.startswith("_")) and (not callable(getattr(settings, k)))
        }
        self._state: dict[str, Any] = {"version": 1, "events": {}, "global": {}}
        self.load()

    def _get(self, key: str, default: Any) -> Any:
        try:
            return self.settings.get(key, default)
        except Exception:
            return default

    def _as_event(self, payload: dict[str, Any]) -> EventWatchState:
        now_iso = utc_now().isoformat()
        return EventWatchState(
            event_id=str(payload.get("event_id", "") or ""),
            first_seen_utc=str(payload.get("first_seen_utc", "") or now_iso),
            last_seen_utc=str(payload.get("last_seen_utc", "") or now_iso),
            total_attempts=max(0, int(payload.get("total_attempts", 0) or 0)),
            hard_failure_streak=max(0, int(payload.get("hard_failure_streak", 0) or 0)),
            provider_429_streak=max(0, int(payload.get("provider_429_streak", 0) or 0)),
            provider_530_streak=max(0, int(payload.get("provider_530_streak", 0) or 0)),
            last_failure_reason=str(payload.get("last_failure_reason", "") or ""),
        )

    def _as_global(self, payload: dict[str, Any]) -> GlobalWatchState:
        return GlobalWatchState(
            hour_bucket_utc=str(payload.get("hour_bucket_utc", "") or hour_bucket()),
            holds_this_hour=max(0, int(payload.get("holds_this_hour", 0) or 0)),
        )

    def load(self) -> dict[str, Any]:
        try:
            if not self.state_path.exists():
                self._state = {
                    "version": 1,
                    "updated_at_utc": utc_now().isoformat(),
                    "events": {},
                    "global": asdict(GlobalWatchState(hour_bucket(), 0)),
                }
                self.save()
                return self._state
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("invalid watchdog state payload")
            events = payload.get("events", {})
            global_row = payload.get("global", {})
            if not isinstance(events, dict):
                events = {}
            if not isinstance(global_row, dict):
                global_row = {}
            clean_events: dict[str, Any] = {}
            for event_id, raw in events.items():
                if not isinstance(raw, dict):
                    continue
                row = self._as_event(raw)
                if not row.event_id:
                    row.event_id = str(event_id or "").strip()
                if not row.event_id:
                    continue
                clean_events[row.event_id] = asdict(row)
            self._state = {
                "version": 1,
                "updated_at_utc": str(payload.get("updated_at_utc", "") or utc_now().isoformat()),
                "events": clean_events,
                "global": asdict(self._as_global(global_row)),
            }
            return self._state
        except Exception:
            self._state = {
                "version": 1,
                "updated_at_utc": utc_now().isoformat(),
                "events": {},
                "global": asdict(GlobalWatchState(hour_bucket(), 0)),
            }
            return self._state

    def save(self) -> bool:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            state = dict(self._state or {})
            state["version"] = 1
            state["updated_at_utc"] = utc_now().isoformat()
            self.state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        except Exception:
            return False

    def _event(self, event_id: str) -> EventWatchState:
        key = str(event_id or "").strip() or "unknown-event"
        events = self._state.setdefault("events", {})
        row = events.get(key, {})
        if not isinstance(row, dict):
            row = {}
        state = self._as_event({"event_id": key, **row})
        events[key] = asdict(state)
        return state

    def _reset_event_if_stale(self, state: EventWatchState, now: datetime) -> EventWatchState:
        wallclock_limit = max(1, int(self._get("max_event_wallclock_minutes", 20) or 20))
        last_touch = parse_utc(state.last_seen_utc) or parse_utc(state.first_seen_utc)
        if last_touch is None:
            return state
        idle_min = (now - last_touch).total_seconds() / 60.0
        if idle_min < float(wallclock_limit):
            return state
        now_iso = now.isoformat()
        state.first_seen_utc = now_iso
        state.last_seen_utc = now_iso
        state.total_attempts = 0
        state.hard_failure_streak = 0
        state.provider_429_streak = 0
        state.provider_530_streak = 0
        state.last_failure_reason = ""
        return state

    def begin_event(self, event_id: str) -> EventWatchState:
        state = self._event(event_id)
        now = utc_now()
        state = self._reset_event_if_stale(state, now)
        now_iso = now.isoformat()
        if not state.first_seen_utc:
            state.first_seen_utc = now_iso
        state.last_seen_utc = now_iso
        state.total_attempts = max(0, int(state.total_attempts)) + 1
        self._state.setdefault("events", {})[state.event_id] = asdict(state)
        self.save()
        return state

    def register_success(self, event_id: str) -> None:
        state = self._event(event_id)
        state.last_seen_utc = utc_now().isoformat()
        state.hard_failure_streak = 0
        state.provider_429_streak = 0
        state.provider_530_streak = 0
        state.last_failure_reason = ""
        if bool(self._get("retry_reset_on_success", True)):
            state.total_attempts = 0
        self._state.setdefault("events", {})[state.event_id] = asdict(state)
        self.save()

    def register_hard_failure(self, event_id: str, reason: str) -> None:
        state = self._event(event_id)
        state.last_seen_utc = utc_now().isoformat()
        state.hard_failure_streak = max(0, int(state.hard_failure_streak)) + 1
        state.last_failure_reason = str(reason or "")[:220]
        self._state.setdefault("events", {})[state.event_id] = asdict(state)
        self.save()

    def register_provider_failure(self, event_id: str, http_code: int) -> None:
        state = self._event(event_id)
        state.last_seen_utc = utc_now().isoformat()
        code = int(http_code or 0)
        if code == 429:
            state.provider_429_streak = max(0, int(state.provider_429_streak)) + 1
            state.provider_530_streak = 0
        elif code == 530:
            state.provider_530_streak = max(0, int(state.provider_530_streak)) + 1
            state.provider_429_streak = 0
        state.last_failure_reason = f"http_{code}" if code else str(state.last_failure_reason or "")
        self._state.setdefault("events", {})[state.event_id] = asdict(state)
        self.save()

    def should_abort_event(self, event_id: str) -> tuple[bool, str]:
        if not self.enabled:
            return False, ""
        state = self._event(event_id)
        max_attempts = max(1, int(self._get("max_event_total_attempts", 6) or 6))
        if int(state.total_attempts) >= max_attempts:
            return True, f"max_event_total_attempts:{state.total_attempts}/{max_attempts}"

        wallclock_limit = max(1, int(self._get("max_event_wallclock_minutes", 20) or 20))
        first_seen = parse_utc(state.first_seen_utc)
        if first_seen is not None:
            elapsed_min = (utc_now() - first_seen).total_seconds() / 60.0
            if elapsed_min >= float(wallclock_limit):
                return True, f"max_event_wallclock_minutes:{int(elapsed_min)}/{wallclock_limit}"

        hard_limit = max(1, int(self._get("max_same_hard_failure_streak", 3) or 3))
        if int(state.hard_failure_streak) >= hard_limit:
            return True, f"max_same_hard_failure_streak:{state.hard_failure_streak}/{hard_limit}"

        max_530 = max(1, int(self._get("max_provider_530_streak", 6) or 6))
        if int(state.provider_530_streak) >= max_530:
            return True, f"max_provider_530_streak:{state.provider_530_streak}/{max_530}"

        max_429 = max(1, int(self._get("max_provider_429_streak", 4) or 4))
        if int(state.provider_429_streak) >= max_429:
            return True, f"max_provider_429_streak:{state.provider_429_streak}/{max_429}"

        return False, ""

    def should_hold_global(self) -> tuple[bool, str]:
        if not self.enabled:
            return False, ""
        limit = max(1, int(self._get("max_global_holds_per_hour", 12) or 12))
        current_bucket = hour_bucket()
        global_state = self._as_global(dict(self._state.get("global", {}) or {}))
        if str(global_state.hour_bucket_utc or "") != current_bucket:
            global_state.hour_bucket_utc = current_bucket
            global_state.holds_this_hour = 0
        projected = int(global_state.holds_this_hour) + 1
        if projected > limit:
            self._state["global"] = asdict(global_state)
            self.save()
            return True, f"max_global_holds_per_hour:{global_state.holds_this_hour}/{limit}"
        global_state.holds_this_hour = projected
        self._state["global"] = asdict(global_state)
        self.save()
        return False, ""

    def compute_backoff_minutes(self, event_id: str, http_code: int) -> int | None:
        _ = event_id
        state = self._event(event_id)
        backoff_map = dict(self._get("backoff_on_provider_failure_minutes", {}) or {})
        code = int(http_code or 0)
        if code == 429:
            arr = list(backoff_map.get("http_429", [5, 15, 30]) or [5, 15, 30])
            streak = max(1, int(state.provider_429_streak))
        elif code == 530:
            arr = list(backoff_map.get("http_530", [30, 60, 120]) or [30, 60, 120])
            streak = max(1, int(state.provider_530_streak))
        else:
            return None
        clean = [max(1, int(x)) for x in arr if str(x).strip()]
        if not clean:
            return None
        idx = min(len(clean) - 1, max(0, streak - 1))
        return int(clean[idx])
