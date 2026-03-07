from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


UTC = timezone.utc
ET = ZoneInfo("America/New_York")


@dataclass
class NewsPackState:
    date_et: str = ""
    generated_total: int = 0
    generated_thumb_bg: int = 0
    generated_inline_bg: int = 0
    mode: str = "normal"  # normal | bootstrap | paused
    last_bootstrap_at: str = ""
    bootstrap_generated_today: int = 0
    last_rate_limit_at: str = ""
    last_success_provider: str = ""
    gemini_fallback_used_today: int = 0
    next_run_at_utc: str = ""
    last_run_at_utc: str = ""
    consecutive_failures: int = 0
    last_error: str = ""


class NewsPackStateStore:
    def __init__(self, root: Path, state_path: str = "storage/state/news_pack_state.json") -> None:
        self.root = root
        self.path = (root / state_path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> NewsPackState:
        state = NewsPackState()
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for key in asdict(state).keys():
                        if key in raw:
                            setattr(state, key, raw[key])
            except Exception:
                state = NewsPackState()
        self._reset_if_new_day(state)
        return state

    def save(self, state: NewsPackState) -> None:
        state.date_et = state.date_et or datetime.now(ET).date().isoformat()
        payload = asdict(state)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def should_run(self, state: NewsPackState, now_utc: datetime | None = None) -> bool:
        now = (now_utc or datetime.now(UTC)).astimezone(UTC)
        if not str(state.next_run_at_utc or "").strip():
            return True
        dt = self._parse_utc(str(state.next_run_at_utc or ""))
        if dt is None:
            return True
        return now >= dt

    def _reset_if_new_day(self, state: NewsPackState) -> None:
        today = datetime.now(ET).date().isoformat()
        if str(state.date_et or "") == today:
            return
        state.date_et = today
        state.generated_total = 0
        state.generated_thumb_bg = 0
        state.generated_inline_bg = 0
        state.mode = "normal"
        state.last_bootstrap_at = ""
        state.bootstrap_generated_today = 0
        state.last_rate_limit_at = ""
        state.last_success_provider = ""
        state.gemini_fallback_used_today = 0
        state.consecutive_failures = 0
        state.last_error = ""
        state.last_run_at_utc = ""
        state.next_run_at_utc = ""

    def _parse_utc(self, text: str) -> datetime | None:
        value = str(text or "").strip()
        if not value:
            return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
