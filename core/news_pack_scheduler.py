from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


UTC = timezone.utc
ET = ZoneInfo("America/New_York")


class NewsPackScheduler:
    def __init__(self, *, interval_minutes_base: int = 150, interval_minutes_jitter: int = 45) -> None:
        self.interval_minutes_base = max(60, int(interval_minutes_base or 150))
        self.interval_minutes_jitter = max(0, int(interval_minutes_jitter or 45))

    def compute_next_run(self, now_utc: datetime | None = None) -> datetime:
        now = (now_utc or datetime.now(UTC)).astimezone(UTC)
        jitter = random.randint(-self.interval_minutes_jitter, self.interval_minutes_jitter)
        minutes = max(60, int(self.interval_minutes_base + jitter))
        return now + timedelta(minutes=minutes)

    def compute_backoff(self, *, failure_kind: str, now_utc: datetime | None = None) -> datetime:
        now = (now_utc or datetime.now(UTC)).astimezone(UTC)
        kind = str(failure_kind or "temporary").strip().lower()
        if kind == "rate_limit":
            minutes = random.randint(90, 180)
        elif kind == "bad_response":
            minutes = random.randint(30, 60)
        else:
            minutes = random.randint(30, 90)
        return now + timedelta(minutes=minutes)

    def compute_bootstrap_next(
        self,
        *,
        now_utc: datetime | None = None,
        min_minutes: int = 10,
        max_minutes: int = 20,
    ) -> datetime:
        now = (now_utc or datetime.now(UTC)).astimezone(UTC)
        lo = max(5, int(min_minutes or 10))
        hi = max(lo, int(max_minutes or 20))
        return now + timedelta(minutes=random.randint(lo, hi))

    def next_day_start_et(self, now_utc: datetime | None = None) -> datetime:
        now = (now_utc or datetime.now(UTC)).astimezone(ET)
        next_day = (now + timedelta(days=1)).date()
        base = datetime(next_day.year, next_day.month, next_day.day, 0, 20, tzinfo=ET)
        jitter_min = random.randint(0, 35)
        return (base + timedelta(minutes=jitter_min)).astimezone(UTC)
