from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


WINDOWS = [
    (6, 30, 9, 30),
    (10, 30, 12, 30),
    (13, 0, 15, 0),
    (16, 30, 19, 30),
    (21, 0, 23, 30),
]


def make_slot(dt_et: datetime) -> dict[str, Any]:
    dt_et = dt_et.astimezone(ET).replace(second=0, microsecond=0)
    dt_utc = dt_et.astimezone(UTC).replace(second=0, microsecond=0)
    return {
        "slot_id": str(uuid.uuid4()),
        "publish_at_et": dt_et.isoformat(),
        "publish_at_utc": dt_utc.isoformat(),
        "status": "pending",
        "post_id": "",
        "reason": "",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def generate_month_schedule(year: int, month: int, output_path: Path) -> list[dict[str, Any]]:
    days: list[dict[str, Any]] = []
    for day in range(1, 32):
        try:
            base = datetime(year, month, day, tzinfo=ET)
        except Exception:
            continue
        slots: list[dict[str, Any]] = []
        for win in WINDOWS:
            hour1, min1, hour2, min2 = win
            start_min = (hour1 * 60) + min1
            end_min = (hour2 * 60) + min2
            pick = random.randint(start_min, end_min)
            dt = base.replace(hour=pick // 60, minute=pick % 60)
            slots.append(make_slot(dt))
        for _ in range(random.randint(1, 2)):
            pick = random.randint((11 * 60), (18 * 60 + 59))
            dt = base.replace(hour=pick // 60, minute=pick % 60)
            slots.append(make_slot(dt))
        slots.sort(key=lambda x: str(x.get("publish_at_utc", "")))
        days.append(
            {
                "date": base.strftime("%Y-%m-%d"),
                "slots": slots,
            }
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(days, ensure_ascii=False, indent=2), encoding="utf-8")
    return days


@dataclass
class MonthlySchedulerConfig:
    enabled: bool = True
    timezone: str = "America/New_York"
    output_dir: str = "storage/schedules"
    publish_slots_per_day: int = 5
    buffer_slots_min: int = 1
    buffer_slots_max: int = 2
    consume_hold_slots: bool = False


class MonthlyScheduler:
    def __init__(self, root: Path, config: MonthlySchedulerConfig) -> None:
        self.root = Path(root)
        self.config = config
        self._tz = ET if str(getattr(config, "timezone", "") or "") == "America/New_York" else ZoneInfo(
            str(getattr(config, "timezone", "America/New_York"))
        )
        self._dir = (self.root / str(getattr(config, "output_dir", "storage/schedules"))).resolve()
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, year: int, month: int) -> Path:
        return self._dir / f"monthly_slots_{year:04d}_{month:02d}.json"

    def ensure_month(self, now_utc: datetime | None = None) -> Path:
        now = (now_utc or datetime.now(timezone.utc)).astimezone(self._tz)
        p = self._path(now.year, now.month)
        if not p.exists():
            generate_month_schedule(now.year, now.month, p)
        return p

    def load_month(self, now_utc: datetime | None = None) -> tuple[Path, list[dict[str, Any]]]:
        p = self.ensure_month(now_utc=now_utc)
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                raise ValueError("invalid_schedule_type")
            return p, data
        except Exception:
            data = generate_month_schedule(
                year=(now_utc or datetime.now(timezone.utc)).astimezone(self._tz).year,
                month=(now_utc or datetime.now(timezone.utc)).astimezone(self._tz).month,
                output_path=p,
            )
            return p, data

    def save_month(self, path: Path, data: list[dict[str, Any]]) -> None:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def acquire_next_pending_slot(self, now_utc: datetime, min_delay_minutes: int = 10) -> dict[str, Any] | None:
        p, data = self.load_month(now_utc=now_utc)
        threshold = now_utc + timedelta(minutes=max(1, int(min_delay_minutes)))
        best: tuple[int, int, datetime, dict[str, Any]] | None = None
        touched = False
        for di, day in enumerate(data):
            slots = list((day or {}).get("slots", []) or [])
            for si, slot in enumerate(slots):
                if str(slot.get("status", "pending") or "pending").strip().lower() != "pending":
                    continue
                try:
                    dt = datetime.fromisoformat(str(slot.get("publish_at_utc", "") or "").replace("Z", "+00:00"))
                except Exception:
                    continue
                if dt < threshold:
                    data[di]["slots"][si]["status"] = "skipped"
                    data[di]["slots"][si]["reason"] = "missed_window_offline"
                    data[di]["slots"][si]["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
                    touched = True
                    continue
                if best is None or dt < best[2]:
                    best = (di, si, dt, slot)
        if touched:
            self.save_month(p, data)
        if best is None:
            return None
        di, si, dt, slot = best
        data[di]["slots"][si]["status"] = "reserved"
        data[di]["slots"][si]["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        self.save_month(p, data)
        return {
            "slot_id": str(slot.get("slot_id", "") or ""),
            "publish_at_utc": dt.astimezone(timezone.utc),
            "path": str(p),
        }

    def mark_slot(
        self,
        slot_id: str,
        *,
        status: str,
        reason: str = "",
        post_id: str = "",
        now_utc: datetime | None = None,
    ) -> bool:
        key = str(slot_id or "").strip()
        if not key:
            return False
        p, data = self.load_month(now_utc=now_utc)
        changed = False
        for day in data:
            slots = list((day or {}).get("slots", []) or [])
            for slot in slots:
                if str(slot.get("slot_id", "") or "").strip() != key:
                    continue
                slot["status"] = str(status or "hold")
                slot["reason"] = str(reason or "")[:220]
                if post_id:
                    slot["post_id"] = str(post_id or "")
                slot["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
                changed = True
                break
            if changed:
                break
        if changed:
            self.save_month(p, data)
        return changed
