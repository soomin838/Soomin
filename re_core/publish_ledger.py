from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from .brain import stable_hash


def make_ledger_key(
    *,
    event_id: str,
    cluster_id: str,
    facet: str,
    blog_id: str,
) -> str:
    raw = f"{str(event_id or '').strip()}|{str(cluster_id or '').strip()}|{str(facet or 'impact').strip() or 'impact'}|{str(blog_id or 'default').strip() or 'default'}"
    hashed = int(stable_hash(raw))
    return f"{max(0, hashed):x}".rjust(24, "0")[:24]


class PublishLedger:
    def __init__(self, path: Path, ttl_days: int = 90) -> None:
        self.path = Path(path).resolve()
        self.ttl_days = max(1, int(ttl_days))
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if not self.path.exists():
                self.path.touch()
        except Exception:
            pass

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _parse_utc(self, value: str) -> datetime | None:
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

    def _iter_records(self) -> Iterator[dict]:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                row = str(line or "").strip()
                if not row:
                    continue
                try:
                    parsed = json.loads(row)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    yield parsed

    def _is_expired(self, created_at_utc: str) -> bool:
        ts = self._parse_utc(created_at_utc)
        if ts is None:
            return True
        return ts < (self._utc_now() - timedelta(days=self.ttl_days))

    def exists(self, key: str) -> bool:
        target = str(key or "").strip()
        if not target:
            return True
        if not self.path.exists():
            return True
        try:
            for row in self._iter_records():
                if str(row.get("key", "") or "").strip() != target:
                    continue
                if self._is_expired(str(row.get("created_at_utc", "") or "")):
                    continue
                return True
            return False
        except Exception:
            # FAIL CLOSED: unreadable ledger means do not publish.
            return True

    def record(self, payload: dict) -> bool:
        row = dict(payload or {})
        row["created_at_utc"] = self._utc_now().isoformat()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            return True
        except Exception:
            return False
