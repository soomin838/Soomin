from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


@dataclass
class NewsPackManifest:
    root: Path
    manifest_path: str = "storage/state/news_pack_manifest.jsonl"

    def __post_init__(self) -> None:
        self.path = (self.root / self.manifest_path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _iter_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not self.path.exists():
            return rows
        try:
            for line in self.path.read_text(encoding="utf-8", errors="ignore").splitlines():
                text = str(line or "").strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except Exception:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
        except Exception:
            return []
        return rows

    def append(self, row: dict[str, Any]) -> None:
        payload = dict(row or {})
        payload.setdefault("ts_utc", datetime.now(timezone.utc).isoformat())
        payload.setdefault("status", "ready")
        payload["prompt"] = str(payload.get("prompt", "") or "")[:800]
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            return

    def has_today_duplicate(self, *, sha1: str = "", prompt_hash: str = "") -> bool:
        today_et = datetime.now(ET).date()
        for row in reversed(self._iter_rows()[-2000:]):
            ts = self._parse_utc(str(row.get("ts_utc", "") or ""))
            if ts is None:
                continue
            if ts.astimezone(ET).date() != today_et:
                continue
            if sha1 and str(row.get("sha1", "") or "") == sha1:
                return True
            if prompt_hash and str(row.get("prompt_hash", "") or "") == prompt_hash:
                return True
        return False

    def get_candidates(
        self,
        *,
        kind: str,
        tags: list[str] | None = None,
        exclude_recent_used_hours: int = 24,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        desired_kind = str(kind or "").strip().lower()
        desired_tags = {str(t or "").strip().lower() for t in (tags or []) if str(t or "").strip()}
        latest_by_url: dict[str, dict[str, Any]] = {}

        for row in self._iter_rows():
            url = str(row.get("r2_url", "") or "").strip()
            if not url:
                continue
            latest_by_url[url] = row

        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=max(0, int(exclude_recent_used_hours)))
        out: list[dict[str, Any]] = []
        for row in latest_by_url.values():
            if str(row.get("status", "") or "").strip().lower() != "ready":
                continue
            if str(row.get("kind", "") or "").strip().lower() != desired_kind:
                continue
            row_tags = {str(t or "").strip().lower() for t in (row.get("tags", []) or []) if str(t or "").strip()}
            if desired_tags and row_tags and not (desired_tags & row_tags):
                continue
            used_at = self._parse_utc(str(row.get("used_at", "") or ""))
            if used_at is not None and used_at >= recent_cutoff:
                continue
            out.append(dict(row))

        def sort_key(item: dict[str, Any]) -> tuple[float, float]:
            used_at = self._parse_utc(str(item.get("used_at", "") or ""))
            ts = self._parse_utc(str(item.get("ts_utc", "") or ""))
            used_score = used_at.timestamp() if used_at is not None else 0.0
            ts_score = ts.timestamp() if ts is not None else 0.0
            return (used_score, -ts_score)

        out.sort(key=sort_key)
        return out[: max(1, int(limit))]

    def mark_used(self, *, r2_url: str, used_by_post_id: str = "", used_at: str = "") -> None:
        clean = str(r2_url or "").strip()
        if not clean:
            return
        latest = None
        for row in reversed(self._iter_rows()):
            if str(row.get("r2_url", "") or "").strip() == clean:
                latest = row
                break
        payload = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "kind": str((latest or {}).get("kind", "") or ""),
            "tags": list((latest or {}).get("tags", []) or []),
            "provider": str((latest or {}).get("provider", "") or ""),
            "prompt": str((latest or {}).get("prompt", "") or "")[:800],
            "prompt_hash": str((latest or {}).get("prompt_hash", "") or ""),
            "local_path": str((latest or {}).get("local_path", "") or ""),
            "r2_key": str((latest or {}).get("r2_key", "") or ""),
            "r2_url": clean,
            "sha1": str((latest or {}).get("sha1", "") or ""),
            "width": int((latest or {}).get("width", 0) or 0),
            "height": int((latest or {}).get("height", 0) or 0),
            "status": "used",
            "used_at": str(used_at or datetime.now(timezone.utc).isoformat()),
            "used_by": str(used_by_post_id or ""),
        }
        self.append(payload)

    def _parse_utc(self, value: str) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

