from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


UTC = timezone.utc
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
                    raw = json.loads(text)
                except Exception:
                    continue
                if isinstance(raw, dict):
                    rows.append(self._normalize_row(raw))
        except Exception:
            return []
        return rows

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = dict(row or {})
        payload.setdefault("ts_utc", datetime.now(UTC).isoformat())
        payload["kind"] = str(payload.get("kind", "") or "").strip().lower()
        payload["tags"] = [
            str(t or "").strip().lower()
            for t in (payload.get("tags", []) or [])
            if str(t or "").strip()
        ][:8]
        payload["provider"] = str(payload.get("provider", "") or "").strip().lower()
        payload["prompt"] = str(payload.get("prompt", "") or "")[:1200]
        payload["prompt_hash"] = str(payload.get("prompt_hash", "") or "").strip()
        payload["local_path"] = str(payload.get("local_path", "") or "").strip()
        payload["r2_key"] = str(payload.get("r2_key", "") or "").strip()
        payload["r2_url"] = str(payload.get("r2_url", "") or "").strip()
        payload["sha1"] = str(payload.get("sha1", "") or "").strip()
        payload["width"] = int(payload.get("width", 0) or 0)
        payload["height"] = int(payload.get("height", 0) or 0)
        payload["byte_size"] = int(payload.get("byte_size", 0) or 0)
        payload["status"] = str(payload.get("status", "ready") or "ready").strip().lower()
        payload["used_at"] = str(payload.get("used_at", "") or "").strip()
        payload["used_by"] = str(payload.get("used_by", "") or "").strip()
        payload["used_count"] = int(payload.get("used_count", 0) or 0)
        payload["source_mode"] = str(payload.get("source_mode", "seeded") or "seeded").strip().lower()
        payload["alt_text_template"] = str(payload.get("alt_text_template", "") or "").strip()[:220]
        payload["caption_template"] = str(payload.get("caption_template", "") or "").strip()[:220]
        payload["overlay_hook_used"] = str(payload.get("overlay_hook_used", "") or "").strip()[:120]
        quality_flags = payload.get("quality_flags", []) or []
        if not isinstance(quality_flags, list):
            quality_flags = []
        payload["quality_flags"] = [
            str(x or "").strip().lower()
            for x in quality_flags
            if str(x or "").strip()
        ][:20]
        payload["last_validation_at"] = str(payload.get("last_validation_at", "") or "").strip()
        return payload

    def append(self, row: dict[str, Any]) -> None:
        payload = self._normalize_row(row)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            return

    def has_today_duplicate(self, *, sha1: str = "", prompt_hash: str = "") -> bool:
        clean_sha = str(sha1 or "").strip()
        clean_prompt = str(prompt_hash or "").strip()
        if not clean_sha and not clean_prompt:
            return False
        today_et = datetime.now(ET).date()
        for row in reversed(self._iter_rows()[-6000:]):
            ts = self._parse_utc(str(row.get("ts_utc", "") or ""))
            if ts is None:
                continue
            if ts.astimezone(ET).date() != today_et:
                continue
            if clean_sha and clean_sha == str(row.get("sha1", "") or ""):
                return True
            if clean_prompt and clean_prompt == str(row.get("prompt_hash", "") or ""):
                return True
        return False

    def _matches_tags(self, row_tags: list[str], wanted: set[str]) -> bool:
        if not wanted:
            return True
        if not row_tags:
            return False
        return bool(set(row_tags) & wanted)

    def _ready_rows(
        self,
        *,
        kind: str,
        tags: list[str] | None = None,
        exclude_recent_used_hours: int = 0,
    ) -> list[dict[str, Any]]:
        desired_kind = str(kind or "").strip().lower()
        wanted_tags = {
            str(t or "").strip().lower()
            for t in (tags or [])
            if str(t or "").strip()
        }
        latest_by_url: dict[str, dict[str, Any]] = {}
        latest_by_sha: dict[str, dict[str, Any]] = {}
        for row in self._iter_rows():
            if str(row.get("kind", "") or "").strip().lower() != desired_kind:
                continue
            key_url = str(row.get("r2_url", "") or "").strip()
            key_sha = str(row.get("sha1", "") or "").strip()
            if key_url:
                latest_by_url[key_url] = row
            elif key_sha:
                latest_by_sha[key_sha] = row

        merged = list(latest_by_url.values()) + list(latest_by_sha.values())
        cutoff = datetime.now(UTC) - timedelta(hours=max(0, int(exclude_recent_used_hours or 0)))
        out: list[dict[str, Any]] = []
        for row in merged:
            if str(row.get("status", "") or "").strip().lower() != "ready":
                continue
            r2_url = str(row.get("r2_url", "") or "").strip()
            if not r2_url.startswith("https://"):
                continue
            if not self._matches_tags(list(row.get("tags", []) or []), wanted_tags):
                continue
            used_at = self._parse_utc(str(row.get("used_at", "") or ""))
            if used_at is not None and used_at >= cutoff:
                continue
            out.append(dict(row))
        return out

    def get_ready(
        self,
        *,
        kind: str,
        tags: list[str] | None = None,
        limit: int = 20,
        exclude_recent_used_hours: int = 0,
    ) -> list[dict[str, Any]]:
        rows = self._ready_rows(
            kind=kind,
            tags=tags,
            exclude_recent_used_hours=exclude_recent_used_hours,
        )
        rows.sort(
            key=lambda item: (
                int(item.get("used_count", 0) or 0),
                self._parse_utc(str(item.get("used_at", "") or "")).timestamp()
                if self._parse_utc(str(item.get("used_at", "") or "")) is not None
                else 0.0,
                -(
                    self._parse_utc(str(item.get("ts_utc", "") or "")).timestamp()
                    if self._parse_utc(str(item.get("ts_utc", "") or "")) is not None
                    else 0.0
                ),
            )
        )
        return rows[: max(1, int(limit or 20))]

    def get_underused(
        self,
        *,
        kind: str,
        tags: list[str] | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        rows = self.get_ready(kind=kind, tags=tags, limit=max(1, int(limit or 20) * 4))
        rows.sort(
            key=lambda item: (
                int(item.get("used_count", 0) or 0),
                self._parse_utc(str(item.get("used_at", "") or "")).timestamp()
                if self._parse_utc(str(item.get("used_at", "") or "")) is not None
                else 0.0,
            )
        )
        return rows[: max(1, int(limit or 20))]

    def get_recent(
        self,
        *,
        kind: str,
        tags: list[str] | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        rows = self._ready_rows(kind=kind, tags=tags, exclude_recent_used_hours=0)
        rows.sort(
            key=lambda item: (
                -(
                    self._parse_utc(str(item.get("ts_utc", "") or "")).timestamp()
                    if self._parse_utc(str(item.get("ts_utc", "") or "")) is not None
                    else 0.0
                ),
                int(item.get("used_count", 0) or 0),
            )
        )
        return rows[: max(1, int(limit or 20))]

    def get_candidates(
        self,
        *,
        kind: str,
        tags: list[str] | None = None,
        exclude_recent_used_hours: int = 24,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        rows = self.get_underused(
            kind=kind,
            tags=tags,
            limit=max(1, int(limit or 20)),
        )
        if rows:
            return rows[: max(1, int(limit or 20))]
        rows = self.get_ready(
            kind=kind,
            tags=tags,
            limit=max(1, int(limit or 20)),
            exclude_recent_used_hours=max(0, int(exclude_recent_used_hours or 0)),
        )
        if rows:
            return rows[: max(1, int(limit or 20))]
        return self.get_recent(kind=kind, tags=tags, limit=max(1, int(limit or 20)))

    def mark_used(self, *, r2_url: str, used_by_post_id: str = "", used_at: str = "") -> None:
        clean = str(r2_url or "").strip()
        if not clean:
            return
        latest = self._latest_for_url(clean)
        used_count = int((latest or {}).get("used_count", 0) or 0) + 1
        payload = dict(latest or {})
        payload.update(
            {
                "ts_utc": datetime.now(UTC).isoformat(),
                "r2_url": clean,
                "status": "used",
                "used_at": str(used_at or datetime.now(UTC).isoformat()),
                "used_by": str(used_by_post_id or ""),
                "used_count": used_count,
            }
        )
        self.append(payload)

    def mark_failed(self, *, r2_url: str = "", prompt_hash: str = "", reason: str = "") -> None:
        clean_url = str(r2_url or "").strip()
        clean_prompt = str(prompt_hash or "").strip()
        latest = self._latest_for_url(clean_url) if clean_url else None
        if latest is None and clean_prompt:
            latest = self._latest_for_prompt_hash(clean_prompt)
        payload = dict(latest or {})
        payload.update(
            {
                "ts_utc": datetime.now(UTC).isoformat(),
                "status": "failed",
                "r2_url": clean_url or str(payload.get("r2_url", "") or ""),
                "prompt_hash": clean_prompt or str(payload.get("prompt_hash", "") or ""),
                "quality_flags": sorted(
                    set(list(payload.get("quality_flags", []) or []) + [str(reason or "failed").strip().lower()])
                ),
            }
        )
        if reason:
            payload["error"] = str(reason or "")[:220]
        self.append(payload)

    def prune_duplicates(self) -> int:
        rows = self._iter_rows()
        if not rows:
            return 0
        unique: dict[str, dict[str, Any]] = {}
        for row in rows:
            key_url = str(row.get("r2_url", "") or "").strip()
            key_sha = str(row.get("sha1", "") or "").strip()
            key_prompt = str(row.get("prompt_hash", "") or "").strip()
            dedupe_key = key_url or key_sha or f"{row.get('kind','')}|{key_prompt}|{row.get('local_path','')}"
            if not dedupe_key:
                continue
            unique[dedupe_key] = row
        compact = list(unique.values())
        removed = max(0, len(rows) - len(compact))
        if removed <= 0:
            return 0
        try:
            with self.path.open("w", encoding="utf-8") as fh:
                for row in compact:
                    fh.write(json.dumps(self._normalize_row(row), ensure_ascii=False) + "\n")
        except Exception:
            return 0
        return removed

    def stats_by_kind_and_tag(self) -> dict[str, Any]:
        rows = self._iter_rows()
        by_kind: dict[str, dict[str, int]] = {}
        by_tag: dict[str, dict[str, int]] = {}
        for row in rows:
            kind = str(row.get("kind", "") or "unknown").strip().lower() or "unknown"
            status = str(row.get("status", "") or "unknown").strip().lower() or "unknown"
            by_kind.setdefault(kind, {})
            by_kind[kind][status] = int(by_kind[kind].get(status, 0)) + 1
            tags = list(row.get("tags", []) or [])
            if not tags:
                tags = ["untagged"]
            for tag in tags:
                tag_key = str(tag or "").strip().lower() or "untagged"
                by_tag.setdefault(tag_key, {})
                by_tag[tag_key][status] = int(by_tag[tag_key].get(status, 0)) + 1
        return {
            "total_rows": len(rows),
            "kinds": by_kind,
            "tags": by_tag,
        }

    def ready_count(self, *, kind: str, tags: list[str] | None = None) -> int:
        return len(self.get_ready(kind=kind, tags=tags, limit=100000, exclude_recent_used_hours=0))

    def _latest_for_url(self, r2_url: str) -> dict[str, Any] | None:
        clean = str(r2_url or "").strip()
        if not clean:
            return None
        for row in reversed(self._iter_rows()):
            if str(row.get("r2_url", "") or "").strip() == clean:
                return row
        return None

    def _latest_for_prompt_hash(self, prompt_hash: str) -> dict[str, Any] | None:
        clean = str(prompt_hash or "").strip()
        if not clean:
            return None
        for row in reversed(self._iter_rows()):
            if str(row.get("prompt_hash", "") or "").strip() == clean:
                return row
        return None

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
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
