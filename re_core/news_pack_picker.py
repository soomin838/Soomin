from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .news_pack_manifest import NewsPackManifest


@dataclass
class NewsPackPickResult:
    thumb_bg: dict[str, Any] | None
    inline_bg: list[dict[str, Any]]

    @property
    def all_images(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if isinstance(self.thumb_bg, dict):
            out.append(self.thumb_bg)
        out.extend([x for x in (self.inline_bg or []) if isinstance(x, dict)])
        return out


class NewsPackPicker:
    def __init__(self, *, root: Path, manifest_path: str = "storage/state/news_pack_manifest.jsonl") -> None:
        self.manifest = NewsPackManifest(root=root, manifest_path=manifest_path)

    def pick_for_post(
        self,
        *,
        tags: list[str],
        thumb_count: int = 1,
        inline_count: int = 4,
    ) -> NewsPackPickResult:
        desired_tags = [str(t or "").strip().lower() for t in (tags or []) if str(t or "").strip()]
        thumb_candidates = self._collect_candidates(
            kind="thumb_bg",
            desired_tags=desired_tags,
            limit=max(20, int(thumb_count) * 8),
        )
        inline_candidates = self._collect_candidates(
            kind="inline_bg",
            desired_tags=desired_tags,
            limit=max(50, int(inline_count) * 10),
        )

        random.shuffle(thumb_candidates)
        random.shuffle(inline_candidates)
        thumb = thumb_candidates[0] if thumb_candidates else None
        inline: list[dict[str, Any]] = []
        seen: set[str] = set()
        if isinstance(thumb, dict):
            seen.add(str(thumb.get("r2_url", "") or ""))
        for row in inline_candidates:
            url = str((row or {}).get("r2_url", "") or "")
            if not url or url in seen:
                continue
            seen.add(url)
            inline.append(row)
            if len(inline) >= max(0, int(inline_count)):
                break
        return NewsPackPickResult(thumb_bg=thumb, inline_bg=inline)

    def _collect_candidates(self, *, kind: str, desired_tags: list[str], limit: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rows in (
            self.manifest.get_underused(kind=kind, tags=desired_tags, limit=limit),
            self.manifest.get_ready(kind=kind, tags=desired_tags, limit=limit, exclude_recent_used_hours=24),
            self.manifest.get_underused(kind=kind, tags=[], limit=limit),
            self.manifest.get_recent(kind=kind, tags=[], limit=limit),
        ):
            for row in rows:
                url = str((row or {}).get("r2_url", "") or "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                out.append(dict(row))
                if len(out) >= max(1, int(limit)):
                    break
            if len(out) >= max(1, int(limit)):
                break
        out.sort(key=self._candidate_rank)
        return out[: max(1, int(limit))]

    def _candidate_rank(self, row: dict[str, Any]) -> tuple[int, int, float]:
        tags = [str(t or "").strip().lower() for t in (row.get("tags", []) or []) if str(t or "").strip()]
        is_generic = 1 if ("generic" in tags or tags == ["untagged"] or (not tags)) else 0
        used_count = int(row.get("used_count", 0) or 0)
        recency = 0.0
        raw_ts = str(row.get("ts_utc", "") or "").strip()
        if raw_ts.endswith("Z"):
            raw_ts = raw_ts[:-1] + "+00:00"
        try:
            recency = -float(datetime.fromisoformat(raw_ts).timestamp())
        except Exception:
            recency = 0.0
        return (is_generic, used_count, recency)
