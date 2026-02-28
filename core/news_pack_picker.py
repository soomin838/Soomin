from __future__ import annotations

import random
from dataclasses import dataclass
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
        thumb_candidates = self.manifest.get_candidates(
            kind="thumb_bg",
            tags=desired_tags,
            exclude_recent_used_hours=24,
            limit=max(20, int(thumb_count) * 6),
        )
        inline_candidates = self.manifest.get_candidates(
            kind="inline_bg",
            tags=desired_tags,
            exclude_recent_used_hours=24,
            limit=max(40, int(inline_count) * 8),
        )

        if not thumb_candidates:
            thumb_candidates = self.manifest.get_candidates(
                kind="thumb_bg",
                tags=[],
                exclude_recent_used_hours=0,
                limit=max(20, int(thumb_count) * 6),
            )
        if not inline_candidates:
            inline_candidates = self.manifest.get_candidates(
                kind="inline_bg",
                tags=[],
                exclude_recent_used_hours=0,
                limit=max(40, int(inline_count) * 8),
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

