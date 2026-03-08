from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from rezero_v2.core.domain.publish_result import PublishArtifact
from rezero_v2.integrations.pollinations_client import PollinationsImageResponse


def make_settings_file(path: Path, overrides: dict[str, Any] | None = None) -> Path:
    data: dict[str, Any] = {
        "runtime": {"default_version": "v2", "v2_enabled": True},
        "budget": {"dry_run": True},
        "blogger": {"blog_id": ""},
        "gemini": {"api_key": "", "model": "gemini-2.0-flash"},
        "local_llm": {"enabled": False},
        "integrations": {"search_console_enabled": False, "search_console_site_url": ""},
        "schedule": {"interval_hours": 2.4},
        "content_mode": {"mode": "tech_news_only"},
        "v2": {
            "content_mix": {"hot": 2, "search_derived": 2, "evergreen": 1},
            "content_lengths": {
                "hot_min": 700,
                "hot_max": 1000,
                "search_derived_min": 1100,
                "search_derived_max": 1500,
                "evergreen_min": 1600,
                "evergreen_max": 2200,
            },
            "image_policy": {
                "provider": "pollinations",
                "model": "flux",
                "allow_inline_optional": True,
                "allow_reuse": False,
                "allow_library_fallback": False,
                "allow_news_pack_fallback": False,
            },
        },
    }
    if overrides:
        _deep_merge(data, overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return path


def article(
    *,
    title: str,
    url: str = "https://example.com/story",
    source: str = "example.com",
    published_date: str | None = None,
    summary: str = "",
    topic: str = "ai",
) -> dict[str, Any]:
    return {
        "title": title,
        "url": url,
        "source": source,
        "published_date": published_date or datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "topic": topic,
        "provider": "gdelt",
    }


class FakeGDELTClient:
    def __init__(self, groups: list[dict[str, Any]]) -> None:
        self.groups = groups

    def fetch_trending_topics(self) -> list[dict[str, Any]]:
        return list(self.groups)


class FakeSearchConsoleClient:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])

    def fetch_rows(self, start_date: str, end_date: str, dimensions=("query", "page"), page_size: int = 250, max_rows: int = 50000) -> list[dict[str, Any]]:
        return list(self.rows)

    def discover_opportunities(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            impressions = float((row or {}).get("impressions", 0.0) or 0.0)
            ctr = float((row or {}).get("ctr", 0.0) or 0.0)
            position = float((row or {}).get("position", 0.0) or 0.0)
            query = str((row or {}).get("query", "") or "").strip()
            if not query or impressions < 50:
                continue
            if impressions >= 200 and float((row or {}).get("clicks", 0.0) or 0.0) == 0:
                action = "intent_fix"
            elif 5 <= position <= 15 and ctr >= 0.01:
                action = "supporting_post"
            else:
                action = "title_rewrite"
            out.append({**row, "action": action})
        return out


class FakeBloggerClient:
    def __init__(self, status: str = "published") -> None:
        self.status = status

    def publish_post(self, *, title: str, html: str, labels: list[str] | None = None) -> PublishArtifact:
        return PublishArtifact(
            status=self.status,
            post_id="fake-post-id",
            post_url="https://example.com/fake-post",
            published_at_utc=datetime.now(timezone.utc).isoformat(),
            internal_links_added=[],
            source_links_kept=[],
        )


class FakePollinationsClient:
    def generate_image_url(self, *, prompt: str, width: int = 1280, height: int = 720, seed: int | None = None) -> PollinationsImageResponse:
        digest = str(abs(hash((prompt, width, height, seed))))[:12]
        return PollinationsImageResponse(
            url=f"https://image.pollinations.ai/prompt/fake-{digest}",
            provider="pollinations",
            generated_at_utc=datetime.now(timezone.utc).isoformat(),
            prompt_digest=digest,
        )


class FakeGeminiClient:
    def __init__(self, text: str) -> None:
        self.text = text

    def generate_text(self, *, system_prompt: str, user_payload: dict[str, Any]) -> str:
        return self.text


def _deep_merge(dst: dict[str, Any], src: dict[str, Any]) -> None:
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            _deep_merge(dst[key], value)
        else:
            dst[key] = value
