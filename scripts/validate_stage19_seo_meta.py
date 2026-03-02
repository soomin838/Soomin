from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.publisher import Publisher  # noqa: E402
from core.settings import load_settings  # noqa: E402
from core.workflow import AgentWorkflow  # noqa: E402


class _PostsIndexStub:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self._rows = list(rows or [])

    def query_recent(self, **kwargs):  # type: ignore[no-untyped-def]
        return list(self._rows)


def _write_settings(temp_root: Path) -> Path:
    payload = {
        "timezone": "Asia/Seoul",
        "gemini": {"api_key": ""},
        "visual": {
            "image_provider": "library",
            "enable_gemini_image_generation": False,
            "target_images_per_post": 5,
            "max_banner_images": 1,
            "max_inline_images": 4,
        },
        "publish": {
            "image_hosting_backend": "r2",
            "min_images_required": 0,
            "max_images_per_post": 5,
            "r2": {
                "endpoint_url": "https://example-r2-endpoint.invalid",
                "bucket": "dummy-bucket",
                "access_key_id": "dummy-access",
                "secret_access_key": "dummy-secret",
                "public_base_url": "https://example-r2-public.invalid",
                "prefix": "news",
            },
        },
        "internal_links": {
            "enabled": True,
            "body_link_count": 1,
            "related_link_count": 2,
            "overlap_threshold": 0.4,
            "canonical_internal_host": "blog.example.com",
        },
        "blogger": {"credentials_path": "config/credentials.json", "blog_id": "dummy-blog"},
        "indexing": {"service_account_path": "config/indexing.json"},
    }
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def _build_workflow_stub(temp_root: Path, settings, posts_rows: list[dict] | None = None) -> AgentWorkflow:
    publisher = Publisher(
        credentials_path=temp_root / "config" / "credentials.json",
        blog_id="dummy-blog",
        service_account_path=temp_root / "config" / "indexing.json",
        image_hosting_backend=settings.publish.image_hosting_backend,
        r2_config=settings.publish.r2,
        max_banner_images=settings.visual.max_banner_images,
        max_inline_images=settings.visual.max_inline_images,
        min_required_images=settings.publish.min_images_required,
    )
    wf = AgentWorkflow.__new__(AgentWorkflow)
    wf.root = temp_root
    wf.settings = settings
    wf.publisher = publisher
    wf.posts_index = _PostsIndexStub(posts_rows)
    wf._publish_ledger_path = temp_root / "storage" / "ledger" / "publish_ledger.jsonl"
    wf._slug_ledger_path = temp_root / "storage" / "state" / "slug_ledger.jsonl"
    wf._slug_ledger_ttl_days = 180
    wf._internal_links_pool_refresh_cooldown_hours = 6
    return wf


def _assert_focus_keywords(wf: AgentWorkflow) -> None:
    title = "Security patch update for account login verification"
    html = (
        "<h2>Quick Take</h2>"
        "<p>This security patch updates login verification and account protection rules.</p>"
        "<p>The rollout changes include patch timing and vulnerability handling.</p>"
    )
    keywords = wf._compute_focus_keywords(title, html, "security")  # noqa: SLF001
    if not (3 <= len(keywords) <= 6):
        raise AssertionError(f"Case1 failed: focus keyword count out of range: {len(keywords)}")
    if not any(k in {"security", "patch"} for k in keywords):
        raise AssertionError(f"Case1 failed: topic keyword missing in {keywords}")
    print("Case 1 PASS: focus keywords are standardized (3~6 + topic seed)")


def _assert_slug_rules_and_dedupe(wf: AgentWorkflow) -> None:
    bad_title = "Guaranteed free click scam security patch update for enterprise account teams"
    raw_slug = wf._compute_seo_slug(bad_title, "security")  # noqa: SLF001
    if not (40 <= len(raw_slug) <= 70):
        raise AssertionError(f"Case2 failed: slug length out of range: {len(raw_slug)} ({raw_slug})")
    banned = {"free", "scam", "guaranteed", "click", "porn", "must"}
    toks = set(raw_slug.split("-"))
    if toks & banned:
        raise AssertionError(f"Case2 failed: banned tokens remained in slug: {sorted(toks & banned)}")
    if "--" in raw_slug or raw_slug.endswith("-"):
        raise AssertionError(f"Case2 failed: malformed slug punctuation: {raw_slug}")

    first = wf._reserve_unique_slug(raw_slug, title="Title A", topic="security")  # noqa: SLF001
    second = wf._reserve_unique_slug(raw_slug, title="Title B", topic="security")  # noqa: SLF001
    if first == second:
        raise AssertionError("Case3 failed: duplicate slug was not rotated.")
    if not second.endswith("-2"):
        raise AssertionError(f"Case3 failed: expected -2 suffix for duplicate slug, got {second}")
    print("Case 2 PASS: slug normalization rules are applied")
    print("Case 3 PASS: slug ledger avoids duplicates with suffix rotation")


def _assert_pool_refresh_cooldown(temp_root: Path, settings) -> None:
    recent_iso = datetime.now(timezone.utc).isoformat()
    posts_rows = [
        {
            "url": "https://blog.example.com/new-security-rollout-note",
            "title": "New security rollout note",
            "focus_keywords": "security,rollout,note",
            "summary": "Latest rollout note for security updates",
            "published_at": recent_iso,
            "last_seen_at": recent_iso,
        }
    ]
    wf = _build_workflow_stub(temp_root, settings, posts_rows=posts_rows)
    pool_path = wf._internal_links_pool_path()  # noqa: SLF001
    pool_path.parent.mkdir(parents=True, exist_ok=True)
    seed_payload = [
        {
            "url": "https://blog.example.com/seed-entry",
            "title": "Seed entry",
            "keywords": ["seed"],
            "tags": ["default"],
            "topic": "default",
            "updated_at_utc": recent_iso,
        }
    ]
    pool_path.write_text(json.dumps(seed_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    now_ts = datetime.now().timestamp()
    os.utime(pool_path, (now_ts, now_ts))
    wf._refresh_internal_links_pool()  # noqa: SLF001
    after_cooldown = json.loads(pool_path.read_text(encoding="utf-8"))
    if after_cooldown != seed_payload:
        raise AssertionError("Case4 failed: cooldown should have skipped refresh for fresh file mtime.")

    old_ts = (datetime.now() - timedelta(hours=7)).timestamp()
    os.utime(pool_path, (old_ts, old_ts))
    wf._refresh_internal_links_pool()  # noqa: SLF001
    refreshed = json.loads(pool_path.read_text(encoding="utf-8"))
    urls = {str((x or {}).get("url", "") or "").strip() for x in refreshed if isinstance(x, dict)}
    if "https://blog.example.com/new-security-rollout-note" not in urls:
        raise AssertionError("Case4 failed: refresh did not run after cooldown expiry.")
    print("Case 4 PASS: internal links pool refresh cooldown works (mtime-based)")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage19_seo_") as td:
        temp_root = Path(td).resolve()
        cfg_path = _write_settings(temp_root)
        settings = load_settings(cfg_path)
        wf = _build_workflow_stub(temp_root, settings, posts_rows=[])
        _assert_focus_keywords(wf)
        _assert_slug_rules_and_dedupe(wf)
        _assert_pool_refresh_cooldown(temp_root, settings)
    print("Stage-19 SEO metadata validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
