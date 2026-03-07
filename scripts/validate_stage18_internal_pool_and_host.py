from __future__ import annotations

import json
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
import sys

import yaml


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.publisher import Publisher  # noqa: E402
from re_core.settings import load_settings  # noqa: E402
from re_core.workflow import AgentWorkflow  # noqa: E402


ALLOWED_TOPICS = {"security", "policy", "platform", "mobile", "ai", "chips", "privacy", "default"}


class _PostsIndexStub:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self._rows = list(rows or [])

    def query_recent(self, **kwargs):  # type: ignore[no-untyped-def]
        return list(self._rows)


def _write_settings(temp_root: Path, canonical_host: str) -> Path:
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
            "related_posts_min": 2,
            "related_posts_max": 3,
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
            "overlap_threshold": 0.2,
            "canonical_internal_host": canonical_host,
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
    return wf


def _write_pool(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def _sample_html() -> str:
    return (
        "<h2>Quick Take</h2>"
        "<p>This update covers a platform security patch and account controls.</p>"
        "<p>Users need practical checks before rollout completes.</p>"
        "<h2>Details</h2>"
        "<p>Security and policy updates changed the verification flow.</p>"
        "<h2>Sources</h2><ul><li><a href=\"https://external-source.example/item\">source</a></li></ul>"
    )


def _parse_hosts_from_hrefs(hrefs: list[str]) -> list[str]:
    hosts: list[str] = []
    for url in hrefs:
        hosts.append((urlparse(url).netloc or "").lower().replace("www.", ""))
    return hosts


def scenario_canonical_host_and_markers(wf: AgentWorkflow) -> None:
    pool_rows = [
        {
            "url": "https://blog.example.com/security-rollout-basics",
            "title": "Security rollout basics for account verification teams",
            "tags": ["security"],
            "keywords": ["security", "rollout", "account", "verification"],
            "topic": "security",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        {
            "url": "https://another.example.com/should-not-appear",
            "title": "Outside domain should be filtered",
            "tags": ["policy"],
            "keywords": ["policy"],
            "topic": "policy",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        {
            "url": "https://blog.example.com/platform-checklist",
            "title": "Platform checklist after a security update",
            "tags": ["platform"],
            "keywords": ["platform", "security", "checklist"],
            "topic": "platform",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        {
            "url": "https://blog.example.com/policy-response-plan",
            "title": "Policy response plan after a security incident",
            "tags": ["policy"],
            "keywords": ["policy", "security", "response"],
            "topic": "policy",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    ]
    _write_pool(wf._internal_links_pool_path(), pool_rows)  # noqa: SLF001
    html = _sample_html()
    out = wf._inject_internal_links_and_related_coverage(  # noqa: SLF001
        html,
        current_title="Security patch update and policy notice",
        current_keywords=["security", "patch", "policy", "account"],
    )
    related_match = re.search(
        r"<!--\s*RZ-RELATED:START\s*-->(.*?)<!--\s*RZ-RELATED:END\s*-->",
        out,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not related_match:
        raise AssertionError("Scenario1 failed: Related Coverage marker block missing.")
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', related_match.group(1), flags=re.IGNORECASE)
    if not hrefs:
        raise AssertionError("Scenario1 failed: expected at least one related internal URL.")
    hosts = _parse_hosts_from_hrefs(hrefs)
    if any(h != "blog.example.com" for h in hosts):
        raise AssertionError("Scenario1 failed: non-canonical host found in related links.")
    if len(re.findall(r"<!--\s*RZ-RELATED:START\s*-->", out, flags=re.IGNORECASE)) != 1:
        raise AssertionError("Scenario1 failed: expected exactly one related start marker on first run.")
    if len(re.findall(r"<!--\s*RZ-RELATED:END\s*-->", out, flags=re.IGNORECASE)) != 1:
        raise AssertionError("Scenario1 failed: expected exactly one related end marker on first run.")
    out2 = wf._inject_internal_links_and_related_coverage(  # noqa: SLF001
        out,
        current_title="Security patch update and policy notice",
        current_keywords=["security", "patch", "policy", "account"],
    )
    start_count = len(re.findall(r"<!--\s*RZ-RELATED:START\s*-->", out2, flags=re.IGNORECASE))
    end_count = len(re.findall(r"<!--\s*RZ-RELATED:END\s*-->", out2, flags=re.IGNORECASE))
    if start_count > 1 or end_count > 1 or start_count != end_count:
        raise AssertionError("Scenario1 failed: related marker block is not idempotent after rerun.")
    print("Scenario 1 PASS: canonical host filter + related marker idempotency")


def scenario_refresh_pool_and_cleanup(temp_root: Path, settings) -> None:
    old_iso = (datetime.now(timezone.utc) - timedelta(days=250)).isoformat()
    recent_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    posts_rows = [
        {
            "url": "https://blog.example.com/privacy-controls-update",
            "title": "Privacy controls update for account settings",
            "focus_keywords": "privacy,account,controls",
            "summary": "Privacy controls changed in the account center.",
            "published_at": recent_iso,
            "last_seen_at": recent_iso,
        },
        {
            "url": "https://blog.example.com/privacy-controls-update",
            "title": "Duplicate URL from posts index",
            "focus_keywords": "privacy",
            "summary": "duplicate row",
            "published_at": recent_iso,
            "last_seen_at": recent_iso,
        },
        {
            "url": "https://outside.example.com/external-row",
            "title": "External row should be filtered by canonical host",
            "focus_keywords": "security",
            "summary": "external row",
            "published_at": recent_iso,
            "last_seen_at": recent_iso,
        },
    ]
    wf = _build_workflow_stub(temp_root, settings, posts_rows=posts_rows)
    _write_pool(
        wf._internal_links_pool_path(),  # noqa: SLF001
        [
            {
                "url": "https://blog.example.com/old-entry",
                "title": "Old entry",
                "topic": "default",
                "tags": ["default"],
                "updated_at_utc": old_iso,
            }
        ],
    )
    wf._refresh_internal_links_pool(force=True)  # noqa: SLF001
    pool_path = wf._internal_links_pool_path()  # noqa: SLF001
    if not pool_path.exists():
        raise AssertionError("Scenario2 failed: pool file was not created.")
    rows = json.loads(pool_path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise AssertionError("Scenario2 failed: pool format is not list.")
    if len(rows) > 500:
        raise AssertionError("Scenario2 failed: pool size exceeded 500.")
    seen_urls: set[str] = set()
    for row in rows:
        url = str((row or {}).get("url", "") or "").strip()
        host = (urlparse(url).netloc or "").lower().replace("www.", "")
        if host != "blog.example.com":
            raise AssertionError("Scenario2 failed: non-canonical host remained in pool.")
        if url in seen_urls:
            raise AssertionError("Scenario2 failed: duplicate URL remained in pool.")
        seen_urls.add(url)
        updated = datetime.fromisoformat(str((row or {}).get("updated_at_utc", "") or "").replace("Z", "+00:00"))
        if updated < datetime.now(timezone.utc) - timedelta(days=180):
            raise AssertionError("Scenario2 failed: old row beyond 180 days was not pruned.")
        topic = str((row or {}).get("topic", "") or "").strip().lower()
        if topic not in ALLOWED_TOPICS:
            raise AssertionError("Scenario2 failed: invalid topic in pool.")
        tags = (row or {}).get("tags", [])
        if not isinstance(tags, list) or not tags:
            raise AssertionError("Scenario2 failed: missing tags in pool row.")
    print("Scenario 2 PASS: pool refresh/cleanup works with canonical host + TTL")


def scenario_topic_bonus_priority(temp_root: Path, settings) -> None:
    wf = _build_workflow_stub(temp_root, settings, posts_rows=[])
    now_iso = datetime.now(timezone.utc).isoformat()
    _write_pool(
        wf._internal_links_pool_path(),  # noqa: SLF001
        [
            {
                "url": "https://blog.example.com/security-rollout-checklist",
                "title": "Security rollout checklist and impact review",
                "keywords": ["security", "rollout", "impact"],
                "tags": ["security"],
                "topic": "security",
                "updated_at_utc": now_iso,
            },
            {
                "url": "https://blog.example.com/mobile-feature-roundup",
                "title": "Mobile feature roundup and rollout highlights",
                "keywords": ["mobile", "rollout", "impact"],
                "tags": ["mobile"],
                "topic": "mobile",
                "updated_at_utc": now_iso,
            },
        ],
    )
    candidates = wf._collect_internal_link_candidates(  # noqa: SLF001
        current_title="Security rollout update for account protection",
        current_keywords=["security", "rollout", "account", "protection"],
        current_html="<p>Security teams need rollout checks.</p>",
    )
    if not candidates:
        raise AssertionError("Scenario3 failed: expected candidates from pool.")
    top = str((candidates[0] or {}).get("url", "") or "")
    if "security-rollout-checklist" not in top:
        raise AssertionError("Scenario3 failed: topic bonus did not prioritize matching topic.")
    print("Scenario 3 PASS: topic cluster bonus prioritizes relevant internal links")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage18_links_") as td:
        temp_root = Path(td).resolve()
        cfg_path = _write_settings(temp_root, canonical_host="blog.example.com")
        settings = load_settings(cfg_path)
        wf = _build_workflow_stub(temp_root, settings, posts_rows=[])
        scenario_canonical_host_and_markers(wf)
        scenario_refresh_pool_and_cleanup(temp_root, settings)
        scenario_topic_bonus_priority(temp_root, settings)
    print("Stage-18 internal host/pool/topic validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
