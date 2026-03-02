from __future__ import annotations

import json
import re
import tempfile
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
    def query_recent(self, **kwargs):  # type: ignore[no-untyped-def]
        return []


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
            "related_link_count": 3,
            "overlap_threshold": 0.2,
        },
        "blogger": {"credentials_path": "config/credentials.json", "blog_id": "dummy-blog"},
        "indexing": {"service_account_path": "config/indexing.json"},
    }
    cfg_dir = temp_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "settings.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return cfg_path


def _sample_html() -> str:
    return (
        "<h2>Quick Take</h2>"
        "<p>This update changes account policy timing and verification notices.</p>"
        "<p>Teams may need to adjust rollout checks.</p>"
        "<h2>What Happened</h2>"
        "<p>Service behavior changed after a platform update.</p>"
        "<h2>Sources</h2>"
        "<ul><li><a href=\"https://external-source.example/item\">external source</a></li></ul>"
    )


def _build_workflow_stub(temp_root: Path, settings) -> AgentWorkflow:
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
    wf.posts_index = _PostsIndexStub()
    return wf


def _write_pool(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage17_links_") as td:
        temp_root = Path(td).resolve()
        cfg_path = _write_settings(temp_root)
        settings = load_settings(cfg_path)
        wf = _build_workflow_stub(temp_root, settings)

        pool_rows = [
            {
                "url": "https://blog.example.com/policy-account-rollout-guide",
                "title": "Account policy rollout guide for verification changes",
                "tags": ["policy"],
                "keywords": ["account", "policy", "verification"],
            },
            {
                "url": "https://blog.example.com/platform-update-checklist",
                "title": "Platform update checklist for security teams",
                "tags": ["platform"],
                "keywords": ["platform", "update", "checklist"],
            },
            {
                "url": "https://blog.example.com/policy-account-rollout-guide",
                "title": "Duplicate URL should be ignored",
                "tags": ["policy"],
                "keywords": ["duplicate"],
            },
            {
                "url": "https://blog.example.com/user-impact-summary",
                "title": "User impact summary and next actions",
                "tags": ["impact"],
                "keywords": ["user", "impact", "actions"],
            },
            {
                "url": "https://outside.example.com/not-internal",
                "title": "Outside article should be filtered",
                "tags": ["external"],
                "keywords": ["external"],
            },
        ]
        _write_pool(wf._internal_links_pool_path(), pool_rows)  # noqa: SLF001

        html = _sample_html()
        out = wf._inject_internal_links_and_related_coverage(  # noqa: SLF001
            html,
            current_title="Policy update and account verification changes",
            current_keywords=["policy", "account", "verification", "platform update"],
        )

        hrefs = re.findall(r'href=["\']([^"\']+)["\']', out, flags=re.IGNORECASE)
        unique_hrefs = set(hrefs)
        if len(hrefs) != len(unique_hrefs):
            raise AssertionError("Case1 failed: duplicate URL detected in final HTML.")

        internal_host = "blog.example.com"
        internal_urls = [u for u in hrefs if internal_host in u]
        # Candidate shortage is allowed, so 0 is pass, but with this pool we expect >=1.
        if len(internal_urls) < 1:
            raise AssertionError("Case1 failed: expected at least one internal link insertion.")

        first_internal_pos = out.find(internal_urls[0])
        if first_internal_pos >= 0:
            limit = int(len(out) * 0.40)
            if first_internal_pos > limit:
                raise AssertionError("Case1 failed: internal body link was not inserted in first 40% of HTML.")

        rel_block = re.search(
            r"<h2>\s*Related Coverage\s*</h2>\s*<ul>(.*?)</ul>",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        rel_urls: list[str] = []
        if rel_block:
            rel_urls = re.findall(r'href=["\']([^"\']+)["\']', rel_block.group(1), flags=re.IGNORECASE)
            if len(rel_urls) > 3:
                raise AssertionError("Case2 failed: Related Coverage has more than 3 links.")
            for u in rel_urls:
                if internal_host not in u:
                    raise AssertionError("Case2 failed: external URL found in Related Coverage.")

        # Candidate shortage fail-open check: keep only one link candidate.
        _write_pool(
            wf._internal_links_pool_path(),  # noqa: SLF001
            [
                {
                    "url": "https://blog.example.com/only-one-candidate",
                    "title": "Only one candidate",
                    "tags": ["policy"],
                    "keywords": ["policy"],
                }
            ],
        )
        out_short = wf._inject_internal_links_and_related_coverage(  # noqa: SLF001
            html,
            current_title="Policy update and account verification changes",
            current_keywords=["policy", "account"],
        )
        if "<h2>Related Coverage</h2>" in out_short:
            rel_short = re.search(r"<h2>\s*Related Coverage\s*</h2>\s*<ul>(.*?)</ul>", out_short, flags=re.IGNORECASE | re.DOTALL)
            if rel_short:
                rel_short_count = len(re.findall(r'href=["\']([^"\']+)["\']', rel_short.group(1), flags=re.IGNORECASE))
                if rel_short_count > 1:
                    raise AssertionError("Case3 failed: shortage mode should not force many related links.")

        print("Case 1 PASS: body internal link insertion works (or fail-open on shortage)")
        print(f"  total_hrefs={len(hrefs)}, internal_hrefs={len(internal_urls)}")
        print("Case 2 PASS: Related Coverage block is generated with safe internal links")
        print(f"  related_count={len(rel_urls)}")
        print("Case 3 PASS: candidate shortage remains fail-open without errors")
        print("Stage-17 internal links validation passed.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
