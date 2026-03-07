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

from re_core.publisher import Publisher  # noqa: E402
from re_core.run_metrics import RunMetricsLogger, parse_reason_codes  # noqa: E402
from re_core.settings import load_settings  # noqa: E402
from re_core.visual import ImageAsset  # noqa: E402
from re_core.workflow import AgentWorkflow  # noqa: E402


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


def _build_workflow_stub(temp_root: Path):
    settings = load_settings(_write_settings(temp_root))
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
    wf.run_metrics_logger = RunMetricsLogger(temp_root)
    wf._run_metrics_context = {}
    wf._run_metrics_emitted_keys = set()
    wf._workflow_perf_run_id = ""
    wf._workflow_perf_last_run_id = "stage20stub"
    return wf


def _validate_logger_and_reason_codes(temp_root: Path) -> None:
    logger = RunMetricsLogger(temp_root)
    logger.log(
        {
            "run_id": "r1",
            "status": "success",
            "reason_codes": ["ok"],
            "topic_cluster": "security",
            "focus_keywords": ["security", "patch", "rollout"],
            "seo_slug": "security-patch-rollout-overview-guide",
            "title": "Security patch rollout overview",
            "published_url": "https://blog.example.com/post-a",
            "publish_at_utc": "2026-03-02T00:00:00+00:00",
            "images_count": 3,
            "internal_links_count": 2,
            "related_links_count": 2,
            "ctr_risk_low_visual_density": False,
            "entropy_ok": True,
        }
    )
    logger.log(
        {
            "run_id": "r2",
            "status": "skipped",
            "reason_codes": ["entropy_fail", "ledger_skip"],
            "topic_cluster": "policy",
            "focus_keywords": ["policy", "compliance", "rollout"],
            "seo_slug": "policy-compliance-rollout-update-guide",
            "title": "Policy compliance rollout update",
            "published_url": "",
            "publish_at_utc": "",
            "images_count": 0,
            "internal_links_count": 0,
            "related_links_count": 0,
            "ctr_risk_low_visual_density": True,
            "entropy_ok": False,
        }
    )
    metrics_path = temp_root / "storage" / "logs" / "run_metrics.jsonl"
    rows = [x for x in metrics_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    if len(rows) != 2:
        raise AssertionError(f"Case1 failed: expected 2 rows, got {len(rows)}")
    codes = parse_reason_codes("entropy_fail|entropy_fail, ledger_skip  ledger_skip")
    if sorted(codes) != ["entropy_fail", "ledger_skip"]:
        raise AssertionError(f"Case2 failed: reason_codes dedupe failed: {codes}")
    print("Case 1 PASS: run_metrics logger appends one JSONL row per call")
    print("Case 2 PASS: reason_codes parser normalizes and deduplicates tokens")


def _validate_link_and_image_counts(wf: AgentWorkflow) -> None:
    html = (
        "<h2>Quick Take</h2>"
        "<p>Summary with links.</p>"
        '<p><a href="https://blog.example.com/a">A</a> '
        '<a href="https://blog.example.com/b">B</a> '
        '<a href="https://external.example.com/c">C</a></p>'
        "<!-- RZ-RELATED:START --><h2>Related Coverage</h2><ul>"
        '<li><a href="https://blog.example.com/r1">R1</a></li>'
        '<li><a href="https://blog.example.com/r2">R2</a></li>'
        "</ul><!-- RZ-RELATED:END -->"
    )
    internal_count = wf._count_internal_links_by_canonical_host(html)  # noqa: SLF001
    related_count = wf._count_related_links_in_html(html)  # noqa: SLF001
    if internal_count != 4:
        raise AssertionError(f"Case3 failed: internal link count mismatch: {internal_count}")
    if related_count != 2:
        raise AssertionError(f"Case3 failed: related link count mismatch: {related_count}")

    images = [
        ImageAsset(
            path=Path("virtual_a.png"),
            alt="a",
            anchor_text="",
            source_kind="generated_r2",
            source_url="https://example-r2-public.invalid/a.png",
            license_note="",
        ),
        ImageAsset(
            path=Path("virtual_b.png"),
            alt="b",
            anchor_text="",
            source_kind="generated_r2",
            source_url="https://example-r2-public.invalid/b.png",
            license_note="",
        ),
        ImageAsset(
            path=Path("virtual_c.png"),
            alt="c",
            anchor_text="",
            source_kind="generated_r2",
            source_url="https://outside.invalid/c.png",
            license_note="",
        ),
    ]
    img_count = wf._count_allowed_image_urls(images)  # noqa: SLF001
    if img_count != 2:
        raise AssertionError(f"Case4 failed: allowed image count mismatch: {img_count}")
    print("Case 3 PASS: internal/related link counters work with marker + canonical host")
    print("Case 4 PASS: allowed image URL counter respects publisher backend host policy")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stage20_metrics_") as td:
        temp_root = Path(td).resolve()
        _validate_logger_and_reason_codes(temp_root)
        wf = _build_workflow_stub(temp_root)
        _validate_link_and_image_counts(wf)
    print("Stage-20 run metrics validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
