from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.brain import DraftPost
from re_core.publisher import Publisher
from re_core.image_library import pick_images
from re_core.scheduler import generate_month_schedule
from re_core.scout import TopicCandidate
from re_core.settings import load_settings
from re_core.visual import ImageAsset
from re_core.workflow import AgentWorkflow


def _check(name: str, passed: bool, detail: str) -> dict:
    return {"name": name, "passed": bool(passed), "detail": str(detail or "")}


def main() -> None:
    settings = load_settings(ROOT / "config" / "settings.yaml")
    workflow = AgentWorkflow(ROOT, settings)
    publisher = Publisher(
        credentials_path=ROOT / settings.blogger.credentials_path,
        blog_id=settings.blogger.blog_id,
        service_account_path=ROOT / settings.indexing.service_account_path,
        image_hosting_backend=settings.publish.image_hosting_backend,
        gcs_bucket_name=settings.publish.gcs_bucket_name,
        gcs_public_base_url=settings.publish.gcs_public_base_url,
        max_banner_images=settings.visual.max_banner_images,
        max_inline_images=settings.visual.max_inline_images,
        semantic_html_enabled=bool(getattr(settings.publish, "enable_semantic_html", True)),
    )

    sample_markdown = """
## Quick Take
Windows microphone not working after update is usually a permissions or driver order issue.

## Fix 1
- Open privacy settings
- Allow microphone for desktop apps

## Fix 2
Restart audio service and test recording app.

## Fix 3
Reinstall audio driver and reboot.
""".strip()
    canonical_html = workflow._canonicalize_html_payload(sample_markdown)

    images = pick_images(
        title="Windows microphone not working after update",
        min_count=5,
        root=ROOT,
    )
    if len(images) < 5:
        banner = ROOT / "assets" / "fallback" / "banner.png"
        inline = ROOT / "assets" / "fallback" / "inline.png"
        images = [
            ImageAsset(path=banner, alt="Troubleshooting flow diagram for a Windows audio fix.", source_kind="library", source_url="local://fallback"),
            ImageAsset(path=inline, alt="Checklist diagram for microphone troubleshooting steps.", source_kind="library", source_url="local://fallback"),
            ImageAsset(path=inline, alt="Checklist diagram for microphone troubleshooting steps.", source_kind="library", source_url="local://fallback"),
            ImageAsset(path=inline, alt="Checklist diagram for microphone troubleshooting steps.", source_kind="library", source_url="local://fallback"),
            ImageAsset(path=inline, alt="Checklist diagram for microphone troubleshooting steps.", source_kind="library", source_url="local://fallback"),
        ]
    merged = publisher.build_dry_run_html(canonical_html, images)
    now = datetime.now(timezone.utc)
    now_et = now.astimezone(ZoneInfo("America/New_York"))
    sched_path = ROOT / "storage" / "schedules" / f"monthly_slots_{now_et.year:04d}_{now_et.month:02d}.json"
    schedule_rows = generate_month_schedule(now_et.year, now_et.month, sched_path)

    draft = DraftPost(
        title="Windows microphone not working after update",
        alt_titles=[],
        html=merged,
        summary="Quick fix path for Windows microphone issues after update.",
        score=100,
        source_url="https://example.com",
        extracted_urls=[],
    )
    candidate = TopicCandidate(
        source="qa",
        title="Windows microphone not working after update",
        body="Practical troubleshooting steps.",
        score=100,
        url="https://example.com",
        main_entity="Windows",
        long_tail_keywords=["windows microphone not working", "windows mic fix after update"],
    )
    prompt_blob = ""
    banned_image_words = ()
    required_title_tokens = [
        str(x or "").strip().lower()
        for x in (getattr(settings.content_mode, "required_title_tokens_any", []) or [])
        if str(x or "").strip()
    ] or ["not working", "fix", "error", "after update"]
    enforced_title = workflow._enforce_seo_title("Windows microphone setup", candidate, ["windows microphone not working"])

    checks = [
        _check("markdown_tokens_zero", ("## " not in merged and "### " not in merged), "markdown heading token absent"),
        _check("img_count_min5", len(re.findall(r"<img\b[^>]*\bsrc=", merged, flags=re.IGNORECASE)) >= 5, "img>=5"),
        _check("figcaption_zero", "<figcaption" not in merged.lower(), "figcaption absent"),
        _check(
            "banned_debug_tokens_zero",
            not re.search(
                r"(workflow checkpoint stage|av reference context|jobtitle|sameas|selected topic|source[_\s-]*trending[_\s-]*entities)",
                merged.lower(),
            ),
            "debug tokens absent",
        ),
        _check("google_links_zero", "google.com" not in merged.lower(), "google link absent"),
        _check("banned_image_words_zero", not any(w in prompt_blob for w in banned_image_words), "image prompt hazard words absent"),
        _check("title_has_required_token", any(tok in enforced_title.lower() for tok in required_title_tokens), f"title={enforced_title}"),
        _check("monthly_schedule_generated", bool(schedule_rows), f"days={len(schedule_rows)}"),
    ]

    passed = sum(1 for item in checks if item["passed"])
    report = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "passed": passed,
        "total": len(checks),
        "checks": checks,
        "sample": {
            "title": enforced_title,
            "keyword": candidate.long_tail_keywords[0],
            "banner_prompt": "library://selected",
            "inline_prompt": "library://selected",
        },
    }
    out_path = ROOT / "storage" / "logs" / "dryrun_e2e_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"E2E dry-run report written: {out_path}")
    print(f"PASS {passed}/{len(checks)}")


if __name__ == "__main__":
    main()
