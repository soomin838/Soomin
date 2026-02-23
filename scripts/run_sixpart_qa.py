from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.preflight import validate_runtime_settings
from core.quality import ContentQAGate
from core.settings import load_settings


def _ok(name: str, passed: bool, detail: str) -> dict:
    return {"name": name, "passed": passed, "detail": detail}


def main() -> None:
    root = ROOT
    settings_path = root / "config" / "settings.yaml"
    settings = load_settings(settings_path)
    checks: list[dict] = []

    # 1) Publishing readiness
    has_blog_id = settings.blogger.blog_id.isdigit()
    checks.append(_ok("part1_publish_ready", has_blog_id, f"blog_id={settings.blogger.blog_id}"))

    # 2) Auth validation preflight
    errors = validate_runtime_settings(root, settings)
    checks.append(_ok("part2_auth_preflight", len(errors) == 0, "; ".join(errors) if errors else "ok"))

    # 3) Free mode expectation
    gemini_required = (not settings.budget.free_mode) or settings.visual.enable_gemini_image_generation
    free_ok = (not gemini_required) or bool(settings.gemini.api_key.strip())
    checks.append(
        _ok(
            "part3_free_mode_budget",
            free_ok,
            f"free_mode={settings.budget.free_mode}, gemini_required={gemini_required}",
        )
    )

    # 4) Failure recovery policy
    backoff_ok = settings.schedule.max_retry_backoff_minutes >= 5
    checks.append(
        _ok(
            "part4_failure_recovery",
            backoff_ok,
            f"max_retry_backoff_minutes={settings.schedule.max_retry_backoff_minutes}",
        )
    )

    # 5) Quality gate
    qa = ContentQAGate(settings.quality, settings.authority_links)
    sample_html = (
        "<h2>Overview</h2><p>Short sample text.</p>"
        "<h3>Sources And License</h3>"
        "<ul><li><a href='https://developers.google.com/search/docs/fundamentals/creating-helpful-content'>Original source</a></li></ul>"
    )
    result = qa.evaluate(sample_html)
    checks.append(
        _ok(
            "part5_quality_gate",
            settings.quality.min_quality_score > result.score,
            f"sample_score={result.score}, min_quality_score={settings.quality.min_quality_score}",
        )
    )

    # 6) Install/update persistence
    appdata_root = Path.home() / "AppData" / "Roaming" / "RezeroAgent"
    persistence_ok = appdata_root.exists()
    checks.append(_ok("part6_install_persistence", persistence_ok, f"path={appdata_root}"))

    passed = sum(1 for c in checks if c["passed"])
    summary = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "passed": passed,
        "total": len(checks),
        "checks": checks,
    }

    out_dir = root / "storage" / "logs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sixpart_qa_report.json"
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"QA report written: {out_path}")
    print(f"Passed {passed}/{len(checks)}")


if __name__ == "__main__":
    main()
