from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.quality import QAResult
from core.settings import load_settings
from core.workflow import AgentWorkflow


def _check(name: str, ok: bool, detail: str) -> dict:
    return {"name": name, "passed": bool(ok), "detail": str(detail or "")}


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
        except Exception:
            continue
    return rows


def main() -> int:
    settings = load_settings(ROOT / "config" / "settings.yaml")
    wf = AgentWorkflow(ROOT, settings)

    # Fast deterministic validation knobs.
    wf.settings.workflow.retry_enabled = True
    wf.settings.workflow.retry_max_attempts_per_event = 4
    wf.settings.workflow.retry_debounce_seconds = [0, 0, 0, 0]
    wf.settings.workflow.retry_reset_on_success = True
    wf.settings.watchdog.enabled = True
    wf.settings.watchdog.max_same_hard_failure_streak = 3
    wf.settings.watchdog.max_event_wallclock_minutes = 20
    wf.settings.watchdog.max_event_total_attempts = 6
    wf.settings.watchdog.max_global_holds_per_hour = 999
    wf.settings.watchdog.max_pollinations_530_streak = 3
    wf.settings.watchdog.max_pollinations_429_streak = 2

    # Reset state/log files for this validation run.
    for p in [wf._news_retry_state_path, wf._global_pause_state_path, wf._watchdog_log_path]:
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass

    started = datetime.now(timezone.utc)
    results: list[dict] = []

    # 1) FAQ hard fail => auto-fix + retry, then watchdog discard.
    event_faq = "evt_validation_faq"
    qa_faq = QAResult(score=80, hard_failures=["faq_detected"], checks=[])
    html_faq = "<h2>FAQ</h2><p>Frequently Asked Questions</p><h2>Sources</h2><ul><li>x</li></ul>"
    actions: list[str] = []
    fixed_once = False
    for _ in range(4):
        action, html_faq = wf._news_handle_qa_failure_with_retry(
            event_faq,
            qa_faq,
            html_faq,
            "pre_images",
            min_quality=91,
        )
        actions.append(action)
        if "<h2>FAQ</h2>" not in html_faq and "Frequently Asked Questions" not in html_faq:
            fixed_once = True
        if action in {"discard", "hold", "retry_later"}:
            break
    results.append(
        _check(
            "faq_autofix_then_discard",
            fixed_once and ("discard" in actions or "hold" in actions),
            f"actions={actions}, faq_removed={fixed_once}",
        )
    )

    # 2) google.com link => sanitizer removes => re-QA hard-fail clears.
    wf.settings.quality.min_word_count = 120
    wf.settings.quality.max_word_count = 3000
    paragraphs = " ".join(["market update signal" for _ in range(220)])
    html_google = (
        "<h2>Quick Take</h2><p>" + paragraphs + "</p>"
        "<h2>What Happened</h2><p>" + paragraphs + "</p>"
        "<h2>Why It Matters</h2><p>" + paragraphs + "</p>"
        "<h2>Key Details</h2><ul><li>a</li><li>b</li><li>c</li></ul>"
        "<h2>What To Watch Next</h2><p>" + paragraphs + "</p>"
        "<h2>What To Do Now</h2><ul><li>1</li><li>2</li><li>3</li></ul>"
        "<h2>Sources</h2><ul>"
        "<li><a href=\"https://www.google.com/search?q=test\">Google</a></li>"
        "<li><a href=\"https://www.cisa.gov/news-events\">CISA</a></li>"
        "</ul>"
    )
    qa_before = wf._qa_evaluate(
        html_google,
        title="Tech news validation title",
        domain=wf._news_domain,
        keyword="tech news validation",
        phase="pre_images",
    )
    fixed_google, applied_google = wf._auto_fix_hard_failures(html_google, qa_before, phase="pre_images")
    qa_after = wf._qa_evaluate(
        fixed_google,
        title="Tech news validation title",
        domain=wf._news_domain,
        keyword="tech news validation",
        phase="pre_images",
    )
    results.append(
        _check(
            "google_link_autofix",
            ("google.com" not in fixed_google.lower()) and (not qa_after.has_hard_failure),
            f"applied={applied_google}, hard_before={qa_before.hard_failures}, hard_after={qa_after.hard_failures}",
        )
    )

    # 3) Repeated 530 => watchdog pauses image seeding.
    event_530 = "evt_validation_530"
    pause_actions: list[str] = []
    for _ in range(4):
        tripped, reason = wf._record_provider_failure_watchdog(event_530, "http_530")
        pause_actions.append(f"{tripped}:{reason}")
        if tripped:
            break
    paused_img, paused_reason = wf._is_global_pause_active(scope="image_seeding")
    results.append(
        _check(
            "provider_530_watchdog_pause",
            bool(paused_img),
            f"actions={pause_actions}, paused_reason={paused_reason}",
        )
    )

    # 4) No infinite loop: attempts capped by max_event_total_attempts.
    wf.settings.workflow.retry_max_attempts_per_event = 20
    wf.settings.watchdog.max_event_total_attempts = 4
    event_loop = "evt_validation_loop"
    qa_low = QAResult(score=10, hard_failures=[], checks=[])
    loop_actions: list[str] = []
    html_loop = "<h2>Quick Take</h2><p>alpha beta</p>"
    for _ in range(10):
        action, html_loop = wf._news_handle_qa_failure_with_retry(
            event_loop,
            qa_low,
            html_loop,
            "pre_images",
            min_quality=91,
        )
        loop_actions.append(action)
        if action != "retry_now":
            break
    state = wf._load_news_retry_state()
    loop_row = dict(state.get(event_loop, {}) or {})
    total_attempts = int(loop_row.get("total_attempts", loop_row.get("attempts", 0)) or 0)
    results.append(
        _check(
            "attempt_cap_no_infinite_loop",
            total_attempts <= int(wf.settings.watchdog.max_event_total_attempts),
            f"actions={loop_actions}, total_attempts={total_attempts}, cap={wf.settings.watchdog.max_event_total_attempts}",
        )
    )

    # 5) Log/label verification.
    qa_rows = _read_jsonl(wf.root / "storage" / "logs" / "qa_runtime.jsonl")
    wd_rows = _read_jsonl(wf._watchdog_log_path)
    qa_runtime_has_hard = any(
        (row.get("event") == "news_qa_autofix")
        and bool(row.get("hard_failures"))
        and (wf._parse_iso_utc(str(row.get("ts_utc", "") or "")) or started) >= started
        for row in qa_rows
    )
    watchdog_logged = len(wd_rows) > 0

    # Explicit label prefix checks.
    wf.settings.workflow.retry_enabled = False
    event_label_hard = "evt_validation_label_hard"
    wf._news_handle_qa_failure_with_retry(
        event_label_hard,
        QAResult(score=80, hard_failures=["faq_detected"], checks=[]),
        "<h2>FAQ</h2><p>x</p>",
        "pre_images",
        min_quality=91,
    )
    wf.settings.workflow.retry_enabled = True
    wf.settings.workflow.retry_max_attempts_per_event = 0
    event_label_retry = "evt_validation_label_retry"
    wf._news_handle_qa_failure_with_retry(
        event_label_retry,
        QAResult(score=40, hard_failures=[], checks=[]),
        "<h2>Quick Take</h2><p>x</p>",
        "pre_images",
        min_quality=91,
    )
    labels_state = wf._load_news_retry_state()
    reasons = [
        str((labels_state.get(event_label_hard, {}) or {}).get("last_hold_reason", "") or ""),
        str((labels_state.get(event_label_retry, {}) or {}).get("last_hold_reason", "") or ""),
        str((labels_state.get(event_faq, {}) or {}).get("last_hold_reason", "") or ""),
    ]
    label_ok = any(r.startswith("qa_hard_failure:") for r in reasons) and any(
        r.startswith("qa_retry_exceeded:") or r.startswith("discard:") or r.startswith("pause:") for r in reasons
    )

    results.append(
        _check(
            "logs_and_labels",
            qa_runtime_has_hard and watchdog_logged and label_ok,
            f"qa_runtime_has_hard={qa_runtime_has_hard}, watchdog_rows={len(wd_rows)}, reasons={reasons}",
        )
    )

    passed = all(bool(r.get("passed", False)) for r in results)
    print(json.dumps({"passed": passed, "results": results}, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
