from __future__ import annotations

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.brain import stable_hash  # noqa: E402
from re_core.news_facets import ensure_what_to_do_now_section, resolve_facet_context  # noqa: E402


def validate_rotation_order() -> tuple[list[str], list[str]]:
    event_id = "stage2-event-rotation"
    run_start_minute = "2026-03-02T13:00"
    selected: list[str] = []
    top_ref: list[str] = []
    for retry in range(4):
        ctx = resolve_facet_context(
            event_id=event_id,
            run_start_minute=run_start_minute,
            title="Platform policy update impact",
            body="Official rollout timeline and user risk details.",
            category="platform",
            source_url="https://example.com/news",
            retry_index=retry,
            llm_candidates=[],
            state_path=None,
            stable_hash_fn=stable_hash,
        )
        if retry == 0:
            top_ref = list(ctx.top_facets)
        if list(ctx.top_facets) != top_ref:
            raise AssertionError("Rotation test failed: Top-4 candidates changed across fixed seed.")
        expected = top_ref[retry % 4]
        if ctx.selected_facet != expected:
            raise AssertionError(
                f"Rotation test failed: retry={retry}, expected={expected}, got={ctx.selected_facet}."
            )
        selected.append(ctx.selected_facet)
    return top_ref, selected


def validate_seed_change_effect() -> tuple[str, str]:
    event_id = "stage2-event-rotation"
    base = resolve_facet_context(
        event_id=event_id,
        run_start_minute="2026-03-02T13:00",
        title="Platform policy update impact",
        body="Official rollout timeline and user risk details.",
        category="platform",
        source_url="https://example.com/news",
        retry_index=0,
        llm_candidates=[],
        state_path=None,
        stable_hash_fn=stable_hash,
    )
    probe_minutes = [
        "2026-03-02T13:01",
        "2026-03-02T13:02",
        "2026-03-02T13:03",
        "2026-03-02T13:04",
    ]
    changed = ""
    for ts in probe_minutes:
        other = resolve_facet_context(
            event_id=event_id,
            run_start_minute=ts,
            title="Platform policy update impact",
            body="Official rollout timeline and user risk details.",
            category="platform",
            source_url="https://example.com/news",
            retry_index=0,
            llm_candidates=[],
            state_path=None,
            stable_hash_fn=stable_hash,
        )
        if (other.top_facets != base.top_facets) or (other.selected_facet != base.selected_facet):
            changed = ts
            break
    if not changed:
        raise AssertionError("Seed-change test failed: no facet candidate/selection difference detected.")
    return base.selected_facet, changed


def validate_what_to_do() -> tuple[int, int]:
    ctx = resolve_facet_context(
        event_id="stage2-event-actions",
        run_start_minute="2026-03-02T14:40",
        title="Security advisory update",
        body="Policy and platform guidance with confirmed statements.",
        category="security",
        source_url="https://security.example.com/advisory",
        retry_index=2,
        llm_candidates=[],
        state_path=None,
        stable_hash_fn=stable_hash,
    )
    if not (3 <= int(ctx.action_count) <= 6):
        raise AssertionError(f"What-To-Do action_count out of range: {ctx.action_count}")
    if len(ctx.action_items) != int(ctx.action_count):
        raise AssertionError(
            f"What-To-Do action_items count mismatch: items={len(ctx.action_items)} action_count={ctx.action_count}"
        )

    html = "<h2>Quick Take</h2><p>Example</p><h2>Sources</h2><ul><li>Source</li></ul>"
    out = ensure_what_to_do_now_section(html=html, action_items=ctx.action_items)
    if not re.search(r"<h2[^>]*>\s*What\s*To\s*Do\s*Now\s*</h2>", out, flags=re.IGNORECASE):
        raise AssertionError("What-To-Do section missing in output HTML.")
    section = re.search(
        r"<h2[^>]*>\s*What\s*To\s*Do\s*Now\s*</h2>(.*?)(?=<h2\b|$)",
        out,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not section:
        raise AssertionError("What-To-Do section extraction failed.")
    li_count = len(re.findall(r"<li\b", str(section.group(1) or ""), flags=re.IGNORECASE))
    if li_count != int(ctx.action_count):
        raise AssertionError(f"What-To-Do <li> count mismatch: li_count={li_count}, expected={ctx.action_count}")
    return int(ctx.action_count), li_count


def main() -> int:
    top4, rotated = validate_rotation_order()
    base_facet, changed_minute = validate_seed_change_effect()
    action_count, li_count = validate_what_to_do()
    print("Case 1 OK: deterministic rotate-per-retry")
    print(f"  top4={top4}")
    print(f"  selected_by_retry={rotated}")
    print("Case 2 OK: run_start_minute can alter facet candidates/selection")
    print(f"  base_selected={base_facet}, changed_at={changed_minute}")
    print("Case 3 OK: What To Do Now always included with deterministic 3-6 actions")
    print(f"  action_count={action_count}, li_count={li_count}")
    print("Stage-2 facet validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

