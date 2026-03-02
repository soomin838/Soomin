from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.brain import build_structure, render_from_plan, stable_hash  # noqa: E402


def _signature(rendered: dict[str, object]) -> tuple:
    section_ids = tuple(str(x or "").strip() for x in (rendered.get("section_ids", []) or []))
    return (
        int(rendered.get("section_count", 0) or 0),
        section_ids,
        int(rendered.get("intro_index", 0) or 0),
        int(rendered.get("conclusion_index", 0) or 0),
    )


def validate_case_a() -> list[tuple]:
    event_id = "stage1-event-001"
    publish_timestamps = [
        "2026-03-02T09:01",
        "2026-03-02T12:17",
        "2026-03-02T18:43",
    ]
    signatures: list[tuple] = []
    for ts in publish_timestamps:
        seed = stable_hash(f"{event_id}{ts}")
        plan = build_structure(seed, {"event_id": event_id, "run_start_minute": ts})
        rendered = render_from_plan(plan, {"event_id": event_id, "run_start_minute": ts})
        signatures.append(_signature(rendered))
    if len(set(signatures)) != len(signatures):
        raise AssertionError("Case A failed: structure did not vary across timestamps.")
    return signatures


def validate_case_b() -> tuple:
    event_id = "stage1-event-001"
    run_start_minute = "2026-03-02T12:17"
    seed = stable_hash(f"{event_id}{run_start_minute}")
    plan1 = build_structure(seed, {"event_id": event_id, "run_start_minute": run_start_minute})
    plan2 = build_structure(seed, {"event_id": event_id, "run_start_minute": run_start_minute})
    rendered1 = render_from_plan(plan1, {"event_id": event_id, "run_start_minute": run_start_minute})
    rendered2 = render_from_plan(plan2, {"event_id": event_id, "run_start_minute": run_start_minute})
    sig1 = _signature(rendered1)
    sig2 = _signature(rendered2)
    if sig1 != sig2:
        raise AssertionError("Case B failed: same seed produced different structures.")
    return sig1


def main() -> int:
    case_a = validate_case_a()
    case_b = validate_case_b()
    print("Case A OK")
    for idx, sig in enumerate(case_a, start=1):
        print(f"  A{idx}: {sig}")
    print("Case B OK")
    print(f"  B1: {case_b}")
    print("Stage-1 structural variability validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
