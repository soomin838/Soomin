from __future__ import annotations

import hashlib
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.brain import (  # noqa: E402
    NEWS_CONCLUSION_TEMPLATES,
    NEWS_INTRO_TEMPLATES,
    build_structure,
    render_from_plan,
    stable_hash,
)
from re_core.settings import QualitySettings  # noqa: E402


def _signature(rendered: dict[str, object]) -> tuple:
    section_ids = tuple(str(x or "").strip() for x in (rendered.get("section_ids", []) or []))
    return (
        int(rendered.get("requested_section_count", 0) or 0),
        int(rendered.get("section_count", 0) or 0),
        section_ids,
        int(rendered.get("intro_index", 0) or 0),
        int(rendered.get("conclusion_index", 0) or 0),
    )


def validate_hash_impl() -> None:
    probe = "stage1-hash-probe"
    expected = int(hashlib.sha256(probe.encode("utf-8")).hexdigest(), 16)
    actual = stable_hash(probe)
    if actual != expected:
        raise AssertionError("stable_hash is not sha256-based.")
    builtin_like = hash(probe)
    if actual == builtin_like:
        raise AssertionError("stable_hash unexpectedly matches Python hash(); implementation check failed.")


def validate_seed_binding() -> None:
    event_id = "stage1-event-seed"
    t1 = "2026-03-02T12:17"
    t2 = "2026-03-02T12:18"
    s1 = stable_hash(f"{event_id}{t1}")
    s2 = stable_hash(f"{event_id}{t2}")
    if s1 == s2:
        raise AssertionError("Seed binding failed: run_start_minute change did not alter seed.")
    if s1 != stable_hash(f"{event_id}{t1}"):
        raise AssertionError("Seed binding failed: same input changed seed value.")


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
        requested = int(rendered.get("requested_section_count", 0) or 0)
        section_count = int(rendered.get("section_count", 0) or 0)
        intro_index = int(rendered.get("intro_index", 0) or 0)
        conclusion_index = int(rendered.get("conclusion_index", 0) or 0)
        if not (3 <= requested <= 6):
            raise AssertionError(f"Case A failed: requested_section_count out of range ({requested}).")
        if not (5 <= section_count <= 6):
            raise AssertionError(f"Case A failed: section_count out of range ({section_count}).")
        if not (0 <= intro_index < len(NEWS_INTRO_TEMPLATES)):
            raise AssertionError(f"Case A failed: intro_index out of range ({intro_index}).")
        if not (0 <= conclusion_index < len(NEWS_CONCLUSION_TEMPLATES)):
            raise AssertionError(f"Case A failed: conclusion_index out of range ({conclusion_index}).")
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


def validate_min_h2_alignment() -> int:
    min_h2 = int(getattr(QualitySettings(), "min_h2", 0) or 0)
    if min_h2 != 5:
        raise AssertionError(f"min_h2 alignment failed: expected 5, got {min_h2}.")
    return min_h2


def main() -> int:
    validate_hash_impl()
    validate_seed_binding()
    case_a = validate_case_a()
    case_b = validate_case_b()
    min_h2 = validate_min_h2_alignment()
    print("Hash/Seed checks OK")
    print("Case A OK")
    for idx, sig in enumerate(case_a, start=1):
        print(f"  A{idx}: {sig}")
    print("Case B OK")
    print(f"  B1: {case_b}")
    print(f"min_h2 OK: {min_h2}")
    print("Stage-1 structural variability validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
