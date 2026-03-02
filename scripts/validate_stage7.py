from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.brain import stable_hash  # noqa: E402
from core.title_diversity import choose_diverse_title, normalize_title  # noqa: E402


SETTINGS = {
    "enabled": True,
    "patterns_total": 6,
    "cluster_pattern_ttl_days": 14,
    "numeric_ratio": 0.40,
    "question_ratio": 0.20,
    "analysis_ratio": 0.40,
    "min_title_chars": 45,
    "max_title_chars": 70,
}


def _state_path(name: str) -> Path:
    return ROOT / "storage" / "state" / f"_validate_stage7_{name}.json"


def _cleanup(paths: list[Path]) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


def _choose(path: Path, *, cluster_id: str, run_start_minute: str) -> dict:
    return choose_diverse_title(
        base_title="Microsoft security update rollout for enterprise endpoint admins",
        cluster_id=cluster_id,
        facet="risk",
        category="security",
        run_start_minute=run_start_minute,
        stable_hash_fn=stable_hash,
        state_path=path,
        settings=SETTINGS,
    )


def validate_deterministic_same_input() -> tuple[str, int]:
    p1 = _state_path("case1_a")
    p2 = _state_path("case1_b")
    _cleanup([p1, p2])
    r1 = _choose(p1, cluster_id="cluster-alpha", run_start_minute="2026-03-02T14:10")
    r2 = _choose(p2, cluster_id="cluster-alpha", run_start_minute="2026-03-02T14:10")
    if str(r1.get("title", "")).strip() != str(r2.get("title", "")).strip():
        raise AssertionError("Case1 failed: deterministic title mismatch for same cluster/run.")
    if int(r1.get("pattern_id", -1)) != int(r2.get("pattern_id", -1)):
        raise AssertionError("Case1 failed: deterministic pattern_id mismatch for same cluster/run.")
    return str(r1.get("title", "")).strip(), int(r1.get("pattern_id", -1))


def validate_rotation_no_repeat() -> tuple[int, int]:
    path = _state_path("case2")
    _cleanup([path])
    first = _choose(path, cluster_id="cluster-rotate", run_start_minute="2026-03-02T14:20")
    second = _choose(path, cluster_id="cluster-rotate", run_start_minute="2026-03-02T14:20")
    p1 = int(first.get("pattern_id", -1))
    p2 = int(second.get("pattern_id", -1))
    if p1 < 0 or p2 < 0:
        raise AssertionError("Case2 failed: invalid pattern_id.")
    if p1 == p2:
        raise AssertionError("Case2 failed: same cluster used same pattern consecutively.")
    return p1, p2


def validate_candidates_and_length() -> tuple[int, int]:
    path = _state_path("case3")
    _cleanup([path])
    out = _choose(path, cluster_id="cluster-length", run_start_minute="2026-03-02T14:30")
    candidates = list(out.get("candidates", []) or [])
    if len(candidates) < 6:
        raise AssertionError(f"Case3 failed: expected >=6 candidates, got {len(candidates)}")
    title = str(out.get("title", "")).strip()
    if len(title) < 45 or len(title) > 70:
        raise AssertionError(f"Case3 failed: title length out of range 45~70 (got {len(title)}).")
    return len(candidates), len(title)


def validate_banned_word_normalization() -> str:
    normalized = normalize_title("This guaranteed proven scam must end!!!")
    lowered = normalized.lower()
    banned = ["guaranteed", "proven", "must", "scam"]
    if any(token in lowered for token in banned):
        raise AssertionError(f"Case4 failed: banned token remained in normalized title: {normalized}")
    return normalized


def validate_ttl_expiry_ignored() -> tuple[int, int]:
    fresh_path = _state_path("case5_fresh")
    stale_path = _state_path("case5_stale")
    _cleanup([fresh_path, stale_path])

    baseline = _choose(fresh_path, cluster_id="cluster-ttl", run_start_minute="2026-03-02T14:40")
    baseline_pid = int(baseline.get("pattern_id", -1))

    stale_payload = {
        "version": 1,
        "updated_at_utc": "2026-01-01T00:00:00+00:00",
        "clusters": {
            "cluster-ttl": {
                "last_pattern_id": baseline_pid,
                "updated_at_utc": (baseline.get("updated_at_utc", "") or ""),
            }
        },
    }
    stale_payload["clusters"]["cluster-ttl"]["updated_at_utc"] = (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    stale_path.write_text(json.dumps(stale_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    got = _choose(stale_path, cluster_id="cluster-ttl", run_start_minute="2026-03-02T14:40")
    got_pid = int(got.get("pattern_id", -1))
    if got_pid != baseline_pid:
        raise AssertionError(
            f"Case5 failed: stale state should be ignored (baseline={baseline_pid}, got={got_pid})."
        )
    return baseline_pid, got_pid


def main() -> int:
    paths = [
        _state_path("case1_a"),
        _state_path("case1_b"),
        _state_path("case2"),
        _state_path("case3"),
        _state_path("case5_fresh"),
        _state_path("case5_stale"),
    ]
    _cleanup(paths)
    try:
        case1_title, case1_pid = validate_deterministic_same_input()
        p1, p2 = validate_rotation_no_repeat()
        cand_count, title_len = validate_candidates_and_length()
        normalized = validate_banned_word_normalization()
        ttl_base, ttl_got = validate_ttl_expiry_ignored()
        print("Case 1 OK: deterministic title selection for same cluster/run")
        print(f"  pattern_id={case1_pid}, title={case1_title}")
        print("Case 2 OK: same cluster pattern rotates to avoid immediate repetition")
        print(f"  first_pattern={p1}, second_pattern={p2}")
        print("Case 3 OK: >=6 candidates and final title length is within 45~70 chars")
        print(f"  candidates={cand_count}, title_len={title_len}")
        print("Case 4 OK: banned words are removed/replaced in normalize_title")
        print(f"  normalized={normalized}")
        print("Case 5 OK: TTL(14d) expired state is ignored")
        print(f"  baseline_pattern={ttl_base}, selected_pattern={ttl_got}")
        print("Stage-7 title diversity validation passed.")
        return 0
    finally:
        _cleanup(paths)


if __name__ == "__main__":
    raise SystemExit(main())
