from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.brain import stable_hash  # noqa: E402
from core.news_clustering import NewsClusterEngine, should_skip_same_run, similarity  # noqa: E402


def _state_path() -> Path:
    return ROOT / "storage" / "state" / "_validate_stage3_state.json"


def _new_engine(path: Path) -> NewsClusterEngine:
    return NewsClusterEngine(
        state_path=path,
        stable_hash_fn=stable_hash,
        threshold=0.82,
        ttl_days=14,
    )


def validate_similar_same_cluster(path: Path) -> tuple[str, float]:
    engine = _new_engine(path)
    title_a = "microsoft security update causes sign in timeout for users"
    body_a = "official statement says staged rollout mitigation in progress for enterprise users this week."
    title_b = "microsoft security update causes sign in timeout for users"
    body_b = "official statement says staged rollout mitigation in progress for enterprise users this week."
    raw_sim = similarity(title_a, body_a, title_b, body_b)
    first = engine.assign_cluster(
        event_id="evt-sim-1",
        title=title_a,
        body=body_a,
        run_start_minute="2026-03-02T16:00",
    )
    second = engine.assign_cluster(
        event_id="evt-sim-2",
        title=title_b,
        body=body_b,
        run_start_minute="2026-03-02T16:01",
    )
    if raw_sim < 0.82:
        raise AssertionError(f"Case1 setup failed: similarity below threshold ({raw_sim:.4f}).")
    if first.cluster_id != second.cluster_id:
        raise AssertionError("Case1 failed: highly similar texts were assigned different cluster_id.")
    return first.cluster_id, raw_sim


def validate_different_clusters(path: Path) -> tuple[str, str]:
    engine = _new_engine(path)
    left = engine.assign_cluster(
        event_id="evt-diff-1",
        title="Apple releases iOS emergency patch for wireless bug",
        body="Patch notes focus on bluetooth reliability and urgent mitigation guidance.",
        run_start_minute="2026-03-02T16:02",
    )
    right = engine.assign_cluster(
        event_id="evt-diff-2",
        title="NVIDIA reports quarterly data center revenue surge",
        body="Investors reacted to margin expansion and supply-chain outlook in earnings call.",
        run_start_minute="2026-03-02T16:03",
    )
    if left.cluster_id == right.cluster_id:
        raise AssertionError("Case2 failed: clearly different texts produced same cluster_id.")
    return left.cluster_id, right.cluster_id


def validate_ttl_expiry(path: Path) -> tuple[str, bool]:
    old_seen = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    payload = {
        "version": 1,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "clusters": [
            {
                "cluster_id": "expired-cluster-0001",
                "rep_title": "Google platform update changes sync policy",
                "rep_text": "google platform update changes sync policy for enterprise devices",
                "last_seen_utc": old_seen,
                "seen_count": 5,
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    engine = _new_engine(path)
    fresh = engine.assign_cluster(
        event_id="evt-ttl-1",
        title="Google platform update changes sync policy",
        body="Enterprise admins report updated sync defaults and rollout guidance.",
        run_start_minute="2026-03-02T16:04",
    )
    if fresh.matched_existing:
        raise AssertionError("Case3 failed: expired cluster should not be matched.")
    if fresh.cluster_id == "expired-cluster-0001":
        raise AssertionError("Case3 failed: expired cluster_id was reused directly.")
    return fresh.cluster_id, fresh.matched_existing


def validate_same_run_skip(path: Path) -> tuple[bool, bool]:
    engine = _new_engine(path)
    seen: set[str] = set()
    first = engine.assign_cluster(
        event_id="evt-run-1",
        title="anthropic policy update changes model access tiers",
        body="official note says rollout starts with enterprise accounts this week.",
        run_start_minute="2026-03-02T16:05",
    )
    second = engine.assign_cluster(
        event_id="evt-run-2",
        title="anthropic policy update changes model access tiers",
        body="official note says rollout starts with enterprise accounts this week.",
        run_start_minute="2026-03-02T16:05",
    )
    skip_first = should_skip_same_run(first.cluster_id, seen)
    skip_second = should_skip_same_run(second.cluster_id, seen)
    if skip_first:
        raise AssertionError("Case4 failed: first cluster in run should not be skipped.")
    if not skip_second:
        raise AssertionError("Case4 failed: second same-cluster candidate in run should be skipped.")
    return skip_first, skip_second


def main() -> int:
    path = _state_path()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass

    try:
        cluster_same, sim = validate_similar_same_cluster(path)
        left, right = validate_different_clusters(path)
        ttl_cluster, ttl_matched = validate_ttl_expiry(path)
        skip_first, skip_second = validate_same_run_skip(path)
        print("Case 1 OK: similar texts map to same cluster_id")
        print(f"  cluster_id={cluster_same}, similarity={sim:.4f}")
        print("Case 2 OK: different texts map to different cluster_id")
        print(f"  left={left}, right={right}")
        print("Case 3 OK: expired clusters are excluded by 14-day TTL")
        print(f"  new_cluster_id={ttl_cluster}, matched_existing={ttl_matched}")
        print("Case 4 OK: same-run duplicate cluster is skipped")
        print(f"  first_skip={skip_first}, second_skip={skip_second}")
        print("Stage-3 clustering validation passed.")
        return 0
    finally:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
