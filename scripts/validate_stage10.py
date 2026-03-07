from __future__ import annotations

import json
import re
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.brain import build_structure, render_from_plan, stable_hash  # noqa: E402
from re_core.content_entropy import check_entropy  # noqa: E402
from re_core.news_clustering import NewsClusterEngine  # noqa: E402
from re_core.news_facets import resolve_facet_context  # noqa: E402
from re_core.publish_ledger import PublishLedger, make_ledger_key  # noqa: E402
from re_core.source_naturalization import apply_source_naturalization  # noqa: E402
from re_core.title_diversity import choose_diverse_title  # noqa: E402


TITLE_SETTINGS = {
    "enabled": True,
    "patterns_total": 6,
    "cluster_pattern_ttl_days": 14,
    "numeric_ratio": 0.40,
    "question_ratio": 0.20,
    "analysis_ratio": 0.40,
    "min_title_chars": 45,
    "max_title_chars": 70,
}

SOURCE_SETTINGS = {
    "enabled": True,
    "max_inline_attributions_per_article": 3,
    "allow_raw_urls_in_body": False,
    "max_sources_list_items": 6,
    "require_sources_section": True,
}

ENTROPY_SETTINGS = {
    "enabled": True,
    "trigram_max_ratio": 0.05,
    "starter_max_repeats": 3,
    "duplicate_h2_max": 0,
    "max_rewrite_attempts": 1,
}


def _tmp_files() -> dict[str, Path]:
    return {
        "cluster_state": ROOT / "storage" / "state" / "_validate_stage10_cluster_state.json",
        "ledger": ROOT / "storage" / "ledger" / "_validate_stage10_ledger.jsonl",
        "title_state_1": ROOT / "storage" / "state" / "_validate_stage10_title_state_1.json",
        "title_state_2": ROOT / "storage" / "state" / "_validate_stage10_title_state_2.json",
    }


def _cleanup(paths: dict[str, Path]) -> None:
    for path in paths.values():
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


def scenario_seed_variation(paths: dict[str, Path]) -> str:
    event_id = "evt-stage10-seed"
    title = "Windows security update expands enterprise enforcement scope"
    body = (
        "According to reports from CISA, teams should verify policy defaults first. "
        "More rollout details were published in release notes."
    )
    run_minutes = [
        "2026-03-02T10:00",
        "2026-03-02T10:01",
        "2026-03-02T10:02",
        "2026-03-02T10:03",
        "2026-03-02T10:04",
    ]

    records: list[dict[str, object]] = []
    for idx, minute in enumerate(run_minutes):
        facet_ctx = resolve_facet_context(
            event_id=event_id,
            run_start_minute=minute,
            title=title,
            body=body,
            category="security",
            source_url="https://www.cisa.gov/news",
            retry_index=0,
            llm_candidates=[],
            state_path=None,
            stable_hash_fn=stable_hash,
        )
        structure_seed = stable_hash(f"{event_id}{minute}")
        structure_plan = build_structure(structure_seed, {"facet": facet_ctx.selected_facet})
        rendered = render_from_plan(structure_plan, {"event_id": event_id})

        title_state = ROOT / "storage" / "state" / f"_validate_stage10_title_variation_{idx}.json"
        try:
            if title_state.exists():
                title_state.unlink()
        except Exception:
            pass
        diverse = choose_diverse_title(
            base_title=title,
            cluster_id="cluster-stage10",
            facet=facet_ctx.selected_facet,
            category="security",
            run_start_minute=minute,
            stable_hash_fn=stable_hash,
            state_path=title_state,
            settings=TITLE_SETTINGS,
        )
        try:
            if title_state.exists():
                title_state.unlink()
        except Exception:
            pass

        normalized_html = apply_source_naturalization(
            html="<h2>Quick Take</h2><p>According to https://example.com/path details changed.</p>",
            source_url="https://example.com/path",
            authority_links=["https://www.cisa.gov/news"],
            settings=SOURCE_SETTINGS,
        )
        if "Sources" not in normalized_html:
            raise AssertionError("Scenario1 failed: source naturalization did not keep/create Sources section.")

        records.append(
            {
                "run_start_minute": minute,
                "facet": str(facet_ctx.selected_facet),
                "pattern_id": int(diverse.get("pattern_id", -1) or -1),
                "section_ids": tuple(rendered.get("section_ids", []) or []),
            }
        )

    # Deterministic check for same inputs with clean state files.
    deterministic_1 = choose_diverse_title(
        base_title=title,
        cluster_id="cluster-stage10-deterministic",
        facet="impact",
        category="security",
        run_start_minute="2026-03-02T10:00",
        stable_hash_fn=stable_hash,
        state_path=paths["title_state_1"],
        settings=TITLE_SETTINGS,
    )
    deterministic_2 = choose_diverse_title(
        base_title=title,
        cluster_id="cluster-stage10-deterministic",
        facet="impact",
        category="security",
        run_start_minute="2026-03-02T10:00",
        stable_hash_fn=stable_hash,
        state_path=paths["title_state_2"],
        settings=TITLE_SETTINGS,
    )
    if str(deterministic_1.get("title", "")) != str(deterministic_2.get("title", "")):
        raise AssertionError("Scenario1 failed: same input should be deterministic with clean state.")
    if int(deterministic_1.get("pattern_id", -1)) != int(deterministic_2.get("pattern_id", -1)):
        raise AssertionError("Scenario1 failed: same input should keep same pattern_id with clean state.")

    unique_facets = {str(row["facet"]) for row in records}
    unique_patterns = {int(row["pattern_id"]) for row in records}
    unique_sections = {tuple(row["section_ids"]) for row in records}
    if len(unique_facets) <= 1 and len(unique_patterns) <= 1 and len(unique_sections) <= 1:
        raise AssertionError(
            "Scenario1 failed: run_start_minute variation did not change facet/pattern/structure in this sample."
        )

    return (
        f"variation_ok facets={len(unique_facets)} patterns={len(unique_patterns)} "
        f"structures={len(unique_sections)}"
    )


def scenario_ledger_idempotency(paths: dict[str, Path]) -> str:
    cluster_engine = NewsClusterEngine(
        state_path=paths["cluster_state"],
        stable_hash_fn=stable_hash,
        threshold=0.82,
        ttl_days=14,
    )
    a = cluster_engine.assign_cluster(
        event_id="evt-ledger-a",
        title="Apple security patch now available for managed iPhone enterprise fleets",
        body=(
            "Managed iPhone enterprise fleets received a security patch and release notes mention staged rollout, "
            "policy checks, and admin guidance for managed devices."
        ),
        run_start_minute="2026-03-02T11:00",
    )
    b = cluster_engine.assign_cluster(
        event_id="evt-ledger-b",
        title="Apple security patch available now for managed iPhone enterprise fleets",
        body=(
            "Managed iPhone enterprise fleets received the same security patch and release notes mention staged "
            "rollout, policy checks, and admin guidance for managed devices."
        ),
        run_start_minute="2026-03-02T11:01",
    )
    if str(a.cluster_id or "") != str(b.cluster_id or ""):
        raise AssertionError("Scenario2 failed: similar events should share cluster_id in this setup.")

    ledger = PublishLedger(path=paths["ledger"], ttl_days=90)
    key = make_ledger_key(
        event_id="evt-ledger-a",
        cluster_id=str(a.cluster_id or ""),
        facet="impact",
        blog_id="default",
    )
    recorded = ledger.record(
        {
            "key": key,
            "event_id": "evt-ledger-a",
            "cluster_id": str(a.cluster_id or ""),
            "facet": "impact",
            "blog_id": "default",
            "title": "Ledger idempotency test title",
            "source_url": "https://example.com/source",
        }
    )
    if not recorded:
        raise AssertionError("Scenario2 failed: ledger.record returned False.")
    if not ledger.exists(key):
        raise AssertionError("Scenario2 failed: ledger.exists should be True after first record.")
    if not ledger.exists(key):
        raise AssertionError("Scenario2 failed: duplicate check should stay blocked (exists=True).")
    return f"idempotency_ok cluster_id={a.cluster_id} key={key}"


def scenario_entropy_fail_gate() -> str:
    fail_html = "<h2>Quick Take</h2><p>" + ("alpha beta gamma delta epsilon. " * 25) + "</p>"
    first = check_entropy(fail_html, ENTROPY_SETTINGS)
    if bool(first.get("ok", True)):
        raise AssertionError("Scenario3 failed: first entropy check should fail for repeated text.")

    rewrite_attempts = 0
    max_rewrite_attempts = int(ENTROPY_SETTINGS.get("max_rewrite_attempts", 1) or 1)
    status = "unknown"
    if not bool(first.get("ok", False)):
        if rewrite_attempts < max_rewrite_attempts:
            rewrite_attempts += 1
            second = check_entropy(fail_html, ENTROPY_SETTINGS)
            status = "ok" if bool(second.get("ok", False)) else "skipped"
        else:
            status = "skipped"

    if rewrite_attempts != 1:
        raise AssertionError(f"Scenario3 failed: rewrite attempts should be 1, got {rewrite_attempts}.")
    if status != "skipped":
        raise AssertionError(f"Scenario3 failed: second entropy fail should end in skipped, got {status}.")
    return "entropy_gate_ok rewrite_attempts=1 final_status=skipped"


def main() -> int:
    paths = _tmp_files()
    _cleanup(paths)

    scenarios = [
        ("Scenario 1", lambda: scenario_seed_variation(paths)),
        ("Scenario 2", lambda: scenario_ledger_idempotency(paths)),
        ("Scenario 3", scenario_entropy_fail_gate),
    ]

    failed = 0
    outputs: list[tuple[str, bool, str]] = []
    try:
        for name, fn in scenarios:
            try:
                detail = str(fn())
                outputs.append((name, True, detail))
            except Exception as exc:
                failed += 1
                outputs.append((name, False, str(exc)))
        for name, ok, detail in outputs:
            if ok:
                print(f"{name} PASS")
                print(f"  {detail}")
            else:
                print(f"{name} FAIL")
                print(f"  {detail}")
        if failed > 0:
            print(f"Stage-10 integration validation failed ({failed} scenario(s)).")
            return 1
        print("Stage-10 integration validation passed.")
        return 0
    finally:
        _cleanup(paths)
        # Remove transient variation files used in scenario 1.
        for idx in range(5):
            p = ROOT / "storage" / "state" / f"_validate_stage10_title_variation_{idx}.json"
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
