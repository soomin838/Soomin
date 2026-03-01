from __future__ import annotations

import json
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.news_pool import NewsPoolStore

WORKFLOW_IMPORT_ERROR = ""
try:
    from core.scout import TopicCandidate
    from core.settings import load_settings
    from core.workflow import AgentWorkflow
except Exception as exc:  # pragma: no cover - defensive import guard for local smoke environments
    TopicCandidate = None  # type: ignore[assignment]
    load_settings = None  # type: ignore[assignment]
    AgentWorkflow = None  # type: ignore[assignment]
    WORKFLOW_IMPORT_ERROR = str(exc)


def _check(name: str, passed: bool, detail: str) -> dict:
    return {"name": str(name), "passed": bool(passed), "detail": str(detail or "")}


def _db_count(path: Path, query: str) -> int:
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(query).fetchone()
    finally:
        conn.close()
    return int(row[0] if row else 0)


def test_event_level_clustering(tmp_db: Path) -> dict:
    store = NewsPoolStore(tmp_db)
    rows = [
        {
            "url": "https://site-a.example/news/alpha",
            "title": "OpenAI adds enterprise controls to model deployment",
            "snippet": "Enterprise controls and policy timeline for model rollout were announced today.",
            "source": "site-a.example",
            "category": "ai",
            "score": 90,
        },
        {
            "url": "https://site-b.example/coverage/alpha",
            "title": "OpenAI adds enterprise controls to model deployment",
            "snippet": "Model rollout policy timeline and enterprise controls were announced in official notes.",
            "source": "site-b.example",
            "category": "ai",
            "score": 88,
        },
    ]
    store.upsert_items(rows)
    event_count = _db_count(tmp_db, "SELECT COUNT(*) FROM news_events")
    return _check(
        "event_level_cluster",
        event_count == 1,
        f"news_events={event_count} (expected 1)",
    )


def test_publish_ledger_and_restart(tmp_db: Path) -> tuple[dict, dict]:
    store = NewsPoolStore(tmp_db)
    claimed = store.claim_one(top_k=10)
    if not claimed:
        return (
            _check("publish_ledger_insert", False, "claim_one returned no event"),
            _check("restart_no_republish", False, "claim_one returned no event"),
        )

    event_id = str((claimed or {}).get("event_id", "") or (claimed or {}).get("id", "") or "").strip()
    marked = store.mark_used(event_id, "https://example.com/posts/ledger-check")
    ledger_count = _db_count(tmp_db, "SELECT COUNT(*) FROM publish_ledger")
    check_ledger = _check(
        "publish_ledger_insert",
        bool(marked) and ledger_count == 1,
        f"marked={marked}, publish_ledger={ledger_count}",
    )

    # Simulate restart: reopen store and claim again.
    store2 = NewsPoolStore(tmp_db)
    claimed_after_restart = store2.claim_one(top_k=10)
    check_restart = _check(
        "restart_no_republish",
        claimed_after_restart is None,
        f"claimed_after_restart={bool(claimed_after_restart)}",
    )
    return check_ledger, check_restart


def test_title_diversity(workflow: AgentWorkflow) -> dict:
    titles: list[str] = []
    seeds = [
        "Cloud provider updates enterprise auth defaults",
        "Chip vendor revises AI accelerator roadmap",
        "Major app store changes ranking disclosure policy",
        "Browser vendor expands memory safety rollout",
        "Messaging platform adds business compliance controls",
    ]
    for seed in seeds:
        candidate = TopicCandidate(
            source="validation",
            title=seed,
            body=f"{seed} with operational impact notes and policy timeline.",
            score=90,
            url="https://example.com/source",
            main_entity="",
            long_tail_keywords=[seed.lower(), "what to watch next"],
            meta={"news_category": "ai"},
        )
        title = workflow._enforce_seo_title(  # noqa: SLF001
            title=seed,
            candidate=candidate,
            global_keywords=list(candidate.long_tail_keywords or []),
            preferred_keyword=seed,
        )
        titles.append(title)

    first4 = {
        workflow._title_first_words_key(t, n=4)  # noqa: SLF001
        for t in titles
        if str(t or "").strip()
    }
    return _check(
        "title_diversity_5_posts",
        len(first4) >= 5,
        f"unique_first4={len(first4)}, titles={titles}",
    )


def test_news_html_rules(workflow: AgentWorkflow) -> dict:
    candidate = TopicCandidate(
        source="validation",
        title="Cloud security policy update changes enterprise rollout timing",
        body="Enterprise rollout timing and policy update details with practical impact.",
        score=90,
        url="https://techcrunch.com/example-news",
        main_entity="Cloud security policy",
        long_tail_keywords=["cloud security policy update", "enterprise rollout timing"],
        meta={"news_category": "security"},
    )
    draft = workflow._build_news_post_local_fallback(  # noqa: SLF001
        selected=candidate,
        category="security",
        authority_links=[
            "https://www.cisa.gov/news-events",
            "https://www.nist.gov/cyberframework",
        ],
    )
    html = str(getattr(draft, "html", "") or "")
    has_faq = bool(re.search(r"<h[23][^>]*>\s*faq\s*</h[23]>", html, flags=re.IGNORECASE))
    has_google_link = bool(re.search(r"https?://(?:www\\.)?google\\.com", html, flags=re.IGNORECASE))
    has_sources = "<h2>Sources</h2>" in html
    source_label_ok = bool(
        re.search(r"<li><a href=\"https?://[^\"]+\"[^>]*>[^<]+</a></li>", html, flags=re.IGNORECASE)
    )
    passed = (not has_faq) and (not has_google_link) and has_sources and source_label_ok
    detail = (
        f"has_faq={has_faq}, has_google_link={has_google_link}, "
        f"has_sources={has_sources}, source_label_ok={source_label_ok}"
    )
    return _check("html_rules_no_faq_no_google_labeled_sources", passed, detail)


def main() -> int:
    results: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="news_engine_validation_") as tmp:
        tmp_db = Path(tmp) / "news_pool.sqlite3"
        results.append(test_event_level_clustering(tmp_db))
        ledger_check, restart_check = test_publish_ledger_and_restart(tmp_db)
        results.append(ledger_check)
        results.append(restart_check)

    if AgentWorkflow is None or load_settings is None or TopicCandidate is None:
        results.append(
            _check(
                "title_diversity_5_posts",
                False,
                f"workflow_import_error={WORKFLOW_IMPORT_ERROR or 'unknown'}",
            )
        )
        results.append(
            _check(
                "html_rules_no_faq_no_google_labeled_sources",
                False,
                f"workflow_import_error={WORKFLOW_IMPORT_ERROR or 'unknown'}",
            )
        )
    else:
        settings = load_settings(ROOT / "config" / "settings.yaml")
        workflow = AgentWorkflow(ROOT, settings)
        results.append(test_title_diversity(workflow))
        results.append(test_news_html_rules(workflow))

    passed = all(bool(r.get("passed", False)) for r in results)
    report = {"passed": passed, "results": results}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
