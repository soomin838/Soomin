from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from re_core.scout import TopicCandidate
from re_core.services.content_allocation import ContentAllocationEngine
from re_core.workflow import AgentWorkflow


class ContentAllocationRuntimeTests(unittest.TestCase):
    def test_engine_sequence_and_targets(self) -> None:
        engine = ContentAllocationEngine(enabled=True, mix_hot=2, mix_search_derived=2, mix_evergreen=1)
        self.assertEqual(engine.targets(), {"hot": 2, "search_derived": 2, "evergreen": 1})
        sequence = engine.daily_sequence()
        self.assertEqual(len(sequence), 5)
        self.assertEqual(sequence.count("hot"), 2)
        self.assertEqual(sequence.count("search_derived"), 2)
        self.assertEqual(sequence.count("evergreen"), 1)

    def test_engine_next_types_moves_to_remaining_bucket(self) -> None:
        engine = ContentAllocationEngine(enabled=True, mix_hot=2, mix_search_derived=2, mix_evergreen=1)
        order = engine.next_content_types(
            day="2026-03-08",
            published_counts={"hot": 1, "search_derived": 0, "evergreen": 0},
        )
        self.assertEqual(order[0], "search_derived")

    def test_workflow_assigns_search_derived_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workflow = AgentWorkflow.__new__(AgentWorkflow)
            workflow.root = Path(tmp)
            workflow._kst = timezone(timedelta(hours=9))
            workflow.settings = SimpleNamespace(
                content_mode=SimpleNamespace(mode="tech_news_only", banned_topic_keywords=[]),
                content_allocation=SimpleNamespace(enabled=True, mix_hot=2, mix_search_derived=2, mix_evergreen=1),
            )
            workflow.content_allocator = ContentAllocationEngine(enabled=True, mix_hot=2, mix_search_derived=2, mix_evergreen=1)
            candidate = TopicCandidate(
                source="search_console",
                title="best password manager for iphone",
                body="Search Console shows strong impressions for this query.",
                score=88,
                url="https://example.com/internal-page",
                meta={"opportunity_source": True, "news_category": "search_derived"},
            )
            meta = workflow._annotate_candidate_content_policy(candidate, requested_type="search_derived")
            self.assertEqual(meta["content_type"], "search_derived")
            self.assertEqual(meta["content_policy"]["min_words"], 1100)
            self.assertEqual(meta["content_policy"]["max_words"], 1500)
            self.assertEqual(meta["content_policy"]["source_strategy"], "authority_first")

    def test_workflow_counts_today_published_types(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log_path = root / "storage" / "logs" / "publish_metadata.jsonl"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            rows = [
                {"ts_utc": datetime(2026, 3, 8, 1, 0, tzinfo=timezone.utc).isoformat(), "content_type": "hot"},
                {"ts_utc": datetime(2026, 3, 8, 2, 0, tzinfo=timezone.utc).isoformat(), "content_type": "hot"},
                {"ts_utc": datetime(2026, 3, 8, 3, 0, tzinfo=timezone.utc).isoformat(), "content_type": "evergreen"},
            ]
            log_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            workflow = AgentWorkflow.__new__(AgentWorkflow)
            workflow.root = root
            workflow._kst = timezone(timedelta(hours=9))
            counts = workflow._today_content_type_counts()
            self.assertEqual(counts["hot"], 2)
            self.assertEqual(counts["evergreen"], 1)
            self.assertEqual(counts["search_derived"], 0)


if __name__ == "__main__":
    unittest.main()
