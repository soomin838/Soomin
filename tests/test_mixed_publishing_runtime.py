from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from re_core.news_pool import NewsPoolStore
from re_core.services.content_allocation import ContentAllocationEngine
from re_core.services.search_intent import IntentBundle, SearchIntentGenerator
from re_core.services.topic_scoring import TopicScoring


class MixedPublishingRuntimeTests(unittest.TestCase):
    def test_allocator_uses_configured_lengths_and_slot_metadata(self) -> None:
        lengths = SimpleNamespace(
            hot_news_min=720,
            hot_news_max=980,
            search_derived_min=1120,
            search_derived_max=1480,
            evergreen_min=1650,
            evergreen_max=2190,
        )
        engine = ContentAllocationEngine(
            enabled=True,
            mix_hot=2,
            mix_search_derived=2,
            mix_evergreen=1,
            content_lengths=lengths,
        )
        slot = engine.slot_for("search_derived")
        policy = engine.policy_for("evergreen")
        self.assertEqual(slot.content_type, "search_derived")
        self.assertEqual(slot.source_type, "search_console_or_news_seed")
        self.assertEqual(slot.generation_mode_hint, "search_answer")
        self.assertEqual(slot.target_word_range, (1120, 1480))
        self.assertEqual(policy.min_words, 1650)
        self.assertEqual(policy.max_words, 2190)

    def test_search_intent_builds_publishable_search_candidates(self) -> None:
        generator = SearchIntentGenerator()
        bundle = IntentBundle(
            primary_query="iphone privacy update what changed",
            supporting_queries=[
                "iphone privacy update what changed",
                "should you change iphone privacy settings",
                "iphone privacy update comparison",
                "iphone privacy update alternatives",
            ],
            questions=[
                "What changed in the iPhone privacy update?",
                "Should users change settings right now?",
            ],
            content_kind="hot",
        )
        specs = generator.build_search_candidates(
            bundle=bundle,
            headline="Apple rolls out a new iPhone privacy update",
            category="platform",
            source_url="https://example.com/apple-privacy",
            max_candidates=4,
        )
        self.assertGreaterEqual(len(specs), 3)
        self.assertTrue(all(spec.content_type == "search_derived" for spec in specs))
        self.assertTrue(any(spec.candidate_kind == "what-changed" for spec in specs))
        self.assertTrue(any(spec.candidate_kind == "should-you" for spec in specs))
        self.assertTrue(any("privacy" in spec.title.lower() for spec in specs))

    def test_topic_scoring_differs_by_content_type(self) -> None:
        scoring = TopicScoring()
        hot = scoring.score_for_type(
            "chip launch",
            content_type="hot",
            freshness=95,
            trend_score=92,
            search=50,
            ctr=40,
            cluster=60,
            relevance=70,
            usefulness=72,
            search_demand=58,
        )
        evergreen = scoring.score_for_type(
            "best password manager",
            content_type="evergreen",
            search=75,
            ctr=55,
            cluster=68,
            relevance=82,
            durability=90,
            freshness=48,
            search_demand=78,
            usefulness=86,
            evergreen_potential=92,
        )
        self.assertEqual(hot.content_type, "hot")
        self.assertEqual(evergreen.content_type, "evergreen")
        self.assertGreater(evergreen.evergreen_potential, hot.evergreen_potential)
        self.assertGreater(hot.trend, evergreen.freshness)
        self.assertNotEqual(evergreen.total, hot.total)

    def test_news_pool_recent_items_exposes_seed_input(self) -> None:
        fd, raw_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        db_path = Path(raw_path)
        try:
            store = NewsPoolStore(db_path)
            store.upsert_items(
                [
                    {
                        "url": "https://example.com/apple-chip",
                        "title": "Apple chip update explained",
                        "source": "example.com",
                        "provider": "gdelt",
                        "topic": "chips",
                        "published_at": "2026-03-08T00:00:00+00:00",
                        "snippet": "What changed in Apple's chip roadmap.",
                        "category": "chips",
                        "score": 88,
                    }
                ]
            )
            rows = store.recent_items(days=30, limit=5, min_score=70)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["title"], "Apple chip update explained")
            del store
        finally:
            try:
                db_path.unlink(missing_ok=True)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
