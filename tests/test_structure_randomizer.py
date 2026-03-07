from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from re_core.scout import TopicCandidate
from re_core.services.search_intent import IntentBundle
from re_core.structure_randomizer import StructureRandomizer


class StructureRandomizerTests(unittest.TestCase):
    def test_pick_outline_returns_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = StructureRandomizer(state_path=Path(tmp) / "state.json", max_attempts=3)
            plan = engine.pick_outline(
                candidate=TopicCandidate(source="news", title="Title", body="Body", score=90, url="https://example.com"),
                intent_bundle=IntentBundle(
                    primary_query="policy change",
                    recommended_archetypes=["policy_change_decode", "news_risk_watch"],
                    outline_brief=["Open fast", "Explain change", "Close with next steps"],
                ),
                category="policy",
                cluster_id="policy_cluster",
            )
        self.assertTrue(plan.fingerprint)
        self.assertGreaterEqual(len(plan.section_titles), 5)
        self.assertTrue(plan.heading_signature)

    def test_raises_when_similarity_stays_too_high(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = StructureRandomizer(state_path=Path(tmp) / "state.json", max_attempts=3, similarity_threshold=0.75)
            candidate = TopicCandidate(source="news", title="Same Title", body="Same Body", score=90, url="https://example.com")
            bundle = IntentBundle(primary_query="same title", recommended_archetypes=["news_impact_explainer"])
            with patch.object(
                engine,
                "_load_recent",
                return_value=[{"fingerprint_text": "same", "section_ids": ["quick_take", "what_happened"]}],
            ), patch.object(engine, "_hybrid_similarity", return_value=0.95):
                with self.assertRaisesRegex(RuntimeError, "template_similarity_too_high"):
                    engine.pick_outline(candidate=candidate, intent_bundle=bundle, category="platform", cluster_id="same")

    def test_heading_signature_repeat_is_treated_as_high_similarity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = StructureRandomizer(state_path=Path(tmp) / "state.json", max_attempts=1, similarity_threshold=0.75)
            candidate = TopicCandidate(source="news", title="Policy shift", body="Body", score=90, url="https://example.com")
            bundle = IntentBundle(primary_query="policy change", recommended_archetypes=["policy_change_decode"])
            with patch.object(
                engine,
                "_load_recent",
                return_value=[
                    {
                        "fingerprint_text": "different",
                        "section_ids": ["quick_take", "timeline", "key_details", "why_it_matters", "sources"],
                        "heading_signature": "quick take|how this unfolded|the important details|why readers care|sources",
                    }
                ],
            ), patch.object(engine, "_hybrid_similarity", return_value=0.20), patch.object(
                engine,
                "_heading_similarity",
                return_value=0.96,
            ):
                with self.assertRaisesRegex(RuntimeError, "template_similarity_too_high"):
                    engine.pick_outline(candidate=candidate, intent_bundle=bundle, category="policy", cluster_id="policy")


if __name__ == "__main__":
    unittest.main()
