from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from re_core.services.search_intent import IntentBundle, SearchIntentGenerator
from re_core.services.topic_clusters import TopicClusterBuilder
from re_core.services.topic_scoring import TopicScoring


class TopicIntelligenceRuntimeTests(unittest.TestCase):
    def test_search_intent_expansion_produces_multiple_publishable_families(self) -> None:
        generator = SearchIntentGenerator()
        bundle = IntentBundle(
            primary_query="OpenAI new model",
            supporting_queries=[
                "OpenAI model pricing comparison",
                "how to use OpenAI new model",
                "OpenAI new model alternatives",
                "should you upgrade to OpenAI new model",
            ],
            questions=[
                "What changed in the new OpenAI model?",
                "How should users compare it with the previous model?",
            ],
            content_kind="hot",
        )
        specs = generator.build_search_candidates(
            bundle=bundle,
            headline="OpenAI releases a new model for developers",
            category="ai",
            source_url="https://example.com/openai-model",
            max_candidates=6,
        )
        families = {spec.intent_family for spec in specs}
        self.assertGreaterEqual(len(specs), 4)
        self.assertIn("what_changed", families)
        self.assertIn("comparison", families)
        self.assertIn("how_to", families)

    def test_topic_cluster_builder_remembers_pillar_and_supporting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            builder = TopicClusterBuilder(state_path=Path(tmp) / "clusters.json")
            pillar = builder.preview_assignment(
                title="AI productivity tools guide",
                primary_query="ai productivity tools",
                content_type="evergreen",
                cluster_id="ai-productivity",
                intent_family="guide",
            )
            self.assertEqual(pillar.cluster_role, "pillar")
            builder.remember_published(
                assignment=pillar,
                title="AI productivity tools guide",
                url="https://example.com/pillar",
            )
            supporting = builder.preview_assignment(
                title="Best AI productivity tools for small teams",
                primary_query="best ai productivity tools",
                content_type="search_derived",
                cluster_id="ai-productivity",
                intent_family="comparison",
            )
            self.assertEqual(supporting.cluster_role, "supporting")
            self.assertEqual(supporting.pillar_title, "AI productivity tools guide")
            self.assertEqual(supporting.pillar_query, "ai productivity tools")

    def test_topic_scoring_uses_intent_signals(self) -> None:
        scoring = TopicScoring()
        comparison = scoring.score_for_type(
            "openai model comparison",
            content_type="search_derived",
            freshness=64,
            search_demand=84,
            usefulness=82,
            comparison_potential=90,
            tutorial_potential=20,
            competition_inverse=72,
        )
        generic = scoring.score_for_type(
            "openai model article",
            content_type="search_derived",
            freshness=64,
            search_demand=84,
            usefulness=68,
            comparison_potential=20,
            tutorial_potential=20,
            competition_inverse=72,
        )
        self.assertGreater(comparison.total, generic.total)


if __name__ == "__main__":
    unittest.main()
