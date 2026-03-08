from __future__ import annotations

import unittest

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.grounding import GroundingPacket
from rezero_v2.core.domain.intent import IntentBundle, IntentExpansion
from rezero_v2.core.services.outline_engine import OutlineEngine


class V2DynamicOutlineTest(unittest.TestCase):
    def test_dynamic_outline_without_archetype_dependency(self) -> None:
        candidate = Candidate(
            candidate_id="1",
            content_type="hot",
            source_type="gdelt",
            title="OpenAI new model",
            source_title="OpenAI new model",
            source_url="https://example.com",
            source_domain="example.com",
            source_snippet="OpenAI released a new model and confirmed pricing and rollout timing.",
            category="ai",
            published_at_utc="",
            provider="gdelt",
            language="en",
            source_headline="OpenAI new model",
            normalized_source_headline="OpenAI new model",
            derived_primary_query="openai model what changed",
            entity_terms=["OpenAI"],
            topic_terms=["model", "pricing"],
            tags=["ai"],
            raw_meta={"intent_family": "what_changed"},
        )
        intent = IntentBundle(
            primary_query="openai model what changed",
            content_type="hot",
            title_strategy="timely_explainer",
            source_strategy="source_grounded",
            image_strategy="hero_only",
            chosen_intent_family="what_changed",
            normalized_source_headline="OpenAI new model",
            derived_primary_query="openai model what changed",
            contract_id="contract",
            expansions=[IntentExpansion("what_changed", "OpenAI new model", "openai model", [], 60, 60, 60, "")],
            source_grounded=True,
            source_model="rules",
        )
        packet = GroundingPacket(
            source_headline="OpenAI new model",
            normalized_source_headline="OpenAI new model",
            derived_primary_query="openai model what changed",
            canonical_source_title="OpenAI new model",
            source_snippet="OpenAI released a new model and confirmed pricing and rollout timing.",
            source_domain="example.com",
            required_named_entities=["OpenAI"],
            required_topic_nouns=["model", "pricing", "rollout"],
            required_source_facts=["OpenAI released a new model", "pricing was confirmed"],
            forbidden_drift_terms=[],
            packet_quality_score=80.0,
            content_type="hot",
            intent_family="what_changed",
            category="ai",
            mixed_domain=False,
            dominant_axis="tech",
        )
        plan = OutlineEngine().generate(candidate=candidate, intent_bundle=intent, grounding_packet=packet, recent_signatures=[])
        self.assertGreaterEqual(len(plan.section_titles), 4)
        self.assertNotIn("Fix 1", " ".join(plan.section_titles))
        self.assertIn(plan.debug_outline_source, {"rules", "ollama"})
