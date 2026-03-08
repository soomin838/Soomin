from __future__ import annotations

import unittest

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.draft import DraftArtifact
from rezero_v2.core.domain.grounding import GroundingPacket
from rezero_v2.core.domain.intent import IntentBundle, IntentExpansion
from rezero_v2.core.domain.outline import OutlinePlan
from rezero_v2.core.pipeline.draft_stage import DraftStage


class _DraftEngine:
    def __init__(self) -> None:
        self.generate_calls = 0
        self.repair_calls = 0

    def generate(self, **kwargs):
        self.generate_calls += 1
        return DraftArtifact(
            title="Bad draft",
            intro="generic filler",
            html="<p>workflow main tradeoff source frame</p>",
            plain_text="workflow main tradeoff source frame generic filler",
            section_titles=["Generic section"],
            word_count=10,
            repair_attempted=False,
            repair_succeeded=False,
            source_citations=[],
        )

    def repair(self, **kwargs):
        self.repair_calls += 1
        return DraftArtifact(
            title="Still bad",
            intro="generic filler",
            html="<p>workflow main tradeoff source frame</p>",
            plain_text="workflow main tradeoff source frame generic filler",
            section_titles=["Generic section"],
            word_count=10,
            repair_attempted=True,
            repair_succeeded=False,
            source_citations=[],
        )


class _Context:
    class allocation:
        target_word_range = (700, 1000)


class V2RepairPassLimitTest(unittest.TestCase):
    def test_one_repair_pass_maximum(self) -> None:
        candidate = Candidate(
            candidate_id="1",
            content_type="hot",
            source_type="gdelt",
            title="OpenAI model update",
            source_title="OpenAI model update",
            source_url="https://example.com",
            source_domain="example.com",
            source_snippet="OpenAI released a model and confirmed pricing.",
            category="ai",
            published_at_utc="",
            provider="gdelt",
            language="en",
            source_headline="OpenAI model update",
            normalized_source_headline="OpenAI model update",
            derived_primary_query="openai model update what changed",
            entity_terms=["OpenAI"],
            topic_terms=["model"],
            tags=["ai"],
            raw_meta={"intent_family": "what_changed"},
        )
        intent = IntentBundle(
            primary_query="openai model update",
            content_type="hot",
            title_strategy="timely_explainer",
            source_strategy="source_grounded",
            image_strategy="hero_only",
            chosen_intent_family="what_changed",
            normalized_source_headline="OpenAI model update",
            derived_primary_query="openai model update what changed",
            contract_id="contract",
            expansions=[IntentExpansion("what_changed", "OpenAI model update", "openai model update", [], 50, 50, 50, "")],
            source_grounded=True,
            source_model="rules",
        )
        packet = GroundingPacket(
            source_headline="OpenAI model update",
            normalized_source_headline="OpenAI model update",
            derived_primary_query="openai model update what changed",
            canonical_source_title="OpenAI model update",
            source_snippet="OpenAI released a model and confirmed pricing.",
            source_domain="example.com",
            required_named_entities=["OpenAI"],
            required_topic_nouns=["model", "pricing"],
            required_source_facts=["OpenAI released a model", "pricing was confirmed"],
            forbidden_drift_terms=[],
            packet_quality_score=80.0,
            content_type="hot",
            intent_family="what_changed",
            category="ai",
            mixed_domain=False,
            dominant_axis="tech",
        )
        outline = OutlinePlan(["What happened"], ["event summary"], "what happened", 50.0, 80.0, "rules")
        engine = _DraftEngine()
        stage = DraftStage(draft_engine=engine, coherence_guard=__import__("rezero_v2.core.guards.coherence_guard", fromlist=["CoherenceGuard"]).CoherenceGuard())
        result = stage.run(_Context(), candidate=candidate, intent_bundle=intent, grounding_packet=packet, outline_plan=outline)
        self.assertEqual(result.status, "skipped")
        self.assertEqual(engine.generate_calls, 1)
        self.assertEqual(engine.repair_calls, 1)
