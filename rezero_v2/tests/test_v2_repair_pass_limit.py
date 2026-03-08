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
        candidate = Candidate("1", "hot", "gdelt", "OpenAI model update", "OpenAI model update", "https://example.com", "example.com", "OpenAI released a model and confirmed pricing.", "ai", "", "gdelt", "en", ["OpenAI"], ["model"], ["ai"], {})
        intent = IntentBundle("openai model update", "hot", "timely_explainer", "source_grounded", "hero_only", [IntentExpansion("what_changed", "OpenAI model update", "openai model update", [], 50, 50, 50, "")], True, "rules")
        packet = GroundingPacket("OpenAI model update", "OpenAI released a model and confirmed pricing.", "example.com", ["OpenAI"], ["model", "pricing"], ["OpenAI released a model", "pricing was confirmed"], [], "hot", "what_changed", "ai", False, "tech")
        outline = OutlinePlan(["What happened"], ["event summary"], "what happened", 50.0, 80.0, "rules")
        engine = _DraftEngine()
        stage = DraftStage(draft_engine=engine, coherence_guard=__import__("rezero_v2.core.guards.coherence_guard", fromlist=["CoherenceGuard"]).CoherenceGuard())
        result = stage.run(_Context(), candidate=candidate, intent_bundle=intent, grounding_packet=packet, outline_plan=outline)
        self.assertEqual(result.status, "skipped")
        self.assertEqual(engine.generate_calls, 1)
        self.assertEqual(engine.repair_calls, 1)
