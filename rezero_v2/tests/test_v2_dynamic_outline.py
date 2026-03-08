from __future__ import annotations

import unittest

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.grounding import GroundingPacket
from rezero_v2.core.domain.intent import IntentBundle, IntentExpansion
from rezero_v2.core.services.outline_engine import OutlineEngine


class V2DynamicOutlineTest(unittest.TestCase):
    def test_dynamic_outline_without_archetype_dependency(self) -> None:
        candidate = Candidate("1", "hot", "gdelt", "OpenAI new model", "OpenAI new model", "https://example.com", "example.com", "OpenAI released a new model and confirmed pricing and rollout timing.", "ai", "", "gdelt", "en", ["OpenAI"], ["model", "pricing"], ["ai"], {})
        intent = IntentBundle("openai model", "hot", "timely_explainer", "source_grounded", "hero_only", [IntentExpansion("what_changed", "OpenAI new model", "openai model", [], 60, 60, 60, "")], True, "rules")
        packet = GroundingPacket("OpenAI new model", "OpenAI released a new model and confirmed pricing and rollout timing.", "example.com", ["OpenAI"], ["model", "pricing", "rollout"], ["OpenAI released a new model", "pricing was confirmed"], [], "hot", "what_changed", "ai", False, "tech")
        plan = OutlineEngine().generate(candidate=candidate, intent_bundle=intent, grounding_packet=packet, recent_signatures=[])
        self.assertGreaterEqual(len(plan.section_titles), 4)
        self.assertNotIn("Fix 1", " ".join(plan.section_titles))
        self.assertIn(plan.debug_outline_source, {"rules", "ollama"})
