from __future__ import annotations

import unittest

from re_core.services.search_intent import SearchIntentGenerator
from re_core.settings import SearchIntentSettings


class _FakeOllamaSuccess:
    timeout = 60

    def generate_json(self, system_prompt: str, user_payload: dict, purpose: str = "generic") -> dict:
        return {
            "primary_query": "mullvad vpn banned ad what changed",
            "supporting_queries": [
                "mullvad vpn banned ad details",
                "mullvad vpn campaign why banned",
                "what regulators objected to in the ad",
                "what users should watch next",
            ],
            "questions": [
                "What changed?",
                "Who is affected first?",
                "What does the ban actually mean?",
            ],
            "audience": "US mainstream readers",
            "content_kind": "hot",
            "recommended_archetypes": ["news_risk_watch", "policy_change_decode"],
            "outline_brief": [
                "Lead with the policy change.",
                "Explain the ad issue.",
                "Translate the user impact.",
                "Close with what to watch next.",
            ],
            "negative_angles": ["fear framing", "template recap"],
        }


class _FakeOllamaFail:
    timeout = 60

    def generate_json(self, system_prompt: str, user_payload: dict, purpose: str = "generic") -> dict:
        raise TimeoutError("timeout")


class SearchIntentTests(unittest.TestCase):
    def test_ollama_success_returns_bundle_shape(self) -> None:
        generator = SearchIntentGenerator(
            settings=SearchIntentSettings(enabled=True, provider="ollama_then_rules", timeout_sec=15),
            ollama_client=_FakeOllamaSuccess(),
        )
        bundle = generator.generate(
            headline="Mullvad VPN banned TV ad in Brussels",
            snippet="A campaign was pulled after complaints.",
            body_excerpt="Authorities said the ad could mislead viewers about digital privacy.",
            category="policy",
            source_url="https://example.com/ad",
        )
        self.assertEqual(bundle.primary_query, "mullvad vpn banned ad what changed")
        self.assertGreaterEqual(len(bundle.supporting_queries), 4)
        self.assertGreaterEqual(len(bundle.questions), 3)
        self.assertGreaterEqual(len(bundle.recommended_archetypes), 2)

    def test_timeout_falls_back_to_rules(self) -> None:
        generator = SearchIntentGenerator(
            settings=SearchIntentSettings(enabled=True, provider="ollama_then_rules", timeout_sec=15),
            ollama_client=_FakeOllamaFail(),
        )
        bundle = generator.generate(
            headline="Best air purifiers 2026 ranking shift",
            snippet="A consumer ranking made waves.",
            body_excerpt="Shoppers are comparing filter cost, noise, and room coverage.",
            category="consumer",
            source_url="https://example.com/purifier",
        )
        self.assertEqual(generator.last_source, "rules")
        self.assertIn("buyers", bundle.primary_query.lower())
        self.assertGreaterEqual(len(bundle.questions), 3)


if __name__ == "__main__":
    unittest.main()
