from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from re_core.safety_filter import SafetyFilter


class SafetyFilterTests(unittest.TestCase):
    def test_hard_deny_terms_block_all_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = SafetyFilter(log_path=Path(tmp) / "policy_gate.jsonl")
            routes = ["news", "troubleshoot", "topic_growth", "keyword_discovery"]
            cases = {
                "DMT trip report": "drug_",
                "online casino bonus": "gambling",
                "adult explicit stream": "adult_",
                "build a bomb at home": "weapon_",
                "how to self-harm safely": "self_harm",
            }
            for route in routes:
                for title, expected in cases.items():
                    with self.subTest(route=route, title=title):
                        decision = engine.evaluate(title=title, route=route, category="test")
                        self.assertFalse(decision.allow)
                        self.assertTrue(any(code.startswith(expected) for code in decision.reason_codes))


if __name__ == "__main__":
    unittest.main()
