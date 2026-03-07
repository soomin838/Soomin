from __future__ import annotations

import unittest
from pathlib import Path


class PromptCleanupTests(unittest.TestCase):
    def test_banned_prompt_strings_removed(self) -> None:
        root = Path(__file__).resolve().parents[1]
        prompt_factory = (root / "re_core" / "prompt_factory.py").read_text(encoding="utf-8").lower()
        brain = (root / "re_core" / "brain.py").read_text(encoding="utf-8").lower()
        banned = [
            "clickbait headline",
            "physically impossible not to click",
            "fool ai detectors",
            "ai evasion mandate",
            "extreme perplexity",
            "extreme burstiness",
        ]
        merged = prompt_factory + "\n" + brain
        for token in banned:
            with self.subTest(token=token):
                self.assertNotIn(token, merged)


if __name__ == "__main__":
    unittest.main()
