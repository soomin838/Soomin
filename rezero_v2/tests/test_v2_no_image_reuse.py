from __future__ import annotations

import unittest

from rezero_v2.core.domain.image_asset import ImageArtifact
from rezero_v2.core.guards.image_relevance_guard import ImageRelevanceGuard


class V2NoImageReuseTest(unittest.TestCase):
    def test_reused_asset_blocked(self) -> None:
        guard = ImageRelevanceGuard()
        ok, reason, curated = guard.validate(
            [
                ImageArtifact(
                    role="hero",
                    url="https://example.com/reused.png",
                    alt_text="old image",
                    provider="pollinations",
                    generated_at_utc="2026-03-08T00:00:00+00:00",
                    generated_in_current_run=False,
                    reused_asset=True,
                    prompt_digest="abc",
                )
            ]
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "v2_news_images_cannot_reuse_assets")
        self.assertEqual(curated, [])
