from __future__ import annotations

import unittest

from rezero_v2.core.domain.draft import DraftArtifact
from rezero_v2.core.pipeline.image_stage import ImageStage
from rezero_v2.core.guards.image_relevance_guard import ImageRelevanceGuard
from rezero_v2.tests.helpers import FakePollinationsClient


class _Candidate:
    title = "OpenAI model update"


class _Draft:
    section_titles = ["What happened", "Why it matters"]


class _Policy:
    provider = "pollinations"
    allow_inline_optional = True


class V2PollinationsOnlyTest(unittest.TestCase):
    def test_pollinations_only_image_path(self) -> None:
        stage = ImageStage(pollinations_client=FakePollinationsClient(), image_guard=ImageRelevanceGuard(), image_policy=_Policy())
        result = stage.run(object(), candidate=_Candidate(), draft=_Draft())
        self.assertEqual(result.status, "success")
        self.assertTrue(result.payload)
        self.assertTrue(all(image.provider == "pollinations" for image in result.payload))
