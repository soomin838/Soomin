from __future__ import annotations

import unittest

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.guards.story_guard import StoryGuard
from rezero_v2.core.guards.support_drift_guard import SupportDriftGuard
from rezero_v2.core.pipeline.gate_stage import GateStage


class _Store:
    def __init__(self) -> None:
        self.rows = []

    def record_decision(self, candidate_id, decision, reason_code, payload):
        self.rows.append((candidate_id, decision, reason_code, payload))


class _Settings:
    class content_mode:
        mode = "tech_news_only"


class _Context:
    settings = _Settings()


class V2SupportDriftGuardTest(unittest.TestCase):
    def test_support_drift_rejected(self) -> None:
        candidate = Candidate(
            candidate_id="1",
            content_type="hot",
            source_type="gdelt",
            title="App not working after release",
            source_title="App not working after release",
            source_url="https://example.com",
            source_domain="example.com",
            source_snippet="Users say the app is not working and want step-by-step repair tips.",
            category="software",
            published_at_utc="2026-03-08T00:00:00+00:00",
            provider="gdelt",
            language="en",
            entity_terms=["App"],
            topic_terms=["app", "release"],
            tags=["software"],
            raw_meta={"score": 80},
        )
        store = _Store()
        stage = GateStage(story_guard=StoryGuard(), support_guard=SupportDriftGuard(), candidate_store=store)
        result = stage.run(_Context(), [candidate])
        self.assertEqual(result.status, "skipped")
        self.assertEqual(store.rows[0][2], "support_drift_rejected")
