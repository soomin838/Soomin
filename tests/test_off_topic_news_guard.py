from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

from re_core.scout import TopicCandidate
from re_core.story_profile import assess_tech_news_topic, filter_relevant_authority_links
from re_core.visual import ImageAsset
from re_core.workflow import AgentWorkflow


class OffTopicNewsGuardTests(unittest.TestCase):
    def _workflow(self) -> AgentWorkflow:
        workflow = AgentWorkflow.__new__(AgentWorkflow)
        workflow.settings = SimpleNamespace(
            content_mode=SimpleNamespace(mode="tech_news_only", banned_topic_keywords=[]),
            quality=SimpleNamespace(
                prompt_leak_patterns=[
                    "source frame:",
                    "main tradeoff:",
                    "write from the reader's lived experience",
                    "keep a one-line status update tied to",
                ]
            ),
        )
        workflow.qa = SimpleNamespace(authority_links=[], write=lambda *args, **kwargs: None)
        workflow._news_guard_logged = set()
        workflow._append_workflow_perf = lambda *args, **kwargs: None
        return workflow

    def test_physicswallah_title_is_rejected_for_tech_news_only(self) -> None:
        assessment = assess_tech_news_topic(
            title="PhysicsWallah students secure ranks 3, 4 and 8 in state board exam results",
            snippet="Students achieved top exam ranks after the latest school results announcement.",
            source_url="https://example.com/physicswallah-results",
        )
        self.assertFalse(assessment.allow)
        self.assertIn("education", assessment.off_topic_hits)

    def test_workflow_mode_gate_rejects_off_topic_exam_story(self) -> None:
        workflow = self._workflow()
        candidate = TopicCandidate(
            source="news_pool",
            title="PhysicsWallah students secure ranks 3, 4 and 8 in state board exam results",
            body="School exam results and admissions guidance for students and parents.",
            score=91,
            url="https://example.com/physicswallah-results",
            meta={"news_category": "", "news_topic": ""},
        )
        self.assertFalse(workflow._candidate_matches_content_mode(candidate))

    def test_generic_body_is_blocked_by_coherence_gate(self) -> None:
        workflow = self._workflow()
        candidate = TopicCandidate(
            source="news_pool",
            title="PhysicsWallah students secure ranks 3, 4 and 8 in state board exam results",
            body="Students achieved top exam ranks after the latest school results announcement.",
            score=91,
            url="https://example.com/physicswallah-results",
            meta={"news_category": "", "news_topic": ""},
        )
        generic_html = (
            "<h2>Quick Take</h2><p>This analysis focuses on workflow cost, platform timing, and rollout tradeoffs.</p>"
            "<h2>What Happened</h2><p>Source frame: teams must compare AI pricing, platform fit, and vendor tradeoffs.</p>"
            "<h2>Sources</h2><ul><li><a href=\"https://docs.python.org/3/\">docs</a></li></ul>"
        )
        ok, reason, _ = workflow._evaluate_news_topic_coherence(
            candidate=candidate,
            html=generic_html,
            title=candidate.title,
            category="platform",
        )
        self.assertFalse(ok)
        self.assertIn(reason, {"topic_mismatch_low_overlap", "generic_body_not_grounded", "entity_mismatch_before_publish"})

    def test_irrelevant_authority_links_are_dropped_for_off_topic_story(self) -> None:
        filtered = filter_relevant_authority_links(
            [
                "https://docs.python.org/3/",
                "https://github.com/python/cpython",
                "https://www.cisa.gov/news-events/cybersecurity-advisories",
            ],
            title="PhysicsWallah students secure ranks 3, 4 and 8 in state board exam results",
            snippet="Students achieved top exam ranks after the latest school results announcement.",
            category="",
            source_url="https://example.com/physicswallah-results",
            topic="",
        )
        self.assertEqual(filtered, [])

    def test_news_images_are_trimmed_and_rewritten(self) -> None:
        workflow = self._workflow()
        candidate = TopicCandidate(
            source="news_pool",
            title="PhysicsWallah students secure ranks 3, 4 and 8 in state board exam results",
            body="Students achieved top exam ranks after the latest school results announcement.",
            score=91,
            url="https://example.com/physicswallah-results",
            main_entity="PhysicsWallah",
            long_tail_keywords=["PhysicsWallah exam results"],
            meta={"news_category": "", "news_topic": ""},
        )
        images = [
            ImageAsset(path=Path("thumb.png"), alt="Editorial illustration", source_url="https://r2/thumb.png"),
            ImageAsset(path=Path("inline1.png"), alt="Editorial illustration", source_url="https://r2/inline1.png"),
            ImageAsset(path=Path("inline2.png"), alt="Editorial illustration", source_url="https://r2/inline2.png"),
        ]
        curated = workflow._curate_news_images(images, candidate)
        self.assertLessEqual(len(curated), 2)
        self.assertTrue(all("related to" in str(image.alt or "").lower() for image in curated))


if __name__ == "__main__":
    unittest.main()
