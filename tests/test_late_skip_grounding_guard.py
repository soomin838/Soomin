from __future__ import annotations

import inspect
import unittest
from types import SimpleNamespace

import main as main_module
from re_core.scout import TopicCandidate
from re_core.services.search_intent import IntentBundle, SearchIntentGenerator
from re_core.story_profile import assess_tech_news_topic
from re_core.workflow import AgentWorkflow, WorkflowResult


class LateSkipGroundingGuardTests(unittest.TestCase):
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
            gemini=SimpleNamespace(api_key=""),
        )
        workflow.qa = SimpleNamespace(authority_links=[], write=lambda *args, **kwargs: None)
        workflow._news_guard_logged = set()
        workflow._progress_events = []
        workflow._progress = lambda phase, message, percent: workflow._progress_events.append(
            (str(phase or ""), str(message or ""), int(percent or 0))
        )
        workflow._append_workflow_perf = lambda *args, **kwargs: None
        workflow._news_runtime_context = {}
        workflow._latest_workflow_final_context = {}
        workflow._sanitize_publish_html = lambda html, domain="": html
        workflow._canonicalize_html_payload = lambda html: html
        workflow._strip_forbidden_news_links = lambda html: (html, 0)
        workflow.brain = SimpleNamespace(
            repair_news_grounding=lambda **kwargs: kwargs.get("html", ""),
        )
        return workflow

    def _intent_bundle(self, primary: str) -> IntentBundle:
        return IntentBundle(
            primary_query=primary,
            supporting_queries=[f"{primary} explained", f"{primary} key details"],
            questions=[f"What changed with {primary}?", f"Why does {primary} matter?"],
            audience="US readers",
            content_kind="hot",
            recommended_archetypes=[],
            outline_brief=["Lead with the event and the direct implications."],
            negative_angles=[],
        )

    def test_candidate_skips_before_draft_when_grounding_is_too_weak(self) -> None:
        workflow = self._workflow()
        candidate = TopicCandidate(
            source="news_pool",
            title="Delhi-NCR schools join India's semiconductor mission",
            body="Schools contribute.",
            score=88,
            url="https://ianslive.in/example",
            meta={"news_category": "chips", "news_topic": "semiconductor mission"},
        )
        intent_bundle = self._intent_bundle("delhi schools what changed")
        grounding_packet = workflow._build_news_grounding_packet(
            candidate=candidate,
            category="chips",
            intent_bundle=intent_bundle,
            intent_source="rules",
        )
        ok, reason, detail = workflow._evaluate_pre_draft_groundability(
            candidate=candidate,
            category="chips",
            intent_bundle=intent_bundle,
            intent_source="rules",
            grounding_packet=grounding_packet,
        )
        self.assertFalse(ok)
        self.assertIn(
            reason,
            {
                "off_topic_education_no_explicit_tech_angle",
                "pre_draft_source_fact_density_too_low",
                "pre_draft_entity_overlap_too_low",
                "pre_draft_intent_source_mismatch",
                "pre_draft_low_grounding",
                "mixed_domain_requires_explicit_tech_angle",
                "mixed_domain_education_tech_but_tech_not_dominant",
            },
        )
        self.assertIsInstance(detail, dict)

    def test_mixed_domain_education_and_tech_is_classified_explicitly(self) -> None:
        assessment = assess_tech_news_topic(
            title="Delhi-NCR schools celebrate exam rankings as semiconductor mission expands",
            snippet="Schools, admissions, and exam results are framed alongside a brief mention of chip mission training.",
            category="chips",
            source_url="https://ianslive.in/example",
            topic="semiconductor mission schools",
        )
        self.assertTrue(assessment.mixed_domain)
        self.assertFalse(assessment.allow)
        self.assertIn(
            assessment.reason,
            {
                "mixed_domain_requires_explicit_tech_angle",
                "mixed_domain_education_tech_but_tech_not_dominant",
            },
        )

    def test_first_draft_mismatch_triggers_exactly_one_repair_pass_in_source(self) -> None:
        source = inspect.getsource(AgentWorkflow._run_once_news_mode_impl)
        self.assertEqual(source.count("_attempt_news_grounding_repair_once("), 1)
        self.assertIn('_progress("post_draft_validate"', source)
        self.assertIn('_progress("post_draft_repair"', source)
        self.assertIn('_progress("post_draft_skip"', source)

    def test_repaired_draft_can_pass_coherence_if_grounding_improves(self) -> None:
        workflow = self._workflow()
        candidate = TopicCandidate(
            source="news_pool",
            title="Delhi-NCR schools add chip design labs for India's semiconductor mission",
            body="Delhi-NCR schools are adding chip design labs and workforce training as part of India's semiconductor mission.",
            score=91,
            url="https://ianslive.in/example",
            meta={"news_category": "chips", "news_topic": "semiconductor mission"},
        )
        intent_bundle = self._intent_bundle("india semiconductor mission what changed")
        grounding_packet = workflow._build_news_grounding_packet(
            candidate=candidate,
            category="chips",
            intent_bundle=intent_bundle,
            intent_source="rules",
        )
        generic_html = (
            "<h2>Quick Take</h2><p>This article compares workflow, platform timing, and pricing tradeoffs.</p>"
            "<h2>Why It Matters</h2><p>Source frame: teams should compare vendors and operating costs.</p>"
        )
        ok_before, _, _ = workflow._evaluate_news_topic_coherence(
            candidate=candidate,
            html=generic_html,
            title=candidate.title,
            category="chips",
        )
        repaired_html = workflow._rule_based_news_grounding_repair(
            html=generic_html,
            source_title=candidate.title,
            source_snippet=candidate.body,
            grounding_packet=grounding_packet,
        )
        ok_after, _, _ = workflow._evaluate_news_topic_coherence(
            candidate=candidate,
            html=repaired_html,
            title=candidate.title,
            category="chips",
        )
        self.assertFalse(ok_before)
        self.assertTrue(ok_after)

    def test_second_failure_after_repair_has_explicit_skip_marker(self) -> None:
        source = inspect.getsource(AgentWorkflow._run_once_news_mode_impl)
        self.assertIn("repaired_then_skipped:", source)

    def test_draft_to_idle_silent_transition_is_impossible(self) -> None:
        source = inspect.getsource(main_module.AgentController.loop)
        run_idx = source.find("result = self.workflow.run_once")
        self.assertGreaterEqual(run_idx, 0)
        after_run = source[run_idx:]
        self.assertIn('self.phase_key = "workflow_final"', after_run)
        self.assertNotIn('self.phase_key = "idle"', after_run)

    def test_rules_fallback_intent_generation_stays_source_grounded(self) -> None:
        generator = SearchIntentGenerator(
            settings=SimpleNamespace(enabled=True, provider="ollama_then_rules", timeout_sec=15),
            ollama_client=None,
            log_path=None,
        )
        bundle = generator.generate(
            headline="Delhi-NCR schools bolster India's semiconductor ambitions",
            snippet="Educational institutions in Delhi-NCR are contributing to India's semiconductor mission through chip design labs and workforce training.",
            body_excerpt="Delhi-NCR schools are building chip design labs and semiconductor workforce programs tied to the national mission.",
            category="chips",
            source_url="https://ianslive.in/example",
        )
        blob = " ".join(
            [
                bundle.primary_query,
                *list(bundle.supporting_queries or []),
                *list(bundle.questions or []),
                *list(bundle.outline_brief or []),
            ]
        ).lower()
        self.assertEqual(generator.last_source, "rules")
        self.assertIn("semiconductor", blob)
        self.assertTrue(("schools" in blob) or ("training" in blob) or ("delhi" in blob))
        self.assertNotIn("practical impact for americans", blob)

    def test_final_summary_contains_reason_stage_and_timing(self) -> None:
        workflow = self._workflow()
        workflow._update_news_runtime_context(
            selected_title="Delhi-NCR Schools Bolster India's Semiconductor Ambitions",
            source_domain="ianslive.in",
            stage="post_draft_validate",
            repair_attempted=True,
            repair_succeeded=False,
            timing_summary={
                "news_pool_ms": 189000,
                "intent_ms": 30000,
                "outline_ms": 20000,
                "draft_ms": 101000,
                "repair_ms": 4000,
                "publish_ms": 0,
            },
        )
        result = workflow._finalize_news_mode_result(
            WorkflowResult("skipped", "topic_mismatch_low_overlap")
        )
        self.assertIn("reason=topic_mismatch_low_overlap", result.message)
        self.assertIn("stage=post_draft_validate", result.message)
        self.assertIn("repair_attempted=true", result.message)
        self.assertIn("draft_ms=101000", result.message)


if __name__ == "__main__":
    unittest.main()
