from __future__ import annotations

import unittest

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.intent import IntentBundle
from rezero_v2.core.guards.grounding_guard import GroundingGuard
from rezero_v2.core.pipeline.intent_stage import IntentStage
from rezero_v2.core.pipeline.outline_stage import OutlineStage
from rezero_v2.core.services.intent_engine import IntentEngine, normalize_candidate_identity


class _Store:
    def enqueue_candidates(self, candidates, priority=50.0):
        self.rows = list(candidates)


class _TopicScorer:
    def score_search_derived(self, **kwargs):
        return 70.0

    def score_evergreen(self, **kwargs):
        return 65.0


class _OutlineEngine:
    def generate(self, **kwargs):
        raise AssertionError('outline generation should not run on contract mismatch test')


class _Context:
    class allocation:
        slot_type = 'search_derived'

    class run_store:
        @staticmethod
        def list_recent_heading_signatures(limit=30):
            return []


def _candidate(**overrides) -> Candidate:
    data = dict(
        candidate_id='cand-1',
        content_type='search_derived',
        source_type='search_console',
        title='OpenAI releases new model comparison',
        source_title='OpenAI releases new model',
        source_url='https://example.com/story',
        source_domain='example.com',
        source_snippet='OpenAI released a new model today.',
        category='AI',
        published_at_utc='2026-03-08T00:00:00+00:00',
        provider='gdelt',
        language='en',
        source_headline='OpenAI releases new model',
        normalized_source_headline='',
        derived_primary_query='',
        entity_terms=['OpenAI'],
        topic_terms=['model'],
        tags=['AI'],
        raw_meta={'intent_family': 'comparison', 'primary_query': 'openai new model comparison'},
    )
    data.update(overrides)
    return Candidate(**data)


class V2SearchDerivedGroundingTest(unittest.TestCase):
    def test_default_rules_bundle_stays_conservative_without_explicit_signals(self) -> None:
        candidate = IntentEngine().prepare_candidate(_candidate(), allocation_slot='search_derived')
        bundle = IntentEngine().build_bundle(candidate=candidate, allocation_slot='search_derived')
        families = [item.intent_family for item in bundle.expansions]
        self.assertEqual(families, ['what_changed', 'why_it_matters', 'should_you'])

    def test_comparison_family_requires_explicit_signal(self) -> None:
        candidate = _candidate(
            title='OpenAI versus Anthropic for coding models',
            source_title='OpenAI versus Anthropic for coding models',
            source_headline='OpenAI versus Anthropic for coding models',
            source_snippet='The report directly compares OpenAI and Anthropic for coding.',
            raw_meta={'intent_family': 'comparison', 'primary_query': 'openai versus anthropic coding comparison'},
        )
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        bundle = IntentEngine().build_bundle(candidate=prepared, allocation_slot='search_derived')
        families = [item.intent_family for item in bundle.expansions]
        self.assertIn('comparison', families)

    def test_pricing_family_requires_explicit_signal(self) -> None:
        candidate = _candidate(
            source_snippet='The company confirmed price, subscription, and billing changes for the new model.',
            raw_meta={'intent_family': 'pricing', 'primary_query': 'openai model pricing'},
        )
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        bundle = IntentEngine().build_bundle(candidate=prepared, allocation_slot='search_derived')
        self.assertIn('pricing', [item.intent_family for item in bundle.expansions])

    def test_performance_family_requires_explicit_signal(self) -> None:
        candidate = _candidate(
            source_snippet='The benchmark shows faster latency and better coding performance.',
            raw_meta={'intent_family': 'performance', 'primary_query': 'openai model performance'},
        )
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        bundle = IntentEngine().build_bundle(candidate=prepared, allocation_slot='search_derived')
        self.assertIn('performance', [item.intent_family for item in bundle.expansions])

    def test_how_to_family_requires_tutorial_signal(self) -> None:
        candidate = _candidate(
            source_snippet='The release guide explains how to set up the API and configure the app.',
            raw_meta={'intent_family': 'how_to', 'primary_query': 'how to use openai api'},
        )
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        bundle = IntentEngine().build_bundle(candidate=prepared, allocation_slot='search_derived')
        self.assertIn('how_to', [item.intent_family for item in bundle.expansions])

    def test_source_headline_is_not_polluted_by_query_suffix(self) -> None:
        candidate = _candidate(title='OpenAI releases new model comparison')
        prepared = normalize_candidate_identity(candidate)
        self.assertEqual(prepared.source_headline, 'OpenAI releases new model')
        self.assertNotEqual(prepared.title.lower(), 'openai releases new model comparison')

    def test_candidate_title_does_not_become_headline_plus_comparison(self) -> None:
        candidate = _candidate(title='OpenAI releases new model comparison')
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        self.assertNotEqual(prepared.title.lower(), 'openai releases new model comparison')
        self.assertIn('compare', prepared.title.lower())

    def test_intent_family_stays_consistent_across_stages(self) -> None:
        candidate = IntentEngine().prepare_candidate(_candidate(), allocation_slot='search_derived')
        bundle = IntentBundle(
            primary_query='openai new model what changed',
            content_type='search_derived',
            title_strategy='query_match',
            source_strategy='authority_first',
            image_strategy='hero_plus_optional_inline',
            chosen_intent_family='what_changed',
            normalized_source_headline=candidate.normalized_source_headline,
            derived_primary_query='openai new model what changed',
            contract_id='bad-contract',
            expansions=[],
            source_grounded=True,
            source_model='rules',
        )
        result = OutlineStage(grounding_guard=GroundingGuard(), outline_engine=_OutlineEngine()).run(_Context(), candidate=candidate, intent_bundle=bundle, story_decision=type('Story', (), {'mixed_domain': False, 'dominant_axis': 'tech'})())
        self.assertEqual(result.status, 'skipped')
        self.assertEqual(result.reason_code, 'intent_stage_contract_mismatch')

    def test_gdelt_seen_snippet_is_sanitized_out_of_grounding_packet(self) -> None:
        candidate = _candidate(source_snippet='Seen by GDELT at 20260308T094500Z')
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        bundle = IntentEngine().build_bundle(candidate=prepared, allocation_slot='search_derived')
        packet = GroundingGuard().build_packet(prepared, bundle)
        self.assertNotIn('Seen by GDELT', packet.source_snippet)
        self.assertNotIn('seen', [x.lower() for x in packet.required_topic_nouns])
        self.assertNotIn('gdelt', [x.lower() for x in packet.required_topic_nouns])

    def test_non_english_headline_normalization_runs_before_search_derived_expansion(self) -> None:
        candidate = _candidate(
            title='Німеччина робить ставку на промисловий ШІ comparison',
            source_title='Німеччина робить ставку на промисловий ШІ',
            source_headline='Німеччина робить ставку на промисловий ШІ',
            source_domain='www.dw.com',
            source_snippet='Seen by GDELT at 20260308T094500Z',
            language='non_english',
            entity_terms=[],
            topic_terms=['seen', 'gdelt'],
            raw_meta={'intent_family': 'comparison', 'primary_query': 'Німеччина робить ставку на промисловий ШІ comparison'},
        )
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        self.assertEqual(prepared.raw_meta.get('source_language'), 'non_english')
        self.assertTrue(prepared.normalized_source_headline)
        self.assertNotEqual(prepared.title, candidate.title)

    def test_weak_packet_quality_triggers_early_skip(self) -> None:
        candidate = _candidate(
            title='ai comparison',
            source_title='ai comparison',
            source_headline='ai comparison',
            source_snippet='Seen by GDELT at 20260308T094500Z',
            entity_terms=[],
            topic_terms=['seen', 'gdelt'],
            raw_meta={'intent_family': 'comparison', 'primary_query': 'ai comparison'},
        )
        prepared = IntentEngine().prepare_candidate(candidate, allocation_slot='search_derived')
        bundle = IntentEngine().build_bundle(candidate=prepared, allocation_slot='search_derived')
        allowed, reason, _ = GroundingGuard().evaluate_pre_draft(prepared, GroundingGuard().build_packet(prepared, bundle), bundle)
        self.assertFalse(allowed)
        self.assertEqual(reason, 'grounding_packet_quality_too_low')

    def test_comparison_intent_is_blocked_when_unjustified_for_non_english(self) -> None:
        candidate = _candidate(
            title='Німеччина робить ставку на промисловий ШІ comparison',
            source_title='Німеччина робить ставку на промисловий ШІ',
            source_headline='Німеччина робить ставку на промисловий ШІ',
            source_domain='www.dw.com',
            source_snippet='Seen by GDELT at 20260308T094500Z',
            language='non_english',
            entity_terms=[],
            topic_terms=['seen', 'gdelt'],
            raw_meta={'intent_family': 'comparison', 'primary_query': 'Німеччина робить ставку на промисловий ШІ comparison'},
        )
        stage = IntentStage(intent_engine=IntentEngine(), candidate_store=_Store(), topic_scorer=_TopicScorer())
        result = stage.run(_Context(), candidate)
        self.assertEqual(result.status, 'skipped')
        self.assertEqual(result.reason_code, 'search_derived_family_not_allowed_for_normalized_story')


if __name__ == '__main__':
    unittest.main()
