from __future__ import annotations

import time

from rezero_v2.core.domain.stage_result import StageResult


class IntentStage:
    def __init__(self, *, intent_engine, candidate_store, topic_scorer, story_guard=None) -> None:
        self.intent_engine = intent_engine
        self.candidate_store = candidate_store
        self.topic_scorer = topic_scorer
        self.story_guard = story_guard

    def run(self, context, candidate):
        started = time.perf_counter()
        candidate = self.intent_engine.prepare_candidate(candidate, allocation_slot=context.allocation.slot_type)
        if self.story_guard is not None:
            story = self.story_guard.evaluate(
                candidate,
                mode=self._resolve_mode(context),
            )
            if not story.allow:
                return StageResult(
                    'intent_stage',
                    'skipped',
                    story.reason_code,
                    '정규화 후 후보를 다시 평가한 결과 기술 기사 기준을 충족하지 못해 건너뜁니다.',
                    int((time.perf_counter() - started) * 1000),
                    {'candidate': candidate},
                    {
                        'source_headline': candidate.source_headline,
                        'normalized_source_headline': candidate.normalized_source_headline,
                        'story_reason': story.reason_code,
                        'source_language': candidate.raw_meta.get('source_language', ''),
                    },
                )
        valid, reason_code, meta = self.intent_engine.validate_selected_candidate(candidate=candidate, allocation_slot=context.allocation.slot_type)
        if not valid:
            return StageResult(
                'intent_stage',
                'skipped',
                reason_code,
                '검색형 후보가 원문 해석 규칙을 만족하지 못해 건너뜁니다.',
                int((time.perf_counter() - started) * 1000),
                {'candidate': candidate},
                meta,
            )
        bundle = self.intent_engine.build_bundle(candidate=candidate, allocation_slot=context.allocation.slot_type)
        if str(candidate.raw_meta.get('intent_family', bundle.chosen_intent_family) or bundle.chosen_intent_family) != bundle.chosen_intent_family:
            return StageResult(
                'intent_stage',
                'skipped',
                'intent_stage_contract_mismatch',
                '선택된 후보의 의도 분류가 단계 간 일치하지 않아 건너뜁니다.',
                int((time.perf_counter() - started) * 1000),
                {'candidate': candidate, 'intent_bundle': bundle},
                {'candidate_intent_family': candidate.raw_meta.get('intent_family', ''), 'bundle_intent_family': bundle.chosen_intent_family},
            )
        expansions = self.intent_engine.expansions_to_candidates(candidate=candidate, bundle=bundle)
        if expansions:
            for expansion in expansions:
                if expansion.content_type == 'search_derived':
                    score = self.topic_scorer.score_search_derived(search_demand=72, usefulness_score=70, competition_inverse=60, freshness=55, cluster_fit=58)
                else:
                    score = self.topic_scorer.score_evergreen(durability=75, usefulness_score=68, cluster_gap=60, search_demand=52, authority_source_availability=62)
                expansion.raw_meta['score'] = score
            self.candidate_store.enqueue_candidates(expansions, priority=70.0)
        return StageResult(
            'intent_stage',
            'success',
            'intent_built',
            f'검색 의도 번들과 파생 후보 {len(expansions)}개를 만들었습니다.',
            int((time.perf_counter() - started) * 1000),
            {'candidate': candidate, 'intent_bundle': bundle, 'derived_candidates': expansions},
            {
                'source_model': bundle.source_model,
                'source_headline': candidate.source_headline,
                'normalized_source_headline': candidate.normalized_source_headline,
                'derived_primary_query': bundle.derived_primary_query,
                'chosen_intent_family': bundle.chosen_intent_family,
            },
        )

    def _resolve_mode(self, context) -> str:
        mode = str(getattr(getattr(context.settings, 'content_mode', None), 'mode', '') or '').strip().lower()
        if mode in {'', 'news_pool', 'news_interpretation', 'news_interpretation_only'}:
            return 'tech_news_only'
        return mode
