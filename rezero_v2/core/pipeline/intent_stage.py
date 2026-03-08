from __future__ import annotations

import time

from rezero_v2.core.domain.stage_result import StageResult


class IntentStage:
    def __init__(self, *, intent_engine, candidate_store, topic_scorer) -> None:
        self.intent_engine = intent_engine
        self.candidate_store = candidate_store
        self.topic_scorer = topic_scorer

    def run(self, context, candidate):
        started = time.perf_counter()
        bundle = self.intent_engine.build_bundle(candidate=candidate, allocation_slot=context.allocation.slot_type)
        expansions = self.intent_engine.expansions_to_candidates(candidate=candidate, bundle=bundle)
        if expansions:
            for expansion in expansions:
                if expansion.content_type == 'search_derived':
                    score = self.topic_scorer.score_search_derived(search_demand=72, usefulness_score=70, competition_inverse=60, freshness=55, cluster_fit=58)
                else:
                    score = self.topic_scorer.score_evergreen(durability=75, usefulness_score=68, cluster_gap=60, search_demand=52, authority_source_availability=62)
                expansion.raw_meta['score'] = score
            self.candidate_store.enqueue_candidates(
                expansions,
                priority=70.0,
                cluster_id=str(candidate.raw_meta.get('cluster_id', '') or ''),
                intent_family=bundle.expansions[0].intent_family if bundle.expansions else '',
            )
        return StageResult(
            'intent_stage',
            'success',
            'intent_built',
            f'검색 의도 번들과 파생 후보 {len(expansions)}개를 만들었습니다.',
            int((time.perf_counter() - started) * 1000),
            {'candidate': candidate, 'intent_bundle': bundle, 'derived_candidates': expansions},
            {'source_model': bundle.source_model},
        )
