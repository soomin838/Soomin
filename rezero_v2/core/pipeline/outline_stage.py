from __future__ import annotations

import time

from rezero_v2.core.domain.stage_result import StageResult


class OutlineStage:
    def __init__(self, *, grounding_guard, outline_engine) -> None:
        self.grounding_guard = grounding_guard
        self.outline_engine = outline_engine

    def run(self, context, *, candidate, intent_bundle, story_decision) -> StageResult[dict]:
        started = time.perf_counter()
        candidate_family = str(candidate.raw_meta.get('intent_family', intent_bundle.chosen_intent_family) or intent_bundle.chosen_intent_family)
        if candidate_family != intent_bundle.chosen_intent_family:
            return StageResult(
                'outline_stage',
                'skipped',
                'intent_stage_contract_mismatch',
                '의도 분류 계약이 단계 사이에서 달라져 건너뜁니다.',
                int((time.perf_counter() - started) * 1000),
                {'candidate': candidate, 'intent_bundle': intent_bundle},
                {'candidate_intent_family': candidate_family, 'bundle_intent_family': intent_bundle.chosen_intent_family},
            )
        grounding_packet = self.grounding_guard.build_packet(
            candidate,
            intent_bundle,
            mixed_domain=bool(getattr(story_decision, 'mixed_domain', False)),
            dominant_axis=str(getattr(story_decision, 'dominant_axis', 'tech') or 'tech'),
        )
        if grounding_packet.intent_family != intent_bundle.chosen_intent_family:
            return StageResult(
                'outline_stage',
                'skipped',
                'intent_stage_contract_mismatch',
                'grounding packet 의도와 선택된 의도가 일치하지 않아 건너뜁니다.',
                int((time.perf_counter() - started) * 1000),
                {'candidate': candidate, 'grounding_packet': grounding_packet, 'intent_bundle': intent_bundle},
                {'packet_intent_family': grounding_packet.intent_family, 'bundle_intent_family': intent_bundle.chosen_intent_family},
            )
        allowed, reason_code, meta = self.grounding_guard.evaluate_pre_draft(candidate, grounding_packet, intent_bundle)
        if not allowed:
            return StageResult(
                'outline_stage',
                'skipped',
                reason_code,
                '초안 전에 소스 근거가 약하다고 판단되어 건너뜁니다.',
                int((time.perf_counter() - started) * 1000),
                {'candidate': candidate, 'grounding_packet': grounding_packet},
                meta,
            )
        recent_signatures = context.run_store.list_recent_heading_signatures(limit=30)
        try:
            outline_plan = self.outline_engine.generate(
                candidate=candidate,
                intent_bundle=intent_bundle,
                grounding_packet=grounding_packet,
                recent_signatures=recent_signatures,
            )
        except RuntimeError as exc:
            return StageResult(
                'outline_stage',
                'held',
                str(exc),
                '동적 아웃라인 검증에서 최근 구조 반복 또는 근거 부족이 감지되었습니다.',
                int((time.perf_counter() - started) * 1000),
                {'candidate': candidate, 'grounding_packet': grounding_packet},
                {'recent_signatures': recent_signatures},
            )
        return StageResult(
            'outline_stage',
            'success',
            'outline_ready',
            '동적 아웃라인을 생성했습니다.',
            int((time.perf_counter() - started) * 1000),
            {'candidate': candidate, 'grounding_packet': grounding_packet, 'outline_plan': outline_plan},
            {
                'recent_signatures_used': len(recent_signatures),
                'source_headline': grounding_packet.source_headline,
                'normalized_source_headline': grounding_packet.normalized_source_headline,
                'derived_primary_query': grounding_packet.derived_primary_query,
                'chosen_intent_family': grounding_packet.intent_family,
                'packet_required_named_entities': list(grounding_packet.required_named_entities),
                'packet_required_topic_nouns': list(grounding_packet.required_topic_nouns),
                'packet_quality_score': grounding_packet.packet_quality_score,
            },
        )
