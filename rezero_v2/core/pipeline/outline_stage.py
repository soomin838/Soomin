from __future__ import annotations

import time

from rezero_v2.core.domain.stage_result import StageResult


class OutlineStage:
    def __init__(self, *, grounding_guard, outline_engine) -> None:
        self.grounding_guard = grounding_guard
        self.outline_engine = outline_engine

    def run(self, context, *, candidate, intent_bundle, story_decision) -> StageResult[dict]:
        started = time.perf_counter()
        grounding_packet = self.grounding_guard.build_packet(
            candidate,
            intent_bundle,
            mixed_domain=bool(getattr(story_decision, 'mixed_domain', False)),
            dominant_axis=str(getattr(story_decision, 'dominant_axis', 'tech') or 'tech'),
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
            {'recent_signatures_used': len(recent_signatures)},
        )
