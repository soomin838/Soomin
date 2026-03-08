from __future__ import annotations

import time

from rezero_v2.core.domain.stage_result import StageResult


class DraftStage:
    def __init__(self, *, draft_engine, coherence_guard) -> None:
        self.draft_engine = draft_engine
        self.coherence_guard = coherence_guard

    def run(self, context, *, candidate, intent_bundle, grounding_packet, outline_plan) -> StageResult[dict]:
        started = time.perf_counter()
        if grounding_packet.intent_family != intent_bundle.chosen_intent_family:
            return StageResult(
                'draft_stage',
                'skipped',
                'intent_stage_contract_mismatch',
                'draft 단계에서 의도 계약 불일치가 감지되어 건너뜁니다.',
                int((time.perf_counter() - started) * 1000),
                {'candidate': candidate, 'grounding_packet': grounding_packet, 'intent_bundle': intent_bundle},
                {'packet_intent_family': grounding_packet.intent_family, 'bundle_intent_family': intent_bundle.chosen_intent_family},
            )
        draft = self.draft_engine.generate(
            candidate=candidate,
            intent_bundle=intent_bundle,
            grounding_packet=grounding_packet,
            outline_plan=outline_plan,
            target_word_range=context.allocation.target_word_range,
        )
        ok, reason_code, meta = self.coherence_guard.evaluate(candidate, draft, grounding_packet)
        if ok:
            return StageResult(
                'draft_stage',
                'success',
                'draft_ready',
                '초안이 coherence 가드를 통과했습니다.',
                int((time.perf_counter() - started) * 1000),
                {'draft': draft},
                {'repair_attempted': False, **meta},
            )
        repaired = self.draft_engine.repair(
            candidate=candidate,
            grounding_packet=grounding_packet,
            outline_plan=outline_plan,
            original=draft,
        )
        repaired_ok, repaired_reason, repaired_meta = self.coherence_guard.evaluate(candidate, repaired, grounding_packet)
        if repaired_ok:
            return StageResult(
                'draft_stage',
                'success',
                'repaired_then_published',
                '초안을 한 번 보정한 뒤 coherence 가드를 통과했습니다.',
                int((time.perf_counter() - started) * 1000),
                {'draft': repaired},
                {'repair_attempted': True, 'repair_succeeded': True, 'initial_reason': reason_code, **repaired_meta},
            )
        return StageResult(
            'draft_stage',
            'skipped',
            repaired_reason,
            '초안과 한 번의 보정 모두 소스 coherence 기준을 충족하지 못했습니다.',
            int((time.perf_counter() - started) * 1000),
            {'draft': repaired},
            {'repair_attempted': True, 'repair_succeeded': False, 'initial_reason': reason_code, **repaired_meta},
        )
