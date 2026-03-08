from __future__ import annotations

import time

from rezero_v2.core.domain.stage_result import StageResult


class GateStage:
    def __init__(self, *, story_guard, support_guard, candidate_store) -> None:
        self.story_guard = story_guard
        self.support_guard = support_guard
        self.candidate_store = candidate_store

    def run(self, context, candidates):
        started = time.perf_counter()
        accepted = []
        skipped = []
        for candidate in candidates:
            story = self.story_guard.evaluate(
                candidate,
                mode=str(getattr(context.settings.content_mode, 'mode', 'tech_news_only') or 'tech_news_only'),
            )
            if not story.allow:
                skipped.append({'title': candidate.title, 'reason': story.reason_code})
                self.candidate_store.record_decision(candidate.candidate_id, 'skipped', story.reason_code, {'title': candidate.title})
                continue
            allow_support = bool(candidate.raw_meta.get('allow_support', False))
            support_ok, support_reason = self.support_guard.evaluate_text(candidate.title + ' ' + candidate.source_snippet, allow_support=allow_support)
            if not support_ok:
                skipped.append({'title': candidate.title, 'reason': support_reason})
                self.candidate_store.record_decision(candidate.candidate_id, 'skipped', support_reason, {'title': candidate.title})
                continue
            accepted.append((candidate, story))
        if not accepted:
            return StageResult(
                'gate_stage',
                'skipped',
                'all_candidates_rejected',
                '모든 후보가 정책 또는 드리프트 가드에서 제외되었습니다.',
                int((time.perf_counter() - started) * 1000),
                [],
                {'skipped': skipped},
            )
        accepted.sort(key=lambda item: float(item[0].raw_meta.get('score', 0.0) or 0.0), reverse=True)
        payload = [{'candidate': item[0], 'story': item[1]} for item in accepted]
        return StageResult(
            'gate_stage',
            'success',
            'gate_passed',
            f'{len(accepted)}개의 후보가 가드를 통과했습니다.',
            int((time.perf_counter() - started) * 1000),
            payload,
            {'skipped': skipped},
        )
