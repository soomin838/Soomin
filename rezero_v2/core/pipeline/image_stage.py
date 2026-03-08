from __future__ import annotations

import time

from rezero_v2.core.domain.image_asset import ImageArtifact
from rezero_v2.core.domain.stage_result import StageResult


class ImageStage:
    def __init__(self, *, pollinations_client, image_guard, image_policy) -> None:
        self.pollinations_client = pollinations_client
        self.image_guard = image_guard
        self.image_policy = image_policy

    def run(self, context, *, candidate, draft) -> StageResult[list[ImageArtifact]]:
        started = time.perf_counter()
        if str(getattr(self.image_policy, 'provider', 'pollinations') or 'pollinations').lower() != 'pollinations':
            return StageResult(
                'image_stage',
                'failed',
                'v2_news_images_must_be_pollinations_only',
                'V2 뉴스 이미지는 Pollinations만 허용합니다.',
                int((time.perf_counter() - started) * 1000),
                [],
            )
        images: list[ImageArtifact] = []
        hero_prompt = f"{candidate.title}, editorial illustration, no text, clean modern scene"
        hero = self.pollinations_client.generate_image_url(prompt=hero_prompt, width=1280, height=720)
        images.append(
            ImageArtifact(
                role='hero',
                url=hero.url,
                alt_text=f"{candidate.title} related illustration",
                provider='pollinations',
                generated_at_utc=hero.generated_at_utc,
                generated_in_current_run=True,
                reused_asset=False,
                prompt_digest=hero.prompt_digest,
            )
        )
        if bool(getattr(self.image_policy, 'allow_inline_optional', True)) and len(draft.section_titles) >= 2:
            inline_title = draft.section_titles[1]
            inline_prompt = f"{candidate.title}, {inline_title}, supporting illustration, no text"
            inline = self.pollinations_client.generate_image_url(prompt=inline_prompt, width=1024, height=683)
            images.append(
                ImageArtifact(
                    role='inline',
                    url=inline.url,
                    alt_text=f"{inline_title} supporting visual for {candidate.title}",
                    provider='pollinations',
                    generated_at_utc=inline.generated_at_utc,
                    generated_in_current_run=True,
                    reused_asset=False,
                    prompt_digest=inline.prompt_digest,
                )
            )
        ok, reason_code, curated = self.image_guard.validate(images)
        return StageResult(
            'image_stage',
            'success' if ok else 'failed',
            reason_code,
            '이번 실행에서 새 Pollinations 이미지를 생성했습니다.' if ok else '이미지 정책 검증을 통과하지 못했습니다.',
            int((time.perf_counter() - started) * 1000),
            curated,
            {'image_count': len(curated)},
        )
