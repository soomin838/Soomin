from __future__ import annotations

from rezero_v2.core.domain.image_asset import ImageArtifact


class ImageRelevanceGuard:
    def validate(self, images: list[ImageArtifact]) -> tuple[bool, str, list[ImageArtifact]]:
        seen_alt = set()
        out: list[ImageArtifact] = []
        for image in images:
            if image.provider != 'pollinations':
                return False, 'v2_news_images_must_be_pollinations_only', []
            if image.reused_asset:
                return False, 'v2_news_images_cannot_reuse_assets', []
            alt_key = str(image.alt_text or '').strip().lower()
            if alt_key in seen_alt:
                continue
            seen_alt.add(alt_key)
            out.append(image)
        return True, 'image_relevance_ok', out[:2]
