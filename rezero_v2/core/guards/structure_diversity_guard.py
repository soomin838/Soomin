from __future__ import annotations

import re
from difflib import SequenceMatcher


class StructureDiversityGuard:
    def heading_signature(self, titles: list[str]) -> str:
        parts = [re.sub(r'\s+', ' ', str(title or '').strip().lower()) for title in titles if str(title or '').strip()]
        return ' | '.join(parts)

    def evaluate(self, titles: list[str], recent_signatures: list[str], threshold: float = 0.82) -> tuple[bool, str, float]:
        signature = self.heading_signature(titles)
        best = 0.0
        for recent in recent_signatures:
            best = max(best, SequenceMatcher(None, signature, str(recent or '')).ratio())
        if best >= float(threshold):
            return False, 'template_similarity_too_high', round(best, 4)
        return True, 'structure_diversity_ok', round(best, 4)
