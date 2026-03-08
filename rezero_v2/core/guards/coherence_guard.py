from __future__ import annotations

import re
from typing import Any

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.draft import DraftArtifact
from rezero_v2.core.domain.grounding import GroundingPacket

GENERIC_PENALTIES = ['workflow', 'platform tradeoff', 'main tradeoff', 'source frame', 'lived experience']


class CoherenceGuard:
    def evaluate(self, candidate: Candidate, draft: DraftArtifact, packet: GroundingPacket) -> tuple[bool, str, dict[str, Any]]:
        title_terms = self._terms(candidate.title)
        source_terms = self._terms(candidate.source_title + ' ' + candidate.source_snippet)
        body_terms = self._terms(draft.plain_text)
        heading_terms = self._terms(' '.join(draft.section_titles))
        entity_hits = sum(1 for term in packet.required_named_entities if str(term).lower() in draft.plain_text.lower())
        overlap = len((title_terms | source_terms) & (body_terms | heading_terms))
        generic_hits = sum(1 for term in GENERIC_PENALTIES if term in draft.plain_text.lower())
        meta = {'overlap': overlap, 'entity_hits': entity_hits, 'generic_hits': generic_hits}
        if entity_hits == 0 and packet.required_named_entities:
            return False, 'entity_mismatch_before_publish', meta
        if generic_hits >= 2:
            return False, 'generic_body_not_grounded', meta
        if overlap < max(3, len(title_terms) // 3):
            return False, 'topic_mismatch_low_overlap', meta
        return True, 'coherence_ok', meta

    def _terms(self, text: str) -> set[str]:
        return {word for word in re.findall(r"\b[a-zA-Z][a-zA-Z0-9-]{2,}\b", str(text or '').lower()) if len(word) >= 3}