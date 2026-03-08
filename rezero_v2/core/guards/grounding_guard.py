from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.grounding import GroundingPacket
from rezero_v2.core.domain.intent import IntentBundle

GENERIC_DRIFT = [
    'workflow',
    'pricing',
    'cost',
    'platform tradeoff',
    'source frame',
    'main tradeoff',
    'lived experience',
    'keep a one-line status update',
]


class GroundingGuard:
    def build_packet(self, candidate: Candidate, intent_bundle: IntentBundle, *, mixed_domain: bool = False, dominant_axis: str = 'tech') -> GroundingPacket:
        entities = self._extract_entities(candidate.title + ' ' + candidate.source_title)
        nouns = self._extract_nouns(candidate.title + ' ' + candidate.source_snippet + ' ' + candidate.category)
        facts = self._extract_facts(candidate.source_title, candidate.source_snippet)
        return GroundingPacket(
            canonical_source_title=candidate.source_title or candidate.title,
            source_snippet=candidate.source_snippet,
            source_domain=candidate.source_domain,
            required_named_entities=entities[:5],
            required_topic_nouns=nouns[:6],
            required_source_facts=facts[:5],
            forbidden_drift_terms=list(GENERIC_DRIFT),
            content_type=candidate.content_type,
            intent_family=intent_bundle.expansions[0].intent_family if intent_bundle.expansions else 'what_changed',
            category=candidate.category,
            mixed_domain=bool(mixed_domain),
            dominant_axis=str(dominant_axis or 'tech'),
        )

    def evaluate_pre_draft(self, candidate: Candidate, packet: GroundingPacket, intent_bundle: IntentBundle) -> tuple[bool, str, dict[str, Any]]:
        entity_overlap = len(packet.required_named_entities)
        fact_density = len(packet.required_source_facts)
        intent_family = intent_bundle.expansions[0].intent_family if intent_bundle.expansions else 'what_changed'
        if packet.mixed_domain and packet.dominant_axis != 'tech':
            return False, 'mixed_domain_education_tech_but_tech_not_dominant', {'packet': asdict(packet)}
        if not packet.required_topic_nouns:
            return False, 'pre_draft_low_grounding', {'packet': asdict(packet)}
        if entity_overlap == 0:
            return False, 'pre_draft_entity_overlap_too_low', {'packet': asdict(packet)}
        if fact_density < 2:
            return False, 'pre_draft_source_fact_density_too_low', {'packet': asdict(packet)}
        if intent_family == 'how_to' and candidate.content_type == 'hot':
            return False, 'pre_draft_intent_source_mismatch', {'packet': asdict(packet)}
        return True, 'grounding_ok', {'entity_overlap': entity_overlap, 'fact_density': fact_density}

    def coverage_score(self, packet: GroundingPacket, section_titles: list[str], section_purposes: list[str]) -> float:
        blob = (' '.join(section_titles) + ' ' + ' '.join(section_purposes)).lower()
        required_terms = [*packet.required_named_entities, *packet.required_topic_nouns]
        if not required_terms:
            return 0.0
        hits = sum(1 for term in required_terms if str(term or '').lower() in blob)
        return round(100.0 * hits / max(1, len(required_terms)), 2)

    def _extract_entities(self, text: str) -> list[str]:
        values = re.findall(r"\b[A-Z][a-zA-Z0-9&.-]{2,}\b", str(text or ''))
        out = []
        for value in values:
            if value.lower() in {'the', 'and', 'for', 'with', 'from', 'this'}:
                continue
            if value not in out:
                out.append(value)
        return out

    def _extract_nouns(self, text: str) -> list[str]:
        words = re.findall(r"\b[a-zA-Z][a-zA-Z0-9-]{3,}\b", str(text or '').lower())
        stop = {'that', 'this', 'with', 'from', 'they', 'have', 'will', 'what', 'when', 'where', 'which', 'their', 'about', 'today', 'latest'}
        out = []
        for word in words:
            if word in stop:
                continue
            if word not in out:
                out.append(word)
        return out

    def _extract_facts(self, source_title: str, source_snippet: str) -> list[str]:
        text = ' '.join([str(source_title or ''), str(source_snippet or '')]).strip()
        pieces = [re.sub(r"\s+", ' ', piece).strip() for piece in re.split(r"[.;]|\s+-\s+", text) if piece.strip()]
        return [piece[:180] for piece in pieces if len(piece.split()) >= 4]