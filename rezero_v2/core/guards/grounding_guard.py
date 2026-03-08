from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.grounding import GroundingPacket
from rezero_v2.core.domain.intent import IntentBundle
from rezero_v2.core.services.intent_engine import sanitize_source_headline, sanitize_source_snippet

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
INGESTION_NOISE = {
    'seen',
    'gdelt',
    'comparison',
    'pricing',
    'performance',
    'alternatives',
    'what',
    'changed',
    'change',
    'details',
    'explained',
    'latest',
    'today',
}


class GroundingGuard:
    def build_packet(self, candidate: Candidate, intent_bundle: IntentBundle, *, mixed_domain: bool = False, dominant_axis: str = 'tech') -> GroundingPacket:
        source_headline = sanitize_source_headline(candidate.source_headline or candidate.source_title or candidate.title)
        normalized_source_headline = sanitize_source_headline(candidate.normalized_source_headline or source_headline)
        source_snippet = sanitize_source_snippet(candidate.source_snippet)
        entities = self._extract_entities(' '.join([source_headline, normalized_source_headline]))
        nouns = self._extract_nouns(' '.join([normalized_source_headline, source_snippet, candidate.category]))
        facts = self._extract_facts(source_headline, source_snippet)
        packet = GroundingPacket(
            source_headline=source_headline,
            normalized_source_headline=normalized_source_headline,
            derived_primary_query=str(intent_bundle.derived_primary_query or candidate.derived_primary_query or candidate.raw_meta.get('primary_query', '') or '').strip(),
            canonical_source_title=source_headline or candidate.title,
            source_snippet=source_snippet,
            source_domain=candidate.source_domain,
            required_named_entities=entities[:5],
            required_topic_nouns=nouns[:6],
            required_source_facts=facts[:5],
            forbidden_drift_terms=list(GENERIC_DRIFT),
            packet_quality_score=0.0,
            content_type=candidate.content_type,
            intent_family=intent_bundle.chosen_intent_family,
            category=candidate.category,
            mixed_domain=bool(mixed_domain),
            dominant_axis=str(dominant_axis or 'tech'),
        )
        quality = self._packet_quality_score(packet)
        return GroundingPacket(
            source_headline=packet.source_headline,
            normalized_source_headline=packet.normalized_source_headline,
            derived_primary_query=packet.derived_primary_query,
            canonical_source_title=packet.canonical_source_title,
            source_snippet=packet.source_snippet,
            source_domain=packet.source_domain,
            required_named_entities=list(packet.required_named_entities),
            required_topic_nouns=list(packet.required_topic_nouns),
            required_source_facts=list(packet.required_source_facts),
            forbidden_drift_terms=list(packet.forbidden_drift_terms),
            packet_quality_score=quality,
            content_type=packet.content_type,
            intent_family=packet.intent_family,
            category=packet.category,
            mixed_domain=packet.mixed_domain,
            dominant_axis=packet.dominant_axis,
        )

    def evaluate_pre_draft(self, candidate: Candidate, packet: GroundingPacket, intent_bundle: IntentBundle) -> tuple[bool, str, dict[str, Any]]:
        entity_overlap = len(packet.required_named_entities)
        fact_density = len(packet.required_source_facts)
        if packet.intent_family != intent_bundle.chosen_intent_family:
            return False, 'intent_stage_contract_mismatch', self._debug_packet(packet)
        if packet.packet_quality_score < 45.0:
            return False, 'grounding_packet_quality_too_low', self._debug_packet(packet)
        if packet.mixed_domain and packet.dominant_axis != 'tech':
            return False, 'mixed_domain_education_tech_but_tech_not_dominant', self._debug_packet(packet)
        if not packet.required_topic_nouns:
            return False, 'pre_draft_low_grounding', self._debug_packet(packet)
        if entity_overlap == 0:
            return False, 'pre_draft_entity_overlap_too_low', self._debug_packet(packet)
        if fact_density < 1:
            return False, 'pre_draft_source_fact_density_too_low', self._debug_packet(packet)
        if packet.intent_family == 'how_to' and candidate.content_type == 'hot':
            return False, 'pre_draft_intent_source_mismatch', self._debug_packet(packet)
        return True, 'grounding_ok', {
            **self._debug_packet(packet),
            'entity_overlap': entity_overlap,
            'fact_density': fact_density,
        }

    def coverage_score(self, packet: GroundingPacket, section_titles: list[str], section_purposes: list[str]) -> float:
        blob = (' '.join(section_titles) + ' ' + ' '.join(section_purposes)).lower()
        required_terms = [*packet.required_named_entities, *packet.required_topic_nouns]
        if not required_terms:
            return 0.0
        hits = sum(1 for term in required_terms if str(term or '').lower() in blob)
        return round(100.0 * hits / max(1, len(required_terms)), 2)

    def _packet_quality_score(self, packet: GroundingPacket) -> float:
        score = 0.0
        if packet.source_headline:
            score += 15.0
        if packet.source_snippet:
            score += 20.0
        score += min(25.0, 12.5 * len(packet.required_named_entities))
        score += min(20.0, 5.0 * len(packet.required_topic_nouns))
        score += min(20.0, 10.0 * len(packet.required_source_facts))
        if packet.intent_family in {'comparison', 'pricing', 'performance', 'how_to'} and not packet.source_snippet:
            score -= 15.0
        return round(max(0.0, min(100.0, score)), 2)

    def _debug_packet(self, packet: GroundingPacket) -> dict[str, Any]:
        return {
            'packet': asdict(packet),
            'source_headline': packet.source_headline,
            'normalized_source_headline': packet.normalized_source_headline,
            'derived_primary_query': packet.derived_primary_query,
            'chosen_intent_family': packet.intent_family,
            'packet_required_named_entities': list(packet.required_named_entities),
            'packet_required_topic_nouns': list(packet.required_topic_nouns),
            'packet_quality_score': packet.packet_quality_score,
        }

    def _extract_entities(self, text: str) -> list[str]:
        out: list[str] = []
        for token in re.findall(r"[^\W\d_][^\W_'-]*", str(text or ''), flags=re.UNICODE):
            value = token.strip()
            if len(value) < 3:
                continue
            if value.lower() in INGESTION_NOISE:
                continue
            if value[0].isupper() or value.isupper() or value.istitle():
                if value not in out:
                    out.append(value)
        return out

    def _extract_nouns(self, text: str) -> list[str]:
        out: list[str] = []
        for token in re.findall(r"[^\W\d_][^\W_'-]*", str(text or '').lower(), flags=re.UNICODE):
            value = token.strip()
            if len(value) < 3:
                continue
            if value in INGESTION_NOISE:
                continue
            if value not in out:
                out.append(value)
        return out

    def _extract_facts(self, source_title: str, source_snippet: str) -> list[str]:
        pieces: list[str] = []
        for item in [str(source_title or '').strip(), str(source_snippet or '').strip()]:
            if not item:
                continue
            cleaned = re.sub(r'\s+', ' ', item).strip(' -:;,.')
            cleaned = sanitize_source_snippet(cleaned)
            if not cleaned:
                continue
            if cleaned not in pieces:
                pieces.append(cleaned[:180])
        return pieces
