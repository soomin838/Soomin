from __future__ import annotations

import re

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.grounding import GroundingPacket
from rezero_v2.core.domain.intent import IntentBundle
from rezero_v2.core.domain.outline import OutlinePlan
from rezero_v2.core.guards.grounding_guard import GroundingGuard
from rezero_v2.core.guards.structure_diversity_guard import StructureDiversityGuard
from rezero_v2.core.guards.support_drift_guard import SupportDriftGuard
from rezero_v2.integrations.ollama_client import OllamaClient


class OutlineEngine:
    def __init__(self, *, ollama_client: OllamaClient | None = None, grounding_guard: GroundingGuard | None = None, diversity_guard: StructureDiversityGuard | None = None, support_guard: SupportDriftGuard | None = None) -> None:
        self.ollama_client = ollama_client
        self.grounding_guard = grounding_guard or GroundingGuard()
        self.diversity_guard = diversity_guard or StructureDiversityGuard()
        self.support_guard = support_guard or SupportDriftGuard()

    def generate(self, *, candidate: Candidate, intent_bundle: IntentBundle, grounding_packet: GroundingPacket, recent_signatures: list[str]) -> OutlinePlan:
        if self.ollama_client is not None:
            try:
                return self._generate_with_ollama(candidate, intent_bundle, grounding_packet, recent_signatures)
            except Exception:
                pass
        return self._generate_rules(candidate, intent_bundle, grounding_packet, recent_signatures)

    def _generate_with_ollama(self, candidate: Candidate, intent_bundle: IntentBundle, grounding_packet: GroundingPacket, recent_signatures: list[str]) -> OutlinePlan:
        parsed = self.ollama_client.generate_json(system_prompt='Return JSON only with section_titles and section_purposes. Use 4-5 source-grounded sections. Do not use generic troubleshooting sections.', payload={'title': candidate.title, 'source_title': candidate.source_title, 'source_snippet': candidate.source_snippet, 'grounding_packet': {'canonical_source_title': grounding_packet.canonical_source_title, 'source_snippet': grounding_packet.source_snippet, 'required_named_entities': grounding_packet.required_named_entities, 'required_topic_nouns': grounding_packet.required_topic_nouns, 'required_source_facts': grounding_packet.required_source_facts, 'forbidden_drift_terms': grounding_packet.forbidden_drift_terms}, 'primary_query': intent_bundle.primary_query, 'content_type': candidate.content_type}, purpose='v2_outline')
        titles = [str(x or '').strip() for x in list(parsed.get('section_titles', []) or []) if str(x or '').strip()][:5]
        purposes = [str(x or '').strip() for x in list(parsed.get('section_purposes', []) or []) if str(x or '').strip()][:5]
        return self._finalize_plan(titles, purposes, grounding_packet, recent_signatures, source='ollama')

    def _generate_rules(self, candidate: Candidate, intent_bundle: IntentBundle, grounding_packet: GroundingPacket, recent_signatures: list[str]) -> OutlinePlan:
        titles = []
        purposes = []
        main_entity = grounding_packet.required_named_entities[0] if grounding_packet.required_named_entities else 'the story'
        main_noun = grounding_packet.required_topic_nouns[0] if grounding_packet.required_topic_nouns else candidate.category or 'the update'
        titles.append(f'What happened with {main_entity}'); purposes.append('event summary')
        titles.append(f'Why {main_noun} matters now'); purposes.append('reader impact')
        if grounding_packet.required_source_facts:
            titles.append(f'What the source confirms about {main_noun}'); purposes.append('source facts')
        titles.append(f'What changes next for {main_noun}'); purposes.append('forward implications')
        titles.append('What to verify before acting'); purposes.append('practical action')
        return self._finalize_plan(titles[:5], purposes[:5], grounding_packet, recent_signatures, source='rules')

    def _finalize_plan(self, titles: list[str], purposes: list[str], grounding_packet: GroundingPacket, recent_signatures: list[str], *, source: str) -> OutlinePlan:
        titles = [re.sub(r'\s+', ' ', title).strip() for title in titles if str(title or '').strip()]
        purposes = [re.sub(r'\s+', ' ', purpose).strip() for purpose in purposes if str(purpose or '').strip()]
        titles = titles[: max(1, len(purposes) or len(titles))]
        while len(purposes) < len(titles):
            purposes.append('topic analysis')
        ok_support, reason = self.support_guard.evaluate_text(' '.join(titles))
        if not ok_support:
            raise RuntimeError(reason)
        coverage = self.grounding_guard.coverage_score(grounding_packet, titles, purposes)
        if coverage < 20.0:
            raise RuntimeError('outline_grounding_too_weak')
        ok_div, reason_code, best = self.diversity_guard.evaluate(titles, recent_signatures)
        if not ok_div:
            raise RuntimeError(reason_code)
        signature = self.diversity_guard.heading_signature(titles)
        return OutlinePlan(section_titles=titles, section_purposes=purposes[:len(titles)], heading_signature=signature, grounding_coverage_score=coverage, diversity_score=max(0.0, 100.0 - (best * 100.0)), debug_outline_source=source)
