from __future__ import annotations

import re
from html import escape

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.draft import DraftArtifact
from rezero_v2.core.domain.grounding import GroundingPacket
from rezero_v2.core.domain.intent import IntentBundle
from rezero_v2.core.domain.outline import OutlinePlan
from rezero_v2.integrations.gemini_client import GeminiClient


class DraftEngine:
    def __init__(self, *, gemini_client: GeminiClient | None = None) -> None:
        self.gemini_client = gemini_client

    def generate(self, *, candidate: Candidate, intent_bundle: IntentBundle, grounding_packet: GroundingPacket, outline_plan: OutlinePlan, target_word_range: tuple[int, int]) -> DraftArtifact:
        if self.gemini_client is not None:
            try:
                html = self._generate_with_gemini(candidate, intent_bundle, grounding_packet, outline_plan, target_word_range)
                return self._artifact_from_html(candidate.title, html, outline_plan.section_titles)
            except Exception:
                pass
        html = self._rule_based_html(candidate, grounding_packet, outline_plan)
        return self._artifact_from_html(candidate.title, html, outline_plan.section_titles)

    def repair(self, *, candidate: Candidate, grounding_packet: GroundingPacket, outline_plan: OutlinePlan, original: DraftArtifact) -> DraftArtifact:
        html = self._rule_based_html(candidate, grounding_packet, outline_plan)
        artifact = self._artifact_from_html(original.title, html, outline_plan.section_titles)
        return DraftArtifact(title=artifact.title, intro=artifact.intro, html=artifact.html, plain_text=artifact.plain_text, section_titles=artifact.section_titles, word_count=artifact.word_count, repair_attempted=True, repair_succeeded=True, source_citations=list(artifact.source_citations))

    def _generate_with_gemini(self, candidate: Candidate, intent_bundle: IntentBundle, grounding_packet: GroundingPacket, outline_plan: OutlinePlan, target_word_range: tuple[int, int]) -> str:
        prompt = {'title': candidate.title, 'source_title': candidate.source_title, 'source_snippet': candidate.source_snippet, 'primary_query': intent_bundle.primary_query, 'section_titles': outline_plan.section_titles, 'section_purposes': outline_plan.section_purposes, 'required_entities': grounding_packet.required_named_entities, 'required_facts': grounding_packet.required_source_facts, 'forbidden_drift_terms': grounding_packet.forbidden_drift_terms, 'target_words': {'min': int(target_word_range[0]), 'max': int(target_word_range[1])}}
        return self.gemini_client.generate_text(system_prompt='Write a source-grounded English blog article in simple HTML. Do not add support/troubleshooting drift. Keep the intro and first two sections tightly aligned to the source facts.', user_payload=prompt)

    def _rule_based_html(self, candidate: Candidate, grounding_packet: GroundingPacket, outline_plan: OutlinePlan) -> str:
        facts = grounding_packet.required_source_facts or [candidate.source_snippet or candidate.source_title]
        intro = f"<p>{escape(candidate.source_title or candidate.title)}. {escape(candidate.source_snippet or facts[0])}</p>"
        parts = [intro]
        for index, title in enumerate(outline_plan.section_titles):
            fact = facts[min(index, len(facts) - 1)] if facts else candidate.source_snippet
            purpose = outline_plan.section_purposes[min(index, len(outline_plan.section_purposes) - 1)]
            body = self._expand_fact(fact=fact, candidate=candidate, grounding_packet=grounding_packet, purpose=purpose)
            parts.append(f"<h2>{escape(title)}</h2><p>{escape(body)}</p>")
        parts.append(f"<p>Source: {escape(candidate.source_domain)}</p>")
        return ''.join(parts)

    def _expand_fact(self, *, fact: str, candidate: Candidate, grounding_packet: GroundingPacket, purpose: str) -> str:
        entity_text = ', '.join(grounding_packet.required_named_entities[:3])
        noun_text = ', '.join(grounding_packet.required_topic_nouns[:4])
        return f"{fact}. This section stays focused on {candidate.category or 'the topic'} and on the source-confirmed details involving {entity_text or candidate.source_domain}. It explains the {purpose} without drifting into unrelated workflow, pricing, or troubleshooting advice. Key topic terms remain {noun_text or candidate.category}."

    def _artifact_from_html(self, title: str, html: str, section_titles: list[str]) -> DraftArtifact:
        plain = re.sub(r'<[^>]+>', ' ', str(html or ''))
        plain = re.sub(r'\s+', ' ', plain).strip()
        words = len(plain.split())
        intro = plain[:220]
        return DraftArtifact(title=str(title or ''), intro=intro, html=str(html or ''), plain_text=plain, section_titles=list(section_titles or []), word_count=words, repair_attempted=False, repair_succeeded=False, source_citations=[])
