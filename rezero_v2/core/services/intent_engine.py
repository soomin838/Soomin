from __future__ import annotations

import re
from typing import Any

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.intent import IntentBundle, IntentExpansion
from rezero_v2.integrations.ollama_client import OllamaClient


class IntentEngine:
    def __init__(self, *, ollama_client: OllamaClient | None = None) -> None:
        self.ollama_client = ollama_client

    def build_bundle(self, *, candidate: Candidate, allocation_slot: str) -> IntentBundle:
        if self.ollama_client is not None:
            try:
                payload = self._generate_with_ollama(candidate, allocation_slot)
                bundle = self._normalize_payload(payload, candidate=candidate, allocation_slot=allocation_slot, source_model='ollama')
                return bundle
            except Exception:
                pass
        return self._build_rules_bundle(candidate, allocation_slot)

    def _generate_with_ollama(self, candidate: Candidate, allocation_slot: str) -> dict[str, Any]:
        system_prompt = 'Generate a strict JSON intent bundle for a factual blog article. Return keys: primary_query, title_strategy, source_strategy, image_strategy, expansions. Each expansion must have: intent_family, title, primary_query, supporting_queries, usefulness_score, search_demand_score, evergreen_score, candidate_body_hint. Stay close to source facts and avoid generic abstraction.'
        payload = {'title': candidate.title, 'source_title': candidate.source_title, 'source_snippet': candidate.source_snippet, 'category': candidate.category, 'content_type': allocation_slot}
        return self.ollama_client.generate_json(system_prompt=system_prompt, payload=payload, purpose='v2_intent')

    def _normalize_payload(self, payload: dict[str, Any], *, candidate: Candidate, allocation_slot: str, source_model: str) -> IntentBundle:
        expansions_raw = payload.get('expansions', []) or []
        expansions = []
        for row in expansions_raw:
            if not isinstance(row, dict):
                continue
            family = str(row.get('intent_family', 'what_changed') or 'what_changed').strip().lower()
            if family == 'how_to' and allocation_slot == 'hot':
                continue
            expansions.append(IntentExpansion(intent_family=family if family in {'what_changed', 'comparison', 'pricing', 'performance', 'should_you', 'alternatives', 'how_to'} else 'what_changed', title=str(row.get('title', candidate.title) or candidate.title).strip()[:140], primary_query=str(row.get('primary_query', candidate.title) or candidate.title).strip()[:160], supporting_queries=[str(x or '').strip()[:140] for x in list(row.get('supporting_queries', []) or []) if str(x or '').strip()][:4], usefulness_score=float(row.get('usefulness_score', 60.0) or 60.0), search_demand_score=float(row.get('search_demand_score', 55.0) or 55.0), evergreen_score=float(row.get('evergreen_score', 45.0) or 45.0), candidate_body_hint=str(row.get('candidate_body_hint', '') or '').strip()[:240]))
        if not expansions:
            return self._build_rules_bundle(candidate, allocation_slot)
        primary_query = str(payload.get('primary_query', expansions[0].primary_query) or expansions[0].primary_query).strip()[:160]
        return IntentBundle(primary_query=primary_query, content_type=allocation_slot, title_strategy=str(payload.get('title_strategy', 'timely_explainer') or 'timely_explainer'), source_strategy=str(payload.get('source_strategy', 'source_grounded') or 'source_grounded'), image_strategy=str(payload.get('image_strategy', 'hero_plus_optional_inline') or 'hero_plus_optional_inline'), expansions=expansions, source_grounded=True, source_model=source_model)

    def _build_rules_bundle(self, candidate: Candidate, allocation_slot: str) -> IntentBundle:
        topic = self._topic(candidate)
        expansions = []
        families = ['what_changed', 'comparison', 'pricing', 'performance', 'should_you', 'alternatives']
        if self._how_to_allowed(candidate):
            families.append('how_to')
        for family in families:
            expansions.append(self._expansion_for_family(candidate, topic, family))
        title_strategy = 'timely_explainer' if allocation_slot == 'hot' else ('query_match' if allocation_slot == 'search_derived' else 'evergreen_utility')
        source_strategy = 'source_grounded' if allocation_slot == 'hot' else 'authority_first'
        image_strategy = 'hero_plus_optional_inline' if allocation_slot in {'hot', 'search_derived'} else 'hero_only_or_one_inline'
        primary = expansions[0].primary_query if expansions else topic
        return IntentBundle(primary_query=primary, content_type=allocation_slot, title_strategy=title_strategy, source_strategy=source_strategy, image_strategy=image_strategy, expansions=expansions, source_grounded=True, source_model='rules')

    def expansions_to_candidates(self, *, candidate: Candidate, bundle: IntentBundle) -> list[Candidate]:
        out: list[Candidate] = []
        for index, expansion in enumerate(bundle.expansions, start=1):
            if candidate.content_type == 'hot' and expansion.intent_family == 'what_changed':
                continue
            content_type = 'search_derived' if expansion.intent_family in {'comparison', 'pricing', 'performance', 'should_you', 'alternatives', 'how_to'} else 'evergreen'
            if expansion.intent_family == 'what_changed':
                content_type = 'search_derived'
            out.append(Candidate(candidate_id=f"{candidate.candidate_id}-exp-{index}", content_type=content_type, source_type='cluster_seed' if content_type == 'evergreen' else 'search_console', title=expansion.title, source_title=candidate.source_title, source_url=candidate.source_url, source_domain=candidate.source_domain, source_snippet=candidate.source_snippet, category=candidate.category, published_at_utc=candidate.published_at_utc, provider=candidate.provider, language=candidate.language, entity_terms=list(candidate.entity_terms), topic_terms=list(candidate.topic_terms), tags=list(candidate.tags), raw_meta={**candidate.raw_meta, 'intent_family': expansion.intent_family, 'primary_query': expansion.primary_query, 'candidate_body_hint': expansion.candidate_body_hint}))
        return out

    def _expansion_for_family(self, candidate: Candidate, topic: str, family: str) -> IntentExpansion:
        if family == 'comparison':
            title = f'{topic} comparison'; query = f'{topic} comparison'
        elif family == 'pricing':
            title = f'{topic} pricing explained'; query = f'{topic} pricing'
        elif family == 'performance':
            title = f'{topic} performance'; query = f'{topic} performance'
        elif family == 'should_you':
            title = f'Should you care about {topic}?'; query = f'should you care about {topic}'
        elif family == 'alternatives':
            title = f'{topic} alternatives'; query = f'{topic} alternatives'
        elif family == 'how_to':
            title = f'How to use {topic}'; query = f'how to use {topic}'
        else:
            title = f'{topic}: what changed'; query = f'{topic} what changed'
        return IntentExpansion(intent_family=family, title=title[:140], primary_query=query[:160], supporting_queries=[f'{topic} explained', f'{topic} details', f'{topic} next steps'], usefulness_score=72.0 if family in {'comparison', 'should_you', 'alternatives', 'how_to'} else 60.0, search_demand_score=70.0 if family in {'comparison', 'pricing', 'how_to'} else 55.0, evergreen_score=68.0 if family in {'alternatives', 'how_to', 'comparison'} else 45.0, candidate_body_hint=f'Stay close to the source facts about {topic} and avoid generic platform filler.')

    def _topic(self, candidate: Candidate) -> str:
        title = re.sub(r'[:|].*$', '', candidate.source_title or candidate.title).strip()
        return re.sub(r'\s+', ' ', title).strip()[:100] or 'latest update'

    def _how_to_allowed(self, candidate: Candidate) -> bool:
        blob = f"{candidate.title} {candidate.source_title} {candidate.source_snippet}".lower()
        return any(term in blob for term in {'app', 'software', 'platform', 'model', 'api', 'tool', 'rollout', 'release'}) and not any(term in blob for term in {'exam', 'ranking', 'election', 'lawsuit'})
