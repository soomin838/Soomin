from __future__ import annotations

import hashlib
import re
from dataclasses import replace
from typing import Any

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.intent import IntentBundle, IntentExpansion
from rezero_v2.integrations.ollama_client import OllamaClient

DEFAULT_RULES_FAMILIES = (
    'what_changed',
    'why_it_matters',
    'should_you',
)
SIGNAL_GATED_FAMILIES = (
    'comparison',
    'pricing',
    'performance',
    'alternatives',
    'how_to',
)
ALL_SUPPORTED_FAMILIES = set(DEFAULT_RULES_FAMILIES + SIGNAL_GATED_FAMILIES)
INGESTION_MARKERS = {
    'seen',
    'gdelt',
    'comparison',
    'pricing',
    'performance',
    'alternatives',
    'details',
    'explained',
    'next',
    'steps',
    'latest',
    'today',
}
SAFE_NON_ENGLISH_FAMILIES = {'what_changed', 'why_it_matters', 'should_you'}
COMPARISON_SIGNALS = {'compare', 'comparison', 'versus', 'vs', 'rival', 'alternative', 'alternatives', 'choice'}
PRICING_SIGNALS = {'pricing', 'price', 'cost', 'subscription'}
PERFORMANCE_SIGNALS = {'performance', 'benchmark', 'speed', 'latency'}
ALTERNATIVE_SIGNALS = {'alternative', 'alternatives', 'option', 'options', 'replace', 'replacement'}
HOW_TO_SIGNALS = {'how to', 'guide', 'tutorial', 'setup', 'install', 'configure', 'use'}


def sanitize_source_snippet(text: str) -> str:
    value = str(text or '').strip()
    if not value:
        return ''
    value = re.sub(r'Seen by GDELT at\s+\d{8}T\d{6}Z', '', value, flags=re.IGNORECASE)
    value = re.sub(r'\b\d{8}T\d{6}Z\b', '', value)
    value = re.sub(r'\b(?:seen|gdelt)\b', '', value, flags=re.IGNORECASE)
    value = re.sub(r'\s+', ' ', value)
    return value.strip(' -:;,.' )


def sanitize_source_headline(text: str) -> str:
    value = str(text or '').strip()
    if not value:
        return ''
    value = re.sub(r'Seen by GDELT at\s+\d{8}T\d{6}Z', '', value, flags=re.IGNORECASE)
    value = re.sub(r'\b\d{8}T\d{6}Z\b', '', value)
    value = re.sub(r'\s+', ' ', value)
    return value.strip(' -:;,.' )


def looks_non_english(text: str) -> bool:
    value = str(text or '')
    return any(ord(ch) > 127 and ch.isalpha() for ch in value)


def _unicode_words(text: str) -> list[str]:
    return re.findall(r"[^\W\d_][^\W_'-]*", str(text or ''), flags=re.UNICODE)


def _topic_display(normalized_source_headline: str, fallback: str) -> str:
    value = sanitize_source_headline(normalized_source_headline or fallback)
    value = re.sub(r'\s+', ' ', value).strip()
    if len(value) > 96:
        value = value[:96].rsplit(' ', 1)[0].strip() or value[:96].strip()
    return value or 'the update'


def build_candidate_title(*, normalized_source_headline: str, family: str, fallback: str) -> str:
    topic = _topic_display(normalized_source_headline, fallback)
    if family == 'why_it_matters':
        title = f'Why {topic} matters'
    elif family == 'comparison':
        title = f'What to compare about {topic}'
    elif family == 'pricing':
        title = f'What {topic} could mean for pricing'
    elif family == 'performance':
        title = f'What {topic} could mean for performance'
    elif family == 'should_you':
        title = f'Should you pay attention to {topic}?'
    elif family == 'alternatives':
        title = f'What alternatives matter around {topic}'
    elif family == 'how_to':
        title = f'How to use {topic}'
    else:
        title = f'What changed in {topic}'
    return title[:140]


def build_primary_query(*, normalized_source_headline: str, family: str, fallback: str) -> str:
    topic = _topic_display(normalized_source_headline, fallback)
    if family == 'why_it_matters':
        query = f'why {topic} matters'
    elif family == 'comparison':
        query = f'compare {topic}'
    elif family == 'pricing':
        query = f'{topic} pricing'
    elif family == 'performance':
        query = f'{topic} performance'
    elif family == 'should_you':
        query = f'should you care about {topic}'
    elif family == 'alternatives':
        query = f'{topic} alternatives'
    elif family == 'how_to':
        query = f'how to use {topic}'
    else:
        query = f'{topic} what changed'
    return query[:160]


def candidate_title_is_polluted(candidate: Candidate, family: str) -> bool:
    title = str(candidate.title or '').strip().lower()
    headline = sanitize_source_headline(candidate.source_headline or candidate.source_title or candidate.title).lower()
    if not title or not headline:
        return True
    if title == headline:
        return False
    if title == f'{headline} {family}'.strip():
        return True
    return bool(re.search(r'\b(comparison|pricing explained|performance|alternatives)$', title))


def normalize_candidate_identity(candidate: Candidate, *, family: str | None = None) -> Candidate:
    source_headline = sanitize_source_headline(candidate.source_headline or candidate.source_title or candidate.title)
    normalized_source_headline = sanitize_source_headline(candidate.normalized_source_headline or source_headline)
    derived_family = str(family or candidate.raw_meta.get('intent_family', '') or 'what_changed').strip().lower()
    if derived_family not in ALL_SUPPORTED_FAMILIES:
        derived_family = 'what_changed'
    derived_primary_query = str(candidate.derived_primary_query or candidate.raw_meta.get('primary_query', '') or '').strip()
    if not derived_primary_query:
        derived_primary_query = build_primary_query(normalized_source_headline=normalized_source_headline, family=derived_family, fallback=source_headline)
    title = sanitize_source_headline(candidate.title or source_headline)
    if candidate.content_type == 'search_derived' and candidate_title_is_polluted(candidate, derived_family):
        title = build_candidate_title(normalized_source_headline=normalized_source_headline, family=derived_family, fallback=source_headline)
    snippet = sanitize_source_snippet(candidate.source_snippet)
    raw_meta = {
        **candidate.raw_meta,
        'intent_family': derived_family,
        'primary_query': derived_primary_query,
        'source_headline': source_headline,
        'normalized_source_headline': normalized_source_headline,
        'non_english_story': looks_non_english(source_headline),
    }
    return replace(
        candidate,
        title=title,
        source_title=source_headline,
        source_headline=source_headline,
        normalized_source_headline=normalized_source_headline,
        derived_primary_query=derived_primary_query,
        source_snippet=snippet,
        raw_meta=raw_meta,
    )


class IntentEngine:
    def __init__(self, *, ollama_client: OllamaClient | None = None) -> None:
        self.ollama_client = ollama_client

    def prepare_candidate(self, candidate: Candidate, *, allocation_slot: str) -> Candidate:
        prepared = normalize_candidate_identity(candidate, family=str(candidate.raw_meta.get('intent_family', '') or 'what_changed'))
        normalized_headline, language, normalization_source = self._normalize_headline(prepared.source_headline)
        family = str(prepared.raw_meta.get('intent_family', 'what_changed') or 'what_changed').strip().lower()
        if family not in ALL_SUPPORTED_FAMILIES:
            family = 'what_changed'
        prepared = replace(
            prepared,
            normalized_source_headline=normalized_headline,
            derived_primary_query=build_primary_query(normalized_source_headline=normalized_headline, family=family, fallback=prepared.source_headline),
            title=(
                build_candidate_title(normalized_source_headline=normalized_headline, family=family, fallback=prepared.source_headline)
                if prepared.content_type == 'search_derived'
                else prepared.title
            ),
            raw_meta={
                **prepared.raw_meta,
                'intent_family': family,
                'source_language': language,
                'normalization_source': normalization_source,
                'source_headline': prepared.source_headline,
                'normalized_source_headline': normalized_headline,
                'primary_query': build_primary_query(normalized_source_headline=normalized_headline, family=family, fallback=prepared.source_headline),
                'non_english_story': looks_non_english(prepared.source_headline),
            },
        )
        return prepared

    def validate_selected_candidate(self, *, candidate: Candidate, allocation_slot: str) -> tuple[bool, str, dict[str, Any]]:
        requested_family = str(candidate.raw_meta.get('intent_family', 'what_changed') or 'what_changed').strip().lower()
        allowed_families = self._allowed_families(candidate, allocation_slot)
        if looks_non_english(candidate.source_headline or candidate.source_title) and not str(candidate.normalized_source_headline or '').strip():
            return False, 'non_english_story_requires_normalization', {
                'source_headline': candidate.source_headline,
                'normalized_source_headline': candidate.normalized_source_headline,
                'requested_intent_family': requested_family,
                'allowed_families': allowed_families,
            }
        if looks_non_english(candidate.source_headline or candidate.source_title) and str(candidate.raw_meta.get('normalization_source', 'rules') or 'rules'):
            if requested_family not in allowed_families:
                return False, 'search_derived_family_not_allowed_for_normalized_story', {
                    'source_headline': candidate.source_headline,
                    'normalized_source_headline': candidate.normalized_source_headline,
                    'requested_intent_family': requested_family,
                    'allowed_families': allowed_families,
                }
        return True, 'intent_candidate_ok', {
            'source_headline': candidate.source_headline,
            'normalized_source_headline': candidate.normalized_source_headline,
            'requested_intent_family': requested_family,
            'allowed_families': allowed_families,
        }

    def build_bundle(self, *, candidate: Candidate, allocation_slot: str) -> IntentBundle:
        desired_family = str(candidate.raw_meta.get('intent_family', 'what_changed') or 'what_changed').strip().lower()
        if desired_family not in ALL_SUPPORTED_FAMILIES:
            desired_family = 'what_changed'
        if self.ollama_client is not None:
            try:
                payload = self._generate_with_ollama(candidate, allocation_slot, desired_family)
                bundle = self._normalize_payload(payload, candidate=candidate, allocation_slot=allocation_slot, source_model='ollama', desired_family=desired_family)
                return bundle
            except Exception:
                pass
        return self._build_rules_bundle(candidate, allocation_slot, desired_family=desired_family)

    def _generate_with_ollama(self, candidate: Candidate, allocation_slot: str, desired_family: str) -> dict[str, Any]:
        system_prompt = 'Generate a strict JSON intent bundle for a factual blog article. Return keys: primary_query, title_strategy, source_strategy, image_strategy, chosen_intent_family, expansions. Each expansion must have: intent_family, title, primary_query, supporting_queries, usefulness_score, search_demand_score, evergreen_score, candidate_body_hint. Keep the source headline intact. Never append raw suffixes like comparison to the original headline. Stay close to source facts and avoid generic abstraction.'
        payload = {
            'title': candidate.title,
            'source_title': candidate.source_title,
            'source_headline': candidate.source_headline,
            'normalized_source_headline': candidate.normalized_source_headline,
            'source_snippet': candidate.source_snippet,
            'category': candidate.category,
            'content_type': allocation_slot,
            'desired_family': desired_family,
            'allowed_families': self._allowed_families(candidate, allocation_slot),
        }
        return self.ollama_client.generate_json(system_prompt=system_prompt, payload=payload, purpose='v2_intent')

    def _normalize_payload(self, payload: dict[str, Any], *, candidate: Candidate, allocation_slot: str, source_model: str, desired_family: str) -> IntentBundle:
        allowed_families = self._allowed_families(candidate, allocation_slot)
        expansions_raw = payload.get('expansions', []) or []
        expansions: list[IntentExpansion] = []
        for row in expansions_raw:
            if not isinstance(row, dict):
                continue
            family = str(row.get('intent_family', desired_family) or desired_family).strip().lower()
            if family not in ALL_SUPPORTED_FAMILIES or family not in allowed_families:
                continue
            if family == 'how_to' and allocation_slot == 'hot':
                continue
            title = str(row.get('title', '') or '').strip()
            primary_query = str(row.get('primary_query', '') or '').strip()
            expansions.append(
                IntentExpansion(
                    intent_family=family,
                    title=(title or build_candidate_title(normalized_source_headline=candidate.normalized_source_headline, family=family, fallback=candidate.source_headline))[:140],
                    primary_query=(primary_query or build_primary_query(normalized_source_headline=candidate.normalized_source_headline, family=family, fallback=candidate.source_headline))[:160],
                    supporting_queries=[str(x or '').strip()[:140] for x in list(row.get('supporting_queries', []) or []) if str(x or '').strip()][:4],
                    usefulness_score=float(row.get('usefulness_score', 60.0) or 60.0),
                    search_demand_score=float(row.get('search_demand_score', 55.0) or 55.0),
                    evergreen_score=float(row.get('evergreen_score', 45.0) or 45.0),
                    candidate_body_hint=str(row.get('candidate_body_hint', '') or '').strip()[:240],
                )
            )
        if not expansions:
            return self._build_rules_bundle(candidate, allocation_slot, desired_family=desired_family)
        chosen_family = str(payload.get('chosen_intent_family', desired_family) or desired_family).strip().lower()
        if chosen_family not in allowed_families:
            chosen_family = expansions[0].intent_family
        chosen_expansion = next((item for item in expansions if item.intent_family == chosen_family), expansions[0])
        primary_query = str(payload.get('primary_query', chosen_expansion.primary_query) or chosen_expansion.primary_query).strip()[:160]
        return IntentBundle(
            primary_query=primary_query,
            content_type=allocation_slot,
            title_strategy=str(payload.get('title_strategy', 'timely_explainer') or 'timely_explainer'),
            source_strategy=str(payload.get('source_strategy', 'source_grounded') or 'source_grounded'),
            image_strategy=str(payload.get('image_strategy', 'hero_plus_optional_inline') or 'hero_plus_optional_inline'),
            chosen_intent_family=chosen_family,
            normalized_source_headline=candidate.normalized_source_headline,
            derived_primary_query=primary_query,
            contract_id=self._contract_id(candidate, chosen_family, primary_query),
            expansions=expansions,
            source_grounded=True,
            source_model=source_model,
            source_language=str(candidate.raw_meta.get('source_language', 'en') or 'en'),
            normalization_source=str(candidate.raw_meta.get('normalization_source', 'rules') or 'rules'),
        )

    def _build_rules_bundle(self, candidate: Candidate, allocation_slot: str, desired_family: str) -> IntentBundle:
        allowed_families = self._allowed_families(candidate, allocation_slot)
        families = [family for family in DEFAULT_RULES_FAMILIES if family in allowed_families]
        for family in SIGNAL_GATED_FAMILIES:
            if family in allowed_families:
                families.append(family)
        if allocation_slot == 'hot':
            families = [family for family in families if family != 'how_to']
        expansions = [self._expansion_for_family(candidate, family) for family in families]
        title_strategy = 'timely_explainer' if allocation_slot == 'hot' else ('query_match' if allocation_slot == 'search_derived' else 'evergreen_utility')
        source_strategy = 'source_grounded' if allocation_slot == 'hot' else 'authority_first'
        image_strategy = 'hero_plus_optional_inline' if allocation_slot in {'hot', 'search_derived'} else 'hero_only_or_one_inline'
        chosen_family = desired_family if desired_family in families else (families[0] if families else 'what_changed')
        chosen_expansion = next((item for item in expansions if item.intent_family == chosen_family), expansions[0]) if expansions else self._expansion_for_family(candidate, 'what_changed')
        primary = chosen_expansion.primary_query
        return IntentBundle(
            primary_query=primary,
            content_type=allocation_slot,
            title_strategy=title_strategy,
            source_strategy=source_strategy,
            image_strategy=image_strategy,
            chosen_intent_family=chosen_family,
            normalized_source_headline=candidate.normalized_source_headline,
            derived_primary_query=primary,
            contract_id=self._contract_id(candidate, chosen_family, primary),
            expansions=expansions,
            source_grounded=True,
            source_model='rules',
            source_language=str(candidate.raw_meta.get('source_language', 'en') or 'en'),
            normalization_source=str(candidate.raw_meta.get('normalization_source', 'rules') or 'rules'),
        )

    def expansions_to_candidates(self, *, candidate: Candidate, bundle: IntentBundle) -> list[Candidate]:
        out: list[Candidate] = []
        for index, expansion in enumerate(bundle.expansions, start=1):
            if candidate.content_type == 'hot' and expansion.intent_family == 'what_changed':
                continue
            content_type = 'search_derived' if expansion.intent_family in {'what_changed', 'comparison', 'pricing', 'performance', 'should_you'} else 'evergreen'
            out.append(
                Candidate(
                    candidate_id=f"{candidate.candidate_id}-exp-{index}",
                    content_type=content_type,
                    source_type='cluster_seed' if content_type == 'evergreen' else 'search_console',
                    title=build_candidate_title(normalized_source_headline=candidate.normalized_source_headline, family=expansion.intent_family, fallback=candidate.source_headline),
                    source_title=candidate.source_title,
                    source_url=candidate.source_url,
                    source_domain=candidate.source_domain,
                    source_snippet=candidate.source_snippet,
                    category=candidate.category,
                    published_at_utc=candidate.published_at_utc,
                    provider=candidate.provider,
                    language=candidate.language,
                    source_headline=candidate.source_headline,
                    normalized_source_headline=candidate.normalized_source_headline,
                    derived_primary_query=expansion.primary_query,
                    entity_terms=list(candidate.entity_terms),
                    topic_terms=list(candidate.topic_terms),
                    tags=list(candidate.tags),
                    raw_meta={
                        **candidate.raw_meta,
                        'intent_family': expansion.intent_family,
                        'primary_query': expansion.primary_query,
                        'candidate_body_hint': expansion.candidate_body_hint,
                        'source_headline': candidate.source_headline,
                        'normalized_source_headline': candidate.normalized_source_headline,
                        'source_language': str(candidate.raw_meta.get('source_language', 'en') or 'en'),
                        'normalization_source': str(candidate.raw_meta.get('normalization_source', 'rules') or 'rules'),
                    },
                )
            )
        return out

    def _expansion_for_family(self, candidate: Candidate, family: str) -> IntentExpansion:
        title = build_candidate_title(normalized_source_headline=candidate.normalized_source_headline, family=family, fallback=candidate.source_headline)
        query = build_primary_query(normalized_source_headline=candidate.normalized_source_headline, family=family, fallback=candidate.source_headline)
        return IntentExpansion(
            intent_family=family,
            title=title,
            primary_query=query,
            supporting_queries=[f'{candidate.normalized_source_headline or candidate.source_headline} explained', f'{candidate.normalized_source_headline or candidate.source_headline} details', f'{candidate.normalized_source_headline or candidate.source_headline} implications'],
            usefulness_score=72.0 if family in {'comparison', 'should_you', 'alternatives', 'how_to', 'why_it_matters'} else 60.0,
            search_demand_score=70.0 if family in {'comparison', 'pricing', 'how_to'} else 55.0,
            evergreen_score=68.0 if family in {'alternatives', 'how_to', 'why_it_matters'} else 45.0,
            candidate_body_hint=f'Stay close to the source facts about {candidate.source_headline or candidate.source_title} and avoid generic platform filler.',
        )

    def _normalize_headline(self, source_headline: str) -> tuple[str, str, str]:
        headline = sanitize_source_headline(source_headline)
        language = 'en'
        if looks_non_english(headline):
            language = 'non_english'
            if self.ollama_client is not None:
                try:
                    payload = self.ollama_client.generate_json(
                        system_prompt='Return JSON only with normalized_headline and language. Preserve named entities, remove ingestion noise, and write a concise normalized headline without adding comparison/how-to framing.',
                        payload={'source_headline': headline},
                        purpose='v2_headline_normalize',
                    )
                    normalized = sanitize_source_headline(str(payload.get('normalized_headline', '') or headline))
                    detected = str(payload.get('language', '') or language).strip() or language
                    return normalized or headline, detected, 'ollama'
                except Exception:
                    pass
        return headline, language, 'rules'

    def _allowed_families(self, candidate: Candidate, allocation_slot: str) -> list[str]:
        blob = ' '.join([
            candidate.source_headline,
            candidate.normalized_source_headline,
            candidate.source_snippet,
            candidate.category,
            ' '.join(candidate.tags),
        ]).lower()
        non_english = looks_non_english(candidate.source_headline or candidate.source_title)
        families = list(DEFAULT_RULES_FAMILIES)
        if self._comparison_allowed(blob):
            families.append('comparison')
        if self._pricing_allowed(blob):
            families.append('pricing')
        if self._performance_allowed(blob):
            families.append('performance')
        if self._alternatives_allowed(blob):
            families.append('alternatives')
        if self._how_to_allowed(blob):
            families.append('how_to')
        if non_english:
            allowed = [family for family in families if family in SAFE_NON_ENGLISH_FAMILIES]
            if self._comparison_allowed(blob):
                allowed.append('comparison')
            if self._pricing_allowed(blob):
                allowed.append('pricing')
            if self._performance_allowed(blob):
                allowed.append('performance')
            if self._alternatives_allowed(blob):
                allowed.append('alternatives')
            families = []
            for family in allowed:
                if family not in families:
                    families.append(family)
        if allocation_slot == 'hot':
            families = [family for family in families if family != 'how_to']
        return families or ['what_changed']

    def _comparison_allowed(self, blob: str) -> bool:
        return any(term in blob for term in COMPARISON_SIGNALS)

    def _pricing_allowed(self, blob: str) -> bool:
        return any(term in blob for term in PRICING_SIGNALS)

    def _performance_allowed(self, blob: str) -> bool:
        return any(term in blob for term in PERFORMANCE_SIGNALS)

    def _alternatives_allowed(self, blob: str) -> bool:
        return any(term in blob for term in ALTERNATIVE_SIGNALS)

    def _how_to_allowed(self, blob: str) -> bool:
        return any(term in blob for term in HOW_TO_SIGNALS) and any(term in blob for term in {'app', 'software', 'platform', 'model', 'api', 'tool', 'rollout', 'release'}) and not any(term in blob for term in {'exam', 'ranking', 'election', 'lawsuit'})

    def _contract_id(self, candidate: Candidate, family: str, primary_query: str) -> str:
        text = '|'.join([
            candidate.candidate_id,
            candidate.source_headline,
            candidate.normalized_source_headline,
            family,
            primary_query,
        ])
        return hashlib.sha1(text.encode('utf-8')).hexdigest()[:16]
