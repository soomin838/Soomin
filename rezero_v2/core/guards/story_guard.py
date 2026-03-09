from __future__ import annotations

from dataclasses import dataclass
import re

from rezero_v2.core.domain.candidate import Candidate

TECH_TERMS = {
    'ai', 'artificial intelligence', 'machine learning', 'llm', 'model', 'models',
    'software', 'platform', 'privacy', 'cybersecurity', 'iphone', 'android',
    'semiconductor', 'chip', 'chips', 'gpu', 'cloud', 'api', 'browser',
    'encryption', 'release', 'rollout', 'developer', 'open source', 'robotics',
    'app', 'apps',
}
EDUCATION_TERMS = {'exam', 'ranking', 'admission', 'school', 'schools', 'student', 'students', 'campus', 'university', 'college'}
PUBLIC_AFFAIRS_TERMS = {
    'election', 'electoral', 'politics', 'political', 'government', 'minister', 'ministry',
    'parliament', 'policy vote', 'protest', 'union', 'labor', 'strike', 'workers',
    'public affairs', 'reforma', 'electoral', 'voto', 'morena', 'pan',
}
AGRICULTURE_MINING_TERMS = {
    'agriculture', 'agricultural', 'farmer', 'farmers', 'farm', 'farmland', 'crop', 'crops',
    'cocoa', 'cacao', 'land', 'lands', 'soil', 'mining', 'miner', 'miners', 'illegal mining',
    'agricultores', 'minería', 'mineria', 'tierras', 'agricole',
}
ECONOMIC_NON_TECH_TERMS = {
    'price', 'prices', 'inflation', 'cost of living', 'funding', 'housing', 'rent',
    'wages', 'salary', 'economy', 'economic', 'market', 'commodity', 'commodities',
    'precio', 'precios', 'mercado', 'fundos',
}
OFF_TOPIC_TERMS = {
    *EDUCATION_TERMS,
    'celebrity', 'sports', 'entertainment', 'award', 'festival', 'human interest',
    *PUBLIC_AFFAIRS_TERMS,
    *AGRICULTURE_MINING_TERMS,
}
PR_WIRE_DOMAINS = {'prnewswire.com', 'globenewswire.com', 'businesswire.com'}
NEGATED_TECH_PATTERNS = (
    r'\b(?:no|without)\b[^.]{0,64}\b(?:technology|tech|software|ai|app|api|model|platform|product)\b',
    r'\bsin\b[^.]{0,64}\b(?:tecnolog[íi]a|software|ai|app|api|modelo|platform|producto)\b',
)


@dataclass(frozen=True)
class StoryGuardDecision:
    allow: bool
    reason_code: str
    mixed_domain: bool
    dominant_axis: str
    explicit_tech_angle: bool
    tech_score: float
    off_topic_score: float


class StoryGuard:
    def evaluate(self, candidate: Candidate, *, mode: str = 'tech_news_only') -> StoryGuardDecision:
        source_blob = self._source_blob(candidate)
        meta_blob = ' '.join([candidate.category, ' '.join(candidate.tags)]).lower()
        tech_hits = self._count_matches(source_blob, TECH_TERMS)
        off_hits = self._count_matches(source_blob, OFF_TOPIC_TERMS)
        education_hits = self._count_matches(source_blob, EDUCATION_TERMS)
        public_hits = self._count_matches(source_blob, PUBLIC_AFFAIRS_TERMS)
        agriculture_hits = self._count_matches(source_blob, AGRICULTURE_MINING_TERMS)
        economic_hits = self._count_matches(source_blob, ECONOMIC_NON_TECH_TERMS)
        explicit_tech = self._has_explicit_tech_signal(source_blob)
        source_domain = str(candidate.source_domain or '').lower()
        non_english = self._is_non_english(candidate)
        mixed_domain = (education_hits > 0 or public_hits > 0 or agriculture_hits > 0) and tech_hits > 0
        dominant_axis = 'tech' if tech_hits >= off_hits else 'off_topic'
        if source_domain in PR_WIRE_DOMAINS:
            return StoryGuardDecision(False, 'off_topic_pr_wire_low_tech_overlap', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
        if mode == 'tech_news_only':
            if non_english and not explicit_tech:
                if agriculture_hits > 0:
                    return StoryGuardDecision(False, 'off_topic_agriculture_mining_story', mixed_domain, 'off_topic', False, float(tech_hits), float(off_hits))
                if public_hits > 0:
                    return StoryGuardDecision(False, 'multilingual_non_tech_public_affairs', mixed_domain, 'off_topic', False, float(tech_hits), float(off_hits))
                if economic_hits > 0:
                    return StoryGuardDecision(False, 'non_tech_economic_story_without_explicit_technology_angle', mixed_domain, 'off_topic', False, float(tech_hits), float(off_hits))
                return StoryGuardDecision(False, 'mixed_domain_requires_explicit_tech_angle', mixed_domain, 'off_topic', False, float(tech_hits), float(off_hits))
            if agriculture_hits > 0 and not explicit_tech:
                return StoryGuardDecision(False, 'off_topic_agriculture_mining_story', mixed_domain, 'off_topic', False, float(tech_hits), float(off_hits))
            if public_hits > 0 and not explicit_tech:
                return StoryGuardDecision(False, 'multilingual_non_tech_public_affairs' if non_english else 'off_topic_non_tech_signal_dominant', mixed_domain, 'off_topic', False, float(tech_hits), float(off_hits))
            if economic_hits > 0 and not explicit_tech and not self._has_meta_tech_support(meta_blob):
                return StoryGuardDecision(False, 'non_tech_economic_story_without_explicit_technology_angle', mixed_domain, 'off_topic', False, float(tech_hits), float(off_hits))
            if mixed_domain and not explicit_tech:
                return StoryGuardDecision(False, 'mixed_domain_requires_explicit_tech_angle', True, dominant_axis, False, float(tech_hits), float(off_hits))
            if mixed_domain and dominant_axis != 'tech':
                return StoryGuardDecision(False, 'mixed_domain_education_tech_but_tech_not_dominant', True, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
            if tech_hits <= 0:
                return StoryGuardDecision(False, 'tech_signal_overlap_below_threshold', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
            if off_hits > tech_hits + 1:
                return StoryGuardDecision(False, 'off_topic_non_tech_signal_dominant', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
        return StoryGuardDecision(True, 'accepted', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))

    def _source_blob(self, candidate: Candidate) -> str:
        parts = [
            candidate.source_headline,
            candidate.normalized_source_headline,
            candidate.source_title,
            candidate.source_snippet,
            ' '.join(candidate.entity_terms),
            ' '.join(candidate.topic_terms),
        ]
        blob = ' '.join(str(part or '').strip() for part in parts if str(part or '').strip()).lower()
        for pattern in NEGATED_TECH_PATTERNS:
            blob = re.sub(pattern, ' ', blob, flags=re.IGNORECASE)
        return re.sub(r'\s+', ' ', blob).strip()

    def _count_matches(self, blob: str, terms: set[str]) -> int:
        return sum(1 for term in terms if self._contains_term(blob, term))

    def _has_explicit_tech_signal(self, blob: str) -> bool:
        explicit_terms = {'ai', 'artificial intelligence', 'machine learning', 'software', 'platform', 'app', 'api', 'model', 'semiconductor', 'chip', 'chips', 'gpu', 'cloud', 'cybersecurity', 'privacy', 'robotics'}
        return any(self._contains_term(blob, term) for term in explicit_terms)

    def _has_meta_tech_support(self, blob: str) -> bool:
        return any(self._contains_term(blob, term) for term in {'ai', 'software', 'platform', 'model', 'semiconductor', 'chip', 'cybersecurity', 'privacy'})

    def _is_non_english(self, candidate: Candidate) -> bool:
        if str(candidate.raw_meta.get('source_language', '') or '').strip().lower() not in {'', 'en', 'english'}:
            return True
        if str(candidate.language or '').strip().lower() in {'non_english', 'multilingual'}:
            return True
        headline = ' '.join([candidate.source_headline, candidate.normalized_source_headline, candidate.source_title])
        return bool(re.search(r'[^\x00-\x7F]', headline))

    def _contains_term(self, blob: str, term: str) -> bool:
        value = str(term or '').strip().lower()
        if not value:
            return False
        if re.fullmatch(r'[a-z0-9_+-]+', value):
            pattern = rf'(?<![a-z0-9_+-]){re.escape(value)}(?![a-z0-9_+-])'
            return re.search(pattern, blob, flags=re.IGNORECASE) is not None
        return value in blob
