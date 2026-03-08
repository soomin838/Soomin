from __future__ import annotations

from dataclasses import dataclass

from rezero_v2.core.domain.candidate import Candidate

TECH_TERMS = {'ai', 'artificial intelligence', 'machine learning', 'software', 'platform', 'privacy', 'cybersecurity', 'mobile', 'iphone', 'android', 'semiconductor', 'chip', 'chips', 'gpu', 'cloud', 'api', 'app', 'apps', 'browser', 'encryption', 'policy', 'data', 'model', 'release', 'rollout', 'developer', 'open source'}
OFF_TOPIC_TERMS = {'exam', 'ranking', 'admission', 'school', 'schools', 'student', 'students', 'celebrity', 'sports', 'election', 'politics', 'entertainment', 'award', 'festival', 'human interest'}
PR_WIRE_DOMAINS = {'prnewswire.com', 'globenewswire.com', 'businesswire.com'}


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
        blob = ' '.join([candidate.title, candidate.source_title, candidate.source_snippet, candidate.category, ' '.join(candidate.tags)]).lower()
        tech_hits = sum(1 for term in TECH_TERMS if term in blob)
        off_hits = sum(1 for term in OFF_TOPIC_TERMS if term in blob)
        explicit_tech = tech_hits >= 2 or any(term in blob for term in {'semiconductor', 'chip', 'software', 'ai', 'privacy', 'cybersecurity'})
        source_domain = str(candidate.source_domain or '').lower()
        mixed_domain = off_hits > 0 and tech_hits > 0
        dominant_axis = 'tech' if tech_hits >= off_hits else 'off_topic'
        if source_domain in PR_WIRE_DOMAINS:
            return StoryGuardDecision(False, 'off_topic_pr_wire_low_tech_overlap', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
        if mode == 'tech_news_only':
            if mixed_domain and not explicit_tech:
                return StoryGuardDecision(False, 'mixed_domain_requires_explicit_tech_angle', True, dominant_axis, False, float(tech_hits), float(off_hits))
            if mixed_domain and dominant_axis != 'tech':
                return StoryGuardDecision(False, 'mixed_domain_education_tech_but_tech_not_dominant', True, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
            if tech_hits <= 0:
                return StoryGuardDecision(False, 'tech_signal_overlap_below_threshold', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
            if off_hits > tech_hits + 1:
                return StoryGuardDecision(False, 'off_topic_non_tech_signal_dominant', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
        return StoryGuardDecision(True, 'accepted', mixed_domain, dominant_axis, explicit_tech, float(tech_hits), float(off_hits))
