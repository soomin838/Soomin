from __future__ import annotations


def _cap(value: float) -> float:
    try:
        numeric = float(value)
    except Exception:
        numeric = 0.0
    return max(0.0, min(100.0, numeric))


class TopicScorer:
    def score_hot(self, *, trend_score: float, freshness: float, explicit_tech_signal: float, grounding_strength: float) -> float:
        return round((0.40 * _cap(trend_score)) + (0.30 * _cap(freshness)) + (0.20 * _cap(explicit_tech_signal)) + (0.10 * _cap(grounding_strength)), 2)

    def score_search_derived(self, *, search_demand: float, usefulness_score: float, competition_inverse: float, freshness: float, cluster_fit: float) -> float:
        return round((0.35 * _cap(search_demand)) + (0.25 * _cap(usefulness_score)) + (0.20 * _cap(competition_inverse)) + (0.10 * _cap(freshness)) + (0.10 * _cap(cluster_fit)), 2)

    def score_evergreen(self, *, durability: float, usefulness_score: float, cluster_gap: float, search_demand: float, authority_source_availability: float) -> float:
        return round((0.35 * _cap(durability)) + (0.25 * _cap(usefulness_score)) + (0.20 * _cap(cluster_gap)) + (0.10 * _cap(search_demand)) + (0.10 * _cap(authority_source_availability)), 2)
