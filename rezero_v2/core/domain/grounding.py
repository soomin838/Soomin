from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class GroundingPacket:
    source_headline: str
    normalized_source_headline: str
    derived_primary_query: str
    canonical_source_title: str
    source_snippet: str
    source_domain: str
    required_named_entities: list[str] = field(default_factory=list)
    required_topic_nouns: list[str] = field(default_factory=list)
    required_source_facts: list[str] = field(default_factory=list)
    forbidden_drift_terms: list[str] = field(default_factory=list)
    packet_quality_score: float = 0.0
    content_type: str = "hot"
    intent_family: str = "what_changed"
    category: str = ""
    mixed_domain: bool = False
    dominant_axis: str = "tech"
