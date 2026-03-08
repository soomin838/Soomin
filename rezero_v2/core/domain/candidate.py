from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    content_type: Literal["hot", "search_derived", "evergreen"]
    source_type: Literal["gdelt", "search_console", "cluster_seed"]
    title: str
    source_title: str
    source_url: str
    source_domain: str
    source_snippet: str
    category: str
    published_at_utc: str
    provider: str
    language: str
    entity_terms: list[str] = field(default_factory=list)
    topic_terms: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    raw_meta: dict[str, Any] = field(default_factory=dict)
