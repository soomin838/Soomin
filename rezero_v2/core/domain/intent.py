from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class IntentExpansion:
    intent_family: Literal["what_changed", "comparison", "pricing", "performance", "should_you", "alternatives", "how_to"]
    title: str
    primary_query: str
    supporting_queries: list[str] = field(default_factory=list)
    usefulness_score: float = 0.0
    search_demand_score: float = 0.0
    evergreen_score: float = 0.0
    candidate_body_hint: str = ""


@dataclass(frozen=True)
class IntentBundle:
    primary_query: str
    content_type: Literal["hot", "search_derived", "evergreen"]
    title_strategy: str
    source_strategy: str
    image_strategy: str
    expansions: list[IntentExpansion] = field(default_factory=list)
    source_grounded: bool = True
    source_model: Literal["ollama", "rules"] = "rules"
