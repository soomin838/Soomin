from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class OutlinePlan:
    section_titles: list[str] = field(default_factory=list)
    section_purposes: list[str] = field(default_factory=list)
    heading_signature: str = ""
    grounding_coverage_score: float = 0.0
    diversity_score: float = 0.0
    debug_outline_source: Literal["ollama", "rules"] = "rules"
