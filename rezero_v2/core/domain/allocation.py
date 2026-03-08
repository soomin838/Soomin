from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class AllocationDecision:
    slot_type: Literal["hot", "search_derived", "evergreen"]
    source_type: Literal["gdelt", "search_console", "cluster_seed"]
    generation_mode_hint: str
    target_word_range: tuple[int, int]
    title_strategy: str
    source_strategy: str
    image_strategy: str
