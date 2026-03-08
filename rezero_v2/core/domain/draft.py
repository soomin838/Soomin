from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DraftArtifact:
    title: str
    intro: str
    html: str
    plain_text: str
    section_titles: list[str] = field(default_factory=list)
    word_count: int = 0
    repair_attempted: bool = False
    repair_succeeded: bool = False
    source_citations: list[str] = field(default_factory=list)
