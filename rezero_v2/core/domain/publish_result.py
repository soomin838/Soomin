from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class PublishArtifact:
    status: Literal["published", "scheduled", "skipped", "held", "failed"]
    post_id: str
    post_url: str
    published_at_utc: str
    internal_links_added: list[str] = field(default_factory=list)
    source_links_kept: list[str] = field(default_factory=list)
