from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ImageArtifact:
    role: Literal["hero", "inline"]
    url: str
    alt_text: str
    provider: Literal["pollinations"]
    generated_at_utc: str
    generated_in_current_run: bool
    reused_asset: bool
    prompt_digest: str
