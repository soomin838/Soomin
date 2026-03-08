from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote_plus


@dataclass(frozen=True)
class PollinationsImageResponse:
    url: str
    provider: str
    generated_at_utc: str
    prompt_digest: str


class PollinationsClient:
    def __init__(self, *, model: str = 'flux') -> None:
        self.model = str(model or 'flux').strip() or 'flux'

    def generate_image_url(self, *, prompt: str, width: int = 1280, height: int = 720, seed: int | None = None) -> PollinationsImageResponse:
        clean = ' '.join(str(prompt or '').split()).strip()
        if not clean:
            raise RuntimeError('empty_pollinations_prompt')
        if seed is None:
            seed = abs(hash((clean, datetime.now(timezone.utc).isoformat()))) % 100000000
        encoded = quote_plus(clean)
        url = f"https://image.pollinations.ai/prompt/{encoded}?model={quote_plus(self.model)}&width={int(width)}&height={int(height)}&seed={int(seed)}&nologo=true&nofeed=true"
        return PollinationsImageResponse(url=url, provider='pollinations', generated_at_utc=datetime.now(timezone.utc).isoformat(), prompt_digest=hashlib.sha1(clean.encode('utf-8')).hexdigest())
