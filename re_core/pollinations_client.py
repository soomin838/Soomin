from __future__ import annotations

import uuid
from pathlib import Path

import requests


def generate_image(prompt: str, negative: str, *, out_dir: Path, timeout_sec: int = 60) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"poll_{uuid.uuid4().hex}.png"

    url = "https://image.pollinations.ai/prompt/" + requests.utils.quote(prompt)
    params = {"negative": negative}

    r = requests.get(url, params=params, timeout=timeout_sec)
    r.raise_for_status()
    out.write_bytes(r.content)
    return out
