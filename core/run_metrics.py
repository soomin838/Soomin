from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_reason_codes(note: str) -> list[str]:
    src = str(note or "").strip().lower()
    if not src:
        return []
    chunks = re.split(r"[\s\|,;]+", src)
    out: list[str] = []
    seen: set[str] = set()
    for raw in chunks:
        token = re.sub(r"[^a-z0-9:_\-\.=]+", "", str(raw or "").strip().lower())
        token = token.strip("._-")
        if not token:
            continue
        if len(token) > 80:
            token = token[:80].rstrip("._-")
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


class RunMetricsLogger:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.path = (self.root / "storage" / "logs" / "run_metrics.jsonl").resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, payload: dict[str, Any]) -> None:
        row = dict(payload or {})
        row.setdefault("ts_utc", datetime.now(timezone.utc).isoformat())
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
