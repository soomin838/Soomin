from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_http_status(message: str) -> int | None:
    m = re.search(r"\b(400|401|403|404|408|409|429|500|502|503|504)\b", message or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def classify_error(message: str) -> str:
    msg = (message or "").lower()
    code = _extract_http_status(message)
    if code == 400:
        return "http_400_bad_request"
    if code == 401:
        return "http_401_unauthorized"
    if code == 429:
        return "http_429_rate_limit"
    if code and 500 <= code <= 599:
        return "http_5xx_server"
    if "timeout" in msg or "timed out" in msg:
        return "timeout"
    if "connection" in msg or "network" in msg or "dns" in msg:
        return "network"
    return "unknown"


def process_memory_mb() -> float | None:
    # Optional dependency to keep installer minimal.
    try:
        import psutil  # type: ignore

        p = psutil.Process(os.getpid())
        return round(float(p.memory_info().rss) / (1024 * 1024), 2)
    except Exception:
        return None


class QALogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, part: str, event: str, payload: dict[str, Any]) -> None:
        row = {
            "ts_utc": _now_utc(),
            "part": part,
            "event": event,
            "memory_mb": process_memory_mb(),
        }
        row.update(payload)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")
