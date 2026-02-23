from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


KEYWORDS = [
    "권위",
    "근거",
    "행동",
    "cta",
    "pattern",
    "패턴",
    "구조",
    "체크리스트",
    "evidence",
    "authority",
    "action",
    "반박",
    "전환",
    "beginner",
    "plain english",
    "practical",
    "checklist",
]


@dataclass
class ReferenceCorpus:
    files: list[Path]

    def build_guidance(self, max_chars: int = 2500) -> str:
        blocked = {
            "seo",
            "e-e-a-t",
            "trustworthiness",
            "algorithm",
            "process disclosure",
            "search ranking",
            "helpful content update",
        }
        lines: list[str] = []
        for file in self.files:
            if not file.exists():
                continue
            text = file.read_text(encoding="utf-8", errors="ignore")
            for raw in text.splitlines():
                line = " ".join(raw.strip().split())
                if len(line) < 12:
                    continue
                lower = line.lower()
                if any(b in lower for b in blocked):
                    continue
                if any(k in lower for k in KEYWORDS):
                    lines.append(line)

        deduped: list[str] = []
        seen: set[str] = set()
        for line in lines:
            if line in seen:
                continue
            seen.add(line)
            deduped.append(line)

        joined = "\n".join(deduped)
        if len(joined) <= max_chars:
            return joined
        return joined[:max_chars]
