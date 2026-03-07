from __future__ import annotations

import re


_CLICKBAIT_MAP: dict[str, str] = {
    "shocking": "notable",
    "disaster": "major issue",
    "scam": "scheme",
    "fraud": "misconduct",
    "criminal": "illegal",
    "exposed": "revealed",
    "destroyed": "disrupted",
    "caught": "reported",
}

_CLICKBAIT_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _CLICKBAIT_MAP.keys()) + r")\b",
    flags=re.IGNORECASE,
)
_TAG_SPLIT_PATTERN = re.compile(r"(<[^>]+>)")


def sanitize_clickbait_terms(html: str) -> tuple[str, list[str]]:
    src = str(html or "")
    if not src:
        return src, []
    replaced: list[str] = []
    seen: set[str] = set()

    def _replace_text_segment(segment: str) -> str:
        def _repl(match: re.Match[str]) -> str:
            raw = str(match.group(0) or "")
            key = raw.lower()
            replacement = _CLICKBAIT_MAP.get(key)
            if not replacement:
                return raw
            if key not in seen:
                seen.add(key)
                replaced.append(key)
            return replacement

        return _CLICKBAIT_PATTERN.sub(_repl, segment)

    out_parts: list[str] = []
    for part in _TAG_SPLIT_PATTERN.split(src):
        if not part:
            continue
        if part.startswith("<") and part.endswith(">"):
            # Keep attributes (including href URLs) untouched.
            out_parts.append(part)
            continue
        out_parts.append(_replace_text_segment(part))
    return "".join(out_parts), replaced
