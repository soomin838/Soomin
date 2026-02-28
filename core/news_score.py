from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


_SECURITY_TOKENS = (
    "security",
    "vulnerability",
    "patch",
    "cve",
    "breach",
    "malware",
    "ransomware",
)
_POLICY_TOKENS = (
    "policy",
    "privacy",
    "regulation",
    "ban",
    "consent",
    "tracking",
)
_AI_TOKENS = (
    "ai",
    "model",
    "openai",
    "anthropic",
    "gemini",
    "copilot",
    "claude",
)
_PLATFORM_TOKENS = (
    "ios",
    "android",
    "iphone",
    "pixel",
    "windows",
    "macos",
    "apple",
    "microsoft",
    "google",
)
_CHIPS_TOKENS = (
    "nvidia",
    "chip",
    "gpu",
    "semiconductor",
    "amd",
    "intel",
)
_SPAM_TOKENS = ("deal", "coupon", "sponsored", "affiliate")
_CLICKBAIT_TOKENS = (
    "shocking",
    "disaster",
    "scam",
    "fraud",
    "criminal",
    "exposed",
    "destroyed",
    "caught",
)


def classify_category(text: str) -> str:
    lower = re.sub(r"\s+", " ", str(text or "").strip().lower())
    if any(tok in lower for tok in _SECURITY_TOKENS):
        return "security"
    if any(tok in lower for tok in _POLICY_TOKENS):
        return "policy"
    if any(tok in lower for tok in _AI_TOKENS):
        return "ai"
    if any(tok in lower for tok in _CHIPS_TOKENS):
        return "chips"
    if any(tok in lower for tok in _PLATFORM_TOKENS):
        return "platform"
    return "platform"


def score_news_item(
    *,
    title: str,
    snippet: str,
    source: str,
    published_at: datetime | None,
    source_weights: dict[str, float] | None = None,
) -> tuple[int, str]:
    merged = f"{str(title or '')} {str(snippet or '')}".strip()
    lower = merged.lower()
    category = classify_category(merged)

    now = datetime.now(timezone.utc)
    recency_score = 0.0
    if isinstance(published_at, datetime):
        ts = published_at if published_at.tzinfo else published_at.replace(tzinfo=timezone.utc)
        age_hours = max(0.0, (now - ts.astimezone(timezone.utc)).total_seconds() / 3600.0)
        recency_score = max(0.0, 60.0 - min(60.0, age_hours * 1.5))

    sw = 1.0
    source_host = str(source or "").strip().lower()
    for key, val in (source_weights or {}).items():
        k = str(key or "").strip().lower()
        if not k:
            continue
        if k in source_host:
            try:
                sw = float(val)
            except Exception:
                sw = 1.0
            break
    source_score = max(0.0, min(20.0, 10.0 * sw))

    category_bonus_map = {
        "security": 10.0,
        "policy": 8.0,
        "ai": 6.0,
        "platform": 4.0,
        "chips": 5.0,
    }
    category_bonus = float(category_bonus_map.get(category, 4.0))

    spam_penalty = -100.0 if any(tok in lower for tok in _SPAM_TOKENS) else 0.0
    clickbait_penalty = -30.0 if any(tok in lower for tok in _CLICKBAIT_TOKENS) else 0.0

    score = recency_score + source_score + category_bonus + spam_penalty + clickbait_penalty
    return max(0, int(round(score))), category


def has_blocked_keywords(text: str, blocked: list[str] | tuple[str, ...] | None) -> bool:
    lower = re.sub(r"\s+", " ", str(text or "").strip().lower())
    for token in (blocked or []):
        t = str(token or "").strip().lower()
        if t and t in lower:
            return True
    return False


def contains_allow_keywords(text: str, allow: list[str] | tuple[str, ...] | None) -> bool:
    allow_list = [str(x or "").strip().lower() for x in (allow or []) if str(x or "").strip()]
    if not allow_list:
        return True
    lower = re.sub(r"\s+", " ", str(text or "").strip().lower())
    return any(tok in lower for tok in allow_list)

