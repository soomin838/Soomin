from __future__ import annotations

import re
from collections import Counter
from html import unescape
from typing import Any


def _setting(settings: dict[str, Any] | Any, key: str, default: Any) -> Any:
    if isinstance(settings, dict):
        return settings.get(key, default)
    return getattr(settings, key, default)


def _strip_tags(text: str) -> str:
    raw = re.sub(r"(?is)<[^>]+>", " ", str(text or ""))
    raw = unescape(raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def strip_html_for_analysis(html: str) -> tuple[list[str], list[str]]:
    src = str(html or "")
    h2_raw = re.findall(r"(?is)<h2[^>]*>(.*?)</h2>", src)
    h2_titles = [
        re.sub(r"\s+", " ", _strip_tags(x).lower()).strip()
        for x in h2_raw
        if re.sub(r"\s+", " ", _strip_tags(x).lower()).strip()
    ]
    text_chunks = re.findall(r"(?is)<(?:p|li)[^>]*>(.*?)</(?:p|li)>", src)
    merged = " ".join([_strip_tags(chunk) for chunk in text_chunks if _strip_tags(chunk)])
    merged = re.sub(r"\s+", " ", merged).strip()
    if not merged:
        return [], h2_titles
    sentences = [re.sub(r"\s+", " ", x).strip() for x in re.split(r"(?<=[.!?])\s+", merged) if str(x).strip()]
    return sentences, h2_titles


def trigram_tokens(text: str) -> list[tuple[str, str, str]]:
    tokens = [
        token
        for token in re.findall(r"[a-z0-9']+", str(text or "").lower())
        if len(token) >= 2
    ]
    if len(tokens) < 3:
        return []
    return [(tokens[i], tokens[i + 1], tokens[i + 2]) for i in range(0, len(tokens) - 2)]


def trigram_repeat_ratio(sentences: list[str]) -> float:
    merged = " ".join([str(x or "") for x in (sentences or []) if str(x or "").strip()])
    grams = trigram_tokens(merged)
    if not grams:
        return 0.0
    counts = Counter(grams)
    most_common = int(max(counts.values()) if counts else 0)
    ratio = float(most_common) / float(len(grams))
    return max(0.0, min(1.0, ratio))


def sentence_starter_repeats(sentences: list[str]) -> tuple[int, str]:
    counts: Counter[str] = Counter()
    for sentence in sentences or []:
        tokens = [
            token
            for token in re.findall(r"[a-z0-9']+", str(sentence or "").lower())
            if len(token) >= 2
        ]
        if len(tokens) < 3:
            continue
        starter = " ".join(tokens[:3]).strip()
        if starter:
            counts[starter] += 1
    if not counts:
        return 0, ""
    starter, count = counts.most_common(1)[0]
    return int(count), str(starter)


def duplicate_h2_count(h2_titles: list[str]) -> int:
    clean = [re.sub(r"\s+", " ", str(x or "").lower()).strip() for x in (h2_titles or []) if str(x or "").strip()]
    counts = Counter(clean)
    dup = sum(1 for _, cnt in counts.items() if int(cnt) > 1)
    return int(max(0, dup))


def check_entropy(html: str, settings: dict[str, Any] | Any) -> dict[str, Any]:
    trigram_max_ratio = float(_setting(settings, "trigram_max_ratio", 0.05) or 0.05)
    starter_max_repeats = int(_setting(settings, "starter_max_repeats", 3) or 3)
    duplicate_h2_max = int(_setting(settings, "duplicate_h2_max", 0) or 0)

    sentences, h2_titles = strip_html_for_analysis(html)
    tri_ratio = trigram_repeat_ratio(sentences)
    max_starter_count, max_starter = sentence_starter_repeats(sentences)
    duplicate_h2 = duplicate_h2_count(h2_titles)

    reasons: list[str] = []
    if tri_ratio > float(trigram_max_ratio):
        reasons.append(f"trigram_ratio>{trigram_max_ratio:.4f}")
    if int(max_starter_count) > int(starter_max_repeats):
        reasons.append(f"starter_repeats>{int(starter_max_repeats)}")
    if int(duplicate_h2) > int(duplicate_h2_max):
        reasons.append(f"duplicate_h2>{int(duplicate_h2_max)}")

    return {
        "ok": len(reasons) == 0,
        "trigram_ratio": float(tri_ratio),
        "max_starter_count": int(max_starter_count),
        "max_starter": str(max_starter or ""),
        "duplicate_h2": int(duplicate_h2),
        "reasons": reasons,
    }

