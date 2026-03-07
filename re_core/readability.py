from __future__ import annotations

import re
from collections import Counter
from html import escape, unescape
from typing import Any


_TRANSITION_START_RE = re.compile(
    r"^\s*(however|therefore|additionally|in conclusion|moreover|furthermore|meanwhile|consequently|thus|overall|to summarize|in summary)\s*,?\s*",
    flags=re.IGNORECASE,
)


def _get_setting_value(settings: Any, key: str, default: int | bool) -> int | bool:
    if isinstance(settings, dict):
        return settings.get(key, default)
    return getattr(settings, key, default)


def _ensure_terminal_punctuation(sentence: str) -> str:
    text = re.sub(r"\s+", " ", str(sentence or "")).strip()
    if not text:
        return ""
    if re.search(r"[.!?]$", text):
        return text
    return f"{text}."


def _starter_key(sentence: str) -> str:
    tokens = re.findall(r"[a-z0-9']+", str(sentence or "").lower())
    if len(tokens) >= 3:
        return " ".join(tokens[:3])
    if len(tokens) >= 2:
        return " ".join(tokens[:2])
    return tokens[0] if tokens else ""


def _rewrite_starter(sentence: str) -> str:
    text = str(sentence or "").strip()
    if not text:
        return text

    cut = re.sub(r"^\s*(this|these|those|the)\s+", "", text, flags=re.IGNORECASE)
    cut = re.sub(r"^\s*according to\s+", "", cut, flags=re.IGNORECASE)
    cut = re.sub(r"^\s*in this update\s*,?\s*", "", cut, flags=re.IGNORECASE)
    if cut and cut != text:
        if cut[0].islower():
            cut = cut[0].upper() + cut[1:]
        return cut

    if text[0].isupper():
        return f"In practice, {text[0].lower()}{text[1:]}"
    return f"In practice, {text}"


def _preferred_split_index(words: list[str], max_words: int) -> int:
    if len(words) <= max_words:
        return len(words)
    hard_max = max(2, int(max_words))
    soft_min = max(2, hard_max // 2)
    connective = {"and", "but", "because", "while", "which", "that", "so", "then", "or"}
    upper = min(hard_max, len(words) - 1)
    for idx in range(upper, soft_min - 1, -1):
        prev = words[idx - 1]
        prev_token = re.sub(r"[^a-z0-9'-]", "", prev.lower())
        next_token = re.sub(r"[^a-z0-9'-]", "", words[idx].lower()) if idx < len(words) else ""
        if prev.endswith((",", ";", ":")):
            return idx
        if prev_token in connective or next_token in connective:
            return idx
    return hard_max


def _chunk_sentences(sentences: list[str], min_sentences: int, max_sentences: int) -> list[list[str]]:
    clean = [s for s in (sentences or []) if str(s or "").strip()]
    if not clean:
        return []
    if len(clean) <= max_sentences:
        return [clean]

    chunks: list[list[str]] = []
    idx = 0
    total = len(clean)
    while idx < total:
        remaining = total - idx
        take = remaining if remaining <= max_sentences else max_sentences
        if remaining > max_sentences and (remaining - take) < min_sentences:
            take = max(min_sentences, remaining - min_sentences)
        take = max(1, min(take, remaining))
        chunks.append(clean[idx : idx + take])
        idx += take

    if len(chunks) >= 2 and len(chunks[-1]) < min_sentences:
        chunks[-2].extend(chunks[-1])
        chunks.pop()
        if len(chunks[-1]) > max_sentences:
            overflow = chunks[-1][max_sentences:]
            chunks[-1] = chunks[-1][:max_sentences]
            if overflow:
                chunks.append(overflow)
    return chunks


def strip_tags_keep_h2(html: str) -> str:
    text = str(html or "")
    if not text:
        return ""
    text = re.sub(r"(?is)<\s*h2[^>]*>", "\n[[H2]] ", text)
    text = re.sub(r"(?is)</\s*h2\s*>", "\n", text)
    text = re.sub(r"(?is)<\s*h3[^>]*>", "\n[[H3]] ", text)
    text = re.sub(r"(?is)</\s*h3\s*>", "\n", text)
    text = re.sub(r"(?is)<\s*br\s*/?\s*>", "\n", text)
    text = re.sub(r"(?is)</\s*p\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def split_sentences(text: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", unescape(str(text or "")).strip())
    if not cleaned:
        return []
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    out: list[str] = []
    for part in parts:
        candidate = re.sub(r"\s+", " ", str(part or "")).strip()
        if candidate:
            out.append(candidate)
    return out


def enforce_max_sentence_words(sent: str, max_words: int) -> list[str]:
    sentence = re.sub(r"\s+", " ", str(sent or "")).strip()
    if not sentence:
        return []
    max_words = max(1, int(max_words))
    terminal_match = re.search(r"[.!?]\s*$", sentence)
    terminal = terminal_match.group(0).strip() if terminal_match else "."
    body = sentence[: terminal_match.start()].strip() if terminal_match else sentence
    words = body.split()
    if len(words) <= max_words:
        if terminal_match:
            return [_ensure_terminal_punctuation(f"{body}{terminal}")]
        return [_ensure_terminal_punctuation(body)]

    out: list[str] = []
    remainder = words[:]
    while len(remainder) > max_words:
        split_idx = _preferred_split_index(remainder, max_words)
        chunk_words = remainder[:split_idx]
        remainder = remainder[split_idx:]
        chunk = " ".join(chunk_words).strip(" ,;:-")
        if chunk:
            out.append(_ensure_terminal_punctuation(chunk))
    tail = " ".join(remainder).strip(" ,;:-")
    if tail:
        if terminal and (not re.search(r"[.!?]$", tail)):
            tail = f"{tail}{terminal}"
        out.append(_ensure_terminal_punctuation(tail))
    return out


def normalize_transitions(sentences: list[str], transition_repeat_max: int) -> list[str]:
    limit = max(0, int(transition_repeat_max))
    out: list[str] = []
    streak = 0
    for sent in sentences or []:
        current = re.sub(r"\s+", " ", str(sent or "")).strip()
        if not current:
            continue
        match = _TRANSITION_START_RE.match(current)
        if match:
            streak += 1
            if streak > limit:
                remainder = current[match.end() :].strip()
                if not remainder:
                    continue
                if remainder[0].islower():
                    remainder = remainder[0].upper() + remainder[1:]
                current = remainder
        else:
            streak = 0
        out.append(_ensure_terminal_punctuation(current))
    return out


def limit_repeated_sentence_starters(sentences: list[str], max_repeat: int) -> list[str]:
    limit = max(1, int(max_repeat))
    counts: Counter[str] = Counter()
    out: list[str] = []
    for sent in sentences or []:
        current = re.sub(r"\s+", " ", str(sent or "")).strip()
        if not current:
            continue
        starter = _starter_key(current)
        if starter:
            counts[starter] += 1
            if counts[starter] > limit:
                current = _rewrite_starter(current)
        out.append(_ensure_terminal_punctuation(current))
    return out


def _optimize_text_block(text: str, settings: Any, allow_paragraph_split: bool) -> list[str]:
    normalized = re.sub(r"\s+", " ", unescape(str(text or "")).strip())
    if not normalized:
        return []
    max_sentence_words = int(_get_setting_value(settings, "max_sentence_words", 25))
    paragraph_sentence_min = int(_get_setting_value(settings, "paragraph_sentence_min", 2))
    paragraph_sentence_max = int(_get_setting_value(settings, "paragraph_sentence_max", 5))
    repeated_sentence_starter_max = int(_get_setting_value(settings, "repeated_sentence_starter_max", 3))
    transition_repeat_max = int(_get_setting_value(settings, "transition_repeat_max", 1))
    paragraph_sentence_min = max(1, paragraph_sentence_min)
    paragraph_sentence_max = max(paragraph_sentence_min, paragraph_sentence_max)

    split = split_sentences(normalized)
    expanded: list[str] = []
    for sentence in split:
        expanded.extend(enforce_max_sentence_words(sentence, max_sentence_words))
    expanded = normalize_transitions(expanded, transition_repeat_max)
    expanded = limit_repeated_sentence_starters(expanded, repeated_sentence_starter_max)
    if not allow_paragraph_split:
        return [" ".join(expanded).strip()] if expanded else []
    chunks = _chunk_sentences(expanded, paragraph_sentence_min, paragraph_sentence_max)
    return [" ".join(chunk).strip() for chunk in chunks if chunk]


def optimize_html_readability(html: str, settings: dict | Any) -> str:
    source = str(html or "")
    if not source.strip():
        return source

    paragraph_pattern = re.compile(r"(?is)<p(?P<attrs>[^>]*)>(?P<inner>.*?)</p>")
    list_item_pattern = re.compile(r"(?is)<li(?P<attrs>[^>]*)>(?P<inner>.*?)</li>")

    def replace_paragraph(match: re.Match[str]) -> str:
        attrs = str(match.group("attrs") or "")
        inner = str(match.group("inner") or "")
        if re.search(r"(?is)<\s*(a|strong|em|code|img|iframe|video|table)\b", inner):
            return match.group(0)
        block_text = re.sub(r"(?is)<[^>]+>", " ", inner)
        paragraphs = _optimize_text_block(block_text, settings, allow_paragraph_split=True)
        if not paragraphs:
            return match.group(0)
            
        def highlight_data(txt: str) -> str:
            # Matches $1.5B, 45%, etc to highlight key data points for scanning readers
            return re.sub(
                r"(?<![\w])(\$\d+(?:\.\d+)?[A-Z]*|\d+(?:\.\d+)?%)(?!\w)", 
                r'<mark style="background-color:#fff3cd; padding:0 3px; border-radius:3px; font-weight:600;">\1</mark>', 
                txt
            )
            
        # Join paragraphs with an extra <br> for breathing room
        return "\n<br>\n".join([f"<p{attrs}>{highlight_data(escape(p, quote=False))}</p>" for p in paragraphs])

    optimized = paragraph_pattern.sub(replace_paragraph, source)

    def replace_list_item(match: re.Match[str]) -> str:
        attrs = str(match.group("attrs") or "")
        inner = str(match.group("inner") or "")
        if re.search(r"(?is)<\s*(a|strong|em|code|img|iframe|video|table)\b", inner):
            return match.group(0)
        block_text = re.sub(r"(?is)<[^>]+>", " ", inner)
        optimized_parts = _optimize_text_block(block_text, settings, allow_paragraph_split=False)
        if not optimized_parts:
            return match.group(0)
        return f"<li{attrs}>{escape(optimized_parts[0], quote=False)}</li>"

    optimized = list_item_pattern.sub(replace_list_item, optimized)
    
    # Anti-AI Cliché Filter
    optimized = re.sub(r"(?i)\b(it is worth noting that|it's important to note that|we delve into|a tapestry of)\b\s*", "", optimized)
    
    # 3. Dynamic Summary Box Injection (Above the fold)
    first_p_match = re.search(r"(?is)<p[^>]*>(.*?)</p>", optimized)
    if first_p_match:
        first_p_text = re.sub(r"(?is)<[^>]+>", "", first_p_match.group(1)).strip()
        if len(first_p_text) > 40:
            summary_box = f"""
<div style="background-color: #f8f9fa; border-left: 4px solid #007bff; padding: 15px 20px; margin-bottom: 25px; border-radius: 0 4px 4px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
    <strong style="display:block; margin-bottom:8px; color:#333; font-size:1.1em;">Quick Brief</strong>
    <p style="margin:0; color:#555; line-height:1.6;">{first_p_text}</p>
</div>
"""
            optimized = summary_box + optimized

    return optimized

