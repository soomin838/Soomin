from __future__ import annotations

import re
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.readability import optimize_html_readability, split_sentences, strip_tags_keep_h2  # noqa: E402


READABILITY_SETTINGS = {
    "enabled": True,
    "max_sentence_words": 25,
    "paragraph_sentence_min": 2,
    "paragraph_sentence_max": 5,
    "repeated_sentence_starter_max": 3,
    "transition_repeat_max": 1,
}


def _word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9']+", str(text or "")))


def _starter_key(sentence: str) -> str:
    tokens = re.findall(r"[a-z0-9']+", str(sentence or "").lower())
    if len(tokens) >= 3:
        return " ".join(tokens[:3])
    if len(tokens) >= 2:
        return " ".join(tokens[:2])
    return tokens[0] if tokens else ""


def _paragraph_texts(html: str) -> list[str]:
    return [m.group(1).strip() for m in re.finditer(r"(?is)<p[^>]*>(.*?)</p>", str(html or ""))]


def validate_long_sentence_split() -> int:
    long_sentence = (
        "This update explains why teams that manage desktop fleets across regions need a clear patch plan, "
        "consistent communication, and explicit rollback criteria before touching endpoint policy baselines, "
        "because unmanaged drift can create avoidable outages for remote workers and support teams."
    )
    html = f"<h2>Overview</h2><p>{long_sentence}</p>"
    optimized = optimize_html_readability(html, READABILITY_SETTINGS)
    all_text = strip_tags_keep_h2(optimized)
    sentences = split_sentences(all_text)
    if len(sentences) < 2:
        raise AssertionError("Case1 failed: long sentence was not split into multiple sentences.")
    for sent in sentences:
        if _word_count(sent) > 25:
            raise AssertionError(f"Case1 failed: sentence exceeds 25 words: {sent}")
    return len(sentences)


def validate_paragraph_split() -> int:
    seven = " ".join([f"Sentence {i} stays short and clear." for i in range(1, 8)])
    html = f"<h2>Timeline</h2><p>{seven}</p>"
    optimized = optimize_html_readability(html, READABILITY_SETTINGS)
    paragraphs = _paragraph_texts(optimized)
    if len(paragraphs) < 2:
        raise AssertionError("Case2 failed: 7-sentence paragraph was not split into multiple paragraphs.")
    total_sentences = 0
    for para in paragraphs:
        count = len(split_sentences(para))
        total_sentences += count
        if count < 2 or count > 5:
            raise AssertionError(f"Case2 failed: paragraph sentence count out of range 2~5 (got {count}).")
    if total_sentences < 7:
        raise AssertionError("Case2 failed: sentence content was dropped during paragraph split.")
    return len(paragraphs)


def validate_transition_normalization() -> list[str]:
    html = (
        "<h2>Impact</h2>"
        "<p>However, teams should validate admin roles first. "
        "However, teams also need to recheck automation scope. "
        "Therefore, rollout notes should be visible.</p>"
    )
    optimized = optimize_html_readability(html, READABILITY_SETTINGS)
    paragraphs = _paragraph_texts(optimized)
    joined = " ".join(paragraphs)
    sentences = split_sentences(joined)
    prev_transition = False
    for sent in sentences:
        current_transition = bool(re.match(r"^\s*however\b", sent, flags=re.IGNORECASE))
        if prev_transition and current_transition:
            raise AssertionError("Case3 failed: consecutive transition starters remain repeated.")
        prev_transition = current_transition
    return sentences


def validate_repeated_starter_limit() -> dict[str, int]:
    repeated = (
        "This update explains baseline scope. "
        "This update explains deployment order. "
        "This update explains rollback timing. "
        "This update explains user communication. "
        "This update explains exception handling."
    )
    html = f"<h2>What Changed</h2><p>{repeated}</p>"
    optimized = optimize_html_readability(html, READABILITY_SETTINGS)
    paragraphs = _paragraph_texts(optimized)
    sentences = split_sentences(" ".join(paragraphs))
    starter_counts: dict[str, int] = {}
    for sent in sentences:
        key = _starter_key(sent)
        if not key:
            continue
        starter_counts[key] = int(starter_counts.get(key, 0)) + 1
    if starter_counts and max(starter_counts.values()) > 3:
        raise AssertionError(f"Case4 failed: repeated sentence starter still exceeds 3 ({starter_counts}).")
    return starter_counts


def validate_html_integrity() -> str:
    html = "<h2>Check</h2><p>This is a clear sentence. This is another sentence.</p>"
    optimized = optimize_html_readability(html, READABILITY_SETTINGS)
    if not re.search(r"(?is)<h2[^>]*>.*?</h2>", optimized):
        raise AssertionError("Case5 failed: <h2> block is missing after optimization.")
    if not re.search(r"(?is)<p[^>]*>.*?</p>", optimized):
        raise AssertionError("Case5 failed: <p> block is missing after optimization.")
    return optimized


def main() -> int:
    s1 = validate_long_sentence_split()
    p2 = validate_paragraph_split()
    s3 = validate_transition_normalization()
    s4 = validate_repeated_starter_limit()
    _ = validate_html_integrity()
    print("Case 1 OK: long sentence split to <=25 words")
    print(f"  sentence_count={s1}")
    print("Case 2 OK: 7-sentence paragraph split into 2~5 sentence paragraphs")
    print(f"  paragraph_count={p2}")
    print("Case 3 OK: consecutive transition starters normalized")
    print(f"  sample_sentences={s3[:3]}")
    print("Case 4 OK: repeated sentence starters limited")
    print(f"  starter_counts={s4}")
    print("Case 5 OK: HTML integrity keeps <h2> and <p>")
    print("Stage-6 readability validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

