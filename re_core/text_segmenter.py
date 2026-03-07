from __future__ import annotations

import re
from typing import Any


def _plain_text(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(html or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def first_paragraph_text(html: str) -> str:
    m = re.search(r"<p[^>]*>(.*?)</p>", str(html or ""), flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return _plain_text(m.group(1))


def _extract_section_by_heading(html: str, heading_regex: str) -> str:
    src = str(html or "")
    pat = re.compile(
        rf"<h[23][^>]*>\s*{heading_regex}\s*</h[23]>(.*?)(?=<h[23]\b|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    m = pat.search(src)
    if not m:
        return ""
    return _plain_text(m.group(1))


def extract_image_planning_sections(html: str) -> dict[str, str]:
    src = str(html or "")
    quick = _extract_section_by_heading(src, r"(quick take|quick answer)")
    fix2 = _extract_section_by_heading(src, r"(fix\s*2|step\s*2)")
    adv = _extract_section_by_heading(src, r"(advanced fix|what i learned|final take)")

    paragraphs = [
        _plain_text(p)
        for p in re.findall(r"<p[^>]*>(.*?)</p>", src, flags=re.IGNORECASE | re.DOTALL)
    ]
    paragraphs = [p for p in paragraphs if p and len(p) >= 40]

    if not quick and paragraphs:
        quick = paragraphs[0]
    if not fix2 and len(paragraphs) >= 2:
        fix2 = paragraphs[1]
    if not adv and len(paragraphs) >= 3:
        adv = paragraphs[min(2, len(paragraphs) - 1)]

    return {
        "quick_answer": re.sub(r"\s+", " ", quick).strip()[:500],
        "fix2": re.sub(r"\s+", " ", fix2).strip()[:500],
        "advanced_fix": re.sub(r"\s+", " ", adv).strip()[:500],
    }


def section_bundle_for_llm(html: str) -> dict[str, Any]:
    sections = extract_image_planning_sections(html)
    compact = {k: v for k, v in sections.items() if str(v or "").strip()}
    return compact if compact else {"quick_answer": "Practical troubleshooting workflow summary."}

