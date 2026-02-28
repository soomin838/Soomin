from __future__ import annotations

import re
from dataclasses import dataclass, field
from html import unescape


@dataclass
class NewsActionabilityGateResult:
    ok: bool
    score: int
    reasons: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)


class NewsActionabilityGate:
    def evaluate(self, *, title: str, html: str) -> NewsActionabilityGateResult:
        src = str(html or "")
        reasons: list[str] = []
        score = 100

        if re.search(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", f"{title}\n{html}"):
            reasons.append("hangul_detected")
            score = 0

        has_todo_h2 = bool(
            re.search(
                r"<h2[^>]*>\s*what\s+to\s+do\s+now\s*</h2>",
                src,
                flags=re.IGNORECASE,
            )
        )
        if not has_todo_h2:
            reasons.append("missing_what_to_do_now_h2")
            score -= 35

        todo_li_count = 0
        m_todo = re.search(
            r"<h2[^>]*>\s*what\s+to\s+do\s+now\s*</h2>(.*?)(?:<h2\b|$)",
            src,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if m_todo:
            todo_li_count = len(
                re.findall(
                    r"<li\b[^>]*>.*?</li>",
                    str(m_todo.group(1) or ""),
                    flags=re.IGNORECASE | re.DOTALL,
                )
            )
        if todo_li_count < 3:
            reasons.append("todo_steps_lt_3")
            score -= 30

        has_sources_h2 = bool(
            re.search(r"<h2[^>]*>\s*sources\s*</h2>", src, flags=re.IGNORECASE)
        )
        if not has_sources_h2:
            reasons.append("missing_sources_h2")
            score -= 25

        external_links = re.findall(r'href=["\'](https?://[^"\']+)["\']', src, flags=re.IGNORECASE)
        ext_count = 0
        for link in external_links:
            host = re.sub(r"^https?://", "", str(link or "").strip().lower()).split("/")[0]
            if not host:
                continue
            if host.startswith("localhost"):
                continue
            ext_count += 1
        if ext_count < 1:
            reasons.append("external_links_missing")
            score -= 25

        if re.search(r"<h[23][^>]*>\s*faq\s*</h[23]>", src, flags=re.IGNORECASE):
            reasons.append("faq_detected")
            score -= 40

        score = max(0, min(100, int(score)))
        ok = (not reasons) and score >= 70
        details = {
            "title": re.sub(r"\s+", " ", str(unescape(title or ""))).strip()[:120],
            "todo_li_count": int(todo_li_count),
            "external_links": int(ext_count),
            "has_what_to_do_now_h2": bool(has_todo_h2),
            "has_sources_h2": bool(has_sources_h2),
        }
        return NewsActionabilityGateResult(ok=ok, score=score, reasons=reasons, details=details)
