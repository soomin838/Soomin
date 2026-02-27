from __future__ import annotations

import re
from dataclasses import dataclass, field
from html import unescape


@dataclass
class ActionabilityGateResult:
    ok: bool
    score: int
    reasons: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)


class ActionabilityGate:
    _IMPERATIVE_VERBS = (
        "check",
        "open",
        "turn",
        "restart",
        "update",
        "disable",
        "enable",
        "reset",
        "reinstall",
        "forget",
        "pair",
        "clear",
        "run",
        "remove",
        "add",
        "switch",
        "try",
        "verify",
        "tap",
        "click",
        "select",
        "go",
    )
    _GENERIC_TOKENS = (
        "maybe",
        "might",
        "generally",
        "often",
        "typically",
        "in conclusion",
        "overall",
        "comprehensive",
        "delve",
    )

    def evaluate(
        self,
        title: str,
        html: str,
        *,
        min_steps: int,
        min_word_count: int,
        max_generic_ratio: float,
    ) -> ActionabilityGateResult:
        raw_html = str(html or "")
        raw_title = str(title or "")
        merged = f"{raw_title}\n{raw_html}"
        text = self._to_text(raw_html)
        lower_text = text.lower()
        lower_merged = merged.lower()

        reasons: list[str] = []
        score = 100

        hangul = bool(re.search(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", merged))
        if hangul:
            reasons.append("hangul_detected")
            score = 0

        h2_headings = [self._to_text(h).strip() for h in re.findall(r"<h2[^>]*>(.*?)</h2>", raw_html, flags=re.IGNORECASE | re.DOTALL)]
        h2_lower = [h.lower() for h in h2_headings if h]
        has_fix_1 = any(re.search(r"\bfix\s*1\b", h) for h in h2_lower)
        has_fix_2 = any(re.search(r"\bfix\s*2\b", h) for h in h2_lower)
        has_fix_3 = any(re.search(r"\bfix\s*3\b", h) for h in h2_lower)
        fix_or_step_h2_count = sum(1 for h in h2_lower if ("fix" in h or "step" in h))
        has_fix_sections = (has_fix_1 and has_fix_2 and has_fix_3) or (fix_or_step_h2_count >= 4)
        if not has_fix_sections:
            reasons.append("missing_fix_sections")
            score -= 22

        li_items = [self._to_text(li).strip() for li in re.findall(r"<li[^>]*>(.*?)</li>", raw_html, flags=re.IGNORECASE | re.DOTALL)]
        imperative_pattern = re.compile(
            r"^(?:step\s*\d+[:.)-]?\s*)?(?:"
            + "|".join(re.escape(v) for v in self._IMPERATIVE_VERBS)
            + r")\b",
            flags=re.IGNORECASE,
        )
        imperative_li = sum(1 for item in li_items if imperative_pattern.search(item))
        explicit_step_markers = len(re.findall(r"\bstep\s*\d+\s*[:.)-]?", lower_merged, flags=re.IGNORECASE))
        actionable_steps = imperative_li + explicit_step_markers
        if actionable_steps < max(1, int(min_steps)):
            reasons.append("too_few_steps")
            score -= 24

        expected_result_hits = len(
            re.findall(
                r"\b(expected(?:\s+result)?|you should see|if it works|result:)\b",
                lower_merged,
                flags=re.IGNORECASE,
            )
        )
        if expected_result_hits < 3:
            reasons.append("missing_expected_results")
            score -= 14

        branching_hits = len(
            re.findall(
                r"\b(if not|if that doesn[’']t|otherwise|next try)\b",
                lower_merged,
                flags=re.IGNORECASE,
            )
        )
        if branching_hits < 2:
            reasons.append("missing_branching")
            score -= 12

        words = re.findall(r"[A-Za-z0-9']+", text)
        word_count = len(words)
        if word_count < max(1, int(min_word_count)):
            reasons.append("too_short")
            score -= 12

        generic_hits = 0
        word_counter = {}
        for token in words:
            key = token.lower()
            word_counter[key] = word_counter.get(key, 0) + 1
        for tok in self._GENERIC_TOKENS:
            if " " in tok:
                generic_hits += len(re.findall(re.escape(tok), lower_text))
            else:
                generic_hits += int(word_counter.get(tok, 0))
        generic_ratio = float(generic_hits) / float(max(1, word_count))
        if generic_ratio > float(max_generic_ratio):
            reasons.append("generic_fluff_high")
            score -= 12

        score = max(0, min(100, int(score)))
        ok = (not reasons) and (score >= 75)
        details = {
            "word_count": int(word_count),
            "h2_count": int(len(h2_headings)),
            "fix_or_step_h2_count": int(fix_or_step_h2_count),
            "has_fix_1": bool(has_fix_1),
            "has_fix_2": bool(has_fix_2),
            "has_fix_3": bool(has_fix_3),
            "li_count": int(len(li_items)),
            "imperative_li_count": int(imperative_li),
            "step_marker_count": int(explicit_step_markers),
            "actionable_steps": int(actionable_steps),
            "expected_result_hits": int(expected_result_hits),
            "branching_hits": int(branching_hits),
            "generic_hits": int(generic_hits),
            "generic_ratio": float(round(generic_ratio, 6)),
            "matched_h2": h2_headings[:12],
        }
        if hangul:
            details["hangul_detected"] = True
        return ActionabilityGateResult(ok=ok, score=score, reasons=reasons, details=details)

    def _to_text(self, html: str) -> str:
        cleaned = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", str(html or ""), flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        cleaned = unescape(cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()
