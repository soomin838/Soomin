"""HTML normalization utilities extracted from Publisher.

These are stateless text-processing helpers with no dependency on
the Blogger API, OAuth, or any external service.
"""
from __future__ import annotations

import html as html_lib
import re


class HtmlNormalizer:
    """Pure HTML/text normalisation — no network, no state."""

    # ── Entity Decoding ──────────────────────────────────────
    @staticmethod
    def normalize_text_entities(text: str) -> str:
        out = str(text or "")
        for _ in range(2):
            dec = html_lib.unescape(out)
            if dec == out:
                break
            out = dec
        return out

    @staticmethod
    def normalize_html_entities(html: str) -> str:
        out = str(html or "")
        for _ in range(2):
            dec = html_lib.unescape(out)
            if dec == out:
                break
            out = dec
        return out

    # ── Meta Description ─────────────────────────────────────
    @classmethod
    def normalize_meta_description(cls, description: str | None) -> str:
        raw = str(description or "").strip()
        if not raw:
            return ""
        out = cls.normalize_text_entities(raw)
        out = re.sub(r"\s+", " ", out).strip()
        if len(out) > 160:
            out = out[:157].rstrip(" ,.;:") + "..."
        return out

    # ── Language Detection ───────────────────────────────────
    @staticmethod
    def contains_hangul(text: str) -> bool:
        return bool(re.search(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", str(text or "")))

    @classmethod
    def assert_english_only_payload(
        cls,
        title: str,
        html: str,
        labels: list[str],
        meta_description: str,
    ) -> None:
        chunks = [
            ("title", str(title or "")),
            ("html", str(html or "")),
            ("labels", " ".join(str(x or "") for x in (labels or []))),
            ("meta_description", str(meta_description or "")),
        ]
        for key, value in chunks:
            if not value:
                continue
            if cls.contains_hangul(value):
                raise RuntimeError(f"english_only_gate_failed:{key}:hangul_detected")

    # ── HTML Cleanup ─────────────────────────────────────────
    @staticmethod
    def clean_html_tags(html: str) -> str:
        out = str(html or "")
        out = re.sub(r"<script\\b[^>]*>.*?</script>", "", out, flags=re.IGNORECASE | re.DOTALL)
        out = re.sub(r"<style\\b[^>]*>.*?</style>", "", out, flags=re.IGNORECASE | re.DOTALL)
        out = re.sub(r"\sstyle=\"[^\"]*\"", "", out, flags=re.IGNORECASE)
        out = re.sub(r"\sstyle='[^']*'", "", out, flags=re.IGNORECASE)
        out = re.sub(r"\son[a-z]+\s*=\s*\"[^\"]*\"", "", out, flags=re.IGNORECASE)
        out = re.sub(r"\son[a-z]+\s*=\s*'[^']*'", "", out, flags=re.IGNORECASE)
        out = re.sub(
            r"(section context visual|concept visual|supporting chart|focused screenshot)",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"<p[^>]*>\s*illustration\s+showing[^<]*</p>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"https?://(?:www\.)?google\.com/[^\s\"<]*",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"(?m)^\s*#{1,6}\s+(.+)$",
            "",
            out,
        )
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()
