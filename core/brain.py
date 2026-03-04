from __future__ import annotations

import json
import hashlib
import random
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from urllib.parse import urlparse

import requests

from .news_facets import (
    FACET_POOL,
    ensure_what_to_do_now_section,
    facet_emphasis_hint,
    normalize_category,
    normalize_facet,
    reorder_optional_sections_for_facet,
    resolve_facet_context,
)
from .scout import TopicCandidate
from .settings import GeminiSettings


@dataclass
class DraftPost:
    title: str
    alt_titles: list[str]
    html: str
    summary: str
    score: int
    source_url: str
    extracted_urls: list[str]


NEWS_SECTION_TITLES = {
    "quick_take": "Quick Take",
    "what_happened": "What Happened",
    "why_it_matters": "Why It Matters (for normal users)",
    "what_to_do_now": "What To Do Now",
    "key_details": "Key Details",
    "what_to_watch_next": "What To Watch Next",
    "background_context": "Background Context",
    "risks": "Risks",
    "timeline": "Timeline",
    "sources": "Sources",
}

NEWS_OPTIONAL_SECTION_IDS = (
    "why_it_matters",
    "key_details",
    "what_to_watch_next",
    "background_context",
    "risks",
    "timeline",
)

NEWS_FALLBACK_SECTION_IDS = (
    "quick_take",
    "what_happened",
    "why_it_matters",
    "what_to_do_now",
    "sources",
)

NEWS_INTRO_TEMPLATES = (
    "Data-led opening: lead with one concrete figure, then explain what changed.",
    "Quote-led opening: start with a short attributed quote, then provide direct context.",
    "Question-led opening: ask one sharp question and answer it immediately.",
    "Context-led opening: describe the current market or platform backdrop before the event details.",
    "Impact-led opening: open with immediate user/business impact before chronology.",
)

NEWS_CONCLUSION_TEMPLATES = (
    "Forward-looking close: summarize what to watch in the next 1-2 weeks.",
    "Decision-oriented close: restate practical implications for readers right now.",
    "Risk-aware close: highlight the main uncertainty and the safest next check.",
    "Timeline close: end with the most likely near-term sequence of updates.",
)


def stable_hash(value: str) -> int:
    digest = hashlib.sha256(str(value or "").encode("utf-8", errors="ignore")).hexdigest()
    return int(digest, 16)


def _fallback_structure_plan() -> dict[str, object]:
    section_ids = list(NEWS_FALLBACK_SECTION_IDS)
    return {
        "seed": stable_hash("fallback_structure"),
        "requested_section_count": 5,
        "intro_index": 0,
        "conclusion_index": 0,
        "section_ids": section_ids,
        "section_count": len(section_ids),
    }


def build_structure(seed, event_data) -> dict[str, object]:
    event_payload = dict(event_data or {}) if isinstance(event_data, dict) else {}
    selected_facet = normalize_facet(str(event_payload.get("facet", "") or ""))
    try:
        seed_value = int(seed)
    except Exception:
        return _fallback_structure_plan()

    rng = random.Random(seed_value)
    requested_count = rng.randint(3, 6)
    target_h2_count = max(5, requested_count)
    optional_needed = max(0, target_h2_count - 4)
    optional = reorder_optional_sections_for_facet(
        optional_sections=list(NEWS_OPTIONAL_SECTION_IDS),
        facet=selected_facet,
        seed=seed_value,
    )
    picked_optional = optional[:optional_needed]
    raw_ids = ["quick_take", "what_happened", *picked_optional, "what_to_do_now", "sources"]

    section_ids: list[str] = []
    seen: set[str] = set()
    for sid in raw_ids:
        key = str(sid or "").strip().lower()
        if (not key) or key in seen or key not in NEWS_SECTION_TITLES:
            continue
        seen.add(key)
        section_ids.append(key)

    if len(section_ids) < 5:
        return _fallback_structure_plan()

    return {
        "seed": seed_value,
        "requested_section_count": requested_count,
        "intro_index": rng.randint(0, len(NEWS_INTRO_TEMPLATES) - 1),
        "conclusion_index": rng.randint(0, len(NEWS_CONCLUSION_TEMPLATES) - 1),
        "section_ids": section_ids,
        "section_count": len(section_ids),
    }


def render_from_plan(structure_plan, event_data) -> dict[str, object]:
    _ = event_data
    plan = dict(structure_plan or {}) if isinstance(structure_plan, dict) else _fallback_structure_plan()
    raw_ids = plan.get("section_ids", [])
    section_ids: list[str] = []
    seen: set[str] = set()
    if isinstance(raw_ids, list):
        for sid in raw_ids:
            key = str(sid or "").strip().lower()
            if (not key) or key in seen or key not in NEWS_SECTION_TITLES:
                continue
            seen.add(key)
            section_ids.append(key)
    if "what_to_do_now" not in section_ids:
        if "sources" in section_ids:
            insert_at = max(0, section_ids.index("sources"))
            section_ids.insert(insert_at, "what_to_do_now")
        else:
            section_ids.append("what_to_do_now")
    if len(section_ids) < 5:
        fb = _fallback_structure_plan()
        section_ids = list(fb.get("section_ids", []))
        plan = fb

    section_count = len(section_ids)
    requested_count = int(plan.get("requested_section_count", section_count) or section_count)
    requested_count = max(3, min(6, requested_count))
    intro_index = int(plan.get("intro_index", 0) or 0)
    conclusion_index = int(plan.get("conclusion_index", 0) or 0)
    intro_index = max(0, min(intro_index, len(NEWS_INTRO_TEMPLATES) - 1))
    conclusion_index = max(0, min(conclusion_index, len(NEWS_CONCLUSION_TEMPLATES) - 1))

    try:
        seed_value = int(plan.get("seed", 0) or 0)
    except Exception:
        seed_value = stable_hash("|".join(section_ids))
    rng = random.Random(seed_value)
    length_labels = ["short", "medium", "long"]
    paragraph_lengths: list[str] = []
    for sid in section_ids:
        if sid == "sources":
            paragraph_lengths.append("short")
            continue
        paragraph_lengths.append(rng.choice(length_labels))

    section_titles = [NEWS_SECTION_TITLES.get(sid, sid.replace("_", " ").title()) for sid in section_ids]
    return {
        "seed": seed_value,
        "requested_section_count": requested_count,
        "intro_index": intro_index,
        "conclusion_index": conclusion_index,
        "section_ids": section_ids,
        "section_titles": section_titles,
        "section_count": section_count,
        "intro_template": NEWS_INTRO_TEMPLATES[intro_index],
        "conclusion_template": NEWS_CONCLUSION_TEMPLATES[conclusion_index],
        "paragraph_lengths": paragraph_lengths,
    }


class GeminiBrain:
    def __init__(self, settings: GeminiSettings) -> None:
        self.settings = settings
        self._endpoint = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{settings.model}:generateContent"
        )
        self.call_count = 0
        self._models_cache: tuple[datetime, list[str]] | None = None
        self._model_quota_blocked_until: dict[str, datetime] = {}
        self._last_request_at: datetime | None = None
        self._news_module_rotation_path = (
            Path(__file__).resolve().parent.parent / "storage" / "state" / "news_module_rotation.json"
        )
        self._news_module_log_path = (
            Path(__file__).resolve().parent.parent / "storage" / "logs" / "news_module_rotation.jsonl"
        )
        self._facet_rotation_state_path = (
            Path(__file__).resolve().parent.parent / "storage" / "state" / "facet_rotation_state.json"
        )

    def choose_best(
        self,
        candidates: list[TopicCandidate],
        recent_urls: list[str] | None = None,
        recent_titles: list[str] | None = None,
        target_keywords: list[str] | None = None,
    ) -> tuple[TopicCandidate | None, int, str]:
        if not candidates:
            return None, 0, "no candidates"

        top = candidates[:5]
        # Free-tier safe path: perform a single ranking call rather than per-item scoring.
        prompt_items = "\n\n".join(
            f"[{idx}] source={item.source} score_hint={item.score}\n"
            f"title={item.title}\n"
            f"body={item.body[:800]}\n"
            f"url={item.url}"
            for idx, item in enumerate(top)
        )
        prompt = (
            "Pick the single best topic for a deep tech news analysis and interpretation blog post. "
            "Output language must be American English only. Never output Korean.\n"
            "Return strict JSON: {\"index\": int, \"score\": int, \"reason\": string}.\n"
            "Score must be 0-100.\n"
            "Maximize potential CTR and editorial depth while staying accurate.\n"
            "Prioritize topics that involve significant industry shifts, policy changes, or technological advancements.\n"
            "Prefer analytical lens: Why this matters, who is affected, and what happens next.\n"
            "Prefer topics related to: AI, Platform updates, Security, or Market trends.\n"
            "Reject simple troubleshooting guides or generic 'how-to' content.\n"
            "Prefer topics aligned with dynamic target keywords when relevant.\n"
            "Avoid topics that duplicate recent history URLs/titles unless no alternative exists.\n"
            f"Dynamic target keywords: {(target_keywords or [])[:8]}\n"
            f"Recent URLs: {(recent_urls or [])[:12]}\n"
            f"Recent titles: {(recent_titles or [])[:12]}\n"
            f"Candidates:\n{prompt_items}"
        )
        parsed = self._extract_json(
            self._generate_text(
                prompt,
                system_instruction=self.settings.editor_persona,
            )
        )

        chosen_idx = int(parsed.get("index", 0))
        chosen_idx = 0 if chosen_idx < 0 or chosen_idx >= len(top) else chosen_idx
        score = int(parsed.get("score", 0))
        reason = str(parsed.get("reason", ""))
        return top[chosen_idx], score, reason

    def choose_best_free(
        self,
        candidates: list[TopicCandidate],
        recent_urls: list[str] | None = None,
        recent_titles: list[str] | None = None,
        target_keywords: list[str] | None = None,
    ) -> tuple[TopicCandidate | None, int, str]:
        if not candidates:
            return None, 0, "no candidates"
        recent_url_set = {u.strip().lower() for u in (recent_urls or []) if u}
        recent_title_set = {t.strip().lower() for t in (recent_titles or []) if t}
        pool = [
            c for c in candidates
            if c.url.strip().lower() not in recent_url_set
            and c.title.strip().lower() not in recent_title_set
        ]
        if not pool:
            pool = candidates[:]
        scored: list[tuple[float, TopicCandidate]] = []
        for c in pool:
            kw_boost = self._keyword_match_score(c.title, c.body, target_keywords)
            audience_boost = self._audience_accessibility_score(c.title, c.body)
            # Keep deterministic score backbone, add keyword relevance boost.
            scored.append((float(c.score) + kw_boost + audience_boost, c))
        ranked = [item for _, item in sorted(scored, key=lambda x: x[0], reverse=True)]
        top_band = ranked[: min(5, len(ranked))]
        best = random.choice(top_band) if top_band else ranked[0]
        score = max(70, min(100, int(best.score)))
        return best, score, "free-mode diversity ranking with dynamic keyword boost"

    def extract_global_keywords(
        self,
        candidates: list[TopicCandidate],
        limit: int = 5,
        avoid_keywords: list[str] | None = None,
    ) -> list[str]:
        if not candidates:
            return []
        top = candidates[: min(20, len(candidates))]
        avoid = [
            re.sub(r"\s+", " ", str(k or "")).strip().lower()
            for k in (avoid_keywords or [])
            if str(k or "").strip()
        ]
        lines: list[str] = []
        for c in top:
            body_compact = re.sub(r"\s+", " ", (c.body or ""))[:350]
            lines.append(
                f"- source={c.source} score={c.score}\n"
                f"  title={c.title}\n"
                f"  body={body_compact}"
            )
        packed = "\n\n".join(lines)
        prompt = (
            "From the candidate signals below, generate exactly 5 high-potential tech news analysis keywords "
            "for today's US editorial audience. Focus on industry impact and future trends. "
            "Language policy: American English only. Never output Korean.\n"
            "Keyword intent must be analytical: industry impact, market shift, regulatory update, future outlook, strategic move.\n"
            "Estimate keywords likely to have strong CTR and advertiser value.\n"
            "Return strict JSON only: {\"keywords\": [\"...\", \"...\", \"...\", \"...\", \"...\"]}\n"
            "Keywords must be short, natural search phrases (2-5 words), no hashtags, no duplicates.\n"
            f"Avoid reusing these recent keywords if possible: {avoid[:8]}\n"
            f"Candidates:\n{packed}"
        )
        try:
            payload = self._extract_json(
                self._generate_text(
                    prompt,
                    system_instruction=self.settings.editor_persona,
                    temperature=random.uniform(0.90, 1.00),
                    top_p=0.97,
                )
            )
            raw = payload.get("keywords", [])
            if isinstance(raw, list):
                out = []
                seen = set()
                for item in raw:
                    kw = re.sub(r"\s+", " ", str(item or "")).strip()
                    if not kw:
                        continue
                    key = kw.lower()
                    if key in seen:
                        continue
                    if key in avoid:
                        continue
                    seen.add(key)
                    out.append(kw)
                    if len(out) >= max(1, int(limit)):
                        break
                if out:
                    return out
        except Exception:
            pass
        return self.extract_global_keywords_free(candidates, limit=limit, avoid_keywords=avoid_keywords)

    def extract_global_keywords_free(
        self,
        candidates: list[TopicCandidate],
        limit: int = 5,
        avoid_keywords: list[str] | None = None,
    ) -> list[str]:
        avoid = {
            re.sub(r"\s+", " ", str(k or "")).strip().lower()
            for k in (avoid_keywords or [])
            if str(k or "").strip()
        }
        title_text = " ".join((c.title or "") for c in candidates[: min(40, len(candidates))]).lower()
        tokens = re.findall(r"[a-z][a-z0-9-]{2,}", title_text)
        stop = {
            "the", "and", "for", "with", "this", "that", "from", "your", "have", "has",
            "about", "what", "when", "where", "which", "while", "there", "their", "then",
            "into", "over", "under", "just", "very", "more", "less", "some", "many",
            "using", "used", "use", "guide", "issue", "problem", "feature", "release",
            "software", "tools", "tool", "news", "post", "blog", "you", "your", "they",
            "them", "hacker", "discussion", "topic", "show", "showing", "test", "tests",
            "real", "best", "today", "latest", "free", "open", "source",
        }
        filtered = [t for t in tokens if t not in stop]
        if not filtered:
            return []
        # Build bigrams from title token order for more natural search phrases.
        bigrams: list[str] = []
        for i in range(len(filtered) - 1):
            a, b = filtered[i], filtered[i + 1]
            if len(a) < 3 or len(b) < 3:
                continue
            if a == b:
                continue
            bigrams.append(f"{a} {b}")
        phrase_freq = Counter(bigrams)
        ranked_phrases = [p for p, _ in phrase_freq.most_common(120)]
        # Filter awkward fragments.
        ranked_phrases = [p for p in ranked_phrases if not re.search(r"\b(and|for|with|from|about)\b", p)]
        out: list[str] = []
        seen = set()
        for p in ranked_phrases:
            key = p.strip().lower()
            if not key or key in seen:
                continue
            if key in avoid:
                continue
            seen.add(key)
            out.append(p.strip())
            if len(out) >= max(1, int(limit)):
                break
        # Unigram fallback when title set is tiny.
        if len(out) < max(1, int(limit)):
            unigram_freq = Counter(filtered)
            for w, _ in unigram_freq.most_common(120):
                if len(w) < 4:
                    continue
                if w in seen:
                    continue
                if w in avoid:
                    continue
                out.append(w)
                seen.add(w)
                if len(out) >= max(1, int(limit)):
                    break
        return out[: max(1, int(limit))]

    def generate_post(
        self,
        candidate: TopicCandidate,
        authority_links: list[str],
        pattern_instruction: str,
        reference_guidance: str,
        domain: str = "news_interpretation",
        plan: dict | None = None,
    ) -> DraftPost:
        main_keyword = self._resolve_main_keyword(candidate)
        long_tail_keywords = [str(x).strip() for x in (getattr(candidate, "long_tail_keywords", []) or []) if str(x).strip()]
        lsi_terms = self._build_lsi_terms(
            main_keyword=main_keyword,
            long_tail_keywords=long_tail_keywords,
            title=candidate.title,
            body=candidate.body,
        )
        effective_system_instruction = (self.settings.editor_persona or "").strip()
        if str(domain or "").strip().lower() in {"office_experiment", "news_interpretation"}:
            effective_system_instruction = (
                effective_system_instruction
                + "\nAudience constraint: non-technical everyday users. "
                "Do not include DevOps/SRE/deployment/staging/prod/on-call/incident terminology."
            ).strip()
        elif str(domain or "").strip().lower() == "ai_prompt_guide":
            effective_system_instruction = (
                effective_system_instruction
                + "\nTutorial constraint: reader-facing prompt examples are allowed, "
                "but never expose internal hidden planning or system metadata."
            ).strip()
        plan_payload = plan if isinstance(plan, dict) else {}
        plan_keyword = re.sub(r"\s+", " ", str(plan_payload.get("primary_keyword", "") or "")).strip()
        effective_main_keyword = plan_keyword or main_keyword
        prompt = (
            "Write a 1300-1800 word news analysis and interpretation article in valid HTML body fragment only.\n"
            "Language policy: US English only. Never output Korean.\n"
            "Output valid HTML only. Do NOT output Markdown headings (#, ##, ###).\n"
            "Audience: US news readers who want insightful commentary on tech trends.\n"
            "Scope: Deep-dive tech news analysis and editorial interpretation.\n"
            "Do not output internal metadata, pipeline status strings, debug tokens, or schema fragments.\n"
            "Do not mention screenshots or 'see image above'.\n"
            "Avoid generic essay style; use a sharp, editorial journalistic voice.\n"
            "Use short, engaging paragraphs and insightful section headers.\n"
            "Required H2 section flow:\n"
            "The Front Line (Summary of the event)\n"
            "The Core Impact (Why this matters right now)\n"
            "Behind the Shift (Background/Analysis)\n"
            "Looking Ahead (Projections/Future trends)\n"
            "What This Means for You (Conclusion/Actionability)\n"
            "Sources\n"
            "Keep legal/ad safety: no defamation, no unverified claims.\n"
            f"Main keyword: {effective_main_keyword}\n"
            "Analysis questions to cover naturally (at least 3):\n"
            + "\n".join(f"- {kw}" for kw in long_tail_keywords[:6])
            + "\n"
            "Required related LSI terms (use naturally, no stuffing):\n"
            + "\n".join(f"- {term}" for term in lsi_terms[:10])
            + "\n"
            "Follow this selected writing pattern instruction:\n"
            f"{pattern_instruction}\n"
            "ANALYSIS PLAN JSON (must follow exactly):\n"
            f"{json.dumps(plan_payload, ensure_ascii=False) if plan_payload else '{}'}\n"
            "Include exactly 2 external authority links from this allow-list:\n"
            + "\n".join(authority_links[:8])
            + "\n"
            "Use these internal playbook reference excerpts:\n"
            f"{reference_guidance}\n"
            "Return strict JSON with keys only: title_draft, meta_description, content_html, summary, focus_keywords.\n"
            "focus_keywords must be a JSON array of short phrases.\n"
            "Title must be analytical and provocative, avoiding 'How to fix' templates.\n"
            f"Domain routing: {domain}\n"
            f"Source platform: {candidate.source}\n"
            f"Source title: {candidate.title}\n"
            f"Source body: {candidate.body[:4000]}\n"
            f"Source URL: {candidate.url}\n"
        )
        if str(domain or "").strip().lower() in {"office_experiment", "news_interpretation"}:
            prompt += (
                "Domain safety rule (tech troubleshooting): do not include DevOps/SRE/deployment/staging/prod/on-call/incident terminology. "
                "Use plain end-user troubleshooting language only.\n"
            )
        elif str(domain or "").strip().lower() == "ai_prompt_guide":
            prompt += (
                "Domain rule (ai_prompt_guide): prompt examples for readers are allowed and should be shown clearly.\n"
                "When showing prompt examples, present them in clean code blocks and explain usage in plain language.\n"
            )

        payload = self._extract_json(
            self._generate_text(
                prompt,
                system_instruction=effective_system_instruction,
            )
        )
        html = self._remove_ai_markers(str(payload.get("content_html", payload.get("html", ""))), domain=domain)
        html = self._enforce_html_minimum(html)
        _focus_keywords = payload.get("focus_keywords", [])
        if not isinstance(_focus_keywords, list):
            _focus_keywords = []
        urls = payload.get("extracted_urls", [])
        if not isinstance(urls, list):
            urls = []

        out_title = str(payload.get("title_draft", payload.get("title", candidate.title))).strip() or candidate.title
        if plan_keyword and plan_keyword.lower() not in out_title.lower():
            out_title = f"{plan_keyword}: {out_title}".strip(" :")

        return DraftPost(
            title=out_title,
            alt_titles=[],
            summary=str(payload.get("summary", "")).strip(),
            html=html,
            score=100,
            source_url=candidate.url,
            extracted_urls=[str(u) for u in urls if isinstance(u, str)][:8],
        )

    def _suggest_news_facets_with_llm(
        self,
        *,
        title: str,
        body: str,
        category: str,
        event_id: str,
        run_start_minute: str,
    ) -> list[str]:
        api_key = str(getattr(self.settings, "api_key", "") or "").strip()
        if (not api_key) or api_key == "GEMINI_API_KEY":
            return []
        prompt = (
            "Select perspective facets for a US tech news explainer.\n"
            f"Allowed facets only: {list(FACET_POOL)}\n"
            "Return strict JSON only: {\"facets\": [\"...\"]}\n"
            "Rules:\n"
            "- Keep only allowed facet tokens.\n"
            "- Order by editorial usefulness for this event.\n"
            "- Return 4-6 items.\n"
            f"Category: {normalize_category(category)}\n"
            f"Event ID: {event_id}\n"
            f"Run start minute: {run_start_minute}\n"
            f"Title: {title}\n"
            f"Body: {body[:1500]}\n"
        )
        try:
            parsed = self._extract_json(
                self._generate_text(
                    prompt,
                    system_instruction=(
                        "You are an editor assistant. Return compact JSON only and never include prose outside JSON."
                    ),
                )
            )
        except Exception:
            return []

        raw = parsed.get("facets", [])
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        seen: set[str] = set()
        for item in raw:
            key = normalize_facet(str(item or ""))
            if (not key) or key in seen:
                continue
            seen.add(key)
            out.append(key)
        return out

    def generate_news_post(
        self,
        candidate: TopicCandidate,
        authority_links: list[str],
        reference_guidance: str,
        category: str,
        plan: dict | None = None,
    ) -> DraftPost:
        source_link = str(getattr(candidate, "url", "") or "").strip()
        safe_authorities = [
            re.sub(r"\s+", " ", str(x or "").strip())
            for x in (authority_links or [])
            if str(x or "").strip()
        ][:8]
        facet_action_items: list[str] = []

        def _render_news_html(payload_obj: dict) -> str:
            body_html = str(payload_obj.get("content_html", payload_obj.get("html", "")) or "")
            body_html = self._remove_ai_markers(body_html, domain="tech_news_explainer")
            body_html = re.sub(
                r"<h[23][^>]*>\s*faq\s*</h[23]>.*?(?=<h2\b|<h3\b|$)",
                "",
                body_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            body_html = self._enforce_html_minimum(body_html)
            out = self._normalize_news_sources_section(
                html=body_html,
                source_url=source_link,
                authority_links=safe_authorities,
            )
            return ensure_what_to_do_now_section(html=out, action_items=facet_action_items)

        module_pool = [
            "impact_by_user_type",
            "timeline_snapshot",
            "official_statement_summary",
            "risk_level_breakdown",
            "rollback_vs_wait_comparison",
            "regional_rollout_note",
            "known_unknowns",
            "cost_or_effort_estimate",
            "what_changed_since_last_update",
            "edge_case_callout",
            "one_concrete_example",
            "what_to_watch_signal_list",
        ]
        selected_modules, _rotation_fallback, _recent_modules = self._select_news_modules(module_pool)
        plan_payload = dict(plan or {})
        category_norm = normalize_category(category)
        primary_topic = re.sub(
            r"\s+",
            " ",
            str(plan_payload.get("primary_keyword", "") or candidate.title or "").strip(),
        )[:160]
        module_slots = {}
        for module_name in selected_modules:
            module_slots[module_name] = random.choice(
                [
                    "What Happened",
                    "Key Details",
                    "What To Watch Next",
                ]
            )
        event_meta = dict(getattr(candidate, "meta", {}) or {})
        event_id = str(
            plan_payload.get("event_id", "")
            or event_meta.get("event_id", "")
            or event_meta.get("news_event_id", "")
            or event_meta.get("news_pool_id", "")
            or source_link
            or candidate.title
        ).strip()
        run_start_minute = str(
            plan_payload.get("run_start_minute", "")
            or event_meta.get("run_start_minute", "")
            or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
        ).strip()
        retry_index = (
            plan_payload.get("retry_index", None)
            if isinstance(plan_payload, dict)
            else None
        )
        if retry_index is None:
            retry_index = event_meta.get("retry_index", None)
        llm_facet_candidates = self._suggest_news_facets_with_llm(
            title=str(candidate.title or ""),
            body=str(candidate.body or ""),
            category=category_norm,
            event_id=event_id,
            run_start_minute=run_start_minute,
        )
        facet_context = resolve_facet_context(
            event_id=event_id,
            run_start_minute=run_start_minute,
            title=str(candidate.title or ""),
            body=str(candidate.body or ""),
            category=category_norm,
            source_url=source_link,
            retry_index=retry_index,
            llm_candidates=llm_facet_candidates,
            state_path=self._facet_rotation_state_path,
            stable_hash_fn=stable_hash,
        )
        try:
            meta = dict(getattr(candidate, "meta", {}) or {})
            meta["selected_facet"] = str(facet_context.selected_facet or "impact")
            candidate.meta = meta
        except Exception:
            pass
        facet_action_items = list(facet_context.action_items)
        structure_seed = stable_hash(f"{event_id}{run_start_minute}")
        structure_plan = build_structure(
            structure_seed,
            {
                "event_id": event_id,
                "run_start_minute": run_start_minute,
                "category": category_norm,
                "facet": facet_context.selected_facet,
            },
        )
        rendered_structure = render_from_plan(
            structure_plan,
            {
                "event_id": event_id,
                "run_start_minute": run_start_minute,
                "category": category_norm,
                "facet": facet_context.selected_facet,
            },
        )
        section_ids = [str(x or "").strip().lower() for x in (rendered_structure.get("section_ids", []) or [])]
        if len(section_ids) < 5:
            section_ids = list(NEWS_FALLBACK_SECTION_IDS)
            rendered_structure = render_from_plan(
                _fallback_structure_plan(),
                {
                    "event_id": event_id,
                    "run_start_minute": run_start_minute,
                    "category": category_norm,
                    "facet": facet_context.selected_facet,
                },
            )
        section_titles = [
            str(x or "").strip()
            for x in (rendered_structure.get("section_titles", []) or [])
            if str(x or "").strip()
        ]
        if not section_titles:
            section_titles = [NEWS_SECTION_TITLES[x] for x in NEWS_FALLBACK_SECTION_IDS]
        section_count = max(5, int(rendered_structure.get("section_count", len(section_titles)) or len(section_titles)))
        section_order_text = "\n".join(section_titles)
        paragraph_lengths = [str(x or "").strip() for x in (rendered_structure.get("paragraph_lengths", []) or [])]
        paragraph_plan = "; ".join(
            f"{title}={paragraph_lengths[idx] if idx < len(paragraph_lengths) else 'medium'}"
            for idx, title in enumerate(section_titles)
        )
        specific_actions = category_norm in {"security", "policy", "platform"}
        what_to_do_rule = (
            "Include a 'What To Do Now' H2 section near the end, immediately before Sources.\n"
            f"'What To Do Now' must contain exactly {facet_context.action_count} bullet steps.\n"
            + (
                "For security/policy/platform events, each step must be concrete and operationally specific.\n"
                if specific_actions
                else "Keep the action steps concise, practical, and verifiable.\n"
            )
        )
        perspective_hint = facet_emphasis_hint(facet_context.selected_facet)
        plan_payload["retry_index"] = int(facet_context.retry_index_effective)
        prompt = (
            "Write a US tech news explainer article in valid HTML body fragment only.\n"
            "Language policy: US English only. Never output Korean.\n"
            "Output valid HTML only. Do NOT output Markdown headings (#, ##, ###).\n"
            "Tone: factual, concise, ad-safe, and attribution-first.\n"
            "No defamation. No unverified allegations.\n"
            "Use attribution phrasing for uncertain claims: reported, may, could, according to.\n"
            "Apply perspective emphasis implicitly in section focus and sentence priority. "
            "Never print internal planning labels.\n"
            f"Perspective emphasis hint: {perspective_hint}\n"
            "Do not write like a template. Vary transitions and avoid repeated phrases like 'In conclusion'.\n"
            "Avoid uniform paragraph lengths; mix 1-line punches with longer analysis.\n"
            "Do not write as a troubleshooting fix guide.\n"
            "Do NOT include FAQ section.\n"
            "Required core H2 sections (must appear exactly once): Quick Take, What Happened, Sources.\n"
            f"Use exactly {section_count} H2 sections for this run (minimum H2 count: 5).\n"
            "Do not enforce a rigid template beyond this run plan.\n"
            "H2 section order for this run (follow exactly):\n"
            f"{section_order_text}\n"
            f"Intro pattern for Quick Take: {rendered_structure.get('intro_template', '')}\n"
            f"Conclusion pattern (place as the final paragraph before Sources): {rendered_structure.get('conclusion_template', '')}\n"
            f"Paragraph-length rhythm by section: {paragraph_plan}\n"
            "Under Quick Take, include:\n"
            "- LEDE: 2-3 short sentences\n"
            "- NUT GRAF: exactly 1 paragraph explaining why now and why it matters\n"
            "- Key Facts box as <ul> with exactly 5 short bullets covering Who/What/When/Scope/Risk.\n"
            "  If unknown, write 'Not confirmed'.\n"
            f"{what_to_do_rule}"
            "The article must include at least 2 question sentences and one explicit comparison/example block.\n"
            "Target depth: roughly 900-1400 words for editorial completeness.\n"
            f"Use 6-8 diversity modules from this pool: {selected_modules}\n"
            f"Module placement map (vary rhythm): {module_slots}\n"
            "In Sources, show publisher-labeled links (not raw URL text).\n"
            "Sources must include the original source URL exactly once and 1-2 authority links when available.\n"
            "Never include google.com/search, googleusercontent, or googleapis links.\n"
            "No screenshots, no logo references, no copyright-sensitive image instructions.\n"
            "Return strict JSON with keys only: title_draft, meta_description, content_html, summary, focus_keywords.\n"
            "focus_keywords must be a JSON array.\n"
            f"News category: {category_norm}\n"
            f"Primary topic phrase: {primary_topic}\n"
            f"Original source URL: {source_link}\n"
            f"Authority links allow-list: {safe_authorities}\n"
            f"Reference guidance: {reference_guidance}\n"
            f"Plan JSON: {json.dumps(plan_payload, ensure_ascii=False) if plan_payload else '{}'}\n"
            f"Source title: {candidate.title}\n"
            f"Source body: {candidate.body[:4000]}\n"
        )
        payload = self._extract_json(
            self._generate_text(
                prompt,
                system_instruction=(
                    "You are a senior US tech news editor writing practical explainers for mainstream readers. "
                    "Be precise, actionable, and legally careful."
                ),
            )
        )
        html = _render_news_html(payload)
        if self._news_requires_key_facts_retry(html):
            retry_prompt = (
                prompt
                + "\n\nREWRITE REQUIRED: previous output missed Quick Take Key Facts format. "
                "Regenerate full HTML with LEDE + NUT GRAF + exactly 5 Key Facts bullets."
            )
            retry_payload = self._extract_json(
                self._generate_text(
                    retry_prompt,
                    system_instruction=(
                        "You are a senior US tech news editor writing practical explainers for mainstream readers. "
                        "Be precise, actionable, and legally careful."
                    ),
                )
            )
            retry_html = _render_news_html(retry_payload)
            if retry_html:
                html = retry_html
                payload = retry_payload
        _focus_keywords = payload.get("focus_keywords", [])
        if not isinstance(_focus_keywords, list):
            _focus_keywords = []
        urls = payload.get("extracted_urls", [])
        if not isinstance(urls, list):
            urls = []
        out_title = str(payload.get("title_draft", payload.get("title", candidate.title))).strip() or candidate.title
        if primary_topic and primary_topic.lower() not in out_title.lower():
            out_title = f"{out_title}: {primary_topic}".strip(" :")
        self._save_news_module_rotation(selected_modules)
        return DraftPost(
            title=out_title[:110],
            alt_titles=[],
            summary=str(payload.get("summary", "")).strip()[:500],
            html=html,
            score=100,
            source_url=source_link,
            extracted_urls=[str(u) for u in urls if isinstance(u, str)][:8],
        )

    def _load_news_module_rotation(self) -> list[str]:
        try:
            if not self._news_module_rotation_path.exists():
                return []
            payload = json.loads(self._news_module_rotation_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return []
            recent = payload.get("recent", [])
            if not isinstance(recent, list):
                return []
            out: list[str] = []
            seen: set[str] = set()
            for item in recent:
                key = re.sub(r"\s+", " ", str(item or "")).strip().lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(key)
            return out[:10]
        except Exception:
            return []

    def _save_news_module_rotation(self, selected_modules: list[str]) -> None:
        recent = self._load_news_module_rotation()
        merged: list[str] = []
        seen: set[str] = set()
        for item in [*(selected_modules or []), *recent]:
            key = re.sub(r"\s+", " ", str(item or "")).strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(key)
        payload = {
            "recent": merged[:10],
            "updated_utc": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self._news_module_rotation_path.parent.mkdir(parents=True, exist_ok=True)
            self._news_module_rotation_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            return

    def _select_news_modules(self, module_pool: list[str]) -> tuple[list[str], bool, list[str]]:
        base = [str(x or "").strip().lower() for x in (module_pool or []) if str(x or "").strip()]
        if not base:
            return [], False, []
        recent = self._load_news_module_rotation()
        avoid = set(recent[:6])
        available = [x for x in base if x not in avoid]
        fallback_used = False
        if len(available) < 6:
            available = list(base)
            fallback_used = True
        random.shuffle(available)
        count = random.randint(6, min(8, len(base)))
        selected = available[:count]
        self._log_news_module_rotation(
            {
                "event": "module_select",
                "fallback_used": bool(fallback_used),
                "recent_avoid_window": recent[:6],
                "selected_modules": selected,
            }
        )
        return selected, fallback_used, recent

    def _log_news_module_rotation(self, payload: dict[str, Any]) -> None:
        row = {"ts_utc": datetime.now(timezone.utc).isoformat()}
        row.update(dict(payload or {}))
        try:
            self._news_module_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._news_module_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _source_label_from_url(self, url: str) -> str:
        host = (urlparse(str(url or "")).netloc or "").lower().strip()
        if not host:
            return "Source"
        if "security.googleblog.com" in host:
            return "Google Security Blog"
        if "aws.amazon.com" in host:
            return "AWS Security Blog"
        if "cisa.gov" in host:
            return "CISA Advisory"
        if "msrc.microsoft.com" in host:
            return "Microsoft Security Response Center"
        if "microsoft.com" in host and "security" in host:
            return "Microsoft Security Blog"
        if "support.apple.com" in host:
            return "Apple Support"
        if "cloudflare.com" in host:
            return "Cloudflare Blog"
        if "nist.gov" in host:
            return "NIST Cybersecurity Framework"
        parts = [p for p in host.split(".") if p and p not in {"www", "com", "org", "net", "gov", "co"}]
        if not parts:
            return "Source"
        return " ".join(x.capitalize() for x in parts[:3]) + " Report"

    def _normalize_news_sources_section(self, *, html: str, source_url: str, authority_links: list[str]) -> str:
        src = str(html or "")
        block = re.search(
            r"(<h2[^>]*>\s*Sources\s*</h2>)(.*?)(?=<h2\b|$)",
            src,
            flags=re.IGNORECASE | re.DOTALL,
        )
        current_section = block.group(2) if block else ""
        urls = [
            re.sub(r"\s+", " ", str(u or "")).strip()
            for u in re.findall(r'href=["\']([^"\']+)["\']', current_section, flags=re.IGNORECASE)
            if str(u or "").strip()
        ]
        if source_url:
            urls.insert(0, re.sub(r"\s+", " ", str(source_url)).strip())
        for link in authority_links[:2]:
            urls.append(re.sub(r"\s+", " ", str(link or "")).strip())
        clean_urls: list[str] = []
        seen: set[str] = set()
        for url in urls:
            low = url.lower()
            if not url:
                continue
            if "google.com" in low or "googleusercontent.com" in low or "googleapis.com" in low:
                continue
            if low in seen:
                continue
            seen.add(low)
            clean_urls.append(url)
        final_urls: list[str] = []
        if source_url:
            final_urls.append(re.sub(r"\s+", " ", str(source_url)).strip())
        for u in clean_urls:
            if u.lower() not in {x.lower() for x in final_urls}:
                final_urls.append(u)
        items: list[str] = []
        for idx, url in enumerate(final_urls[:3]):
            label = "Original report" if idx == 0 and source_url else self._source_label_from_url(url)
            items.append(f'<li><a href="{escape(url)}">{escape(label)}</a></li>')
        if not items:
            items.append("<li>No authoritative source available.</li>")
        sources_html = "<h2>Sources</h2><ul>" + "".join(items) + "</ul>"
        if block:
            return src[: block.start()] + sources_html + src[block.end() :]
        return src + "\n" + sources_html

    def _news_requires_key_facts_retry(self, html: str) -> bool:
        section = re.search(
            r"<h2[^>]*>\s*Quick\s*Take\s*</h2>(.*?)(?=<h2\b|$)",
            str(html or ""),
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not section:
            return True
        body = str(section.group(1) or "")
        p_count = len(re.findall(r"<p\b[^>]*>.*?</p>", body, flags=re.IGNORECASE | re.DOTALL))
        if p_count < 3:
            return True
        ul_match = re.search(r"<ul\b[^>]*>(.*?)</ul>", body, flags=re.IGNORECASE | re.DOTALL)
        if not ul_match:
            return True
        lis = re.findall(r"<li\b[^>]*>(.*?)</li>", str(ul_match.group(1) or ""), flags=re.IGNORECASE | re.DOTALL)
        if len(lis) < 5:
            return True
        joined = " ".join(re.sub(r"<[^>]+>", " ", x) for x in lis).lower()
        required = ("who", "what", "when", "scope", "risk")
        return not all(tok in joined for tok in required)

    def rewrite_to_actionable(self, title: str, html: str, plan: dict | None = None) -> str:
        plan_payload = plan if isinstance(plan, dict) else {}
        prompt = (
            "Rewrite the HTML draft into a concrete troubleshooting guide.\n"
            "Language: US English only.\n"
            "Output: HTML body fragment only (no Markdown, no JSON unless explicitly requested).\n"
            "Keep the same topic and intent, but reduce fluff and increase actionable value.\n"
            "Use these exact H2 titles in order:\n"
            "Quick Take\n"
            "Symptoms (How you know it's this issue)\n"
            "Why This Happens\n"
            "Fix 1\n"
            "Fix 2\n"
            "Fix 3\n"
            "Fix 4\n"
            "Fix 5\n"
            "If None Worked (Safe escalation)\n"
            "Prevention Checklist\n"
            "Each Fix section must include:\n"
            "- 3-5 short bullet steps\n"
            "- one 'Expected result:' line\n"
            "- one 'If not:' line\n"
            "- one 'Time to try:' hint\n"
            "Final checklist must have 6-10 bullets.\n"
            "Remove generic hedging and robotic filler.\n"
            "Do not include internal/debug/system text.\n"
            "TROUBLESHOOTING PLAN JSON (must follow exactly):\n"
            f"{json.dumps(plan_payload, ensure_ascii=False) if plan_payload else '{}'}\n"
            f"Title: {title}\n"
            f"HTML Draft:\n{html[:18000]}"
        )
        raw = self._generate_text(prompt, system_instruction=self.settings.editor_persona, temperature=0.45, top_p=0.9)
        extracted = self._extract_json(raw)
        rewritten_html = ""
        if isinstance(extracted, dict):
            rewritten_html = str(extracted.get("content_html", "") or extracted.get("html", "") or "").strip()
        if not rewritten_html:
            rewritten_html = str(raw or "").strip()
        rewritten_html = self._remove_ai_markers(rewritten_html, domain="news_interpretation")
        rewritten_html = self._enforce_html_minimum(rewritten_html)
        return rewritten_html

    def generate_post_free(
        self,
        candidate: TopicCandidate,
        authority_links: list[str],
    ) -> DraftPost:
        title = candidate.title.strip() or "A Practical Device Troubleshooting Routine I Tested"
        body = re.sub(r"\s+", " ", candidate.body or "").strip()
        key_points = self._extract_key_points(body)
        source_url = candidate.url.strip()
        if re.search(r"^https?://(?:www\.)?google\.com", source_url, flags=re.IGNORECASE):
            source_url = ""

        links_html = "".join(
            f'<li><a href="{escape(link)}" rel="nofollow noopener" target="_blank">{escape(link)}</a></li>'
            for link in authority_links[:2]
        )
        if source_url:
            links_html += (
                f'<li><a href="{escape(source_url)}" rel="nofollow noopener" target="_blank">'
                "Original source</a></li>"
            )

        takeaway_items = key_points[:5] or [
            "The first attempt failed because I changed too many settings at once.",
            "A short checklist made troubleshooting stable and repeatable.",
            "The biggest benefit was getting the device back to normal quickly.",
        ]
        takeaway_html = "".join(f"<li>{escape(self._short_rewrite(p))}</li>" for p in takeaway_items)

        html = (
            "<h2>Quick Take</h2>"
            f"<p>If you are dealing with <strong>{escape(title)}</strong>, this is the short answer: run simple checks first, then apply fixes in order. "
            "In my test, disciplined troubleshooting mattered more than complex tweaks.</p>"
            "<h2>Why I Tried This</h2>"
            f"<p>I was frustrated because <strong>{escape(title)}</strong> kept breaking my routine and most explanations were too technical.</p>"
            "<p>So I tested a simpler path and kept only the steps an everyday user can repeat without stress.</p>"
            "<h2>What Failed First</h2>"
            "<p>My first tries were messy. I copied advanced advice too quickly and spent more time fixing mistakes than getting results.</p>"
            "<ul>"
            "<li>I started with too many settings.</li>"
            "<li>I skipped a small test and had to redo the work.</li>"
            "<li>I focused on optimization before basic stability.</li>"
            "</ul>"
            "<h2>What Finally Worked</h2>"
            "<p>I switched to one clear goal, one fix sequence, and one validation step. That changed everything and made the result reliable.</p>"
            "<ul>"
            "<li><strong>Step 1:</strong> Confirm the exact symptom.</li>"
            "<li><strong>Step 2:</strong> Apply one fix at a time in order.</li>"
            "<li><strong>Step 3:</strong> Validate and log what changed.</li>"
            "</ul>"
            "<h2>Quick Notes For Beginners</h2>"
            f"<ul>{takeaway_html}</ul>"
            "<h2>When This Is Not Ideal</h2>"
            "<p>If your environment requires strict engineering controls, this simplified method may be too light. Use it as a starter, then hand off to advanced support.</p>"
            "<h2>My Final Take</h2>"
            "<p>If technical guides overwhelm you, start smaller than you think. A simple routine you can keep is better than a perfect setup you cannot maintain.</p>"
            "<h3>References</h3>"
            f"<ul>{links_html}</ul>"
        )
        html = self._remove_ai_markers(html)

        extracted = [
            u
            for u in [source_url, *authority_links[:4]]
            if u and not re.search(r"^https?://(?:www\.)?google\.com", u, flags=re.IGNORECASE)
        ]
        return DraftPost(
            title=title,
            alt_titles=[title, f"{title} - Quick Guide", f"{title} - Practical Fixes"],
            html=html,
            summary="First-person practical rewrite for non-technical troubleshooting readers.",
            score=80,
            source_url=source_url,
            extracted_urls=extracted[:8],
        )

    def judge_post(self, title: str, html: str) -> tuple[int, list[str]]:
        prompt = (
            "You are a strict content quality judge for mainstream practical blogs.\n"
            "Score this draft from 0 to 100 for human-likeness, practical value, clarity for non-technical readers, and credibility.\n"
            "Use this human-like checklist lens: avoid repetitive structure, avoid early conclusions, include concrete lived detail, "
            "show trade-offs and imperfect outcomes, reduce robotic transitions, and avoid over-polished generic phrasing.\n"
            "Return strict JSON: {\"score\": int, \"issues\": [string, ...]}.\n"
            "Be strict: generic structure, robotic transitions, weak evidence, and unexplained jargon must be penalized.\n"
            f"Title: {title}\n"
            f"HTML:\n{html[:14000]}"
        )
        data = self._extract_json(self._generate_text(prompt))
        score = int(data.get("score", 0))
        issues = data.get("issues", [])
        if not isinstance(issues, list):
            issues = []
        return max(0, min(100, score)), [str(x) for x in issues[:6]]

    def optimize_headline_ctr(
        self,
        summary: str,
        trending_keywords: list[str] | None,
        current_title: str,
    ) -> tuple[str, list[str]]:
        clean_summary = re.sub(r"\s+", " ", summary or "").strip()
        clean_summary = clean_summary[:1800] if clean_summary else "No summary provided."
        keywords = [re.sub(r"\s+", " ", str(k or "")).strip() for k in (trending_keywords or [])]
        keywords = [k for k in keywords if k][:8]
        prompt = (
            "You are a World-class Tech Copywriter for a global audience.\n"
            "Your task is to rewrite the blog title for maximum Click-Through Rate (CTR).\n"
            "Language policy: American English only. Never output Korean.\n"
            f"Input Draft Summary: {clean_summary}\n"
            f"Input Keywords: {keywords}\n"
            f"Current title: {current_title}\n\n"
            "Hard Rules:\n"
            "- NEVER use generic labels like 'Introduction to...' or 'Guide for...'.\n"
            "- Keep troubleshooting search intent explicit.\n"
            "- Apply one of these 3 formulas:\n"
            "  1) '[Device/Feature] not working after update? [Number] safe fixes in 2026.'\n"
            "  2) 'How I fixed [Problem] after update: [Number] safe steps.'\n"
            "  3) '[Error] fix guide: what to try first and what to skip.'\n"
            "- Tone: Native US English, bold, intriguing, professional, and positive.\n"
            "- Avoid defamation, allegations, or sensational negativity.\n"
            "- Do not use trend/news framing phrases like 'why everyone is talking'.\n"
            "- Deliver exactly 5 variants in JSON format.\n"
            "Return strict JSON only with this schema:\n"
            "{\"variants\": [\"title1\", \"title2\", \"title3\", \"title4\", \"title5\"]}"
        )

        raw = self._generate_text_forced_model(
            prompt=prompt,
            model="gemini-2.0-flash",
            system_instruction=(
                "You write high-CTR, ad-safe, native US English headlines for mainstream tech blogs. "
                "Output must be American English only."
            ),
        )
        payload = self._extract_json(raw)
        variants = self._normalize_headline_variants(payload, raw, current_title)
        best = max(variants, key=lambda t: self._headline_ctr_score(t, keywords))
        return best, variants

    def generate_title_variants(
        self,
        *,
        summary_payload: dict,
        current_title: str,
        recent_titles: list[str],
    ) -> list[str]:
        payload = dict(summary_payload or {})
        short_summary = re.sub(r"\s+", " ", str(payload.get("short_summary", "") or "")).strip()[:420]
        primary_issue = re.sub(r"\s+", " ", str(payload.get("primary_issue_phrase", "") or "")).strip()[:140]
        device = re.sub(r"\s+", " ", str(payload.get("device_family", "") or "")).strip().lower()
        feature = re.sub(r"\s+", " ", str(payload.get("feature", "") or "")).strip().lower()
        must_terms = [
            re.sub(r"\s+", " ", str(x or "")).strip()
            for x in (payload.get("must_include_terms", []) if isinstance(payload.get("must_include_terms", []), list) else [])
            if str(x or "").strip()
        ][:6]
        prompt = (
            "Generate exactly 10 unique troubleshooting blog title candidates.\n"
            "Output must be US English only.\n"
            "Return strict JSON only: {\"titles\": [\"...10 items...\"]}\n"
            "Hard rules:\n"
            "- Every title must include clear fix intent token: not working OR fix OR error OR after update.\n"
            "- Include device and feature when provided.\n"
            "- Prefer 45 to 90 characters.\n"
            "- Avoid boilerplate phrases.\n"
            "- Ban exact phrases: 'fixes that actually work', 'ultimate guide', 'device not working'.\n"
            "- Avoid repeating templates from recent titles.\n"
            f"Summary: {short_summary}\n"
            f"Primary issue phrase: {primary_issue}\n"
            f"Device family: {device}\n"
            f"Feature: {feature}\n"
            f"Must include terms: {must_terms}\n"
            f"Current title: {current_title}\n"
            f"Recent titles to avoid: {recent_titles[:60]}"
        )
        raw = self._generate_text_forced_model(
            prompt=prompt,
            model=(self.settings.model or "gemini-2.0-flash"),
            system_instruction=(
                "You produce high-CTR, ad-safe US-English troubleshooting headlines. "
                "Never output Korean. Output JSON only."
            ),
        )
        payload_out = self._extract_json(raw)
        variants = self._normalize_title_variants_payload(
            payload=payload_out,
            raw_text=raw,
            fallback_title=current_title,
            device=device,
            feature=feature,
            must_terms=must_terms,
        )
        return variants[:10]

    def generate_news_title_variants(
        self,
        *,
        category: str,
        source_title: str,
        source_snippet: str,
        recent_titles: list[str],
        banned_tokens: list[str] | None = None,
        limit: int = 10,
    ) -> list[str]:
        """
        Generate diverse US-English *news explainer* titles.
        Avoid templated patterns; avoid FAQ; avoid clickbait; avoid google.com references.
        Returns up to `limit` unique titles.
        """
        banned = [
            str(x or "").strip().lower()
            for x in (banned_tokens or [])
            if str(x or "").strip()
        ]
        recent = [
            re.sub(r"\s+", " ", str(t or "")).strip()
            for t in (recent_titles or [])
            if str(t or "").strip()
        ][:80]
        cat = re.sub(r"\s+", " ", str(category or "platform")).strip().lower()[:30]
        st = re.sub(r"\s+", " ", str(source_title or "")).strip()[:180]
        sn = re.sub(r"\s+", " ", str(source_snippet or "")).strip()[:420]

        prompt = (
            "You are an experienced US tech news editor.\n"
            "Task: write DISTINCT, non-templated blog headlines for a US tech news explainer article.\n"
            "Language: American English only. Never output Korean.\n"
            "Do NOT include FAQ. Do NOT include 'guide' in a generic way.\n"
            "Avoid clickbait and sensational words. Avoid legal claims. No defamation.\n"
            "Avoid repeated patterns across titles. Vary grammar, verbs, and structure.\n"
            "Hard constraints:\n"
            "- 45 to 95 characters ideal (hard max 100)\n"
            "- Must be specific and factual\n"
            "- Must not contain: shocking, disaster, scam, fraud, criminal, exposed, destroyed, caught\n"
            "- Must not contain google.com or any search-engine references\n"
            "- Must not start with the same first 3 words more than once\n"
            f"- Category hint: {cat}\n"
            f"- Source title: {st}\n"
            f"- Source snippet: {sn}\n"
            f"- Recent titles to avoid repeating: {recent[:50]}\n"
            f"- Additional banned tokens: {banned[:20]}\n"
            "Return strict JSON only: {\"titles\": [\"...\", ...]} with exactly 12 candidates.\n"
        )

        raw = self._generate_text_forced_model(
            prompt=prompt,
            model=(self.settings.model or "gemini-2.0-flash"),
            system_instruction=(
                "You write human, natural, US-native tech news headlines. "
                "You avoid templates and vary structure aggressively while staying factual."
            ),
        )
        payload = self._extract_json(raw)
        titles = payload.get("titles", [])
        if not isinstance(titles, list):
            titles = []

        cleaned: list[str] = []
        seen: set[str] = set()
        for item in titles:
            t = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", str(item or ""))
            t = re.sub(r"\s+", " ", t).strip().strip("\"'")[:110]
            if not t:
                continue
            low = t.lower()
            if "faq" in low or "frequently asked" in low:
                continue
            if "google.com" in low:
                continue
            if re.search(
                r"\b(shocking|disaster|scam|fraud|criminal|exposed|destroyed|caught)\b",
                low,
            ):
                continue
            if any(bt in low for bt in banned):
                continue
            if len(t) > 100:
                t = t[:100].rstrip(" ,.;:-")
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(t)
            if len(cleaned) >= max(1, int(limit)):
                break

        return cleaned[: max(1, int(limit))]

    def _normalize_headline_variants(self, payload: dict, raw_text: str, fallback_title: str) -> list[str]:
        raw = payload.get("variants", [])
        if not isinstance(raw, list):
            raw = payload.get("titles", [])
        if not isinstance(raw, list):
            raw = []
        titles: list[str] = []
        for item in raw:
            title = re.sub(r"\s+", " ", str(item or "")).strip().strip("\"'")
            if title:
                titles.append(title)

        if not titles:
            # JSON parsing fallback: scan quoted fragments from model output.
            titles = [m.strip() for m in re.findall(r"\"([^\"]{20,140})\"", raw_text or "")[:8]]

        out: list[str] = []
        seen: set[str] = set()
        for title in titles:
            cleaned = re.sub(r"\s+", " ", title).strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(cleaned[:130])
            if len(out) >= 5:
                break

        if fallback_title and fallback_title.strip():
            fb = fallback_title.strip()
            if fb.lower() not in seen:
                out.append(fb)

        return out[:5] if len(out) >= 5 else (out + [fallback_title.strip()])[:5]

    def _normalize_title_variants_payload(
        self,
        *,
        payload: dict,
        raw_text: str,
        fallback_title: str,
        device: str,
        feature: str,
        must_terms: list[str],
    ) -> list[str]:
        raw_titles = payload.get("titles", [])
        if not isinstance(raw_titles, list):
            raw_titles = []
        if not raw_titles:
            raw_titles = [m.strip() for m in re.findall(r"\"([^\"]{20,140})\"", raw_text or "")[:20]]
        out: list[str] = []
        seen: set[str] = set()
        banned = ("fixes that actually work", "ultimate guide", "device not working")
        for item in raw_titles:
            title = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", str(item or ""))
            title = re.sub(r"\s+", " ", title).strip(" -:\"'")
            if not title:
                continue
            low = title.lower()
            if any(b in low for b in banned):
                continue
            if not any(tok in low for tok in ("not working", "fix", "error", "after update")):
                continue
            if device and device not in low:
                if device == "windows":
                    title = f"Windows: {title}"
                elif device == "mac":
                    title = f"Mac: {title}"
                elif device == "iphone":
                    title = f"iPhone: {title}"
                elif device == "galaxy":
                    title = f"Galaxy: {title}"
                low = title.lower()
            if feature and feature not in low:
                title = f"{title} ({feature})"
                low = title.lower()
            for term in must_terms[:2]:
                t = re.sub(r"\s+", " ", str(term or "")).strip().lower()
                if not t:
                    continue
                if t not in low:
                    title = f"{title} - {term}"
                    low = title.lower()
                    break
            if len(title) < 45:
                title = f"{title} fix steps"
            title = title[:95].rstrip(" -:")
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(title)
            if len(out) >= 10:
                break
        fallback = re.sub(r"\s+", " ", str(fallback_title or "")).strip()
        base = fallback or "Windows update issue fix: safe steps to try first"
        for phrase in banned:
            base = re.sub(re.escape(phrase), "", base, flags=re.IGNORECASE)
        base = re.sub(r"\s+", " ", base).strip(" -:")
        if not base:
            base = "Windows update issue fix: safe steps to try first"
        while len(out) < 10:
            seed_idx = len(out) + 1
            candidate = f"{base} ({seed_idx})"
            candidate = re.sub(r"\s+", " ", candidate).strip()[:95]
            key = candidate.lower()
            if key in seen:
                candidate = f"{base} - error fix {seed_idx}"[:95]
                key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(candidate)
        return out[:10]

    def _headline_ctr_score(self, title: str, keywords: list[str] | None) -> float:
        t = (title or "").strip()
        lower = t.lower()
        if not t:
            return -1e9
        score = 0.0

        # Length sweet spot for search/social snippets.
        n = len(t)
        if 38 <= n <= 90:
            score += 12.0
        elif n <= 120:
            score += 6.0
        else:
            score -= 8.0

        # Penalize generic robotic patterns.
        generic = (
            "introduction to",
            "guide for",
            "a guide to",
            "overview of",
            "what is ",
        )
        if any(g in lower for g in generic):
            score -= 25.0
        else:
            score += 8.0

        power_words = (
            "revolutionary",
            "game-changing",
            "secret",
            "hidden",
            "surprising",
            "breakthrough",
            "practical",
            "proven",
            "worked",
            "learned",
            "lesson",
            "fix",
            "fixed",
            "repair",
            "troubleshoot",
            "what changed",
            "what i learned",
        )
        pw_hits = sum(1 for w in power_words if w in lower)
        score += min(20.0, float(pw_hits) * 6.0)

        # Curiosity/listicle/story style indicators.
        if "?" in t:
            score += 8.0
        if re.search(r"\bwhy\b|\bhow\b", lower):
            score += 6.0
        if re.search(r"\b\d{1,2}\b", t):
            score += 8.0
        if re.search(r"\bi\b|\bmy\b", lower):
            score += 5.0
        if re.search(r"^the secret of .+", lower):
            score -= 8.0
        if re.search(r"\b(fix|fixed|repair|troubleshoot|not working|error code)\b", lower):
            score += 8.0
        if re.search(r"\b(scam|fraud|lawsuit|illegal|failing|disaster|shocking)\b", lower):
            score -= 14.0

        # Keyword relevance boost.
        for kw in (keywords or [])[:8]:
            norm = re.sub(r"\s+", " ", str(kw or "")).strip().lower()
            if norm and norm in lower:
                score += 3.0

        return score

    def _extract_key_points(self, body: str) -> list[str]:
        cleaned = re.sub(r"`[^`]+`", " ", body or "")
        cleaned = re.sub(r"https?://\S+", " ", cleaned)
        cleaned = re.sub(r"#{1,6}\s*", " ", cleaned)
        sentences = [s.strip() for s in re.split(r"[.!?]\s+", cleaned) if s.strip()]
        points: list[str] = []
        for sent in sentences:
            clean = re.sub(r"\s+", " ", sent).strip()
            if len(clean) < 50:
                continue
            points.append(clean[:220])
            if len(points) >= 8:
                break
        return points

    def _build_source_digest(self, key_points: list[str], title: str) -> str:
        if not key_points:
            return (
                "The source discusses recurring operational friction around this topic. "
                "Teams report repeated uncertainty in setup choices, validation order, and release safety."
            )
        mini = [self._short_rewrite(p) for p in key_points[:3]]
        merged = " ".join(f"Signal {i+1}: {m}." for i, m in enumerate(mini) if m)
        return (
            f"For '{title}', the source thread points to repeated execution pain rather than a one-off incident. "
            + merged
        )

    def _short_rewrite(self, text: str) -> str:
        stop = {
            "the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "with", "is", "are",
            "was", "were", "be", "been", "this", "that", "it", "as", "at", "by", "from", "if",
            "then", "than", "but", "not", "do", "does", "did", "can", "could", "should",
        }
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,}", text.lower())
        kept = [t for t in tokens if t not in stop][:18]
        if not kept:
            return "recurring implementation uncertainty in real workflows"
        return " ".join(kept)

    def _build_deep_dive_sections(self, key_points: list[str], title: str) -> str:
        if not key_points:
            key_points = [
                "Teams repeatedly hit regressions because release checks are not standardized.",
                "The same class of issue reappears when local assumptions differ from production reality.",
                "A short feedback loop with measurable checkpoints is consistently more effective than ad-hoc fixes.",
            ]

        blocks: list[str] = []
        for idx, point in enumerate(key_points[:8], start=1):
            blocks.append(
                "<h3>"
                f"Operational Insight {idx}"
                "</h3>"
                f"<p><strong>Observed signal:</strong> {escape(point)}.</p>"
                "<p>This signal matters because it usually points to a process gap, not a one-off mistake. "
                "Treat it as a system symptom: identify where the decision path lacked guardrails, "
                "then redesign that step so the same failure mode becomes harder to reproduce.</p>"
                "<ul>"
                "<li>Define one measurable objective for this issue category.</li>"
                "<li>Attach one owner and one verification deadline.</li>"
                "<li>Document the accepted rollback condition before rollout.</li>"
                "</ul>"
            )

        blocks.append(
            "<h2>Rollout Strategy By Team Stage</h2>"
            "<h3>Small Team (1-5 engineers)</h3>"
            "<p>Optimize for speed and learning density. Keep the plan lightweight, but never skip instrumentation. "
            "A concise release checklist and one dashboard are enough if they are used consistently.</p>"
            "<h3>Growth Team (6-20 engineers)</h3>"
            "<p>Standardize ownership boundaries, release windows, and post-release review cadence. "
            "At this stage, process consistency usually gives a larger reliability gain than additional tooling.</p>"
            "<h3>Scaled Team (20+ engineers)</h3>"
            "<p>Prioritize change safety. Require explicit impact estimation before production rollout, "
            "enforce progressive delivery, and tie incident response documents to release artifacts.</p>"
        )
        blocks.append(
            "<h2>Measurement Framework</h2>"
            "<p>Track four dimensions per release: reliability, performance, user impact, and operating cost. "
            "Review the trend weekly, not just after incidents. The objective is not only fixing this post's issue, "
            f"but turning {escape(title)}-type failures into a managed risk class.</p>"
            "<ul>"
            "<li>Reliability: error rate, incident count, MTTR.</li>"
            "<li>Performance: p95 latency, queue delay, throughput.</li>"
            "<li>User impact: conversion, activation, churn signals.</li>"
            "<li>Cost: infra spend per successful transaction.</li>"
            "</ul>"
        )

        return "".join(blocks)

    def _generate_text(
        self,
        prompt: str,
        system_instruction: str | None = None,
        temperature: float = 0.8,
        top_p: float = 0.95,
    ) -> str:
        if self.call_count >= self.settings.max_calls_per_run:
            raise RuntimeError("Gemini per-run call limit reached")
        if not self.settings.api_key or self.settings.api_key == "GEMINI_API_KEY":
            raise RuntimeError("Gemini API 키가 설정되지 않았습니다.")
        effective_system = (
            (system_instruction or "").strip()
            or (self.settings.editor_persona or "").strip()
        )

        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": float(max(0.0, min(1.5, temperature))),
                "topP": float(max(0.1, min(1.0, top_p))),
                "maxOutputTokens": 8192,
            },
        }
        if effective_system:
            body["systemInstruction"] = {
                "parts": [{"text": effective_system}]
            }
        response = None
        try:
            self._respect_request_interval()
            response = requests.post(
                self._endpoint,
                params={"key": self.settings.api_key},
                json=body,
                timeout=90,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = self._http_error_detail(response)
            # Retry with alternate models when current model is unavailable or quota-limited.
            if response is not None and response.status_code in (400, 404, 429):
                active_model = self._active_endpoint_model()
                if response.status_code == 429:
                    if self._is_daily_quota_exceeded(detail):
                        # Daily free-tier exhaustion on current model: rotate through priority fallbacks.
                        self._model_quota_blocked_until[active_model] = (
                            datetime.now(timezone.utc) + timedelta(hours=12)
                        )
                        retried = self._try_fallback_model(
                            prompt,
                            body,
                            exclude_models={active_model},
                        )
                        if retried is not None:
                            return retried
                        raise RuntimeError(
                            f"[DAILY_QUOTA_EXCEEDED] Gemini daily quota exhausted: {detail}"
                        ) from exc
                    wait_min = self._temporary_retry_minutes(detail)
                    self._model_quota_blocked_until[active_model] = (
                        datetime.now(timezone.utc) + timedelta(minutes=wait_min)
                    )
                retried = self._try_fallback_model(
                    prompt,
                    body,
                    exclude_models={active_model},
                )
                if retried is not None:
                    return retried
                if response.status_code == 429:
                    wait_min = self._temporary_retry_minutes(detail)
                    raise RuntimeError(
                        f"[TEMP_429_RETRY_MIN={wait_min}] Gemini temporary rate limit: {detail}"
                    ) from exc
            raise RuntimeError(f"Gemini 요청 실패 ({response.status_code if response is not None else 'HTTP'}): {detail}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"Gemini 네트워크 오류: {exc}") from exc

        self.call_count += 1
        self._last_request_at = datetime.now(timezone.utc)
        data = response.json()

        candidates = data.get("candidates", [])
        if not candidates:
            raise RuntimeError("Gemini response had no candidates")
        parts = candidates[0].get("content", {}).get("parts", [])
        text = "\n".join(part.get("text", "") for part in parts if "text" in part)
        if not text.strip():
            raise RuntimeError("Gemini response text is empty")
        return text

    def _generate_text_forced_model(
        self,
        prompt: str,
        model: str,
        system_instruction: str | None = None,
    ) -> str:
        if self.call_count >= self.settings.max_calls_per_run:
            raise RuntimeError("Gemini per-run call limit reached")
        if not self.settings.api_key or self.settings.api_key == "GEMINI_API_KEY":
            raise RuntimeError("Gemini API 키가 설정되지 않았습니다.")

        effective_system = (
            (system_instruction or "").strip()
            or (self.settings.editor_persona or "").strip()
        )
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.95,
                "topP": 0.95,
                "maxOutputTokens": 2048,
            },
        }
        if effective_system:
            body["systemInstruction"] = {"parts": [{"text": effective_system}]}

        response = None
        try:
            self._respect_request_interval()
            response = requests.post(
                endpoint,
                params={"key": self.settings.api_key},
                json=body,
                timeout=90,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = self._http_error_detail(response)
            if response is not None and response.status_code in (400, 404, 429):
                if response.status_code == 429:
                    if self._is_daily_quota_exceeded(detail):
                        self._model_quota_blocked_until[model] = (
                            datetime.now(timezone.utc) + timedelta(hours=12)
                        )
                        retried = self._try_fallback_model(
                            prompt,
                            body,
                            exclude_models={model},
                        )
                        if retried is not None:
                            return retried
                        raise RuntimeError(
                            f"[DAILY_QUOTA_EXCEEDED] Gemini daily quota exhausted: {detail}"
                        ) from exc
                    wait_min = self._temporary_retry_minutes(detail)
                    self._model_quota_blocked_until[model] = (
                        datetime.now(timezone.utc) + timedelta(minutes=wait_min)
                    )
                    retried = self._try_fallback_model(
                        prompt,
                        body,
                        exclude_models={model},
                    )
                    if retried is not None:
                        return retried
                    raise RuntimeError(
                        f"[TEMP_429_RETRY_MIN={wait_min}] Gemini temporary rate limit: {detail}"
                    ) from exc
                retried = self._try_fallback_model(
                    prompt,
                    body,
                    exclude_models={model},
                )
                if retried is not None:
                    return retried
            raise RuntimeError(f"Gemini 요청 실패 ({response.status_code if response is not None else 'HTTP'}): {detail}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"Gemini 네트워크 오류: {exc}") from exc

        self.call_count += 1
        self._last_request_at = datetime.now(timezone.utc)
        data = response.json()
        candidates = data.get("candidates", [])
        if not candidates:
            raise RuntimeError("Gemini response had no candidates")
        parts = candidates[0].get("content", {}).get("parts", [])
        text = "\n".join(part.get("text", "") for part in parts if "text" in part)
        if not text.strip():
            raise RuntimeError("Gemini response text is empty")
        return text

    def _http_error_detail(self, response: requests.Response | None) -> str:
        if response is None:
            return "응답 없음"
        try:
            payload = response.json()
            err = payload.get("error", {})
            message = err.get("message") or payload
            return str(message)
        except Exception:
            return (response.text or "").strip()[:500] or f"HTTP {response.status_code}"

    def _is_daily_quota_exceeded(self, detail: str) -> bool:
        msg = (detail or "").lower()
        # Treat only hard daily/unsupported-free-tier cases as "daily exhausted".
        # Generic 429 with retry hints should be handled as temporary and rotated.
        if "daily quota exceeded" in msg:
            return True
        if "generaterequestsperday" in msg or "perday" in msg:
            return True
        if "limit: 0" in msg and "free_tier" in msg:
            return True
        if "quota exceeded for metric" in msg and "input_token_count" in msg and "limit: 0" in msg:
            return True
        return False

    def _temporary_retry_minutes(self, detail: str) -> int:
        msg = (detail or "").lower()
        retry_sec = None
        m = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", msg)
        if m:
            try:
                retry_sec = float(m.group(1))
            except Exception:
                retry_sec = None
        if retry_sec is None:
            return random.randint(20, 30)
        # User policy: temporary 429 should come back 20-30 min window.
        base = max(20, min(30, int(round(retry_sec / 60.0)) + 20))
        return base

    def _try_fallback_model(
        self,
        prompt: str,
        body: dict,
        exclude_models: set[str] | None = None,
    ) -> str | None:
        fallback_models = self._discover_fallback_models()
        excluded = {
            self._normalize_model_name(m)
            for m in (exclude_models or set())
            if str(m or "").strip()
        }
        for model in fallback_models:
            if model in excluded:
                continue
            blocked_until = self._model_quota_blocked_until.get(model)
            if blocked_until and datetime.now(timezone.utc) < blocked_until:
                continue
            endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            try:
                self._respect_request_interval()
                response = requests.post(
                    endpoint,
                    params={"key": self.settings.api_key},
                    json=body,
                    timeout=90,
                )
                if response.status_code == 429:
                    detail = self._http_error_detail(response)
                    if self._is_daily_quota_exceeded(detail):
                        self._model_quota_blocked_until[model] = (
                            datetime.now(timezone.utc) + timedelta(hours=12)
                        )
                        continue
                    self._model_quota_blocked_until[model] = (
                        datetime.now(timezone.utc) + timedelta(minutes=self._temporary_retry_minutes(detail))
                    )
                    continue
                response.raise_for_status()
                self.call_count += 1
                self._last_request_at = datetime.now(timezone.utc)
                data = response.json()
                candidates = data.get("candidates", [])
                if not candidates:
                    continue
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "\n".join(part.get("text", "") for part in parts if "text" in part).strip()
                if not text:
                    continue
                self._endpoint = endpoint
                self.settings.model = model
                return text
            except Exception:
                continue
        return None

    def _discover_fallback_models(self) -> list[str]:
        preferred: list[str] = []
        preferred.append(self._normalize_model_name(self.settings.model))
        configured = list(getattr(self.settings, "fallback_models", []) or [])
        for m in configured:
            nm = self._normalize_model_name(m)
            if nm and nm not in preferred:
                preferred.append(nm)
        for m in [
            "gemini-2.0-flash",
            "gemini-2.0-flash-001",
            "gemini-2.5-flash-lite",
            "gemini-2.5-flash",
            "gemini-flash-latest",
            "gemini-2.0-flash-lite",
            "gemini-2.0-flash-lite-001",
            "gemini-flash-lite-latest",
            "gemini-pro-latest",
        ]:
            nm = self._normalize_model_name(m)
            if nm and nm not in preferred:
                preferred.append(nm)
        discovered = self._list_generate_models()
        if discovered:
            # Completion-first: try every available text-capable model in priority order.
            # 1) user-configured preferred models
            # 2) all discovered models (stable first, then preview/exp)
            out: list[str] = []
            discovered_set = set(discovered)
            for m in preferred:
                if m in discovered_set and m not in out:
                    out.append(m)
            tail = [m for m in discovered if m not in out]
            tail.sort(key=self._model_priority)
            out.extend(tail)
            for m in preferred:
                if m not in out:
                    out.append(m)
            return out
        return [m for m in preferred if m]

    def _model_priority(self, model: str) -> tuple[int, str]:
        lower = str(model or "").lower()
        rank = 100
        # Prefer flash text models for free-tier robustness.
        if "flash" in lower:
            rank -= 30
        if "2.0-flash" in lower:
            rank -= 20
        if "2.5-flash-lite" in lower:
            rank -= 15
        elif "2.5-flash" in lower:
            rank -= 10
        if "pro" in lower:
            rank += 8
        # Keep dedicated non-text/specialized families as last resort.
        if any(tag in lower for tag in ("image", "vision", "embedding", "tts", "audio")):
            rank += 80
        if any(tag in lower for tag in ("preview", "exp", "experimental")):
            rank += 20
        return (rank, lower)

    def _normalize_model_name(self, model: str) -> str:
        name = str(model or "").strip()
        if name.startswith("models/"):
            name = name.split("/", 1)[1].strip()
        return name

    def _active_endpoint_model(self) -> str:
        endpoint = str(self._endpoint or "")
        m = re.search(r"/models/([^:]+):", endpoint)
        if m:
            return self._normalize_model_name(m.group(1))
        return self._normalize_model_name(self.settings.model)

    def _list_generate_models(self) -> list[str]:
        now = datetime.now(timezone.utc)
        if self._models_cache is not None:
            ts, cached = self._models_cache
            if (now - ts).total_seconds() < 6 * 3600:
                return cached
        discovered: list[str] = []
        try:
            response = requests.get(
                "https://generativelanguage.googleapis.com/v1beta/models",
                params={"key": self.settings.api_key},
                timeout=30,
            )
            response.raise_for_status()
            models = response.json().get("models", []) or []
            for model in models:
                name = str(model.get("name", ""))
                methods = model.get("supportedGenerationMethods", []) or []
                if not name.startswith("models/"):
                    continue
                if "generateContent" not in methods:
                    continue
                discovered.append(name.split("/", 1)[1])
        except Exception:
            discovered = []
        discovered = sorted(set(discovered))
        self._models_cache = (now, discovered)
        return discovered

    def _respect_request_interval(self) -> None:
        min_gap = max(1, int(getattr(self.settings, "min_request_interval_seconds", 30) or 30))
        if self._last_request_at is None:
            return
        delta = (datetime.now(timezone.utc) - self._last_request_at).total_seconds()
        if delta < min_gap:
            time.sleep(max(0.0, float(min_gap - delta)))

    def reset_run_counter(self) -> None:
        self.call_count = 0

    def _extract_json(self, text: str) -> dict:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {}
        snippet = match.group(0)
        try:
            return json.loads(snippet)
        except json.JSONDecodeError:
            return {}

    def _remove_ai_markers(self, html: str, domain: str = "news_interpretation") -> str:
        replacements = {
            "delve": "look closely",
            "comprehensive": "practical",
            "cutting-edge": "advanced",
            "in conclusion": "to wrap up",
            "CUDA kernel": "AI processing engine",
            "CUDA": "AI acceleration stack",
            "LLVM backend": "system translation tool",
            "LLVM": "code translation tool",
            "vector embedding": "meaning map",
            "embeddings": "meaning maps",
            "latency": "response delay",
            "throughput": "work volume per second",
            "orchestration": "automation coordination",
        }
        out = html
        # Strip internal meta blocks before any other cleanup.
        meta_start = re.escape(str(getattr(self.settings, "meta_block_start", "[[META]]") or "[[META]]"))
        meta_end = re.escape(str(getattr(self.settings, "meta_block_end", "[[/META]]") or "[[/META]]"))
        out = re.sub(
            rf"{meta_start}.*?{meta_end}",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for src, dst in replacements.items():
            out = re.sub(rf"\b{re.escape(src)}\b", dst, out, flags=re.IGNORECASE)
        # Remove strategy/meta wording that should never appear in published body.
        banned_meta = [
            r"\bSEO\b",
            r"\bAlgorithm\b",
            r"\bE-?E-?A-?T\b",
            r"\bTrustworthiness\b",
            r"\bProcess Disclosure\b",
            r"\bsearch ranking\b",
            r"\bhelpful content update\b",
            r"\bcontent farm\b",
            r"\b\d*■+\d*\b",
        ]
        for pat in banned_meta:
            out = re.sub(pat, "", out, flags=re.IGNORECASE)
        if str(domain or "").strip().lower() != "ai_prompt_guide":
            dynamic_leaks = list(getattr(self.settings, "prompt_leak_patterns", []) or [])
            for raw in dynamic_leaks:
                token = str(raw or "").strip()
                if not token:
                    continue
                out = re.sub(re.escape(token), "", out, flags=re.IGNORECASE)
            # Remove incomplete template leak lines.
            out = re.sub(
                r"(for quick take[^<\n]*|you are a system that[^<\n]*|for generated image context[^<\n]*)",
                "",
                out,
                flags=re.IGNORECASE,
            )
        # Remove comma-only keyword dump lines (very common leak form).
        out = re.sub(
            r"^(?:\s*[a-z0-9][a-z0-9\- ]{1,30}\s*,){3,}\s*[a-z0-9][a-z0-9\- ]{1,30}\s*$",
            "",
            out,
            flags=re.IGNORECASE | re.MULTILINE,
        )
        out = re.sub(
            r"\b(workflow checkpoint stage|av reference context|jobtitle|sameas|selected topic)\b",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"https?://(?:www\.)?google\.com/search\?[^\"\s<]+",
            "",
            out,
            flags=re.IGNORECASE,
        )
        # Remove common visual placeholder leaks if model emitted template stubs.
        out = re.sub(
            r"\b(section context visual|concept visual|supporting chart|visual\s*\d+|screenshot\s*\d+)\b",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(r"\s{2,}", " ", out)
        return out

    def _enforce_html_minimum(self, html: str) -> str:
        if "<h2>" in html and "<p>" in html:
            return html
        # Return empty so workflow can trigger regeneration loop instead of publishing template text.
        return ""

    def _resolve_main_keyword(self, candidate: TopicCandidate) -> str:
        entity = str(getattr(candidate, "main_entity", "") or "").strip()
        if entity:
            return entity
        title = re.sub(r"\s+", " ", str(candidate.title or "")).strip()
        if not title:
            return "device troubleshooting"
        words = [w for w in re.findall(r"[A-Za-z0-9]+", title) if len(w) >= 3]
        if not words:
            return "device troubleshooting"
        return " ".join(words[:3])

    def _build_lsi_terms(
        self,
        main_keyword: str,
        long_tail_keywords: list[str],
        title: str,
        body: str,
    ) -> list[str]:
        stop = {
            "the", "and", "for", "with", "this", "that", "from", "into", "your", "their",
            "what", "when", "where", "which", "while", "about", "today", "everyone", "talking",
            "changes", "change", "mean", "means", "using", "guide", "introduction", "workflow",
        }
        seed_text = " ".join([main_keyword, title, body[:1200], *long_tail_keywords])
        tokens = [t.lower() for t in re.findall(r"[A-Za-z][A-Za-z0-9-]{3,}", seed_text)]
        freq = Counter(t for t in tokens if t not in stop)

        fixed = [
            "latest news analysis",
            "root cause analysis",
            "step by step fix",
            "beginner friendly setup",
            "update recovery",
            "connectivity repair",
            "performance recovery",
            "prevention routine",
        ]
        out: list[str] = []
        seen: set[str] = set()

        def push(term: str) -> None:
            norm = re.sub(r"\s+", " ", str(term or "")).strip().lower()
            if not norm or norm in seen:
                return
            seen.add(norm)
            out.append(norm)

        if main_keyword:
            push(main_keyword)
            push(f"{main_keyword} troubleshooting")
            push(f"{main_keyword} fix")

        for kw in long_tail_keywords:
            w = re.sub(r"\?$", "", str(kw or "")).strip().lower()
            if w:
                push(w)

        for token, _ in freq.most_common(16):
            push(token)
            if len(out) >= 12:
                break

        for t in fixed:
            push(t)
            if len(out) >= 12:
                break

        # Guarantee at least 5 LSI terms.
        while len(out) < 5:
            push(fixed[len(out) % len(fixed)])

        return out[:12]

    def _keyword_match_score(
        self,
        title: str,
        body: str,
        target_keywords: list[str] | None,
    ) -> float:
        if not target_keywords:
            return 0.0
        text = f"{title} {body}".lower()
        boost = 0.0
        for kw in target_keywords:
            norm = re.sub(r"\s+", " ", str(kw or "")).strip().lower()
            if not norm:
                continue
            if norm in text:
                # Prioritize title hit over body-only hit.
                if norm in (title or "").lower():
                    boost += 8.0
                else:
                    boost += 4.0
        return boost

    def _audience_accessibility_score(self, title: str, body: str) -> float:
        text = f"{title} {body}".lower()
        mainstream = [
            "not working", "fix", "error", "update", "windows", "mac", "iphone", "galaxy",
            "bluetooth", "wifi", "audio", "sound", "battery", "crash", "slow", "beginner", "how to",
        ]
        nerd = [
            "cuda", "kernel", "llvm", "compiler", "webassembly", "microarchitecture",
            "lsp", "ast", "gpgpu", "tensor core",
        ]
        m_hits = sum(1 for t in mainstream if t in text)
        n_hits = sum(1 for t in nerd if t in text)
        return float((m_hits * 3) - (n_hits * 6))
