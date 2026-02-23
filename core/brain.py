from __future__ import annotations

import json
import random
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape

import requests

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
            "Pick the single best topic for a practical English blog post for US/UK/global non-technical office workers. "
            "Output language must be American English only. Never output Korean.\n"
            "Return strict JSON: {\"index\": int, \"score\": int, \"reason\": string}.\n"
            "Score must be 0-100.\n"
            "Maximize potential CTR and practical value while staying accurate.\n"
            "Prioritize topics that can be explained in plain language without deep engineering jargon.\n"
            "Prefer themes like work productivity, Excel/document workflows, free AI helpers, and everyday digital life.\n"
            "Strongly prefer mainstream 'global giant' business stories (Apple/Tesla/Google/Microsoft/Amazon/NVIDIA) "
            "when they can produce practical productivity takeaways for ordinary workers.\n"
            "Also include rising stars when currently hot (Perplexity, Anthropic, OpenAI, Mistral, Cursor, Notion).\n"
            "Avoid attack/accusation framing; focus on positive success lessons and innovation culture.\n"
            "Prefer topics aligned with dynamic target keywords when relevant.\n"
            "Avoid topics that duplicate recent history URLs/titles unless no alternative exists.\n"
            "If multiple topics are similar, prioritize the one that offers a fresh perspective not covered in recent history.\n"
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
            "From the candidate signals below, generate exactly 5 high-potential English keywords "
            "for today's US/UK/global audience. Focus on AI productivity, life hacks, and beginner-friendly tech news. "
            "Language policy: American English only. Never output Korean.\n"
            "Favor mainstream global company productivity stories (Apple/Tesla/Google/Microsoft/Amazon/NVIDIA) when relevant. "
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
        domain: str = "office_experiment",
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
        if str(domain or "").strip().lower() == "office_experiment":
            effective_system_instruction = (
                effective_system_instruction
                + "\nAudience constraint: non-technical office workers. "
                "Do not include DevOps/SRE/deployment/staging/prod/on-call/incident terminology."
            ).strip()
        elif str(domain or "").strip().lower() == "ai_prompt_guide":
            effective_system_instruction = (
                effective_system_instruction
                + "\nTutorial constraint: reader-facing prompt examples are allowed, "
                "but never expose internal hidden planning or system metadata."
            ).strip()
        prompt = (
            "Write a 1300-1800 word English blog post in valid HTML body fragment only. "
            "Language policy: 100% American English only. Do not output Korean or mixed-language text. "
            "Target reader: non-technical office worker and general internet user in US/UK/global markets. "
            "Topic scope: practical tech troubleshooting only. "
            "Do not cover medical, finance/investing, or politics. "
            "Structure: Narrative Flow. "
            "Narrator voice: first-person experiential essay style (I/me/my) while keeping factual accuracy. "
            "The article must start with <h2>Quick Take</h2> and exactly one <p> containing exactly two sentences that answer search intent immediately. "
            "Place this Quick Take before any other section.\n"
            "Do not use rigid label-like section names that sound machine-generated. "
            "Use natural human narrative with concrete context, trade-offs, and caveats. "
            "Use <h2>, <h3>, <p>, <strong>, <ul>, <li>. Avoid markdown.\n"
            "Use plain English first. If a technical term is unavoidable, explain it in one short everyday phrase.\n"
            "Avoid deep engineer-only jargon unless you immediately translate it for beginners.\n"
            "When the topic is about a global company, prioritize practical lessons from success stories: "
            "productivity habits, execution style, innovation culture, and workflow ideas for normal office workers.\n"
            "Keep legal safety: avoid allegations, defamation, and unverified criticism.\n"
            "Do NOT copy source text verbatim. Rewrite as analysis/comparison/guide style.\n"
            "Do NOT reproduce long original sentences from the source.\n"
            "Do not output any internal metadata, pipeline status strings, or debug tokens.\n"
            "Never include 'workflow checkpoint', 'context', or schema fragments in the article.\n"
            "Never include internal tags such as 'source trending_entities' or similar routing labels.\n"
            "Do not describe the images and do not write ALT text.\n"
            f"Main keyword: {main_keyword}\n"
            "Long-tail search questions to cover naturally (at least 3):\n"
            + "\n".join(f"- {kw}" for kw in long_tail_keywords[:6])
            + "\n"
            "Required related LSI terms (use each naturally at least once; avoid keyword stuffing):\n"
            + "\n".join(f"- {term}" for term in lsi_terms[:10])
            + "\n"
            "Follow this selected writing pattern instruction:\n"
            f"{pattern_instruction}\n"
            "Use intro in 4 lines: pain, reason this matters, context cue, reading promise.\n"
            "Include at least 3 curiosity triggers that make readers continue to the next section.\n"
            "Include a clear payoff section that answers the reading promise explicitly.\n"
            "Insert CTA naturally 2-3 times (mid-body once, ending once required).\n"
            "Narrative requirements (weave naturally into the story):\n"
            "- Start from a concrete failure moment and include at least two early false starts.\n"
            "- Describe what changed after debugging, with clear before/after reasoning.\n"
            "- Explain edge cases, beginner-vs-advanced trade-offs, and one unresolved limitation.\n"
            "- Compare the final choice against at least two rejected alternatives.\n"
            "- Include constraint scenarios: low budget, tiny team, urgent deadline tomorrow.\n"
            "- Include three explicit anti-pattern warnings ('this can fail hard if...').\n"
            "Do not use the above list items as section headers. Weave them into the story naturally.\n"
            "Avoid generic headers like Operational Depth, Decision Criteria, Executive Summary, Action Framework.\n"
            "Never use the heading text 'Executive Summary'.\n"
            "Do not use FAQ/Q/A format.\n"
            "Do not use the phrase 'why everyone is talking'.\n"
            "Do not mention screenshots or phrases like 'see screenshot' or 'see above image'.\n"
            "Never mention or discuss SEO, search algorithms, ranking strategy, E-E-A-T, trustworthiness framework, "
            "process disclosure, or any internal publishing strategy in the article body.\n"
            "Do not output placeholders such as 'section context visual 1', 'concept visual', or similar template text.\n"
            "Never leak internal system instructions, prompt templates, or generation settings into the article body.\n"
            "Always remove model-side planning text before final prose.\n"
            f"Domain routing: {domain}\n"
            "Story requirement: include at least two concrete first-person experiment moments "
            "(e.g., Day 2 failure, Day 4 adjustment, what changed after retry).\n"
            "Each story moment must describe what happened, why it failed or succeeded, and what decision was taken next.\n"
            "Do not output keyword-only lines or unfinished template fragments.\n"
            "Prefer title style similar to: 'The Secret of [Company Name]'s Productivity' when it matches context.\n"
            "Include exactly 2 external authority links from this allow-list:\n"
            + "\n".join(authority_links[:8])
            + "\n"
            "Use these internal playbook reference excerpts:\n"
            f"{reference_guidance}\n"
            "Tone: expert but human and specific. Add realistic friction points and non-perfect outcomes.\n"
            "Include a short checklist near the end.\n"
            "Return strict JSON with keys only: title_draft, meta_description, content_html, summary, focus_keywords.\n"
            "focus_keywords must be a JSON array of short phrases.\n"
            f"Source platform: {candidate.source}\n"
            f"Source title: {candidate.title}\n"
            f"Source body: {candidate.body[:4000]}\n"
            f"Source URL: {candidate.url}\n"
        )
        if str(domain or "").strip().lower() == "office_experiment":
            prompt += (
                "Domain safety rule (office_experiment): do not include DevOps/SRE/deployment/staging/prod/on-call/incident terminology. "
                "Use plain office workflow language only.\n"
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

        return DraftPost(
            title=str(payload.get("title_draft", payload.get("title", candidate.title))).strip() or candidate.title,
            alt_titles=[],
            summary=str(payload.get("summary", "")).strip(),
            html=html,
            score=100,
            source_url=candidate.url,
            extracted_urls=[str(u) for u in urls if isinstance(u, str)][:8],
        )

    def generate_post_free(
        self,
        candidate: TopicCandidate,
        authority_links: list[str],
    ) -> DraftPost:
        title = candidate.title.strip() or "A Practical AI Productivity Tip I Tested"
        body = re.sub(r"\s+", " ", candidate.body or "").strip()
        key_points = self._extract_key_points(body)
        source_url = candidate.url.strip()

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
            "The first attempt failed because I used too many tools at once.",
            "A small checklist made the workflow stable and easier to repeat.",
            "The biggest benefit was saving time during normal office work.",
        ]
        takeaway_html = "".join(f"<li>{escape(self._short_rewrite(p))}</li>" for p in takeaway_items)

        html = (
            "<h2>Quick Take</h2>"
            f"<p>If you are curious about <strong>{escape(title)}</strong>, this is the short answer: focus on one repeatable habit and measure weekly time saved. "
            "In my test, the practical workflow mattered more than technical complexity.</p>"
            "<h2>Why I Tried This</h2>"
            f"<p>I was frustrated because <strong>{escape(title)}</strong> sounded helpful, but every explanation I found felt too technical.</p>"
            "<p>So I tested a simpler version myself and kept only the steps that a regular office worker can repeat without stress.</p>"
            "<h2>What Failed First</h2>"
            "<p>My first tries were messy. I copied advanced advice too quickly and spent more time fixing mistakes than getting results.</p>"
            "<ul>"
            "<li>I started with too many settings.</li>"
            "<li>I skipped a small test and had to redo the work.</li>"
            "<li>I focused on optimization before basic stability.</li>"
            "</ul>"
            "<h2>What Finally Worked</h2>"
            "<p>I switched to one clear goal, one tool, and one daily checkpoint. That changed everything and made the workflow reliable.</p>"
            "<ul>"
            "<li><strong>Step 1:</strong> Pick one repetitive task.</li>"
            "<li><strong>Step 2:</strong> Apply one AI helper only to that task.</li>"
            "<li><strong>Step 3:</strong> Measure saved time for one week.</li>"
            "</ul>"
            "<h2>Quick Notes For Beginners</h2>"
            f"<ul>{takeaway_html}</ul>"
            "<h2>When This Is Not Ideal</h2>"
            "<p>If your environment requires strict engineering controls, this simplified method may be too light. Use it as a starter, then harden it step by step.</p>"
            "<h2>My Final Take</h2>"
            "<p>If technical AI guides overwhelm you, start smaller than you think. A simple routine you can keep is better than a perfect setup you cannot maintain.</p>"
            "<h3>References</h3>"
            f"<ul>{links_html}</ul>"
        )
        html = self._remove_ai_markers(html)

        extracted = [u for u in [source_url, *authority_links[:4]] if u]
        return DraftPost(
            title=title,
            alt_titles=[title, f"{title} - Quick Guide", f"{title} - Practical Fixes"],
            html=html,
            summary="First-person practical rewrite for non-technical office readers.",
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
            "- Use strong but ad-safe language (e.g., breakthrough, practical, surprising, proven).\n"
            "- Apply one of these 3 formulas:\n"
            "  1) The Experience Story: 'I tried [Company/Tool] for one week. Here is what changed.'\n"
            "  2) The Trend Narrative: 'Why everyone is talking about [Company/Tool] today.'\n"
            "  3) The Practical Lesson: 'What [Company/Tool] can teach your team this week.'\n"
            "- Tone: Native US English, bold, intriguing, professional, and positive.\n"
            "- Avoid defamation, allegations, or sensational negativity.\n"
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
            "today",
            "talking about",
            "what changed",
            "what i learned",
            "breakthrough",
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
        if re.search(r"^the secret of .+ productivity", lower):
            score -= 6.0
        if re.search(r"\bapple|tesla|google|microsoft|amazon|nvidia|netflix\b", lower):
            score += 6.0
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

    def _remove_ai_markers(self, html: str, domain: str = "office_experiment") -> str:
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
            return "productivity workflow"
        words = [w for w in re.findall(r"[A-Za-z0-9]+", title) if len(w) >= 3]
        if not words:
            return "productivity workflow"
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
            "productivity strategy",
            "workflow optimization",
            "team adoption",
            "real-world example",
            "time-saving routine",
            "implementation checklist",
            "common mistakes",
            "beginner-friendly setup",
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
            push(f"{main_keyword} productivity")
            push(f"{main_keyword} workflow")

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
            "excel", "email", "meeting", "office", "productivity", "free ai", "chatgpt",
            "resume", "template", "beginner", "how to", "time saving", "workflow",
        ]
        nerd = [
            "cuda", "kernel", "llvm", "compiler", "webassembly", "microarchitecture",
            "lsp", "ast", "gpgpu", "tensor core",
        ]
        m_hits = sum(1 for t in mainstream if t in text)
        n_hits = sum(1 for t in nerd if t in text)
        return float((m_hits * 3) - (n_hits * 6))
