from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import requests

from .settings import ContentModeSettings, SourceSettings


@dataclass
class TopicCandidate:
    source: str
    title: str
    body: str
    score: int
    url: str
    main_entity: str = ""
    long_tail_keywords: list[str] = field(default_factory=list)
    meta: dict[str, object] = field(default_factory=dict)


class SourceScout:
    _POPULAR_ENTITIES = {
        "Apple",
        "Tesla",
        "Google",
        "Microsoft",
        "Amazon",
        "OpenAI",
        "Anthropic",
        "Perplexity",
        "Meta",
        "Netflix",
    }
    _ENTITY_ALIASES = {
        "Apple": ["apple", "iphone", "ios", "macbook"],
        "Tesla": ["tesla", "elon musk"],
        "Google": ["google", "alphabet", "gemini"],
        "Microsoft": ["microsoft", "copilot", "azure", "windows"],
        "Amazon": ["amazon", "aws"],
        "NVIDIA": ["nvidia"],
        "Meta": ["meta", "facebook", "instagram", "threads"],
        "Netflix": ["netflix"],
        "OpenAI": ["openai", "chatgpt", "gpt-4", "gpt-5"],
        "Anthropic": ["anthropic", "claude"],
        "Perplexity": ["perplexity"],
        "Mistral": ["mistral ai", "mistral"],
        "Notion": ["notion"],
        "Figma": ["figma"],
        "Canva": ["canva"],
        "Cursor": ["cursor", "anysphere"],
        "Scale AI": ["scale ai", "scale"],
        "xAI": ["xai", "grok"],
    }
    _HOT_TERMS = [
        "launch",
        "released",
        "announce",
        "announced",
        "funding",
        "raises",
        "acquisition",
        "partnership",
        "rollout",
        "breaking",
        "trend",
        "viral",
        "new feature",
    ]
    _MAINSTREAM_TERMS = [
        "breaking",
        "update",
        "released",
        "launch",
        "announced",
        "exclusive",
        "report",
        "analysis",
        "trend",
        "future",
        "market",
        "impact",
        "strategy",
        "windows",
        "mac",
        "iphone",
        "galaxy",
        "android",
        "ai",
        "llm",
        "security",
        "policy",
        "regulation",
        "innovation",
        "roadmap",
        "leak",
        "review",
        "comparison",
        "versus",
    ]
    _NERD_HEAVY_TERMS = [
        "cuda",
        "kernel",
        "llvm",
        "backend",
        "compiler",
        "kubernetes",
        "container runtime",
        "webassembly",
        "assembly",
        "microarchitecture",
        "quantization",
        "tensor core",
        "gpgpu",
        "lsp",
        "ast",
        "monorepo tooling",
        "opcache",
        "syscall",
        "throughput benchmark",
    ]
    _DEEP_TECH_BLOCK_TERMS = [
        "nvidia tax",
        "cuda",
        "kernel",
        "llvm",
        "tensor core",
        "webassembly",
        "compiler backend",
        "driver stack",
        "fp8",
        "quantization",
    ]
    _NEWS_INTENT_TERMS = [
        "breaking",
        "announced",
        "revealed",
        "significant",
        "major update",
        "new feature",
        "industry shift",
        "market move",
        "analysis",
    ]
    _DEVICE_TOKENS = [
        "windows",
        "mac",
        "macos",
        "iphone",
        "ios",
        "galaxy",
        "samsung",
        "android",
    ]
    _FEATURE_TOKENS = [
        "wifi",
        "wi-fi",
        "bluetooth",
        "usb",
        "printer",
        "microphone",
        "mic",
        "camera",
        "keyboard",
        "mouse",
        "driver",
        "vpn",
        "ethernet",
        "audio",
        "sound",
        "wifi",
        "battery",
        "charging",
        "speaker",
    ]

    def __init__(
        self,
        settings: SourceSettings,
        root: Path,
        content_mode: ContentModeSettings | None = None,
        intelligence: Any = None,
    ) -> None:
        self.settings = settings
        self.root = root
        self.content_mode = content_mode or ContentModeSettings()
        self.intelligence = intelligence
        self._longtail_cache: dict[str, tuple[datetime, list[str]]] = {}
        self._longtail_cache_ttl = timedelta(hours=6)

    def collect(self) -> list[TopicCandidate]:
        candidates: list[TopicCandidate] = []
        mode = (self.settings.mode or "mixed").lower()
        strict_mode = str(getattr(self.content_mode, "mode", "") or "").strip().lower() == "news_interpretation_only"
        trend_candidates: list[TopicCandidate] = []

        if mode in {"manual_seed", "seed", "manual"}:
            return self._collect_seeds()

        if mode in {"mixed", "stackexchange"}:
            candidates.extend(self._collect_stackexchange())
        if mode in {"mixed", "hackernews"}:
            candidates.extend(self._collect_hackernews())
        if mode in {"mixed", "github"}:
            candidates.extend(self._collect_github())
        if (not strict_mode) and mode in {"mixed", "hackernews", "manual_seed", "seed", "manual"}:
            trend_candidates = self._collect_trending_entity_topics()
            candidates.extend(trend_candidates)
        # Fixed entity pool is now strict fallback only when real-time scouting yields zero.
        if (not strict_mode) and mode in {"mixed", "manual_seed", "seed", "manual"} and not trend_candidates:
            candidates.extend(self._collect_global_giant_topics())

        if not candidates:
            candidates.extend(self._collect_seeds())

        if strict_mode:
            candidates = self._apply_news_mode_filter(candidates)

        candidates = self._enrich_candidates_with_longtail(candidates)
        candidates = self._filter_mass_market_candidates(candidates)

        for c in candidates:
            c.score = int(max(0, c.score + self._audience_fit_boost(c.title, c.body)))

        sorted_items = sorted(candidates, key=lambda x: x.score, reverse=True)
        if not strict_mode:
            sorted_items = self._ensure_global_giant_presence(sorted_items)
        return sorted_items[: self.settings.max_candidates]

    def get_trending_entities(self, within_hours: int = 24, limit: int = 8) -> list[str]:
        signals = self._build_entity_trend_signals(within_hours=within_hours)
        if not signals:
            return []
        qualified = [
            row for row in signals
            if float(row.get("trend_score", 0.0)) >= 70.0
            or float(row.get("surge_pct", 0.0)) >= 20.0
        ]
        if not qualified:
            return []
        qualified.sort(
            key=lambda x: (
                float(x.get("trend_score", 0.0)),
                float(x.get("surge_pct", 0.0)),
                float(x.get("mentions_24h", 0.0)),
            ),
            reverse=True,
        )
        return [str(row.get("entity", "")).strip() for row in qualified[: max(1, int(limit))] if str(row.get("entity", "")).strip()]

    def _build_entity_trend_signals(self, within_hours: int = 24) -> list[dict]:
        headlines: list[dict] = []
        headlines.extend(self._fetch_hn_headlines(limit=80))
        headlines.extend(self._fetch_reddit_headlines(limit_per_sub=25))
        headlines.extend(self._fetch_it_media_headlines(limit_per_feed=20))
        if not headlines:
            return []

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=max(1, int(within_hours)))
        cutoff_1h = now - timedelta(hours=1)
        mentions_24h: Counter[str] = Counter()
        mentions_1h: Counter[str] = Counter()
        hot_24h: Counter[str] = Counter()

        for row in headlines:
            title = str(row.get("title", "")).strip()
            if not title:
                continue
            ts = row.get("ts")
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
            if isinstance(ts, datetime) and ts < cutoff:
                continue
            lower = title.lower()
            hot_bonus = 1 if any(term in lower for term in self._HOT_TERMS) else 0
            entities = self._extract_entities_from_text(title)
            for entity in entities:
                mentions_24h[entity] += 1 + hot_bonus
                hot_24h[entity] += hot_bonus
                if isinstance(ts, datetime) and ts >= cutoff_1h:
                    mentions_1h[entity] += 1 + hot_bonus

        trend_terms = self._fetch_google_trends_terms(limit=120)
        trend_hits: Counter[str] = Counter()
        for term in trend_terms:
            lower = str(term or "").lower()
            if not lower:
                continue
            for entity, aliases in self._ENTITY_ALIASES.items():
                if any(str(alias).lower() in lower for alias in aliases):
                    trend_hits[entity] += 1

        entities = set(mentions_24h.keys()) | set(trend_hits.keys())
        signals: list[dict] = []
        for entity in entities:
            m24 = float(mentions_24h.get(entity, 0))
            m1 = float(mentions_1h.get(entity, 0))
            trend_hit = float(trend_hits.get(entity, 0))
            hot = float(hot_24h.get(entity, 0))
            baseline_prev_hour = max(0.1, (m24 - m1) / 23.0)
            surge_pct = ((m1 - baseline_prev_hour) / baseline_prev_hour) * 100.0
            mention_score = min(100.0, m24 * 12.0)
            trend_score = min(
                100.0,
                (mention_score * 0.65) + min(30.0, trend_hit * 6.0) + min(20.0, hot * 3.0),
            )
            signals.append(
                {
                    "entity": entity,
                    "mentions_24h": m24,
                    "mentions_1h": m1,
                    "surge_pct": surge_pct,
                    "trend_score": trend_score,
                }
            )
        return signals

    def _collect_trending_entity_topics(self) -> list[TopicCandidate]:
        entities = self.get_trending_entities(within_hours=24, limit=12)
        out: list[TopicCandidate] = []
        title_patterns = [
            "{entity} Breaking News: What this major update means for users",
            "Inside the {entity} Strategic Shift: Analysis and Outlook",
            "Why Everyone is Talking About {entity} This Week",
            "{entity} Future Roadmap: Leaks, Rumors, and Confirmed Features",
            "The Real Impact of {entity}'s Latest Announcement",
        ]
        for idx, entity in enumerate(entities, start=1):
            title = title_patterns[(idx - 1) % len(title_patterns)].format(entity=entity)
            long_tail = self._derive_long_tail_questions(entity, title)
            if len(long_tail) < 5:
                # Drop weak/noisy trends that cannot provide enough semantic expansion.
                continue
            body = (
                f"{entity} has seen a fresh spike in industry discussion and news coverage. "
                "Focus this article on deep analysis and interpretation for a general tech audience, "
                "explaining why this matters, who wins, and what the future holds for this entity. "
                "Maintain a professional editorial tone and focus on long-term implications."
            )
            out.append(
                TopicCandidate(
                    source="trending_entities",
                    title=title,
                    body=body,
                    score=180 - min(60, idx * 3),
                    url=f"https://duckduckgo.com/?q={entity.replace(' ', '+')}+news+analysis",
                    main_entity=entity,
                    long_tail_keywords=long_tail[:6],
                )
            )
        return out

    def _collect_global_giant_topics(self) -> list[TopicCandidate]:
        templates = [
            ("Apple", "future hardware ecosystem and software services"),
            ("Tesla", "autonomous driving progress and market strategy"),
            ("Google", "AI search evolution and ecosystem integration"),
            ("Microsoft", "AI copilot expansion and enterprise strategy"),
            ("Amazon", "logistics innovation and cloud service shifts"),
            ("OpenAI", "next-gen LLM development and safety protocols"),
            ("Anthropic", "AI alignment breakthroughs and market positioning"),
            ("Netflix", "content strategy and interactive streaming trends"),
        ]
        out: list[TopicCandidate] = []
        title_patterns = [
            "{company} Strategic Analysis: The long-term vision for 2026",
            "How {company} is Redefining Tech Trends This Quarter",
            "{company} Market Impact: Who gains and who loses?",
            "The Unfiltered Truth About {company}'s Latest Move",
            "Why {company} Matters More Than Ever in the Current Tech Landscape",
        ]
        for company, angle in templates:
            pick = title_patterns[(len(out)) % len(title_patterns)]
            title = pick.format(company=company)
            long_tail = self._derive_long_tail_questions(company, title)
            body = (
                f"A deep-dive analysis article about {company} and {angle}. "
                "Focus on market implications, technological innovation, and strategic importance. "
                "Keep tone insightful and forward-looking for a mainstream editorial audience."
            )
            out.append(
                TopicCandidate(
                    source="global_giants",
                    title=title,
                    body=body,
                    score=135,
                    url=f"https://www.{company.lower()}.com/",
                    main_entity=company,
                    long_tail_keywords=long_tail[:6],
                )
            )
        return out

    def _ensure_global_giant_presence(self, items: list[TopicCandidate]) -> list[TopicCandidate]:
        if not items:
            return items
        trending = [i for i in items if i.source == "trending_entities"]
        giants = [i for i in items if i.source == "global_giants"]
        others = [i for i in items if i.source not in {"trending_entities", "global_giants"}]
        # Trend-first: prioritize real-time hot entities, then keep a giant-company buffer.
        merged = trending[:10] + giants[:6] + others
        out: list[TopicCandidate] = []
        seen: set[str] = set()
        for row in merged:
            key = (row.title or "").strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out

    def _audience_fit_boost(self, title: str, body: str) -> int:
        text = f"{title} {body}".lower()
        mainstream_hits = sum(1 for term in self._MAINSTREAM_TERMS if term in text)
        nerd_hits = sum(1 for term in self._NERD_HEAVY_TERMS if term in text)
        # Boost practical mainstream topics and heavily down-rank niche engineer-only topics.
        boost = (mainstream_hits * 6) - (nerd_hits * 10)
        if mainstream_hits == 0 and nerd_hits >= 2:
            boost -= 15
        return boost

    def _filter_mass_market_candidates(self, candidates: list[TopicCandidate]) -> list[TopicCandidate]:
        if not candidates:
            return candidates
        out: list[TopicCandidate] = []
        strict_mode = str(getattr(self.content_mode, "mode", "") or "").strip().lower() == "news_interpretation_only"
        for c in candidates:
            title = str(getattr(c, "title", "") or "")
            body = str(getattr(c, "body", "") or "")
            text = f"{title} {body}".lower()
            if strict_mode and not self._passes_news_mode(c):
                continue
            if any(term in text for term in self._DEEP_TECH_BLOCK_TERMS):
                continue

            entity = (getattr(c, "main_entity", "") or "").strip()
            if not entity:
                entity = self._extract_main_entity(f"{title} {body}")
                c.main_entity = entity
            source = str(getattr(c, "source", "") or "").strip().lower()

            mainstream_hits = sum(1 for term in self._MAINSTREAM_TERMS if term in text)
            nerd_hits = sum(1 for term in self._NERD_HEAVY_TERMS if term in text)
            if nerd_hits >= 2 and mainstream_hits == 0:
                continue

            # Keep dynamic trend picks even when entity is not in static pool.
            if source in {"trending_entities", "global_giants"} and not strict_mode:
                out.append(c)
                continue

            if entity and mainstream_hits >= 1:
                out.append(c)
                continue
            if mainstream_hits >= 2:
                out.append(c)

        return out if out else candidates

    def _apply_news_mode_filter(self, candidates: list[TopicCandidate]) -> list[TopicCandidate]:
        out: list[TopicCandidate] = []
        for c in candidates or []:
            if not self._passes_news_mode(c):
                continue
            cleaned_title = re.sub(r"\s+", " ", str(getattr(c, "title", "") or "")).strip(" -:")
            cleaned_title = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", cleaned_title)
            cleaned_title = re.sub(r"\s+", " ", cleaned_title).strip()
            if not cleaned_title:
                continue
            c.title = cleaned_title
            out.append(c)
        if out:
            return out
        # deterministic fallback pool to avoid empty candidate set
        fallback_titles = [
            "How AI is Revolutionizing Personal Productivity in 2026",
            "The Future of Computing: Why Your Next Device Might Not Have a Screen",
            "Breaking Down the Latest Tech Regulation: What Users Need to Know",
            "Beyond the Hype: A Realistic Look at This Year's Innovation Trends",
        ]
        return [
            TopicCandidate(
                source="fallback_news",
                title=t,
                body="Strategic news analysis and trend interpretation for a mainstream tech audience.",
                score=120 - idx,
                url=f"https://duckduckgo.com/?q={quote_plus(t)}",
                main_entity="",
                long_tail_keywords=[],
            )
            for idx, t in enumerate(fallback_titles, start=1)
        ]

    def _passes_news_mode(self, candidate: TopicCandidate) -> bool:
        text = f"{getattr(candidate, 'title', '')} {getattr(candidate, 'body', '')}".lower()
        banned = [str(x or "").strip().lower() for x in (getattr(self.content_mode, "banned_topic_keywords", []) or []) if str(x or "").strip()]
        if any(token in text for token in banned):
            return False
        # 뉴스 모드에서는 특정 장치 토큰이 없어도 괜찮으나, 뉴스성 키워드는 있어야 함
        has_news_intent = any(token in text for token in self._NEWS_INTENT_TERMS)
        has_mainstream = any(token in text for token in self._MAINSTREAM_TERMS)
        return bool(has_news_intent or has_mainstream)

    def _extract_feature_token(self, text: str) -> str:
        if self.intelligence:
            return self.intelligence.infer_feature_token(text)
        return ""

    def _infer_device_token(self, text: str) -> str:
        if self.intelligence:
            return self.intelligence.infer_device_type(text)
        return ""

    def _extract_device_hint(self, text: str, entity: str = "") -> str:
        if self.intelligence:
            return self.intelligence.infer_device_hint(text, entity)
        return ""

    def _extract_feature_hint(self, text: str) -> str:
        if self.intelligence:
            return self.intelligence.infer_feature_hint(text)
        return ""

    def _normalize_troubleshoot_title(self, title: str, entity: str = "", body: str = "") -> str:
        clean = re.sub(r"\s+", " ", str(title or "")).strip()
        if not clean:
            return ""
        # Legacy helper retained for compatibility: never rewrite to generic templates.
        clean = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", clean)
        clean = re.sub(r"\s+", " ", clean).strip(" -:")
        return clean

    def _stackexchange_site_specs(self) -> list[dict[str, str]]:
        specs: list[dict[str, str]] = []
        raw = getattr(self.settings, "stackexchange_sites", None)
        if isinstance(raw, list):
            for row in raw:
                if not isinstance(row, dict):
                    continue
                site = str(row.get("site", "") or "").strip().lower()
                tagged = str(row.get("tagged", "") or "").strip()
                if not site:
                    continue
                specs.append({"site": site, "tagged": tagged})
        if specs:
            return specs
        site = str(getattr(self.settings, "stackexchange_site", "superuser") or "superuser").strip().lower()
        tagged = str(getattr(self.settings, "stackexchange_tagged", "") or "").strip()
        return [{"site": site or "superuser", "tagged": tagged}]

    def _collect_seeds(self) -> list[TopicCandidate]:
        seed_path = self.root / self.settings.seeds_path
        if not seed_path.exists():
            return []

        def _to_candidate(item: dict) -> TopicCandidate:
            long_tail_raw = item.get("long_tail_keywords", [])
            long_tail = (
                [str(x).strip() for x in long_tail_raw if str(x).strip()]
                if isinstance(long_tail_raw, list)
                else []
            )
            return TopicCandidate(
                source=str(item.get("source", "manual")).strip() or "manual",
                title=str(item.get("title", "")).strip(),
                body=str(item.get("body", "")).strip(),
                score=int(item.get("score", 50)),
                url=str(item.get("url", "")).strip(),
                main_entity=str(item.get("main_entity", "")).strip(),
                long_tail_keywords=long_tail[:6],
            )

        out: list[TopicCandidate] = []
        if seed_path.suffix.lower() == ".json":
            try:
                raw = json.loads(seed_path.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                raw = []
            rows = []
            if isinstance(raw, list):
                rows = raw
            elif isinstance(raw, dict) and isinstance(raw.get("topics"), list):
                rows = raw.get("topics", [])
            for row in rows:
                if isinstance(row, dict):
                    out.append(_to_candidate(row))
            return out

        # Legacy JSONL path support.
        for line in seed_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                out.append(_to_candidate(item))
        return out

    def _collect_stackexchange(self) -> list[TopicCandidate]:
        endpoint = "https://api.stackexchange.com/2.3/questions"
        out: list[TopicCandidate] = []
        seen: set[str] = set()
        for spec in self._stackexchange_site_specs():
            site = str(spec.get("site", "") or "").strip().lower()
            tagged = str(spec.get("tagged", "") or "").strip()
            if not site:
                continue
            for sort_mode in ("activity", "creation", "votes"):
                params = {
                    "order": "desc",
                    "sort": sort_mode,
                    "site": site,
                    "pagesize": 35,
                    "filter": "withbody",
                }
                if tagged:
                    params["tagged"] = tagged
                try:
                    response = requests.get(endpoint, params=params, timeout=30)
                    response.raise_for_status()
                    items = response.json().get("items", [])
                except Exception:
                    continue
                for item in items:
                    score = int(item.get("score", 0))
                    if score < self.settings.stackexchange_min_score:
                        continue
                    title = str(item.get("title", "")).strip()
                    url = str(item.get("link", "")).strip()
                    key = (url or title.lower()).strip()
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    out.append(
                        TopicCandidate(
                            source=f"stackexchange:{site}:{sort_mode}",
                            title=title,
                            body=str(item.get("body_markdown", "") or item.get("body", ""))[:4000],
                            score=score,
                            url=url,
                        )
                    )
        return out

    def _collect_hackernews(self) -> list[TopicCandidate]:
        top_ids_resp = requests.get(
            "https://hacker-news.firebaseio.com/v0/topstories.json", timeout=30
        )
        top_ids_resp.raise_for_status()
        story_ids = top_ids_resp.json()[:30]

        out: list[TopicCandidate] = []
        for story_id in story_ids:
            item_resp = requests.get(
                f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json", timeout=30
            )
            if item_resp.status_code != 200:
                continue
            item = item_resp.json() or {}
            score = int(item.get("score", 0))
            if score < self.settings.hn_min_score:
                continue
            title = str(item.get("title", "")).strip()
            url = str(item.get("url", "")).strip() or f"https://news.ycombinator.com/item?id={story_id}"
            out.append(
                TopicCandidate(
                    source="hackernews",
                    title=title,
                    body=f"Hacker News discussion topic: {title}",
                    score=score,
                    url=url,
                )
            )
        return out

    def _fetch_hn_headlines(self, limit: int = 80) -> list[dict]:
        out: list[dict] = []
        try:
            resp = requests.get(
                "https://hn.algolia.com/api/v1/search_by_date",
                params={"tags": "story", "hitsPerPage": max(10, min(100, int(limit)))},
                timeout=25,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", []) or []
            for h in hits:
                title = str(h.get("title") or "").strip()
                if not title:
                    continue
                created = str(h.get("created_at") or "").strip()
                ts = None
                if created:
                    try:
                        ts = datetime.fromisoformat(created.replace("Z", "+00:00")).astimezone(timezone.utc)
                    except Exception:
                        ts = None
                out.append({"title": title, "ts": ts})
        except Exception:
            pass
        return out

    def _fetch_reddit_headlines(self, limit_per_sub: int = 25) -> list[dict]:
        out: list[dict] = []
        subs = ["techsupport", "WindowsHelp", "mac", "iphone", "androidapps", "sysadmin"]
        headers = {"User-Agent": "RezeroAgent/2.5 (trend scout)"}
        for sub in subs:
            try:
                resp = requests.get(
                    f"https://www.reddit.com/r/{sub}/hot.json",
                    params={"limit": max(5, min(50, int(limit_per_sub))), "raw_json": 1},
                    headers=headers,
                    timeout=20,
                )
                if resp.status_code != 200:
                    continue
                rows = (((resp.json() or {}).get("data") or {}).get("children") or [])
                for row in rows:
                    data = row.get("data", {}) if isinstance(row, dict) else {}
                    title = str(data.get("title", "")).strip()
                    if not title:
                        continue
                    ts = None
                    try:
                        if data.get("created_utc") is not None:
                            ts = datetime.fromtimestamp(float(data.get("created_utc")), tz=timezone.utc)
                    except Exception:
                        ts = None
                    out.append({"title": title, "ts": ts})
            except Exception:
                continue
        return out

    def _fetch_it_media_headlines(self, limit_per_feed: int = 20) -> list[dict]:
        feeds = [
            "https://techcrunch.com/feed/",
            "https://www.theverge.com/rss/index.xml",
            "https://venturebeat.com/feed/",
            "https://www.wired.com/feed/rss",
        ]
        out: list[dict] = []
        title_re = re.compile(r"<title>(.*?)</title>", flags=re.IGNORECASE | re.DOTALL)
        pub_re = re.compile(r"<pubDate>(.*?)</pubDate>", flags=re.IGNORECASE | re.DOTALL)
        for feed in feeds:
            try:
                resp = requests.get(feed, timeout=20)
                if resp.status_code != 200:
                    continue
                xml = resp.text or ""
                titles = title_re.findall(xml)
                pubs = pub_re.findall(xml)
                for idx, raw_title in enumerate(titles[: max(1, int(limit_per_feed)) + 1]):
                    title = re.sub(r"<[^>]+>", " ", raw_title)
                    title = re.sub(r"\s+", " ", title).strip()
                    if not title:
                        continue
                    if title.lower() in {"rss", "feed", "techcrunch", "the verge"}:
                        continue
                    ts = None
                    if idx < len(pubs):
                        raw_pub = re.sub(r"\s+", " ", pubs[idx]).strip()
                        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"):
                            try:
                                ts = datetime.strptime(raw_pub, fmt).astimezone(timezone.utc)
                                break
                            except Exception:
                                continue
                    out.append({"title": title, "ts": ts})
            except Exception:
                continue
        return out

    def _extract_entities_from_text(self, text: str) -> list[str]:
        lower = (text or "").lower()
        found: list[str] = []
        for entity, aliases in self._ENTITY_ALIASES.items():
            if any(alias in lower for alias in aliases):
                found.append(entity)
        return found

    def _extract_main_entity(self, text: str) -> str:
        entities = self._extract_entities_from_text(text)
        return entities[0] if entities else ""

    def _enrich_candidates_with_longtail(self, candidates: list[TopicCandidate]) -> list[TopicCandidate]:
        if not candidates:
            return candidates
        out: list[TopicCandidate] = []
        for c in candidates:
            entity = (getattr(c, "main_entity", "") or "").strip()
            if not entity:
                entity = self._extract_main_entity(f"{c.title} {c.body}")
            longtails = self._derive_long_tail_questions(entity, c.title)
            c.main_entity = entity
            c.long_tail_keywords = longtails[:6]
            out.append(c)
        return out

    def _entity_aliases(self, entity: str) -> list[str]:
        if not entity:
            return []
        aliases = list(self._ENTITY_ALIASES.get(entity, []) or [])
        if entity.lower() not in [a.lower() for a in aliases]:
            aliases.append(entity.lower())
        return [a.lower() for a in aliases if a]

    def _derive_long_tail_questions(self, entity: str, fallback_title: str = "") -> list[str]:
        if not entity:
            return []
        key = entity.lower().strip()
        now = datetime.now(timezone.utc)
        cached = self._longtail_cache.get(key)
        if cached and (now - cached[0]) < self._longtail_cache_ttl:
            return list(cached[1])

        aliases = self._entity_aliases(entity)
        signals: list[str] = []
        for term in self._fetch_google_trends_terms(limit=120):
            lower = term.lower()
            if any(a in lower for a in aliases):
                signals.append(term)
        for row in self._fetch_hn_headlines(limit=120):
            title = str(row.get("title", "")).strip()
            lower = title.lower()
            if title and any(a in lower for a in aliases):
                signals.append(title)

        fragments: list[str] = []
        fragment_stop = {
            "show", "ask", "hn", "til", "today", "latest", "thread", "threads",
            "comment", "comments", "news", "update", "updates", "official",
            "launch", "released", "announce", "announced",
            "how", "why", "what", "everyone", "talking", "about",
            "this", "that", "there", "their", "with", "from", "into", "the", "and",
        }
        for s in signals:
            clean = re.sub(r"https?://\\S+", " ", s)
            clean = re.sub(r"[^A-Za-z0-9\\s\\-]", " ", clean)
            clean = re.sub(r"\\s+", " ", clean).strip()
            low = clean.lower()
            for a in aliases:
                low = re.sub(rf"\\b{re.escape(a)}\\b", " ", low, flags=re.IGNORECASE)
            low = re.sub(r"\\s+", " ", low).strip(" -")
            words = [
                w
                for w in re.findall(r"[a-z0-9]+", low)
                if len(w) >= 3 and not w.isdigit() and w not in fragment_stop
            ]
            if len(words) >= 3:
                fragments.append(" ".join(words[:7]))

        if not fragments and fallback_title:
            base = re.sub(r"[^A-Za-z0-9\\s\\-]", " ", fallback_title)
            base = re.sub(r"\\s+", " ", base).strip().lower()
            for a in aliases:
                base = re.sub(rf"\\b{re.escape(a)}\\b", " ", base, flags=re.IGNORECASE)
            base = re.sub(r"\\s+", " ", base).strip(" -")
            if base:
                fragments.append(base)

        # Build question-form long-tail keywords.
        questions: list[str] = []
        seen: set[str] = set()

        def _push(q: str) -> None:
            norm = re.sub(r"\\s+", " ", q).strip()
            if not norm:
                return
            key_q = norm.lower()
            if key_q in seen:
                return
            seen.add(key_q)
            questions.append(norm)

        for frag in fragments[:12]:
            short = " ".join(frag.split()[:6]).strip()
            short = re.sub(rf"\b{re.escape(entity.lower())}\b", " ", short, flags=re.IGNORECASE)
            short = re.sub(r"^(how|why|what)\s+", "", short, flags=re.IGNORECASE)
            short = re.sub(r"\s+", " ", short).strip(" -")
            if not short:
                continue
            _push(f"How to fix {entity} {short}")
            _push(f"Why {entity} {short} is not working")
            _push(f"{entity} {short} troubleshooting steps")
            if len(questions) >= 6:
                break

        if len(questions) < 3:
            fallback = [
                f"{entity} not working fix guide",
                f"How to fix common {entity} setup errors",
                f"{entity} update problems and solutions",
                f"{entity} connectivity troubleshooting steps",
                f"{entity} app crash fix for beginners",
                f"{entity} performance issue checklist",
            ]
            for q in fallback:
                _push(q)

        if len(questions) < 5:
            fill = [
                f"{entity} recovery steps after failed update",
                f"{entity} no sound or mic issue fix",
                f"{entity} reset and prevention checklist",
            ]
            for q in fill:
                _push(q)
                if len(questions) >= 5:
                    break

        out = questions[:6]
        self._longtail_cache[key] = (now, out)
        return out

    def _fetch_google_trends_terms(self, limit: int = 80) -> list[str]:
        out: list[str] = []
        # Google Trends JSON endpoint.
        try:
            resp = requests.get(
                "https://trends.google.com/trends/api/dailytrends",
                params={"hl": "en-US", "tz": "-480", "geo": "US", "ns": "15"},
                timeout=20,
            )
            if resp.status_code == 200:
                text = (resp.text or "").strip()
                text = re.sub(r"^\\)\\]\\}',\\s*", "", text)
                payload = json.loads(text) if text else {}
                days = (((payload or {}).get("default") or {}).get("trendingSearchesDays") or [])
                for day in days:
                    rows = day.get("trendingSearches", []) if isinstance(day, dict) else []
                    for item in rows:
                        title = str(((item or {}).get("title") or {}).get("query") or "").strip()
                        if title:
                            out.append(title)
                        rels = (item or {}).get("relatedQueries", []) or []
                        for rel in rels:
                            q = str((rel or {}).get("query") or "").strip()
                            if q:
                                out.append(q)
                        if len(out) >= limit:
                            break
                    if len(out) >= limit:
                        break
        except Exception:
            pass

        # RSS fallback.
        if len(out) < max(10, limit // 3):
            try:
                rss = requests.get(
                    "https://trends.google.com/trending/rss?geo=US",
                    timeout=20,
                )
                if rss.status_code == 200:
                    titles = re.findall(r"<title>(.*?)</title>", rss.text or "", flags=re.IGNORECASE | re.DOTALL)
                    for t in titles:
                        clean = re.sub(r"<[^>]+>", " ", t)
                        clean = re.sub(r"\\s+", " ", clean).strip()
                        if not clean or clean.lower() in {"google trends", "daily search trends"}:
                            continue
                        out.append(clean)
                        if len(out) >= limit:
                            break
            except Exception:
                pass

        dedup: list[str] = []
        seen: set[str] = set()
        for term in out:
            norm = re.sub(r"\\s+", " ", str(term or "")).strip()
            if not norm:
                continue
            key = norm.lower()
            if key in seen:
                continue
            seen.add(key)
            dedup.append(norm)
            if len(dedup) >= limit:
                break
        return dedup

    def _collect_github(self) -> list[TopicCandidate]:
        if not self.settings.github_repos:
            return []

        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.settings.github_token:
            headers["Authorization"] = f"Bearer {self.settings.github_token}"

        out: list[TopicCandidate] = []
        for repo in self.settings.github_repos:
            endpoint = f"https://api.github.com/repos/{repo}/issues"
            response = requests.get(
                endpoint,
                headers=headers,
                params={"state": "open", "sort": "comments", "direction": "desc", "per_page": 20},
                timeout=30,
            )
            if response.status_code != 200:
                continue

            for issue in response.json():
                if "pull_request" in issue:
                    continue
                reactions = int((issue.get("reactions") or {}).get("total_count", 0))
                if reactions < self.settings.github_min_reactions:
                    continue
                title = str(issue.get("title", "")).strip()
                body = str(issue.get("body", "")).strip()
                out.append(
                    TopicCandidate(
                        source="github",
                        title=title,
                        body=body[:4000],
                        score=max(reactions, int(issue.get("comments", 0))),
                        url=str(issue.get("html_url", "")),
                    )
                )
        return out
