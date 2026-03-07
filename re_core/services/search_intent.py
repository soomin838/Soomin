from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

@dataclass(frozen=True)
class IntentBundle:
    primary_query: str
    supporting_queries: list[str] = field(default_factory=list)
    questions: list[str] = field(default_factory=list)
    audience: str = "US mainstream readers"
    content_kind: str = "hot"
    recommended_archetypes: list[str] = field(default_factory=list)
    outline_brief: list[str] = field(default_factory=list)
    negative_angles: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SearchIntentCandidateSpec:
    content_type: str
    candidate_kind: str
    title: str
    body: str
    primary_query: str
    source_url: str = ""
    title_strategy: str = "query_match"
    source_strategy: str = "authority_first"


class SearchIntentGenerator:
    def __init__(
        self,
        *,
        settings: Any | None = None,
        ollama_client: Any | None = None,
        log_path: Path | None = None,
    ) -> None:
        self.settings = settings
        self.ollama_client = ollama_client
        self.log_path = Path(log_path).resolve() if log_path else None
        self.provider = str(getattr(settings, "provider", "ollama_then_rules") or "ollama_then_rules").strip().lower()
        self.timeout_sec = max(3, int(getattr(settings, "timeout_sec", 15) or 15))
        self.last_source = "rules"
        self.last_note = ""

    def generate(
        self,
        *,
        headline: str,
        snippet: str,
        body_excerpt: str,
        category: str,
        source_url: str = "",
    ) -> IntentBundle:
        started = time.perf_counter()
        fallback_reason = ""
        if self.provider.startswith("ollama") and self.ollama_client is not None:
            try:
                payload = self._generate_with_ollama(
                    headline=headline,
                    snippet=snippet,
                    body_excerpt=body_excerpt,
                    category=category,
                    source_url=source_url,
                )
                bundle = self._normalize_payload(payload, headline=headline, category=category)
                self._log(
                    event="intent_generated",
                    category=category,
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    source="ollama",
                    source_url=source_url,
                    bundle=bundle,
                )
                self.last_source = "ollama"
                self.last_note = ""
                return bundle
            except Exception as exc:
                fallback_reason = str(exc)[:160]
        bundle = self._build_rule_bundle(
            headline=headline,
            snippet=snippet,
            body_excerpt=body_excerpt,
            category=category,
        )
        self._log(
            event="intent_generated",
            category=category,
            latency_ms=int((time.perf_counter() - started) * 1000),
            source="rules",
            source_url=source_url,
            note=fallback_reason,
            bundle=bundle,
        )
        self.last_source = "rules"
        self.last_note = fallback_reason
        return bundle

    def _generate_with_ollama(
        self,
        *,
        headline: str,
        snippet: str,
        body_excerpt: str,
        category: str,
        source_url: str,
    ) -> dict[str, Any]:
        system_prompt = (
            "Turn a news headline into search intent for US readers.\n"
            "Return strict JSON only with keys: primary_query, supporting_queries, questions, audience, "
            "content_kind, recommended_archetypes, outline_brief, negative_angles.\n"
            "Use 1 primary query, 4-6 supporting queries, 3-5 questions, 2-3 recommended_archetypes, "
            "4-5 outline_brief bullets, 2-4 negative_angles."
        )
        user_payload = {
            "headline": str(headline or "").strip(),
            "snippet": str(snippet or "").strip()[:500],
            "body_excerpt": str(body_excerpt or "").strip()[:1200],
            "category": str(category or "").strip().lower(),
            "source_url": str(source_url or "").strip(),
        }
        original_timeout = getattr(self.ollama_client, "timeout", None)
        try:
            self.ollama_client.timeout = int(self.timeout_sec)
            parsed = self.ollama_client.generate_json(
                system_prompt,
                user_payload,
                purpose="search_intent",
            )
        finally:
            if original_timeout is not None:
                self.ollama_client.timeout = original_timeout
        if not isinstance(parsed, dict) or not parsed:
            raise RuntimeError("ollama_invalid_json")
        return dict(parsed)

    def _normalize_payload(self, payload: dict[str, Any], *, headline: str, category: str) -> IntentBundle:
        primary_query = re.sub(r"\s+", " ", str(payload.get("primary_query", "") or "")).strip()
        if not primary_query:
            return self._build_rule_bundle(
                headline=headline,
                snippet="",
                body_excerpt="",
                category=category,
            )
        supporting = self._dedupe_list(payload.get("supporting_queries", []), min_count=4, max_count=6)
        questions = self._dedupe_list(payload.get("questions", []), min_count=3, max_count=5)
        archetypes = self._dedupe_list(payload.get("recommended_archetypes", []), min_count=2, max_count=3)
        outline = self._dedupe_list(payload.get("outline_brief", []), min_count=4, max_count=5)
        negative = self._dedupe_list(payload.get("negative_angles", []), min_count=2, max_count=4)
        if not archetypes:
            archetypes = self._category_archetypes(category)
        return IntentBundle(
            primary_query=primary_query[:160],
            supporting_queries=supporting,
            questions=questions,
            audience=re.sub(r"\s+", " ", str(payload.get("audience", "US mainstream readers") or "US mainstream readers")).strip()[:120],
            content_kind=re.sub(r"\s+", " ", str(payload.get("content_kind", "hot") or "hot")).strip().lower()[:40] or "hot",
            recommended_archetypes=archetypes,
            outline_brief=outline or self._default_outline(primary_query, category),
            negative_angles=negative or self._negative_angles(category),
        )

    def _build_rule_bundle(
        self,
        *,
        headline: str,
        snippet: str,
        body_excerpt: str,
        category: str,
    ) -> IntentBundle:
        topic = self._topic_phrase(headline=headline, snippet=snippet, body_excerpt=body_excerpt)
        archetypes = self._category_archetypes(category)
        primary_query = self._primary_query(topic, category)
        supporting = self._supporting_queries(topic, category)
        questions = self._questions(topic, category)
        return IntentBundle(
            primary_query=primary_query,
            supporting_queries=supporting,
            questions=questions,
            audience="US mainstream readers",
            content_kind="hot",
            recommended_archetypes=archetypes,
            outline_brief=self._default_outline(topic, category),
            negative_angles=self._negative_angles(category),
        )

    def _topic_phrase(self, *, headline: str, snippet: str, body_excerpt: str) -> str:
        merged = " ".join([headline or "", snippet or "", body_excerpt or ""])
        merged = re.sub(r"\s+", " ", merged).strip()
        merged = re.sub(r"[:|].*$", "", merged).strip()
        if not merged:
            return "the latest tech change"
        return merged[:110]

    def _primary_query(self, topic: str, category: str) -> str:
        cat = str(category or "").strip().lower()
        if cat in {"security", "policy", "platform"}:
            return f"{topic} what changed and who is affected"
        if cat in {"consumer", "home", "wellness"}:
            return f"{topic} is it a real shift for buyers"
        if cat in {"research", "ai", "chips"}:
            return f"{topic} what it means in practice"
        return f"{topic} what it means and what to watch"

    def _supporting_queries(self, topic: str, category: str) -> list[str]:
        cat = str(category or "").strip().lower()
        base = re.sub(r"[?]+$", "", topic).strip()
        templates = [
            f"{base} why it matters now",
            f"{base} who is affected first",
            f"{base} risks and tradeoffs",
            f"{base} what to watch next",
            f"{base} what changed from before",
            f"{base} practical impact for Americans",
        ]
        if cat in {"consumer", "home", "wellness"}:
            templates = [
                f"{base} what buyers should compare",
                f"{base} is the ranking credible",
                f"{base} what shoppers notice first",
                f"{base} what coverage leaves out",
                f"{base} how Americans actually compare it",
                f"{base} real differences not marketing",
            ]
        return templates[:6]

    def _questions(self, topic: str, category: str) -> list[str]:
        cat = str(category or "").strip().lower()
        common = [
            f"What changed in {topic}?",
            f"Who feels the impact of {topic} first?",
            f"What would make {topic} more or less significant?",
            f"What should readers watch next around {topic}?",
        ]
        if cat in {"consumer", "home", "wellness"}:
            common = [
                f"Is {topic} actually useful for buyers?",
                f"What variables matter more than the headline around {topic}?",
                f"What would real shoppers compare before trusting {topic}?",
                f"What does the coverage of {topic} still leave unanswered?",
            ]
        return common[:5]

    def _category_archetypes(self, category: str) -> list[str]:
        cat = str(category or "").strip().lower()
        if cat in {"policy", "security", "platform"}:
            return ["policy_change_decode", "news_risk_watch", "news_timeline_explainer"]
        if cat in {"consumer", "home", "wellness"}:
            return ["consumer_ranked_breakdown", "buyer_shift_analysis", "news_impact_explainer"]
        if cat in {"research", "ai", "chips"}:
            return ["research_practical_takeaways", "news_impact_explainer", "news_timeline_explainer"]
        return ["news_impact_explainer", "news_timeline_explainer", "news_risk_watch"]

    def _default_outline(self, topic: str, category: str) -> list[str]:
        cat = str(category or "").strip().lower()
        if cat in {"consumer", "home", "wellness"}:
            return [
                f"Open with the real shopper question behind {topic}.",
                "Explain what changed and why the ranking or recommendation exists.",
                "Compare the practical variables that buyers actually care about.",
                "Call out what the original coverage leaves uncertain.",
                "Close with a grounded buying takeaway.",
            ]
        return [
            f"Open with the sharpest change readers should know about {topic}.",
            "Explain what happened and which facts are confirmed.",
            "Translate the impact into practical reader consequences.",
            "Map the main risks, limits, or unresolved questions.",
            "Close with the next signals worth tracking.",
        ]

    def _negative_angles(self, category: str) -> list[str]:
        cat = str(category or "").strip().lower()
        if cat in {"consumer", "home", "wellness"}:
            return ["generic ranking recap", "empty winner-label summary", "marketing copy repetition"]
        if cat in {"policy", "security", "platform"}:
            return ["unverified fear framing", "fix-guide drift", "sensational risk inflation"]
        return ["generic AI summary", "headline restatement only", "template transition repetition"]

    def _dedupe_list(self, value: Any, *, min_count: int, max_count: int) -> list[str]:
        if not isinstance(value, list):
            value = []
        out: list[str] = []
        seen: set[str] = set()
        for item in value:
            clean = re.sub(r"\s+", " ", str(item or "")).strip()
            key = clean.lower()
            if not clean or key in seen:
                continue
            seen.add(key)
            out.append(clean[:180])
            if len(out) >= max_count:
                break
        if len(out) < min_count:
            return out
        return out[:max_count]

    def _log(
        self,
        *,
        event: str,
        category: str,
        latency_ms: int,
        source: str,
        source_url: str,
        bundle: IntentBundle,
        note: str = "",
    ) -> None:
        if self.log_path is None:
            return
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip(),
            "category": str(category or "").strip().lower(),
            "latency_ms": int(max(0, latency_ms)),
            "source": str(source or "").strip(),
            "source_url": str(source_url or "").strip()[:300],
            "note": str(note or "").strip()[:200],
            "bundle": asdict(bundle),
        }
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def build_search_candidates(
        self,
        *,
        bundle: IntentBundle,
        headline: str,
        category: str,
        source_url: str = "",
        max_candidates: int = 3,
    ) -> list[SearchIntentCandidateSpec]:
        query = re.sub(r"\s+", " ", str(bundle.primary_query or headline or "").strip())
        if not query:
            return []
        clean_headline = re.sub(r"\s+", " ", str(headline or "").strip())
        cat = str(category or "").strip().lower()
        candidate_kinds = self._candidate_kinds(bundle, cat)
        out: list[SearchIntentCandidateSpec] = []
        seen_titles: set[str] = set()
        for kind in candidate_kinds:
            title = self._candidate_title(kind=kind, query=query, headline=clean_headline, category=cat)
            key = title.lower().strip()
            if not title or key in seen_titles:
                continue
            seen_titles.add(key)
            out.append(
                SearchIntentCandidateSpec(
                    content_type="search_derived",
                    candidate_kind=kind,
                    title=title[:110],
                    body=self._candidate_body(
                        kind=kind,
                        query=query,
                        headline=clean_headline,
                        bundle=bundle,
                    )[:900],
                    primary_query=query[:160],
                    source_url=str(source_url or "").strip()[:300],
                )
            )
            if len(out) >= max(1, int(max_candidates)):
                break
        return out

    def _candidate_kinds(self, bundle: IntentBundle, category: str) -> list[str]:
        cat = str(category or "").strip().lower()
        out = ["what-changed", "should-you", "comparison", "alternatives", "how-to"]
        if cat in {"security", "policy"}:
            out = ["what-changed", "how-to", "should-you", "alternatives"]
        elif cat in {"consumer", "home", "wellness"}:
            out = ["comparison", "should-you", "alternatives", "what-changed"]
        elif cat in {"ai", "chips", "platform"}:
            out = ["what-changed", "should-you", "comparison", "alternatives"]
        if str(bundle.content_kind or "").strip().lower() == "supporting":
            out = ["should-you", "comparison", "alternatives", "how-to"]
        return out

    def _candidate_title(self, *, kind: str, query: str, headline: str, category: str) -> str:
        base = re.sub(r"[?]+$", "", query).strip()
        if kind == "how-to":
            return f"How to make sense of {base}"
        if kind == "comparison":
            return f"{base}: what actually matters when you compare it"
        if kind == "what-changed":
            return f"What changed with {base} and why it matters"
        if kind == "should-you":
            return f"Should you care about {base} right now?"
        if kind == "alternatives":
            return f"{base}: better alternatives and tradeoffs to consider"
        return headline or base

    def _candidate_body(
        self,
        *,
        kind: str,
        query: str,
        headline: str,
        bundle: IntentBundle,
    ) -> str:
        supporting = ", ".join(list(bundle.supporting_queries or [])[:3])
        questions = " ".join(list(bundle.questions or [])[:2])
        if kind == "how-to":
            return (
                f"Search-derived support article for '{query}'. "
                f"Use the source event '{headline}' only as context, then answer the practical reader problem directly. "
                f"Relevant supporting angles: {supporting}. Key reader questions: {questions}"
            )
        if kind == "comparison":
            return (
                f"Search-derived comparison article for '{query}'. "
                f"Use the source event '{headline}' as the trigger, but structure the article around real buyer or user tradeoffs. "
                f"Compare practical variables, not generic summaries. Supporting angles: {supporting}"
            )
        if kind == "what-changed":
            return (
                f"Search-derived explainer for '{query}'. "
                f"Ground the article in what actually changed in '{headline}', then explain who is affected, what changed from before, and what to watch next. "
                f"Questions to answer: {questions}"
            )
        if kind == "should-you":
            return (
                f"Search-derived decision article for '{query}'. "
                f"Translate '{headline}' into a clear yes/no or maybe-it-depends decision framework for mainstream readers. "
                f"Use practical consequences and limits. Supporting angles: {supporting}"
            )
        return (
            f"Search-derived alternatives article for '{query}'. "
            f"Use '{headline}' as the context, then show realistic alternatives, tradeoffs, and fallback choices. "
            f"Questions to answer: {questions}"
        )
