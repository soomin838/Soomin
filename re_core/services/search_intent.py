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
