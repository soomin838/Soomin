from __future__ import annotations

import json
import random
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from re_core.scout import TopicCandidate
from re_core.services.search_intent import IntentBundle


NEWS_SECTION_TITLES = {
    "quick_take": "Quick Take",
    "what_happened": "What Happened",
    "why_it_matters": "Why It Matters",
    "what_to_do_now": "What To Do Now",
    "key_details": "Key Details",
    "what_to_watch_next": "What To Watch Next",
    "background_context": "Background Context",
    "risks": "Risks",
    "timeline": "Timeline",
    "sources": "Sources",
}

INTRO_TEMPLATES = (
    "Lead with the concrete change before broad context.",
    "Open with the reader consequence first, then explain the event.",
    "Start with the core question readers are really asking and answer it fast.",
    "Begin with the timeline trigger that moved this story today.",
)

CONCLUSION_TEMPLATES = (
    "Close on the next checkpoint readers should verify.",
    "Close on the lowest-regret takeaway for readers right now.",
    "Close on what is still uncertain and why that matters.",
    "Close on the buyer or user decision this story really changes.",
)

ARCHETYPE_TEMPLATES = {
    "news_impact_explainer": [
        ["quick_take", "what_happened", "why_it_matters", "what_to_watch_next", "what_to_do_now", "sources"],
        ["quick_take", "why_it_matters", "what_happened", "key_details", "what_to_do_now", "sources"],
    ],
    "news_timeline_explainer": [
        ["quick_take", "timeline", "what_happened", "key_details", "what_to_do_now", "sources"],
        ["quick_take", "what_happened", "timeline", "background_context", "what_to_do_now", "sources"],
    ],
    "news_risk_watch": [
        ["quick_take", "what_happened", "risks", "what_to_watch_next", "what_to_do_now", "sources"],
        ["quick_take", "risks", "key_details", "what_happened", "what_to_do_now", "sources"],
    ],
    "policy_change_decode": [
        ["quick_take", "what_happened", "key_details", "timeline", "what_to_do_now", "sources"],
        ["quick_take", "timeline", "key_details", "why_it_matters", "what_to_do_now", "sources"],
    ],
    "consumer_ranked_breakdown": [
        ["quick_take", "what_happened", "key_details", "why_it_matters", "what_to_do_now", "sources"],
        ["quick_take", "why_it_matters", "key_details", "background_context", "what_to_do_now", "sources"],
    ],
    "research_practical_takeaways": [
        ["quick_take", "what_happened", "background_context", "why_it_matters", "what_to_do_now", "sources"],
        ["quick_take", "background_context", "key_details", "what_to_watch_next", "what_to_do_now", "sources"],
    ],
    "buyer_shift_analysis": [
        ["quick_take", "what_happened", "why_it_matters", "risks", "what_to_do_now", "sources"],
        ["quick_take", "key_details", "why_it_matters", "what_to_watch_next", "what_to_do_now", "sources"],
    ],
}


@dataclass(frozen=True)
class OutlinePlan:
    archetype: str
    section_ids: list[str]
    section_titles: list[str]
    intro_template: str
    conclusion_template: str
    paragraph_lengths: list[str]
    fingerprint: str
    best_similarity: float


class StructureRandomizer:
    def __init__(
        self,
        *,
        state_path: Path,
        log_path: Path | None = None,
        similarity_threshold: float = 0.75,
        fingerprint_ttl_days: int = 30,
        max_attempts: int = 3,
    ) -> None:
        self.state_path = Path(state_path).resolve()
        self.log_path = Path(log_path).resolve() if log_path else None
        self.similarity_threshold = max(0.1, min(1.0, float(similarity_threshold)))
        self.fingerprint_ttl_days = max(1, int(fingerprint_ttl_days))
        self.max_attempts = max(1, int(max_attempts))

    def pick_outline(
        self,
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
        cluster_id: str = "",
    ) -> OutlinePlan:
        recent = self._load_recent()
        recommended = [a for a in (intent_bundle.recommended_archetypes or []) if a in ARCHETYPE_TEMPLATES]
        if not recommended:
            recommended = ["news_impact_explainer", "news_timeline_explainer", "news_risk_watch"]
        seed = self._stable_hash(
            f"{getattr(candidate, 'title', '')}|{category}|{cluster_id}|{intent_bundle.primary_query}"
        )
        best_plan: OutlinePlan | None = None
        best_similarity = 1.0
        for attempt in range(self.max_attempts):
            rng = random.Random(seed + attempt)
            archetype = recommended[attempt % len(recommended)]
            variants = ARCHETYPE_TEMPLATES.get(archetype, ARCHETYPE_TEMPLATES["news_impact_explainer"])
            section_ids = list(variants[rng.randrange(0, len(variants))])
            intro_id = rng.randrange(0, len(INTRO_TEMPLATES))
            conclusion_id = rng.randrange(0, len(CONCLUSION_TEMPLATES))
            paragraph_lengths = [rng.choice(["short", "medium", "long"]) if sid != "sources" else "short" for sid in section_ids]
            fingerprint_text = self._fingerprint_text(
                archetype=archetype,
                section_ids=section_ids,
                intro_id=intro_id,
                conclusion_id=conclusion_id,
                paragraph_lengths=paragraph_lengths,
                topic=str(getattr(candidate, "title", "") or ""),
                intent=intent_bundle.primary_query,
                outline_brief=list(intent_bundle.outline_brief or []),
                category=category,
                cluster_id=cluster_id,
            )
            similarity = 0.0
            for row in recent:
                row_similarity = self._hybrid_similarity(
                    fingerprint_text,
                    str(row.get("fingerprint_text", "") or ""),
                    section_ids,
                    list(row.get("section_ids", []) or []),
                )
                similarity = max(similarity, row_similarity)
            plan = OutlinePlan(
                archetype=archetype,
                section_ids=section_ids,
                section_titles=[NEWS_SECTION_TITLES.get(sid, sid.replace("_", " ").title()) for sid in section_ids],
                intro_template=INTRO_TEMPLATES[intro_id],
                conclusion_template=CONCLUSION_TEMPLATES[conclusion_id],
                paragraph_lengths=paragraph_lengths,
                fingerprint=fingerprint_text,
                best_similarity=float(round(similarity, 4)),
            )
            best_similarity = min(best_similarity, similarity)
            best_plan = plan
            self._log("outline_attempt", plan=plan, attempt=attempt + 1, accepted=bool(similarity < self.similarity_threshold))
            if similarity < self.similarity_threshold:
                self._remember(plan)
                self._log("outline_selected", plan=plan, attempt=attempt + 1, accepted=True)
                return plan
        if best_plan is not None:
            self._log("outline_rejected", plan=best_plan, attempt=self.max_attempts, accepted=False, note="template_similarity_too_high")
        raise RuntimeError("template_similarity_too_high")

    def _fingerprint_text(
        self,
        *,
        archetype: str,
        section_ids: list[str],
        intro_id: int,
        conclusion_id: int,
        paragraph_lengths: list[str],
        topic: str,
        intent: str,
        outline_brief: list[str],
        category: str,
        cluster_id: str,
    ) -> str:
        outline_hint = " | ".join(str(x or "").strip().lower() for x in outline_brief[:5] if str(x or "").strip())
        return (
            f"archetype={archetype};"
            f"sections={'|'.join(section_ids)};"
            f"intro={intro_id};"
            f"conclusion={conclusion_id};"
            f"lengths={'|'.join(paragraph_lengths)};"
            f"topic={self._normalize(topic)};"
            f"intent={self._normalize(intent)};"
            f"outline={self._normalize(outline_hint)};"
            f"category={self._normalize(category)};"
            f"cluster={self._normalize(cluster_id)}"
        )

    def _hybrid_similarity(
        self,
        fingerprint_text: str,
        other_text: str,
        section_ids: list[str],
        other_sections: list[str],
    ) -> float:
        sec = self._jaccard(set(section_ids), set(other_sections))
        tok = self._jaccard(self._tokenize(fingerprint_text), self._tokenize(other_text))
        return max(0.0, min(1.0, (0.5 * sec) + (0.5 * tok)))

    def _load_recent(self) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.fingerprint_ttl_days)
        try:
            if not self.state_path.exists():
                return []
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return []
        rows = payload.get("fingerprints", []) if isinstance(payload, dict) else []
        out: list[dict] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            ts = self._parse_iso(str(row.get("created_at_utc", "") or ""))
            if ts is None or ts < cutoff:
                continue
            out.append(row)
        return out[-200:]

    def _remember(self, plan: OutlinePlan) -> None:
        rows = self._load_recent()
        rows.append(
            {
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "archetype": plan.archetype,
                "section_ids": list(plan.section_ids),
                "fingerprint_text": plan.fingerprint,
            }
        )
        payload = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "fingerprints": rows[-200:],
        }
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            return

    def _log(self, event: str, *, plan: OutlinePlan, attempt: int, accepted: bool, note: str = "") -> None:
        if self.log_path is None:
            return
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip(),
            "attempt": int(attempt),
            "accepted": bool(accepted),
            "note": str(note or "").strip()[:120],
            "plan": asdict(plan),
        }
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _stable_hash(self, value: str) -> int:
        text = str(value or "")
        return sum((idx + 1) * ord(ch) for idx, ch in enumerate(text))

    def _normalize(self, text: str) -> str:
        out = re.sub(r"[^a-z0-9\s\-]+", " ", str(text or "").lower())
        out = re.sub(r"\s+", " ", out).strip()
        return out

    def _tokenize(self, text: str) -> set[str]:
        return {tok for tok in self._normalize(text).split(" ") if len(tok) >= 2}

    def _jaccard(self, left: set[str], right: set[str]) -> float:
        if not left and not right:
            return 1.0
        if not left or not right:
            return 0.0
        inter = len(left.intersection(right))
        union = len(left.union(right))
        if union <= 0:
            return 0.0
        return float(inter) / float(union)

    def _parse_iso(self, value: str) -> datetime | None:
        txt = str(value or "").strip()
        if not txt:
            return None
        try:
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
