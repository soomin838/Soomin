from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from re_core.scout import TopicCandidate
from re_core.services.search_intent import IntentBundle


@dataclass(frozen=True)
class OutlinePlan:
    archetype: str
    section_ids: list[str]
    section_titles: list[str]
    section_purposes: list[str] = field(default_factory=list)
    intro_template: str = ""
    conclusion_template: str = ""
    paragraph_lengths: list[str] = field(default_factory=list)
    fingerprint: str = ""
    best_similarity: float = 0.0
    heading_signature: str = ""
    grounding_packet: dict[str, Any] = field(default_factory=dict)


class StructureRandomizer:
    def __init__(
        self,
        *,
        state_path: Path,
        log_path: Path | None = None,
        similarity_threshold: float = 0.75,
        fingerprint_ttl_days: int = 30,
        max_attempts: int = 3,
        ollama_client: Any | None = None,
        outline_timeout_sec: int = 12,
    ) -> None:
        self.state_path = Path(state_path).resolve()
        self.log_path = Path(log_path).resolve() if log_path else None
        self.similarity_threshold = max(0.1, min(1.0, float(similarity_threshold)))
        self.fingerprint_ttl_days = max(1, int(fingerprint_ttl_days))
        self.max_attempts = max(1, int(max_attempts))
        self.ollama_client = ollama_client
        self.outline_timeout_sec = max(4, int(outline_timeout_sec))

    def pick_outline(
        self,
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
        cluster_id: str = "",
        grounding_packet: dict[str, Any] | None = None,
    ) -> OutlinePlan:
        recent = self._load_recent()
        best_plan: OutlinePlan | None = None
        packet = dict(grounding_packet or {})
        for attempt in range(self.max_attempts):
            payload, source = self._generate_outline_payload(
                candidate=candidate,
                intent_bundle=intent_bundle,
                category=category,
                cluster_id=cluster_id,
                grounding_packet=packet,
                attempt=attempt,
            )
            plan = self._normalize_outline_payload(
                payload,
                candidate=candidate,
                intent_bundle=intent_bundle,
                category=category,
                cluster_id=cluster_id,
                source=source,
                grounding_packet=packet,
            )
            grounded, ground_detail = self._outline_grounding_ok(plan, packet)
            similarity = 0.0
            for row in recent:
                row_similarity = self._hybrid_similarity(
                    plan.fingerprint,
                    str(row.get("fingerprint_text", "") or ""),
                    plan.heading_signature,
                    str(row.get("heading_signature", "") or ""),
                )
                similarity = max(similarity, row_similarity)
            plan = OutlinePlan(
                archetype=plan.archetype,
                section_ids=list(plan.section_ids),
                section_titles=list(plan.section_titles),
                section_purposes=list(plan.section_purposes),
                intro_template=plan.intro_template,
                conclusion_template=plan.conclusion_template,
                paragraph_lengths=list(plan.paragraph_lengths),
                fingerprint=plan.fingerprint,
                best_similarity=float(round(similarity, 4)),
                heading_signature=plan.heading_signature,
                grounding_packet=dict(packet),
            )
            best_plan = plan
            accepted = bool((similarity < self.similarity_threshold) and grounded)
            self._log(
                "outline_attempt",
                plan=plan,
                attempt=attempt + 1,
                accepted=accepted,
                grounding_ok=bool(grounded),
                grounding_detail=ground_detail,
            )
            if accepted:
                self._remember(plan)
                self._log("outline_selected", plan=plan, attempt=attempt + 1, accepted=True)
                return plan
        if best_plan is not None:
            grounded, ground_detail = self._outline_grounding_ok(best_plan, packet)
            note = "template_similarity_too_high" if not grounded or best_plan.best_similarity >= self.similarity_threshold else "outline_grounding_too_weak"
            if not grounded:
                note = "outline_grounding_too_weak"
            self._log(
                "outline_rejected",
                plan=best_plan,
                attempt=self.max_attempts,
                accepted=False,
                note=note,
                grounding_detail=ground_detail,
            )
            raise RuntimeError(note)
        raise RuntimeError("outline_grounding_too_weak")

    def _generate_outline_payload(
        self,
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
        cluster_id: str,
        grounding_packet: dict[str, Any],
        attempt: int,
    ) -> tuple[dict[str, Any], str]:
        if self.ollama_client is not None:
            try:
                return (
                    self._generate_outline_with_ollama(
                        candidate=candidate,
                        intent_bundle=intent_bundle,
                        category=category,
                        cluster_id=cluster_id,
                        grounding_packet=grounding_packet,
                        attempt=attempt,
                    ),
                    "ollama",
                )
            except Exception:
                pass
        return (
            self._build_rule_outline(
                candidate=candidate,
                intent_bundle=intent_bundle,
                category=category,
                cluster_id=cluster_id,
                grounding_packet=grounding_packet,
                attempt=attempt,
            ),
            "rules",
        )

    def _generate_outline_with_ollama(
        self,
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
        cluster_id: str,
        grounding_packet: dict[str, Any],
        attempt: int,
    ) -> dict[str, Any]:
        system_prompt = (
            "Generate a dynamic article outline for a factual US English explainer. "
            "Return JSON only with keys: section_titles, section_purposes, intro_template, conclusion_template, paragraph_lengths. "
            "Use 4-6 sections total, include Quick Take as the first section and Sources as the last section, "
            "and make the middle headings topic-specific rather than generic templates. "
            "The outline must stay tightly grounded to the source packet. "
            "The first 3 sections must clearly cover the required entities, topic nouns, and source facts. "
            "Avoid forbidden drift terms unless the source packet explicitly supports them."
        )
        payload = {
            "headline": str(getattr(candidate, "title", "") or "").strip(),
            "category": str(category or "").strip().lower(),
            "primary_query": str(intent_bundle.primary_query or "").strip(),
            "supporting_queries": list(intent_bundle.supporting_queries or [])[:4],
            "questions": list(intent_bundle.questions or [])[:4],
            "outline_brief": list(intent_bundle.outline_brief or [])[:5],
            "negative_angles": list(intent_bundle.negative_angles or [])[:4],
            "content_kind": str(intent_bundle.content_kind or "").strip().lower() or "hot",
            "cluster_id": str(cluster_id or "").strip().lower(),
            "grounding_packet": dict(grounding_packet or {}),
            "attempt": int(attempt),
        }
        original_timeout = getattr(self.ollama_client, "timeout", None)
        try:
            self.ollama_client.timeout = int(self.outline_timeout_sec)
            parsed = self.ollama_client.generate_json(system_prompt, payload, purpose="dynamic_outline")
        finally:
            if original_timeout is not None:
                self.ollama_client.timeout = original_timeout
        if not isinstance(parsed, dict) or not parsed:
            raise RuntimeError("outline_json_invalid")
        return dict(parsed)

    def _build_rule_outline(
        self,
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
        cluster_id: str,
        grounding_packet: dict[str, Any],
        attempt: int,
    ) -> dict[str, Any]:
        topic = self._clean_phrase(str(intent_bundle.primary_query or getattr(candidate, "title", "") or "the latest change"))
        questions = [self._clean_phrase(x) for x in (intent_bundle.questions or []) if self._clean_phrase(x)]
        outline_brief = [self._clean_phrase(x) for x in (intent_bundle.outline_brief or []) if self._clean_phrase(x)]
        entities = [self._headline_case(x) for x in (grounding_packet.get("required_named_entities", []) or []) if self._clean_phrase(x)]
        nouns = [self._headline_case(x) for x in (grounding_packet.get("required_topic_nouns", []) or []) if self._clean_phrase(x)]
        facts = [self._clean_phrase(x) for x in (grounding_packet.get("required_source_facts", []) or []) if self._clean_phrase(x)]
        titles = ["Quick Take"]
        purposes = ["event summary"]

        dynamic_pairs: list[tuple[str, str]] = []
        if entities:
            dynamic_pairs.append((f"What happened to {entities[0]}", "confirmed details"))
        elif nouns:
            dynamic_pairs.append((f"What changed around {nouns[0]}", "confirmed details"))
        for item in outline_brief:
            dynamic_pairs.append(self._section_from_brief(item, topic=topic, category=category, attempt=attempt))
        for fact in facts[:2]:
            dynamic_pairs.append(self._fact_to_section(fact, topic=topic))
        if not dynamic_pairs:
            dynamic_pairs.extend(self._fallback_dynamic_pairs(topic=topic, category=category, questions=questions))

        for title, purpose in dynamic_pairs:
            if len(titles) >= 5:
                break
            if self._is_duplicate_heading(title, titles):
                continue
            titles.append(title)
            purposes.append(purpose)

        while len(titles) < 4:
            for title, purpose in self._fallback_dynamic_pairs(topic=topic, category=category, questions=questions):
                if not self._is_duplicate_heading(title, titles):
                    titles.append(title)
                    purposes.append(purpose)
                if len(titles) >= 4:
                    break

        practical_title, practical_purpose = self._practical_section(topic=topic, category=category)
        if not self._is_duplicate_heading(practical_title, titles):
            titles.append(practical_title)
            purposes.append(practical_purpose)
        titles = titles[:5]
        purposes = purposes[: len(titles)]
        titles.append("Sources")
        purposes.append("source grounding")

        lengths = []
        for idx, purpose in enumerate(purposes):
            lower = purpose.lower()
            if idx == 0 or "summary" in lower:
                lengths.append("short")
            elif "action" in lower or "source" in lower:
                lengths.append("short")
            elif "compare" in lower or "implication" in lower:
                lengths.append("medium")
            else:
                lengths.append("medium")

        return {
            "section_titles": titles,
            "section_purposes": purposes,
            "intro_template": f"Open by answering the sharpest reader question about {topic}.",
            "conclusion_template": f"Close on the next thing readers should verify about {topic}.",
            "paragraph_lengths": lengths,
            "cluster_id": cluster_id,
            "grounding_packet": dict(grounding_packet or {}),
        }

    def _section_from_brief(self, text: str, *, topic: str, category: str, attempt: int) -> tuple[str, str]:
        lower = str(text or "").lower()
        if "compare" in lower or "buyer" in lower:
            return (f"How to compare {topic}", "comparison")
        if "risk" in lower or "uncertain" in lower:
            return (f"What could still change around {topic}", "risk watch")
        if "impact" in lower or "consequence" in lower:
            return (f"Why {topic} matters for readers", "reader impact")
        if "watch" in lower or "next" in lower:
            return (f"What to watch next with {topic}", "forward implications")
        if "what happened" in lower or "confirmed" in lower:
            return (f"What changed with {topic}", "confirmed details")
        if "question" in lower:
            return (self._question_to_heading(text, topic=topic), "reader question")
        fallback = self._headline_case(text)
        if len(fallback.split()) < 3:
            fallback = f"{fallback} for {topic}".strip()
        purpose = "comparison" if str(category or "").strip().lower() in {"consumer", "home", "wellness"} else "reader impact"
        return (fallback[:72], purpose)

    def _fallback_dynamic_pairs(self, *, topic: str, category: str, questions: list[str]) -> list[tuple[str, str]]:
        cat = str(category or "").strip().lower()
        if cat in {"consumer", "home", "wellness"}:
            return [
                (f"What changed with {topic}", "event summary"),
                (f"What buyers should compare about {topic}", "comparison"),
                (f"What the coverage still leaves unclear about {topic}", "evidence limits"),
            ]
        if questions:
            return [
                (self._question_to_heading(questions[0], topic=topic), "reader impact"),
                (f"What changed with {topic}", "event summary"),
                (f"What to watch next with {topic}", "forward implications"),
            ]
        return [
            (f"What changed with {topic}", "event summary"),
            (f"Why {topic} matters for readers", "reader impact"),
            (f"What comes next for {topic}", "forward implications"),
        ]

    def _practical_section(self, *, topic: str, category: str) -> tuple[str, str]:
        cat = str(category or "").strip().lower()
        if cat in {"consumer", "home", "wellness"}:
            return (f"What to verify before buying into {topic}", "practical action")
        return (f"What to verify before acting on {topic}", "practical action")

    def _normalize_outline_payload(
        self,
        payload: dict[str, Any],
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
        cluster_id: str,
        source: str,
        grounding_packet: dict[str, Any],
    ) -> OutlinePlan:
        titles_raw = payload.get("section_titles", []) if isinstance(payload, dict) else []
        purposes_raw = payload.get("section_purposes", []) if isinstance(payload, dict) else []
        titles = self._normalize_titles(titles_raw, candidate=candidate, intent_bundle=intent_bundle, category=category)
        purposes = self._normalize_purposes(purposes_raw, count=len(titles))
        lengths = self._normalize_lengths(payload.get("paragraph_lengths", []), count=len(titles))
        intro = self._clean_phrase(str(payload.get("intro_template", "") or "")) or f"Lead with the main reader consequence of {intent_bundle.primary_query or candidate.title}."
        conclusion = self._clean_phrase(str(payload.get("conclusion_template", "") or "")) or f"End with the next detail readers should verify about {intent_bundle.primary_query or candidate.title}."
        section_ids = [self._slugify(title, idx) for idx, title in enumerate(titles)]
        heading_signature = self._heading_signature(titles)
        fingerprint = self._fingerprint_text(
            titles=titles,
            purposes=purposes,
            lengths=lengths,
            topic=str(getattr(candidate, "title", "") or ""),
            intent=str(intent_bundle.primary_query or ""),
            category=category,
            cluster_id=cluster_id,
            source=source,
        )
        return OutlinePlan(
            archetype="dynamic_intent_outline",
            section_ids=section_ids,
            section_titles=titles,
            section_purposes=purposes,
            intro_template=intro[:180],
            conclusion_template=conclusion[:180],
            paragraph_lengths=lengths,
            fingerprint=fingerprint,
            best_similarity=0.0,
            heading_signature=heading_signature,
            grounding_packet=dict(grounding_packet or {}),
        )

    def _normalize_titles(
        self,
        value: Any,
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
    ) -> list[str]:
        raw_titles = value if isinstance(value, list) else []
        out = ["Quick Take"]
        topic = self._clean_phrase(str(intent_bundle.primary_query or getattr(candidate, "title", "") or "this topic"))
        for item in raw_titles:
            clean = self._headline_case(self._clean_phrase(item))
            if not clean or clean.lower() in {"quick take", "sources"}:
                continue
            if self._is_generic_heading(clean):
                continue
            if self._is_duplicate_heading(clean, out):
                continue
            out.append(clean[:72])
            if len(out) >= 5:
                break
        while len(out) < 4:
            title, _purpose = self._practical_section(topic=topic, category=category)
            if not self._is_duplicate_heading(title, out):
                out.append(title)
            else:
                out.append(f"What matters most about {topic}"[:72])
        out = out[:5]
        out.append("Sources")
        return out

    def _fact_to_section(self, fact: str, *, topic: str) -> tuple[str, str]:
        clean = self._headline_case(self._clean_phrase(fact))
        if not clean:
            clean = f"What changed with {topic}"
        if len(clean.split()) < 3:
            clean = f"{clean} about {topic}".strip()
        return clean[:72], "source grounding"

    def _outline_grounding_ok(self, plan: OutlinePlan, grounding_packet: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        packet = dict(grounding_packet or {})
        required_terms: list[str] = []
        required_terms.extend([self._normalize(x) for x in (packet.get("required_named_entities", []) or []) if self._normalize(x)])
        required_terms.extend([self._normalize(x) for x in (packet.get("required_topic_nouns", []) or []) if self._normalize(x)])
        for fact in (packet.get("required_source_facts", []) or [])[:3]:
            required_terms.extend(list(self._tokenize(str(fact or "")))[:4])
        required_terms = [x for x in required_terms if x]
        required_terms = list(dict.fromkeys(required_terms))[:16]
        if not required_terms:
            return True, {"coverage_ratio": 1.0, "matched_terms": []}
        heading_blob = " ".join(list(plan.section_titles or [])[:4] + list(plan.section_purposes or [])[:4])
        heading_tokens = self._tokenize(heading_blob)
        matched = sorted({term for term in required_terms if term in heading_tokens})
        coverage_ratio = len(matched) / max(1, min(len(required_terms), 8))
        generic_count = sum(1 for title in (plan.section_titles or []) if self._is_generic_heading(title))
        detail = {
            "coverage_ratio": round(float(coverage_ratio), 3),
            "matched_terms": matched[:8],
            "required_terms": required_terms[:8],
            "generic_headings": int(generic_count),
        }
        if coverage_ratio < 0.22:
            return False, detail
        if generic_count >= max(2, len(list(plan.section_titles or [])) - 2):
            return False, detail
        return True, detail

    def _normalize_purposes(self, value: Any, *, count: int) -> list[str]:
        raw = value if isinstance(value, list) else []
        out: list[str] = []
        for item in raw:
            clean = self._clean_phrase(item).lower()
            if not clean:
                continue
            out.append(clean[:80])
            if len(out) >= max(0, count - 2):
                break
        defaults = ["event summary", "reader impact", "forward implications", "practical action"]
        purposes = ["event summary"]
        purposes.extend(out)
        idx = 0
        while len(purposes) < max(0, count - 1):
            purposes.append(defaults[idx % len(defaults)])
            idx += 1
        purposes = purposes[: max(0, count - 1)]
        purposes.append("source grounding")
        return purposes

    def _normalize_lengths(self, value: Any, *, count: int) -> list[str]:
        raw = value if isinstance(value, list) else []
        allowed = {"short", "medium", "long"}
        out = [str(item or "").strip().lower() for item in raw if str(item or "").strip().lower() in allowed]
        while len(out) < count:
            out.append("short" if len(out) in {0, count - 1} else "medium")
        return out[:count]

    def _hybrid_similarity(
        self,
        fingerprint_text: str,
        other_text: str,
        heading_signature: str,
        other_heading_signature: str,
    ) -> float:
        tok = self._jaccard(self._tokenize(fingerprint_text), self._tokenize(other_text))
        head = self._heading_similarity(heading_signature, other_heading_signature)
        return max(0.0, min(1.0, (0.55 * tok) + (0.45 * head)))

    def _heading_signature(self, section_titles: list[str]) -> str:
        return "|".join(self._normalize(title) for title in (section_titles or []) if self._normalize(title))

    def _heading_similarity(self, current: str, other: str) -> float:
        left = [seg for seg in str(current or "").split("|") if seg]
        right = [seg for seg in str(other or "").split("|") if seg]
        if not left and not right:
            return 1.0
        if not left or not right:
            return 0.0
        if left == right:
            return 1.0
        left_pairs = {f"{left[idx]}->{left[idx + 1]}" for idx in range(len(left) - 1)}
        right_pairs = {f"{right[idx]}->{right[idx + 1]}" for idx in range(len(right) - 1)}
        return max(0.0, min(1.0, (0.6 * self._jaccard(set(left), set(right))) + (0.4 * self._jaccard(left_pairs, right_pairs))))

    def _fingerprint_text(
        self,
        *,
        titles: list[str],
        purposes: list[str],
        lengths: list[str],
        topic: str,
        intent: str,
        category: str,
        cluster_id: str,
        source: str,
    ) -> str:
        return (
            f"titles={'|'.join(self._normalize(title) for title in titles)};"
            f"purposes={'|'.join(self._normalize(item) for item in purposes)};"
            f"lengths={'|'.join(lengths)};"
            f"topic={self._normalize(topic)};"
            f"intent={self._normalize(intent)};"
            f"category={self._normalize(category)};"
            f"cluster={self._normalize(cluster_id)};"
            f"source={self._normalize(source)}"
        )

    def _is_duplicate_heading(self, title: str, existing: list[str]) -> bool:
        normalized = self._normalize(title)
        return any(self._normalize(item) == normalized for item in (existing or []))

    def _is_generic_heading(self, title: str) -> bool:
        low = self._normalize(title)
        if not low or len(low.split()) < 2:
            return True
        return low in {"overview", "introduction", "conclusion", "summary", "details"}

    def _question_to_heading(self, question: str, *, topic: str) -> str:
        clean = self._clean_phrase(question).rstrip("?")
        if not clean:
            return f"What to know about {topic}"
        if clean.lower().startswith(("what ", "why ", "how ", "should ")):
            return self._headline_case(clean)
        return self._headline_case(f"What {clean} means for {topic}")

    def _headline_case(self, text: str) -> str:
        clean = self._clean_phrase(text)
        if not clean:
            return ""
        return " ".join(word[:1].upper() + word[1:] for word in clean.split(" "))[:72]

    def _clean_phrase(self, text: Any) -> str:
        clean = re.sub(r"\s+", " ", str(text or "").strip())
        clean = re.sub(r"^[\-:|]+", "", clean).strip()
        return clean[:180]

    def _slugify(self, text: str, index: int) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", self._normalize(text)).strip("_")
        return slug[:40] or f"section_{int(index)}"

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
                "archetype": str(plan.archetype or ""),
                "section_ids": list(plan.section_ids),
                "section_titles": list(plan.section_titles),
                "section_purposes": list(plan.section_purposes),
                "heading_signature": str(plan.heading_signature or ""),
                "fingerprint_text": str(plan.fingerprint or ""),
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

    def _log(
        self,
        event: str,
        *,
        plan: OutlinePlan,
        attempt: int,
        accepted: bool,
        note: str = "",
        grounding_ok: bool | None = None,
        grounding_detail: dict[str, Any] | None = None,
    ) -> None:
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
        if grounding_ok is not None:
            row["grounding_ok"] = bool(grounding_ok)
        if isinstance(grounding_detail, dict) and grounding_detail:
            row["grounding_detail"] = dict(grounding_detail)
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

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
