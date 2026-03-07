from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class PolicyDecision:
    allow: bool
    risk_level: str
    reason_codes: list[str] = field(default_factory=list)
    matched_terms: list[str] = field(default_factory=list)
    route: str = ""
    category: str = ""


class SafetyFilter:
    _HARD_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("drug_hallucinogen", ("dmt", "psychedelic", "hallucinogen", "lsd", "psilocybin", "magic mushroom")),
        ("adult_explicit", ("adult", "porn", "porno", "adult video", "sexual content", "nsfw", "explicit", "explicit sex")),
        ("gambling", ("gambling", "casino", "sportsbook", "betting", "slot machine", "poker odds")),
        ("weapon_explosive", ("weapon", "bomb", "explosive", "detonator", "3d printed gun")),
        ("self_harm", ("suicide", "self-harm", "kill yourself", "cutting yourself")),
        ("hate_violence", ("ethnic cleansing", "violent extremism", "hate group", "mass shooting")),
        ("illegal_activity", ("bypass paywall", "credit card fraud", "carding", "drug trafficking", "pirated serial key")),
        ("medical_direct_advice", ("medical advice", "treatment plan", "prescription", "dosage recommendation")),
        ("finance_direct_advice", ("investment strategy", "stock pick", "buy this stock", "trading signal")),
        ("political_direct_advice", ("political campaign", "election prediction", "voter manipulation")),
    )
    _SOFT_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("breach", ("breach", "data leak", "credential leak")),
        ("lawsuit", ("lawsuit", "class action", "sued by")),
        ("scam", ("scam", "fraud", "ponzi")),
        ("medical_claim", ("medical claim", "clinically proven", "heals", "cures")),
        ("finance_claim", ("guaranteed return", "passive income", "double your money")),
        ("political_claim", ("election fraud", "ballot harvesting", "campaign scandal")),
    )

    def __init__(self, *, log_path: Path | None = None) -> None:
        self.log_path = Path(log_path).resolve() if log_path else None

    def evaluate(
        self,
        *,
        title: str,
        snippet: str = "",
        body_excerpt: str = "",
        category: str = "",
        route: str,
        source_url: str = "",
    ) -> PolicyDecision:
        merged = self._normalize(" ".join([title or "", snippet or "", body_excerpt or ""]))
        matched_terms: list[str] = []
        reason_codes: list[str] = []
        risk_level = "low"

        for code, terms in self._HARD_RULES:
            found = [term for term in terms if self._contains(merged, term)]
            if not found:
                continue
            matched_terms.extend(found)
            reason_codes.append(code)
        if reason_codes:
            decision = PolicyDecision(
                allow=False,
                risk_level="high",
                reason_codes=sorted(set(reason_codes)),
                matched_terms=sorted(set(matched_terms)),
                route=str(route or "").strip().lower(),
                category=str(category or "").strip().lower(),
            )
            self._log(decision, source_url=source_url)
            return decision

        soft_codes: list[str] = []
        soft_terms: list[str] = []
        for code, terms in self._SOFT_RULES:
            found = [term for term in terms if self._contains(merged, term)]
            if not found:
                continue
            soft_codes.append(code)
            soft_terms.extend(found)
        if soft_codes:
            risk_level = "medium"
            reason_codes.extend(sorted(set(soft_codes)))
            matched_terms.extend(sorted(set(soft_terms)))

        decision = PolicyDecision(
            allow=True,
            risk_level=risk_level,
            reason_codes=sorted(set(reason_codes)),
            matched_terms=sorted(set(matched_terms)),
            route=str(route or "").strip().lower(),
            category=str(category or "").strip().lower(),
        )
        if decision.risk_level != "low":
            self._log(decision, source_url=source_url)
        return decision

    def _normalize(self, text: str) -> str:
        lowered = str(text or "").lower()
        lowered = re.sub(r"https?://\S+", " ", lowered)
        lowered = re.sub(r"[^a-z0-9\s\-]+", " ", lowered)
        lowered = re.sub(r"\s+", " ", lowered).strip()
        return lowered

    def _contains(self, text: str, term: str) -> bool:
        needle = self._normalize(term)
        if not needle:
            return False
        if " " in needle:
            return needle in text
        return bool(re.search(rf"\b{re.escape(needle)}\b", text))

    def _log(self, decision: PolicyDecision, *, source_url: str = "") -> None:
        if self.log_path is None:
            return
        row = asdict(decision)
        row["ts_utc"] = datetime.now(timezone.utc).isoformat()
        row["source_url"] = str(source_url or "").strip()[:300]
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return
