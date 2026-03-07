from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class PromptPack:
    purpose: str
    system: str
    user: str
    style_variant_id: str
    must_include: list[str]
    ban_tokens: list[str]
    temperature: float
    top_p: float
    persona_id: str = ""


# ── Persona Pool (5개 로테이션) ───────────────────────
_PERSONA_POOL = [
    {
        "id": "analyst",
        "label": "Tech Analyst",
        "system": (
            "You are a sharp, no-nonsense senior tech analyst writing for a high-traffic blog. "
            "Break down complex topics into digestible insights with data-backed arguments. "
            "Use precise language, cite numbers/benchmarks, and include a clear 'bottom line' takeaway. "
            "Avoid fluff—every sentence must add value."
        ),
        "temperature": 0.80,
    },
    {
        "id": "storyteller",
        "label": "Viral Storyteller",
        "system": (
            "You are a brilliant tech storyteller who makes readers feel like they're watching a thriller unfold. "
            "Use vivid metaphors, dramatic pacing, and cliffhanger transitions. "
            "Turn dry tech news into gripping narratives. Short paragraphs, rapid-fire sentences, "
            "occasional one-liners for emphasis. Keep the pacing strong without overselling the story."
        ),
        "temperature": 0.92,
    },
    {
        "id": "educator",
        "label": "Friendly Educator",
        "system": (
            "You are a patient, approachable tech educator writing for curious minds. "
            "Explain concepts as if talking to a bright friend over coffee. "
            "Use analogies, step-by-step breakdowns, and 'Why should I care?' sections. "
            "Sprinkle in humor to keep things light. Make complex topics feel simple and actionable."
        ),
        "temperature": 0.85,
    },
    {
        "id": "contrarian",
        "label": "Devil's Advocate",
        "system": (
            "You are a provocative tech commentator who challenges mainstream narratives. "
            "Start with a bold, counter-intuitive claim and back it up with evidence. "
            "Use rhetorical questions, strategic sarcasm, and 'unpopular opinion' angles. "
            "Your goal is to make readers think differently, not just consume passively."
        ),
        "temperature": 0.90,
    },
    {
        "id": "practitioner",
        "label": "Hands-On Practitioner",
        "system": (
            "You are a battle-tested developer/engineer sharing real-world experience. "
            "Focus on practical code snippets, config examples, and 'what I actually did' narratives. "
            "Use casual dev-speak ('Let me show you the trick...', 'Here's the gotcha...'). "
            "Include troubleshooting tips and edge cases that only experience reveals."
        ),
        "temperature": 0.82,
    },
]


# ── Ban tokens (AI 감지 회피) ─────────────────────────
_GLOBAL_BAN_TOKENS = [
    "in conclusion", "furthermore", "delve", "testament",
    "tapestry", "seamlessly", "as an AI", "overall", "in summary",
    "it is important to note", "it's worth noting",
]


class PromptFactory:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.base_dir = (root / "storage" / "prompt_packs").resolve()

    def get_pack(self, purpose: str, seed: str = "") -> PromptPack:
        purpose_key = str(purpose or "generic").strip().lower() or "generic"
        day = datetime.now(timezone.utc).date().isoformat()
        pack_path = self.base_dir / purpose_key / f"{day}.json"
        if pack_path.exists():
            try:
                data = json.loads(pack_path.read_text(encoding="utf-8"))
                return PromptPack(**data)
            except Exception:
                pass

        style_variant_id = self._style_variant_id(purpose_key, seed)
        persona = self._select_persona(purpose_key, seed)
        pack = self._build_default_pack(purpose_key, style_variant_id, persona)
        try:
            pack_path.parent.mkdir(parents=True, exist_ok=True)
            pack_path.write_text(
                json.dumps(asdict(pack), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass
        return pack

    def _style_variant_id(self, purpose: str, seed: str) -> str:
        src = f"{datetime.now(timezone.utc).date().isoformat()}:{purpose}:{seed}".encode("utf-8")
        idx = int(hashlib.sha1(src).hexdigest(), 16) % 12
        return f"v{idx + 1}"

    def _select_persona(self, purpose: str, seed: str) -> dict:
        """날짜+seed 해시 기반으로 5개 페르소나 중 하나를 자동 선택."""
        src = f"persona:{datetime.now(timezone.utc).date().isoformat()}:{purpose}:{seed}".encode("utf-8")
        idx = int(hashlib.sha256(src).hexdigest(), 16) % len(_PERSONA_POOL)
        return _PERSONA_POOL[idx]

    def _build_default_pack(self, purpose: str, style_variant_id: str, persona: dict) -> PromptPack:
        system = str(persona.get("system", ""))
        user = "Deliver specific, useful, evidence-first reader value."
        must_include: list[str] = []
        ban_tokens = list(_GLOBAL_BAN_TOKENS)
        temperature = float(persona.get("temperature", 0.85))
        top_p = 0.95
        persona_id = str(persona.get("id", ""))

        if purpose in {"headline", "choose_best"}:
            system = (
                "You are a senior editorial headline strategist. "
                "Write specific, useful, ad-safe headlines that create curiosity without sounding manipulative. "
                "Prioritize clarity, evidence, and strong reader value."
            )
            user = "Generate a specific, useful, evidence-first headline for a tech or consumer story."
            must_include = []
            temperature = 0.72
        elif purpose == "rewrite_to_actionable":
            user = (
                "Rewrite the source material into a practical, readable article. "
                "Vary sentence rhythm naturally, keep transitions fresh, and avoid boilerplate phrasing. "
                "Prefer concrete examples, useful detail, and ad-safe wording."
            )
            temperature = 0.78
        elif purpose == "extract_keywords":
            system = "You are an SEO editor focused on real search demand."
            user = "Extract specific long-tail keywords and questions that reflect real user search intent."
            temperature = 0.5

        return PromptPack(
            purpose=purpose,
            system=system,
            user=user,
            style_variant_id=style_variant_id,
            must_include=must_include,
            ban_tokens=ban_tokens,
            temperature=temperature,
            top_p=top_p,
            persona_id=persona_id,
        )
