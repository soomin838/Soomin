from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

PROMPT_PACK_VERSION = 2
_STALE_CACHE_PATTERNS = (
    "deliver maximum engagement and click-through-rate value",
    "write from the reader's lived experience",
    "source frame:",
    "main tradeoff:",
    "keep a one-line status update tied to",
    "patient, approachable tech educator",
)


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
    version: int = PROMPT_PACK_VERSION


_PERSONA_POOL = [
    {
        "id": "analyst",
        "system": (
            "You are a senior technology analyst. "
            "Write clearly, use evidence, and explain why the topic matters to a practical reader."
        ),
        "temperature": 0.68,
    },
    {
        "id": "editor",
        "system": (
            "You are an experienced blog editor. "
            "Prefer clean structure, readable flow, and useful context over hype."
        ),
        "temperature": 0.64,
    },
    {
        "id": "explainer",
        "system": (
            "You are a careful explainer. "
            "Break down complicated topics into plain language without sounding simplistic."
        ),
        "temperature": 0.66,
    },
    {
        "id": "practitioner",
        "system": (
            "You are a hands-on practitioner. "
            "Focus on what changed, what it means, and what a reader can do with the information."
        ),
        "temperature": 0.7,
    },
    {
        "id": "reviewer",
        "system": (
            "You are a measured reviewer. "
            "Be specific, fair, and grounded in observable facts instead of dramatic claims."
        ),
        "temperature": 0.62,
    },
]

_GLOBAL_BAN_TOKENS = [
    "as an ai",
    "in conclusion",
    "in summary",
    "overall",
    "delve",
    "testament",
    "tapestry",
    "seamlessly",
    "it's worth noting",
    "it is important to note",
    "unbelievable",
    "detector-evasion language",
    "hype-first phrasing",
    "sensational filler",
]


class PromptFactory:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.base_dir = (root / "storage" / "prompt_packs").resolve()
        self._purge_stale_packs()

    def get_pack(self, purpose: str, seed: str = "") -> PromptPack:
        purpose_key = str(purpose or "generic").strip().lower() or "generic"
        day = datetime.now(timezone.utc).date().isoformat()
        pack_path = self.base_dir / purpose_key / f"{day}.json"
        if pack_path.exists():
            try:
                data = json.loads(pack_path.read_text(encoding="utf-8"))
                if self._is_valid_cached_pack(data):
                    return PromptPack(**data)
                pack_path.unlink(missing_ok=True)
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

    def _is_valid_cached_pack(self, data: dict[str, object]) -> bool:
        if not isinstance(data, dict):
            return False
        if int(data.get("version", 0) or 0) != PROMPT_PACK_VERSION:
            return False
        required = {"purpose", "system", "user", "style_variant_id", "must_include", "ban_tokens", "temperature", "top_p"}
        if not required.issubset({str(k or "") for k in data.keys()}):
            return False
        merged = " ".join(
            str(data.get(key, "") or "")
            for key in ("purpose", "system", "user", "style_variant_id", "persona_id")
        ).lower()
        if any(marker in merged for marker in _STALE_CACHE_PATTERNS):
            return False
        return True

    def _purge_stale_packs(self) -> None:
        if not self.base_dir.exists():
            return
        for pack_path in self.base_dir.rglob("*.json"):
            try:
                data = json.loads(pack_path.read_text(encoding="utf-8"))
            except Exception:
                try:
                    pack_path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue
            if self._is_valid_cached_pack(data):
                continue
            try:
                pack_path.unlink(missing_ok=True)
            except Exception:
                continue

    def _style_variant_id(self, purpose: str, seed: str) -> str:
        src = f"{datetime.now(timezone.utc).date().isoformat()}:{purpose}:{seed}".encode("utf-8")
        idx = int(hashlib.sha1(src).hexdigest(), 16) % 12
        return f"v{idx + 1}"

    def _select_persona(self, purpose: str, seed: str) -> dict:
        src = f"persona:{datetime.now(timezone.utc).date().isoformat()}:{purpose}:{seed}".encode("utf-8")
        idx = int(hashlib.sha256(src).hexdigest(), 16) % len(_PERSONA_POOL)
        return _PERSONA_POOL[idx]

    def _build_default_pack(self, purpose: str, style_variant_id: str, persona: dict) -> PromptPack:
        system = str(persona.get("system", ""))
        user = (
            "Deliver a clear, useful, practical article in a natural blog tone. "
            "Prefer informational value, concrete details, and evidence-first framing."
        )
        must_include: list[str] = []
        ban_tokens = list(_GLOBAL_BAN_TOKENS)
        temperature = float(persona.get("temperature", 0.66))
        top_p = 0.9
        persona_id = str(persona.get("id", ""))

        if purpose in {"headline", "choose_best"}:
            system = (
                "You are a senior editorial headline strategist. "
                "Write headlines that are clear, useful, specific, and ad-safe."
            )
            user = (
                "Generate a headline that reflects the real article value. "
                "Do not exaggerate, sensationalize, or manufacture false urgency."
            )
            temperature = 0.55
        elif purpose == "rewrite_to_actionable":
            system = (
                "You are an editorial rewrite assistant. "
                "Improve clarity and usefulness while keeping the tone natural and grounded."
            )
            user = (
                "Rewrite the source into a practical, readable article. "
                "Avoid clickbait phrasing, hype, detector-evasion language, and templated filler."
            )
            temperature = 0.64
        elif purpose == "extract_keywords":
            system = "You are an SEO editor focused on genuine search intent."
            user = (
                "Extract practical long-tail keywords and reader questions. "
                "Favor terms a real searcher would type."
            )
            temperature = 0.4

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
