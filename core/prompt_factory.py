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
        pack = self._build_default_pack(purpose_key, style_variant_id)
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

    def _build_default_pack(self, purpose: str, style_variant_id: str) -> PromptPack:
        system = "Write practical US-English troubleshooting content with deterministic structure."
        user = "Follow search intent and actionable steps."
        must_include = ["Expected result:", "If not:", "Time to try:"]
        ban_tokens = ["as an AI", "in conclusion", "overall", "delve"]
        temperature = 0.35
        top_p = 0.9

        if purpose == "headline":
            user = "Generate a troubleshooting-first headline with concrete user intent."
            must_include = ["fix", "not working"]
        elif purpose == "rewrite_to_actionable":
            user = "Rewrite into step-by-step fixes with clear branch handling."
            temperature = 0.25
        elif purpose == "extract_keywords":
            user = "Extract long-tail troubleshooting keywords with OS + trigger terms."
            must_include = ["after update", "error", "not working"]
        elif purpose == "choose_best":
            user = "Prioritize non-duplicate, high-intent troubleshooting candidates."
            temperature = 0.2

        return PromptPack(
            purpose=purpose,
            system=system,
            user=user,
            style_variant_id=style_variant_id,
            must_include=must_include,
            ban_tokens=ban_tokens,
            temperature=temperature,
            top_p=top_p,
        )

