from __future__ import annotations

import random
import re
from dataclasses import dataclass
from typing import Any

from .ollama_client import OllamaClient


@dataclass
class NewsPromptPack:
    background_prompt: str
    hook_candidates: list[str]
    style_tags: list[str]


class NewsPackPromptFactory:
    def __init__(self, ollama_client: OllamaClient | None = None) -> None:
        self.ollama_client = ollama_client

    def build(
        self,
        *,
        tags: list[str],
        kind: str,
        seed: int,
        context: dict[str, Any] | None = None,
    ) -> NewsPromptPack:
        clean_tags = [str(t or "").strip().lower() for t in (tags or []) if str(t or "").strip()]
        clean_tags = clean_tags[:4] if clean_tags else ["platform"]
        payload = {}
        if self.ollama_client is not None:
            try:
                payload = self.ollama_client.build_news_image_prompt(
                    tags=clean_tags,
                    kind=str(kind or "inline_bg"),
                    seed=int(seed),
                    context=dict(context or {}),
                )
            except Exception:
                payload = {}
        if not isinstance(payload, dict) or not payload:
            payload = self._fallback_payload(tags=clean_tags, kind=str(kind or "inline_bg"), seed=int(seed))

        prompt = self._sanitize_prompt(str(payload.get("background_prompt", "") or ""))
        if not prompt:
            prompt = self._sanitize_prompt(
                "tech news editorial background, abstract technology geometry, "
                "clean modern composition, no text, no logos, no watermark"
            )
        hooks = self._normalize_hooks(payload.get("hook_candidates", []), tags=clean_tags, seed=seed)
        styles = self._normalize_style_tags(payload.get("style_tags", []))
        return NewsPromptPack(background_prompt=prompt, hook_candidates=hooks, style_tags=styles)

    def _sanitize_prompt(self, prompt: str) -> str:
        text = re.sub(r"\s+", " ", str(prompt or "").strip())
        guard = (
            "tech news editorial background, abstract modern tech shapes, high contrast, "
            "no readable text, no logo, no trademark, no watermark, no screenshot"
        )
        if not text:
            return guard
        lower = text.lower()
        if "tech news" not in lower:
            text += ", tech news editorial background"
        for forbidden in ("logo", "watermark", "trademark", "screenshot", "readable text"):
            if forbidden not in lower:
                text += f", no {forbidden}"
        return re.sub(r"\s+", " ", text).strip()[:760]

    def _normalize_hooks(self, raw: Any, *, tags: list[str], seed: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        candidates = raw if isinstance(raw, list) else []
        for item in candidates:
            text = re.sub(r"[^A-Za-z0-9\s]", " ", str(item or "").upper())
            text = re.sub(r"\s+", " ", text).strip()
            text = " ".join(text.split()[:3])
            if not text:
                continue
            if text in seen:
                continue
            seen.add(text)
            out.append(text)
            if len(out) >= 4:
                break
        if out:
            return out

        fallback = {
            "security": ["SECURITY ALERT", "PATCH NOW", "NEW VULN", "DATA RISK"],
            "policy": ["NEW POLICY", "BIG CHANGE", "WHAT CHANGED", "ACT NOW"],
            "ai": ["AI UPDATE", "MODEL SHIFT", "NEW TOOLS", "FAST CHANGE"],
            "platform": ["MAJOR UPDATE", "ROLLING OUT", "WHAT CHANGED", "IMPACT NOW"],
            "mobile": ["MOBILE UPDATE", "NEW FEATURE", "APP CHANGE", "PHONE ALERT"],
            "chips": ["CHIP RACE", "NEW GPU", "PRICE SHIFT", "SUPPLY SHIFT"],
        }
        tag = tags[0] if tags else "platform"
        pool = fallback.get(tag, fallback["platform"])
        rng = random.Random(int(seed))
        rng.shuffle(pool)
        return pool[:3]

    def _normalize_style_tags(self, raw: Any) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for item in (raw if isinstance(raw, list) else []):
            text = re.sub(r"[^a-z0-9_-]", "", str(item or "").lower()).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
            if len(out) >= 5:
                break
        if out:
            return out
        return ["editorial", "abstract", "high-contrast", "minimal"]

    def _fallback_payload(self, *, tags: list[str], kind: str, seed: int) -> dict[str, Any]:
        rng = random.Random(seed)
        tone = rng.choice(["dark teal", "cyan blue", "midnight blue", "graphite"])
        shape = rng.choice(["isometric blocks", "network lines", "layered polygons", "signal waves"])
        label = tags[0] if tags else "platform"
        focus = "thumbnail hero composition" if str(kind).strip().lower() == "thumb_bg" else "section support visual"
        return {
            "background_prompt": (
                f"tech news editorial background about {label}, {focus}, {shape}, {tone}, "
                "clean modern abstract visual, no text, no logos, no watermark, no screenshot"
            ),
            "hook_candidates": [],
            "style_tags": ["editorial", "abstract", "minimal"],
        }

