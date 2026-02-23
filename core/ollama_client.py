from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import requests

from .settings import LocalLLMSettings


@dataclass
class ImagePromptPlan:
    banner_prompt: str
    inline_prompt: str
    alt_suggestions: list[str]
    style_tags: list[str]


class OllamaClient:
    def __init__(self, settings: LocalLLMSettings) -> None:
        self.settings = settings
        self.base_url = str(getattr(settings, "base_url", "http://127.0.0.1:11434") or "http://127.0.0.1:11434").rstrip("/")
        self.model = str(getattr(settings, "model", "qwen2.5:3b") or "qwen2.5:3b").strip()
        self.timeout = max(10, int(getattr(settings, "request_timeout_sec", 60) or 60))
        self.num_ctx = max(1024, int(getattr(settings, "num_ctx", 2048) or 2048))
        self.num_thread = max(1, int(getattr(settings, "num_thread", 2) or 2))

    def _extract_json(self, raw: str) -> dict[str, Any]:
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not m:
            return {}
        try:
            parsed = json.loads(m.group(0))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
        return {}

    def healthcheck(self) -> bool:
        for path in ("/api/version", "/api/tags"):
            try:
                r = requests.get(f"{self.base_url}{path}", timeout=2)
                if r.status_code == 200:
                    return True
            except Exception:
                continue
        return False

    def generate_json(self, system_prompt: str, user_payload: dict) -> dict:
        endpoint = f"{self.base_url}/api/generate"
        user_text = json.dumps(user_payload or {}, ensure_ascii=False, indent=2)
        prompt = (
            f"{str(system_prompt or '').strip()}\n\n"
            "User payload(JSON):\n"
            f"{user_text}\n\n"
            "Return ONLY JSON object."
        ).strip()
        payload = {
            "model": self.model,
            "stream": False,
            "format": "json",
            "prompt": prompt,
            "options": {
                "num_ctx": self.num_ctx,
                "num_thread": self.num_thread,
                "temperature": 0.35,
                "top_p": 0.9,
            },
        }
        last_err: Exception | None = None
        for _ in range(2):
            try:
                r = requests.post(endpoint, json=payload, timeout=self.timeout)
                r.raise_for_status()
                data = r.json() or {}
                content = str(data.get("response", "") or "").strip()
                parsed = self._extract_json(content)
                if parsed:
                    return parsed
            except Exception as exc:
                last_err = exc
        if last_err is not None:
            raise last_err
        return {}

    def build_image_prompt_plan(
        self,
        *,
        keyword: str,
        device_type: str,
        cluster_id: str,
        section_texts: dict[str, str],
    ) -> ImagePromptPlan:
        sections = {k: re.sub(r"\s+", " ", str(v or "")).strip()[:420] for k, v in (section_texts or {}).items()}
        system_prompt = (
            "You are an image prompt planner for blog illustrations.\n"
            "Return JSON only.\n"
            "Language: English only.\n"
            "Do not include any explanation text outside JSON.\n"
            "Rules:\n"
            "- No text/letters/numbers/logos/watermarks in images.\n"
            "- Prompts must be diverse in composition, angle, and metaphor.\n"
            "- No references to screenshots or real UI capture.\n"
            "JSON schema:\n"
            "{\"banner_prompt\": str, \"inline_prompt\": str, \"alt_suggestions\": [str,str,str], \"style_tags\": [str,...]}"
        )
        user_payload = {
            "keyword": keyword,
            "device_type": device_type,
            "cluster_id": cluster_id,
            "section_texts": sections,
            "rules": {
                "banner": "symbolic and concept-rich",
                "inline": "troubleshooting flow or checklist process",
                "alt_suggestions": "natural short sentences",
            },
            "schema": {
                "banner_prompt": "string",
                "inline_prompt": "string",
                "alt_suggestions": ["string", "string", "string"],
                "style_tags": ["string"],
            },
        }
        data = self.generate_json(system_prompt=system_prompt, user_payload=user_payload)
        banner = re.sub(r"\s+", " ", str(data.get("banner_prompt", "") or "")).strip()
        inline = re.sub(r"\s+", " ", str(data.get("inline_prompt", "") or "")).strip()
        alt_raw = data.get("alt_suggestions", [])
        alt_suggestions: list[str] = []
        if isinstance(alt_raw, list):
            for v in alt_raw:
                t = re.sub(r"\s+", " ", str(v or "")).strip()
                if t and t not in alt_suggestions:
                    alt_suggestions.append(t[:180])
                if len(alt_suggestions) >= 3:
                    break
        tags_raw = data.get("style_tags", [])
        style_tags: list[str] = []
        if isinstance(tags_raw, list):
            for v in tags_raw:
                t = re.sub(r"\s+", " ", str(v or "")).strip().lower()
                if t and t not in style_tags:
                    style_tags.append(t[:40])
                if len(style_tags) >= 8:
                    break

        if not banner:
            banner = "Realistic conceptual photo of an organized workflow transformation with clean visual hierarchy."
        if not inline:
            inline = "Realistic process-oriented scene showing a simple troubleshooting flow with clear before-and-after contrast."
        if not alt_suggestions:
            alt_suggestions = [
                "Concept image representing a practical troubleshooting workflow.",
                "Illustration of a structured process used to solve recurring office issues.",
                "Visual summary of a step-by-step productivity improvement flow.",
            ]
        if not style_tags:
            style_tags = ["realistic", "clean", "editorial", "diagrammatic"]

        return ImagePromptPlan(
            banner_prompt=banner,
            inline_prompt=inline,
            alt_suggestions=alt_suggestions,
            style_tags=style_tags,
        )
