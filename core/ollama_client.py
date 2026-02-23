from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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
    def __init__(self, settings: LocalLLMSettings, log_path: Path | None = None) -> None:
        self.settings = settings
        self.base_url = str(getattr(settings, "base_url", "http://127.0.0.1:11434") or "http://127.0.0.1:11434").rstrip("/")
        self.model = str(getattr(settings, "model", "qwen2.5:3b") or "qwen2.5:3b").strip()
        self.timeout = max(10, int(getattr(settings, "request_timeout_sec", 60) or 60))
        self.num_ctx = max(1024, int(getattr(settings, "num_ctx", 2048) or 2048))
        self.num_thread = max(1, int(getattr(settings, "num_thread", 2) or 2))
        self.log_path = log_path

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

    def _log_call(
        self,
        *,
        purpose: str,
        latency_ms: int,
        ok: bool,
        error: str = "",
        fallback_used: bool = False,
        prompt_len: int = 0,
        response_len: int = 0,
    ) -> None:
        if self.log_path is None:
            return
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "purpose": str(purpose or "generic"),
            "model": self.model,
            "endpoint": "/api/generate",
            "latency_ms": int(latency_ms),
            "ok": bool(ok),
            "error": str(error or "")[:400],
            "fallback_used": bool(fallback_used),
            "prompt_len": int(max(0, prompt_len)),
            "response_len": int(max(0, response_len)),
        }
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def generate_json(self, system_prompt: str, user_payload: dict, purpose: str = "generic") -> dict:
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
        started = time.perf_counter()
        for _ in range(2):
            try:
                r = requests.post(endpoint, json=payload, timeout=self.timeout)
                r.raise_for_status()
                data = r.json() or {}
                content = str(data.get("response", "") or "").strip()
                parsed = self._extract_json(content)
                if parsed:
                    latency_ms = int((time.perf_counter() - started) * 1000)
                    self._log_call(
                        purpose=purpose,
                        latency_ms=latency_ms,
                        ok=True,
                        error="",
                        fallback_used=False,
                        prompt_len=len(prompt),
                        response_len=len(content),
                    )
                    return parsed
            except Exception as exc:
                last_err = exc
        latency_ms = int((time.perf_counter() - started) * 1000)
        self._log_call(
            purpose=purpose,
            latency_ms=latency_ms,
            ok=False,
            error=str(last_err or "ollama_generate_failed"),
            fallback_used=True,
            prompt_len=len(prompt),
            response_len=0,
        )
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
        sections = {k: re.sub(r"\s+", " ", str(v or "")).strip()[:220] for k, v in (section_texts or {}).items()}
        system_prompt = (
            "You are generating image prompts for a software troubleshooting blog post.\n"
            "Return JSON only with fields: banner_prompt, inline_prompt, alt_suggestions, style_tags.\n"
            "Hard rules:\n"
            "- The images must represent SOFTWARE troubleshooting, UI settings, checklists, or flow diagrams.\n"
            "- No physical hazards: no fire, no smoke, no explosion, no damaged hardware, no injury, no dangerous scenes.\n"
            "- No literal before/after disaster metaphors.\n"
            "- Style: clean minimal vector, pastel, rounded shapes, simple icons.\n"
            "- No text, no letters, no numbers, no logos, no watermark.\n"
            "- Banner should be a simple troubleshooting flow diagram (3-5 boxes).\n"
            "- Inline should be a checklist/step diagram (3-7 steps) relevant to the article.\n"
            "- Return prompts in US English.\n"
            "JSON schema:\n"
            "{\"banner_prompt\": \"string\", \"inline_prompt\": \"string\", \"alt_suggestions\": [\"string\",\"string\",\"string\"], \"style_tags\": [\"string\", \"string\"]}"
        )
        user_payload = {
            "keyword": keyword,
            "device_type": device_type,
            "cluster_id": cluster_id,
            "section_texts": sections,
            "rules": {
                "banner": "software troubleshooting flow diagram",
                "inline": "software troubleshooting checklist diagram",
                "alt_suggestions": "short natural English sentences",
            },
        }
        data = self.generate_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            purpose="image_plan",
        )
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
            banner = (
                f"minimal software troubleshooting flow diagram for {device_type} issue, "
                "pastel vector, rounded boxes, no text, no letters, no numbers, no logos, no watermark"
            )
        if not inline:
            inline = (
                f"minimal checklist diagram for {device_type} not working issue, "
                "3 to 7 steps, pastel vector, rounded icons, no text, no letters, no numbers, no logos, no watermark"
            )
        if not alt_suggestions:
            alt_suggestions = [
                "Troubleshooting flow diagram for the current software issue.",
                "Checklist-style visual for fixing a common software problem.",
                "Step-by-step troubleshooting concept image for everyday users.",
            ]
        if not style_tags:
            style_tags = ["minimal", "pastel", "rounded", "diagram"]

        return ImagePromptPlan(
            banner_prompt=banner,
            inline_prompt=inline,
            alt_suggestions=alt_suggestions,
            style_tags=style_tags,
        )

    def review_article_quality(
        self,
        *,
        title: str,
        html: str,
        intro_text: str,
        alt_texts: list[str],
    ) -> dict[str, Any]:
        compact_html = re.sub(r"\s+", " ", str(html or "")).strip()[:6000]
        compact_intro = re.sub(r"\s+", " ", str(intro_text or "")).strip()[:500]
        compact_alts = [re.sub(r"\s+", " ", str(a or "")).strip()[:220] for a in (alt_texts or []) if str(a or "").strip()]
        system_prompt = (
            "You are a strict blog QA reviewer.\n"
            "Return JSON only.\n"
            "Language: English only.\n"
            "Detect: internal debug leaks, AI-like repetitive markers, and intro-alt semantic duplication risk.\n"
            "Never include explanations outside JSON.\n"
            "JSON schema:\n"
            "{\"issues\": [str], \"remove_phrases\": [str], \"rewrite_needed\": bool, \"summary\": str}"
        )
        user_payload = {
            "title": str(title or ""),
            "html_excerpt": compact_html,
            "intro_text": compact_intro,
            "alt_texts": compact_alts[:5],
            "rules": {
                "ban_tokens": [
                    "workflow checkpoint stage",
                    "av reference context",
                    "jobtitle",
                    "sameas",
                    "selected topic",
                    "source trending_entities",
                ],
                "max_remove_phrases": 8,
            },
        }
        data = self.generate_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            purpose="qa_review",
        )
        issues: list[str] = []
        remove_phrases: list[str] = []
        for v in (data.get("issues", []) if isinstance(data, dict) else []):
            t = re.sub(r"\s+", " ", str(v or "")).strip()
            if t and t not in issues:
                issues.append(t[:160])
            if len(issues) >= 12:
                break
        for v in (data.get("remove_phrases", []) if isinstance(data, dict) else []):
            t = re.sub(r"\s+", " ", str(v or "")).strip()
            if t and t not in remove_phrases:
                remove_phrases.append(t[:120])
            if len(remove_phrases) >= 8:
                break
        rewrite_needed = bool((data or {}).get("rewrite_needed", False)) if isinstance(data, dict) else False
        summary = re.sub(r"\s+", " ", str((data or {}).get("summary", "") if isinstance(data, dict) else "")).strip()[:220]
        return {
            "issues": issues,
            "remove_phrases": remove_phrases,
            "rewrite_needed": rewrite_needed,
            "summary": summary,
        }
