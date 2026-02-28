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


@dataclass
class TroubleshootingPlan:
    primary_keyword: str
    device_family: str
    issue_summary: str
    symptom_phrases: list[str]
    likely_causes: list[str]
    fix_steps: list[dict[str, str]]
    verification: list[str]
    when_to_stop: list[str]
    safe_warnings: list[str]
    faq: list[dict[str, str]]
    internal_links_anchor_ideas: list[str]
    meta_description_seed: str


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

    def build_troubleshooting_plan(
        self,
        *,
        keyword: str,
        device_type: str,
        cluster_id: str,
        context: dict[str, str] | None = None,
    ) -> TroubleshootingPlan:
        compact_context = {
            k: re.sub(r"\s+", " ", str(v or "")).strip()[:240]
            for k, v in (context or {}).items()
            if str(k or "").strip()
        }
        system_prompt = (
            "You are a troubleshooting planner for US consumer tech readers.\n"
            "Return JSON only.\n"
            "Language: US English only.\n"
            "Scope: SOFTWARE troubleshooting only.\n"
            "Do not include dangerous physical or electrical hazard scenes.\n"
            "Make it beginner-safe and actionable.\n"
            "Required JSON keys:\n"
            "primary_keyword, device_family, issue_summary, symptom_phrases, likely_causes, fix_steps, "
            "verification, when_to_stop, safe_warnings, faq, internal_links_anchor_ideas, meta_description_seed.\n"
            "fix_steps must be an array of 6-10 objects with keys:\n"
            "step_title, action, menu_path, expected_result, if_not_worked_next, risk_level.\n"
            "risk_level must be low or medium.\n"
            "faq must be an array of objects with keys question and answer.\n"
            "Do not output markdown or commentary, only JSON."
        )
        user_payload = {
            "keyword": str(keyword or ""),
            "device_type": str(device_type or ""),
            "cluster_id": str(cluster_id or ""),
            "context": compact_context,
        }
        data = self.generate_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            purpose="plan_json",
        )
        return self._normalize_troubleshooting_plan(
            data=data,
            keyword=keyword,
            device_type=device_type,
            cluster_id=cluster_id,
        )

    def summarize_for_title(
        self,
        *,
        title: str,
        html: str,
        plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        compact_title = re.sub(r"\s+", " ", str(title or "")).strip()[:180]
        compact_html = re.sub(r"\s+", " ", str(html or "")).strip()[:7000]
        plan_payload = dict(plan or {})
        plan_keyword = re.sub(r"\s+", " ", str(plan_payload.get("primary_keyword", "") or "")).strip()[:120]
        plan_device = self._normalize_device_family(str(plan_payload.get("device_family", "") or ""))
        system_prompt = (
            "You summarize troubleshooting content for title generation.\n"
            "Return JSON only.\n"
            "US English only. No markdown.\n"
            "No SEO/meta commentary.\n"
            "short_summary must be 2-3 sentences and <= 400 chars.\n"
            "Schema:\n"
            "{"
            "\"short_summary\":\"string\","
            "\"primary_issue_phrase\":\"string\","
            "\"device_family\":\"windows|mac|iphone|galaxy\","
            "\"feature\":\"wifi|bluetooth|usb|printer|mic|camera|keyboard|mouse|driver|vpn|audio|battery|update|network\","
            "\"must_include_terms\":[\"string\"]"
            "}"
        )
        user_payload = {
            "title": compact_title,
            "html_excerpt": compact_html,
            "plan_primary_keyword": plan_keyword,
            "plan_device_family": plan_device,
        }
        try:
            data = self.generate_json(
                system_prompt=system_prompt,
                user_payload=user_payload,
                purpose="title_summary",
            )
        except Exception:
            data = {}
        summary = self._clean_plan_text(str((data or {}).get("short_summary", "") or ""), max_len=400)
        issue_phrase = self._clean_plan_text(str((data or {}).get("primary_issue_phrase", "") or ""), max_len=140)
        device_family = self._normalize_device_family(str((data or {}).get("device_family", "") or plan_device or "windows"))
        feature = self._extract_feature(
            str((data or {}).get("feature", "") or "")
            or self._extract_feature(f"{compact_title} {compact_html} {plan_keyword}")
        )
        must_include_terms = self._normalize_title_terms(
            raw_terms=(data or {}).get("must_include_terms", []),
            title=compact_title,
            primary_keyword=plan_keyword,
            feature=feature,
        )
        if not issue_phrase:
            issue_phrase = self._clean_plan_text(plan_keyword or compact_title or "software issue fix", max_len=120)
        if not summary:
            summary = self._clean_plan_text(
                f"This guide explains {issue_phrase} and gives ordered software fixes with expected results and fallback actions.",
                max_len=400,
            )
        return {
            "short_summary": summary,
            "primary_issue_phrase": issue_phrase,
            "device_family": device_family,
            "feature": feature,
            "must_include_terms": must_include_terms[:6],
        }

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

    def build_draft_html(
        self,
        *,
        plan: TroubleshootingPlan | dict[str, Any],
        internal_links_block: str = "",
        images_plan: dict[str, Any] | None = None,
        style_variant_id: str = "v1",
        title_hint: str = "",
    ) -> str:
        """
        Local-first full draft generator.
        Returns HTML body fragment only.
        """
        if isinstance(plan, TroubleshootingPlan):
            plan_payload = {
                "primary_keyword": plan.primary_keyword,
                "device_family": plan.device_family,
                "issue_summary": plan.issue_summary,
                "symptom_phrases": plan.symptom_phrases,
                "likely_causes": plan.likely_causes,
                "fix_steps": plan.fix_steps,
                "verification": plan.verification,
                "when_to_stop": plan.when_to_stop,
                "safe_warnings": plan.safe_warnings,
                "faq": plan.faq,
                "internal_links_anchor_ideas": plan.internal_links_anchor_ideas,
                "meta_description_seed": plan.meta_description_seed,
            }
        else:
            plan_payload = dict(plan or {})

        system_prompt = (
            "You write practical US-English troubleshooting guides.\n"
            "Output HTML body fragment only. No markdown. No JSON wrapper.\n"
            "Use this exact H2 order:\n"
            "Quick Take\n"
            "Symptoms (How you know it's this issue)\n"
            "Why This Happens\n"
            "Fix 1\nFix 2\nFix 3\nFix 4\nFix 5\n"
            "If None Worked (Safe escalation)\n"
            "Prevention Checklist\n"
            "Each Fix section must include:\n"
            "- 3-5 concise bullet steps\n"
            "- 'Expected result:' line\n"
            "- 'If not:' line\n"
            "- 'Time to try:' line\n"
            "Software troubleshooting only. No dangerous physical hazards.\n"
            "No internal metadata, no debug strings, no Korean."
        )
        user_payload = {
            "title_hint": str(title_hint or ""),
            "style_variant_id": str(style_variant_id or "v1"),
            "plan": plan_payload,
            "images_plan": dict(images_plan or {}),
            "internal_links_block": str(internal_links_block or "")[:3000],
        }
        data = self.generate_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            purpose="draft_html",
        )
        html = ""
        if isinstance(data, dict):
            html = str(
                data.get("content_html", "")
                or data.get("html", "")
                or data.get("draft_html", "")
                or ""
            ).strip()
        if not html:
            # Minimal deterministic fallback to keep local-first resilient.
            keyword = self._clean_plan_text(str(plan_payload.get("primary_keyword", "") or title_hint), max_len=100)
            device = self._normalize_device_family(str(plan_payload.get("device_family", "") or "windows"))
            summary = self._clean_plan_text(str(plan_payload.get("issue_summary", "") or ""), max_len=200)
            checks = plan_payload.get("verification", []) if isinstance(plan_payload.get("verification", []), list) else []
            checks = [self._clean_plan_text(str(x or ""), max_len=120) for x in checks if str(x or "").strip()][:6]
            if not checks:
                checks = ["Verify the issue does not return after reboot.", "Confirm the setting remains stable."]
            html = (
                f"<h2>Quick Take</h2><p>{summary or f'Use this checklist to fix {keyword} on {device}.'}</p>"
                f"<h2>Symptoms (How you know it's this issue)</h2><ul>"
                + "".join(f"<li>{self._clean_plan_text(str(x or ''), max_len=120)}</li>" for x in plan_payload.get("symptom_phrases", [])[:5])
                + "</ul>"
                "<h2>Why This Happens</h2><ul>"
                + "".join(f"<li>{self._clean_plan_text(str(x or ''), max_len=150)}</li>" for x in plan_payload.get("likely_causes", [])[:4])
                + "</ul>"
            )
            for idx, row in enumerate(list(plan_payload.get("fix_steps", []))[:5], start=1):
                if not isinstance(row, dict):
                    continue
                step_title = self._clean_plan_text(str(row.get("step_title", "") or f"Fix step {idx}"), max_len=80)
                action = self._clean_plan_text(str(row.get("action", "") or "Apply the next safe software fix."), max_len=220)
                expected = self._clean_plan_text(str(row.get("expected_result", "") or "Expected result: the issue improves."), max_len=180)
                fallback = self._clean_plan_text(str(row.get("if_not_worked_next", "") or "If not: continue to the next fix."), max_len=180)
                html += (
                    f"<h2>Fix {idx}</h2>"
                    f"<p><strong>{step_title}</strong></p>"
                    f"<ul><li>{action}</li><li>Expected result: {expected}</li><li>If not: {fallback}</li><li>Time to try: 2 to 5 minutes.</li></ul>"
                )
            html += (
                "<h2>If None Worked (Safe escalation)</h2>"
                "<ul><li>Capture exact error text and timestamp.</li><li>Revert risky changes and contact official support.</li></ul>"
                "<h2>Prevention Checklist</h2><ul>"
                + "".join(f"<li>{x}</li>" for x in checks[:6])
                + "</ul>"
            )
        return re.sub(r"\s+", " ", html).replace("> <", "><").strip()

    def _normalize_troubleshooting_plan(
        self,
        *,
        data: dict[str, Any],
        keyword: str,
        device_type: str,
        cluster_id: str,
    ) -> TroubleshootingPlan:
        device_family = self._normalize_device_family(str(data.get("device_family", "") or device_type))
        primary_keyword = self._clean_plan_text(str(data.get("primary_keyword", "") or keyword), max_len=120)
        if not primary_keyword:
            primary_keyword = self._clean_plan_text(str(keyword or "windows update error fix"), max_len=120)

        issue_summary = self._clean_plan_text(str(data.get("issue_summary", "") or ""), max_len=220)
        if not issue_summary:
            issue_summary = (
                f"This guide helps you fix a common {device_family} software issue with safe steps for everyday users."
            )

        symptom_defaults = self._default_symptoms(primary_keyword, device_family)
        symptom_phrases = self._normalize_text_list(
            data.get("symptom_phrases", []),
            min_items=3,
            max_items=6,
            fallback=symptom_defaults,
            max_len=120,
        )
        likely_causes = self._normalize_text_list(
            data.get("likely_causes", []),
            min_items=4,
            max_items=7,
            fallback=self._default_causes(device_family, cluster_id),
            max_len=150,
        )
        fix_steps = self._normalize_fix_steps(
            data.get("fix_steps", []),
            device_family=device_family,
            cluster_id=cluster_id,
        )
        verification = self._normalize_text_list(
            data.get("verification", []),
            min_items=3,
            max_items=6,
            fallback=self._default_verification(device_family),
            max_len=150,
        )
        when_to_stop = self._normalize_text_list(
            data.get("when_to_stop", []),
            min_items=2,
            max_items=4,
            fallback=self._default_when_to_stop(device_family),
            max_len=160,
        )
        safe_warnings = self._normalize_text_list(
            data.get("safe_warnings", []),
            min_items=2,
            max_items=5,
            fallback=self._default_safe_warnings(device_family),
            max_len=170,
        )
        faq = self._normalize_faq(
            data.get("faq", []),
            primary_keyword=primary_keyword,
            device_family=device_family,
        )
        anchors = self._normalize_text_list(
            data.get("internal_links_anchor_ideas", []),
            min_items=6,
            max_items=10,
            fallback=self._default_anchor_ideas(device_family, cluster_id),
            max_len=90,
        )

        meta_seed = self._clean_plan_text(str(data.get("meta_description_seed", "") or ""), max_len=160)
        if len(meta_seed) < 140:
            suffix = f" Includes expected results, branch steps, and beginner-safe actions for {device_family} users."
            meta_seed = self._clean_plan_text((meta_seed + " " + suffix).strip(), max_len=160)
        if len(meta_seed) < 140:
            meta_seed = self._clean_plan_text(
                f"{primary_keyword}: step-by-step software fixes with expected results, fallback actions, and safe escalation tips for {device_family} users.",
                max_len=160,
            )

        return TroubleshootingPlan(
            primary_keyword=primary_keyword,
            device_family=device_family,
            issue_summary=issue_summary,
            symptom_phrases=symptom_phrases,
            likely_causes=likely_causes,
            fix_steps=fix_steps,
            verification=verification,
            when_to_stop=when_to_stop,
            safe_warnings=safe_warnings,
            faq=faq,
            internal_links_anchor_ideas=anchors,
            meta_description_seed=meta_seed,
        )

    def _normalize_device_family(self, text: str) -> str:
        low = str(text or "").strip().lower()
        if "win" in low:
            return "windows"
        if "mac" in low:
            return "mac"
        if "iphone" in low or "ios" in low:
            return "iphone"
        if "galaxy" in low or "android" in low:
            return "galaxy"
        return "windows"

    def _clean_plan_text(self, text: str, max_len: int = 220) -> str:
        cleaned = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", str(text or ""))
        for banned in self._hazard_terms():
            cleaned = re.sub(re.escape(banned), "software issue", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
        return cleaned[: max(20, int(max_len))]

    def _extract_feature(self, text: str) -> str:
        lower = re.sub(r"\s+", " ", str(text or "").strip().lower())
        for token, canonical in (
            ("wi-fi", "wifi"),
            ("wifi", "wifi"),
            ("bluetooth", "bluetooth"),
            ("usb", "usb"),
            ("printer", "printer"),
            ("microphone", "mic"),
            ("mic", "mic"),
            ("camera", "camera"),
            ("keyboard", "keyboard"),
            ("mouse", "mouse"),
            ("driver", "driver"),
            ("vpn", "vpn"),
            ("ethernet", "network"),
            ("network", "network"),
            ("audio", "audio"),
            ("sound", "audio"),
            ("battery", "battery"),
            ("charging", "battery"),
            ("update", "update"),
        ):
            if token in lower:
                return canonical
        return "network"

    def _normalize_title_terms(self, raw_terms: Any, title: str, primary_keyword: str, feature: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        items = raw_terms if isinstance(raw_terms, list) else []
        for item in items:
            txt = self._clean_plan_text(str(item or ""), max_len=60)
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(txt)
            if len(out) >= 6:
                break
        for seed in (primary_keyword, title, feature, "fix", "not working", "after update"):
            txt = self._clean_plan_text(str(seed or ""), max_len=60)
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(txt)
            if len(out) >= 6:
                break
        while len(out) < 3:
            out.append(["fix", "error", "after update"][len(out) % 3])
        return out[:6]

    def _normalize_text_list(
        self,
        raw: Any,
        *,
        min_items: int,
        max_items: int,
        fallback: list[str],
        max_len: int,
    ) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        if isinstance(raw, list):
            items = raw
        else:
            items = []
        for v in items:
            text = self._clean_plan_text(str(v or ""), max_len=max_len)
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
            if len(out) >= max_items:
                break
        for v in fallback:
            if len(out) >= max_items:
                break
            text = self._clean_plan_text(str(v or ""), max_len=max_len)
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
        while len(out) < min_items:
            out.append(self._clean_plan_text(fallback[(len(out)) % len(fallback)], max_len=max_len))
        return out[:max_items]

    def _normalize_fix_steps(
        self,
        raw: Any,
        *,
        device_family: str,
        cluster_id: str,
    ) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        if isinstance(raw, list):
            rows = raw
        else:
            rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            step = {
                "step_title": self._clean_plan_text(str(row.get("step_title", "") or ""), max_len=80),
                "action": self._clean_plan_text(str(row.get("action", "") or ""), max_len=220),
                "menu_path": self._clean_plan_text(str(row.get("menu_path", "") or ""), max_len=160),
                "expected_result": self._clean_plan_text(str(row.get("expected_result", "") or ""), max_len=220),
                "if_not_worked_next": self._clean_plan_text(str(row.get("if_not_worked_next", "") or ""), max_len=220),
                "risk_level": self._clean_plan_text(str(row.get("risk_level", "") or "low"), max_len=20).lower(),
            }
            if not step["step_title"] or not step["action"]:
                continue
            if step["risk_level"] not in {"low", "medium"}:
                step["risk_level"] = "low"
            if not step["expected_result"]:
                step["expected_result"] = "Expected result: the issue becomes less frequent or stops."
            if not step["if_not_worked_next"]:
                step["if_not_worked_next"] = "If not worked, move to the next fix in order."
            out.append(step)
            if len(out) >= 10:
                break
        fallback = self._default_fix_steps(device_family, cluster_id)
        idx = 0
        while len(out) < 6 and idx < len(fallback):
            out.append(dict(fallback[idx]))
            idx += 1
        while len(out) < 6:
            out.append(
                {
                    "step_title": f"Apply safe software fix {len(out) + 1}",
                    "action": "Restart the app or device and apply one controlled settings change.",
                    "menu_path": "",
                    "expected_result": "Expected result: symptoms improve after one controlled change.",
                    "if_not_worked_next": "If not worked, continue to the next structured fix.",
                    "risk_level": "low",
                }
            )
        return out[:10]

    def _normalize_faq(self, raw: Any, *, primary_keyword: str, device_family: str) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        if isinstance(raw, list):
            rows = raw
        else:
            rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            q = self._clean_plan_text(str(row.get("question", "") or row.get("q", "") or ""), max_len=140)
            a = self._clean_plan_text(str(row.get("answer", "") or row.get("a", "") or ""), max_len=220)
            if not q or not a:
                continue
            out.append({"question": q, "answer": a})
            if len(out) >= 6:
                break
        defaults = [
            {
                "question": f"How long should I test each fix for {primary_keyword}?",
                "answer": "Test each fix for 2 to 5 minutes before switching to the next one.",
            },
            {
                "question": f"Do I need to reinstall apps on {device_family} first?",
                "answer": "No. Start with low-risk settings checks and updates before reinstalling.",
            },
            {
                "question": "Can this be caused by a recent update?",
                "answer": "Yes. Many issues appear after updates when old settings conflict with new defaults.",
            },
            {
                "question": "When should I contact official support?",
                "answer": "Contact support if the issue remains after all software fixes and safety checks.",
            },
            {
                "question": "Will these steps delete my files?",
                "answer": "Most steps are low risk, but always back up important files before reset actions.",
            },
        ]
        i = 0
        while len(out) < 4 and i < len(defaults):
            out.append(defaults[i])
            i += 1
        return out[:6]

    def _default_fix_steps(self, device_family: str, cluster_id: str) -> list[dict[str, str]]:
        cluster_hint = self._clean_plan_text(cluster_id.replace("_", " "), max_len=80) or "core issue"
        return [
            {
                "step_title": "Restart and isolate the symptom",
                "action": f"Restart the {device_family} device and reproduce the {cluster_hint} issue with one app only.",
                "menu_path": "",
                "expected_result": "Expected result: you can confirm whether the issue is system-wide or app-specific.",
                "if_not_worked_next": "If not worked, continue with OS and app updates.",
                "risk_level": "low",
            },
            {
                "step_title": "Install pending system updates",
                "action": "Install all pending system updates, then reboot once.",
                "menu_path": self._device_menu_path(device_family, "update"),
                "expected_result": "Expected result: update-related bugs and compatibility issues are reduced.",
                "if_not_worked_next": "If not worked, update the affected app only and test again.",
                "risk_level": "low",
            },
            {
                "step_title": "Reset the affected app settings",
                "action": "Reset in-app preferences or clear app cache without deleting account data when possible.",
                "menu_path": self._device_menu_path(device_family, "apps"),
                "expected_result": "Expected result: corrupted local settings no longer trigger the issue.",
                "if_not_worked_next": "If not worked, sign out and sign in to refresh app state.",
                "risk_level": "low",
            },
            {
                "step_title": "Toggle network and permissions",
                "action": "Toggle Wi-Fi/Bluetooth/cellular or required permissions off and on once.",
                "menu_path": self._device_menu_path(device_family, "network"),
                "expected_result": "Expected result: stale connectivity or permission states are refreshed.",
                "if_not_worked_next": "If not worked, reset network settings and reconnect.",
                "risk_level": "low",
            },
            {
                "step_title": "Reinstall the affected app safely",
                "action": "Uninstall and reinstall the affected app, then restore minimal settings only.",
                "menu_path": self._device_menu_path(device_family, "apps"),
                "expected_result": "Expected result: broken binaries and cached conflicts are removed.",
                "if_not_worked_next": "If not worked, test in safe/clean boot mode to isolate conflicts.",
                "risk_level": "medium",
            },
            {
                "step_title": "Run safe diagnostics and escalate",
                "action": "Collect logs or error messages and contact official support with exact reproduction steps.",
                "menu_path": self._device_menu_path(device_family, "support"),
                "expected_result": "Expected result: support can identify root cause faster with reproducible evidence.",
                "if_not_worked_next": "If not worked, stop advanced changes and wait for vendor-level fix guidance.",
                "risk_level": "medium",
            },
        ]

    def _default_symptoms(self, keyword: str, device_family: str) -> list[str]:
        return [
            f"{keyword} issue appears after restart",
            f"{device_family} feature stops responding randomly",
            "Settings changes do not persist after reboot",
            "App opens but the core function fails",
        ]

    def _default_causes(self, device_family: str, cluster_id: str) -> list[str]:
        cluster_hint = self._clean_plan_text(cluster_id.replace("_", " "), max_len=80)
        return [
            "A recent update changed default behavior.",
            "Corrupted app cache or stale local configuration.",
            "Permission mismatch after system or app updates.",
            "Network profile conflict or DNS inconsistency.",
            f"Feature-specific conflict around {cluster_hint} settings on {device_family}.",
        ]

    def _default_verification(self, device_family: str) -> list[str]:
        return [
            f"Confirm the core function works twice in a row on {device_family}.",
            "Reboot once and verify the fix persists.",
            "Test with a second app or account to validate system stability.",
        ]

    def _default_when_to_stop(self, device_family: str) -> list[str]:
        return [
            f"Stop if you are asked to perform firmware or BIOS changes you do not understand on {device_family}.",
            "Stop if steps request deleting unknown system files.",
            "Stop if repeated resets show no change after all safe software fixes.",
        ]

    def _default_safe_warnings(self, device_family: str) -> list[str]:
        return [
            "Back up important files before reset or reinstall steps.",
            "Avoid unofficial tools that promise one-click miracle fixes.",
            f"Use official support channels for account or security lockouts on {device_family}.",
        ]

    def _default_anchor_ideas(self, device_family: str, cluster_id: str) -> list[str]:
        cluster_hint = self._clean_plan_text(cluster_id.replace("_", " "), max_len=60)
        return [
            f"{device_family} update issue checklist",
            f"{device_family} settings reset steps",
            f"fix {cluster_hint} safely",
            "how to verify troubleshooting results",
            "when to reinstall app vs reset settings",
            "network reset without data loss",
            "safe escalation to official support",
        ]

    def _device_menu_path(self, device_family: str, mode: str) -> str:
        family = self._normalize_device_family(device_family)
        if family == "windows":
            mapping = {
                "update": "Settings > Windows Update",
                "apps": "Settings > Apps > Installed apps",
                "network": "Settings > Network & Internet",
                "support": "Settings > System > Troubleshoot",
            }
            return mapping.get(mode, "")
        if family == "mac":
            mapping = {
                "update": "System Settings > General > Software Update",
                "apps": "System Settings > General > Login Items",
                "network": "System Settings > Wi-Fi",
                "support": "System Settings > Privacy & Security",
            }
            return mapping.get(mode, "")
        if family == "iphone":
            mapping = {
                "update": "Settings > General > Software Update",
                "apps": "Settings > Apps",
                "network": "Settings > Wi-Fi / Cellular",
                "support": "Settings > Privacy & Security",
            }
            return mapping.get(mode, "")
        mapping = {
            "update": "Settings > Software update",
            "apps": "Settings > Apps",
            "network": "Settings > Connections",
            "support": "Settings > Device care",
        }
        return mapping.get(mode, "")

    def _hazard_terms(self) -> tuple[str, ...]:
        return (
            "fire",
            "smoke",
            "explosion",
            "hazard",
            "injury",
            "blood",
            "electrical",
            "shock",
            "burning",
            "damaged hardware",
            "cracked screen",
        )

