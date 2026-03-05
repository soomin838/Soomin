import re
import time
import json
import hashlib
from typing import Any
from dataclasses import asdict
from datetime import datetime, timezone

from core.scout import TopicCandidate
from core.brain import DraftPost


class OllamaFallbackService:
    def __init__(self, ollama_client, settings, log_event_callback, get_recent_runs_callback, title_fp_callback):
        self.ollama_client = ollama_client
        self.settings = settings
        self.log_event = log_event_callback
        self.get_recent_runs = get_recent_runs_callback
        self.title_fp = title_fp_callback

    def _infer_device_type(self, text: str) -> str:
        low = str(text or "").lower()
        if any(w in low for w in ("iphone", "ipad", "ios", "apple watch")):
            return "ios"
        if any(w in low for w in ("mac", "macbook", "macos")):
            return "mac"
        if any(w in low for w in ("android", "galaxy", "pixel")):
            return "android"
        if any(w in low for w in ("ps5", "xbox", "switch", "playstation")):
            return "console"
        return "windows"

    def _infer_cluster_id_from_keyword(self, text: str) -> str:
        low = str(text or "").lower()
        if "update" in low:
            return "update"
        if "network" in low or "wifi" in low or "internet" in low:
            return "network"
        if "audio" in low or "sound" in low or "mic" in low:
            return "audio"
        if "display" in low or "screen" in low or "monitor" in low:
            return "display"
        if "bluetooth" in low or "pairing" in low:
            return "bluetooth"
        if "battery" in low or "power" in low or "charging" in low:
            return "power"
        if "performance" in low or "slow" in low or "lag" in low:
            return "performance"
        return "software"

    def _infer_feature_token(self, text: str) -> str:
        low = str(text or "").lower()
        tokens = ["wifi", "bluetooth", "audio", "display", "update", "battery", "camera", "microphone"]
        for t in tokens:
            if t in low:
                return t
        return "system"

    def _normalize_excerpt(self, html: str) -> str:
        text = re.sub(r"<[^>]+>", " ", str(html or ""))
        return re.sub(r"\s+", " ", text).strip()

    def _banned_image_words(self) -> tuple[str, ...]:
        return (
            "fire", "smoke", "explosion", "burning", "hazard", 
            "injury", "blood", "damaged", "broken outlet", "electric"
        )

    def _fallback_troubleshooting_plan(self, selected: TopicCandidate) -> dict[str, Any]:
        keyword = re.sub(r"\s+", " ", str(getattr(selected, "title", "") or "")).strip() or "windows update error fix"
        device = self._infer_device_type(f"{keyword}\n{selected.title}")
        cluster = self._infer_cluster_id_from_keyword(keyword)
        
        fix_steps = [
            {
                "step_title": "Check exact failure point first",
                "action": "Verify if the error occurs immediately on launch or after a specific action.",
                "menu_path": "Settings > System > Troubleshoot",
                "expected_result": "Expected result: you know exactly which module is failing.",
                "if_not_worked_next": "If not worked, restart the device cleanly.",
                "risk_level": "low",
            },
            {
                "step_title": "Restart and clear memory",
                "action": "Perform a full system restart, not just a sleep/wake cycle.",
                "menu_path": "Power > Restart",
                "expected_result": "Expected result: stale permission or network state is cleared.",
                "if_not_worked_next": "If not worked, reset network settings.",
                "risk_level": "low",
            },
            {
                "step_title": "Reinstall safely",
                "action": "Reinstall the affected app and keep only minimal settings during first launch.",
                "menu_path": "Settings > Apps > Reinstall",
                "expected_result": "Expected result: broken binaries or stale caches are removed.",
                "if_not_worked_next": "If not worked, test in safe mode / clean boot.",
                "risk_level": "medium",
            },
            {
                "step_title": "Run diagnostics and escalate",
                "action": "Collect exact error text and contact official support with reproduction steps.",
                "menu_path": "Settings > Support",
                "expected_result": "Expected result: support can identify root cause faster.",
                "if_not_worked_next": "If not worked, stop advanced changes and wait for vendor guidance.",
                "risk_level": "medium",
            },
        ]
        return {
            "primary_keyword": keyword,
            "device_family": device,
            "issue_summary": f"Practical troubleshooting plan for {device} users dealing with {cluster} issues.",
            "symptom_phrases": [
                f"{keyword} after update",
                f"{device} feature not responding",
                "app opens but function fails",
                "settings reset keeps returning",
            ],
            "likely_causes": [
                "Recent update changed defaults.",
                "Corrupted cache or local configuration.",
                "Permission mismatch after update.",
                "Network profile conflict.",
                f"{cluster} configuration conflict.",
            ],
            "fix_steps": fix_steps,
            "verification": [
                "Confirm the issue is resolved twice in a row.",
                "Reboot and verify the fix persists.",
                "Test with a second app or account.",
            ],
            "when_to_stop": [
                "Stop if instructions ask for unknown firmware-level changes.",
                "Stop if actions require deleting unknown system files.",
                "Stop when no change after all safe software steps.",
            ],
            "safe_warnings": [
                "Back up important files before reset actions.",
                "Avoid unofficial one-click repair tools.",
                "Use official support for account security lockouts.",
            ],
            "faq": [],
            "internal_links_anchor_ideas": [
                f"{device} update issue checklist",
                f"{device} safe reset steps",
                f"fix {cluster} issue safely",
                "expected result troubleshooting checklist",
                "when to reinstall app",
                "when to contact official support",
            ],
            "meta_description_seed": (
                f"{keyword}: step-by-step software fixes with expected results, fallback actions, "
                f"and safe escalation tips for {device} users."
            )[:160],
            "source": "fallback",
        }

    def _fallback_title_summary_payload(
        self,
        *,
        current_title: str,
        final_html: str,
        troubleshooting_plan: dict[str, Any],
        selected: TopicCandidate,
    ) -> dict[str, Any]:
        issue_phrase = re.sub(
            r"\s+",
            " ",
            str((troubleshooting_plan or {}).get("primary_keyword", "") or current_title or selected.title or "").strip(),
        )[:140]
        device_family = self._infer_device_type(f"{issue_phrase}\n{current_title}\n{selected.title}")
        feature = self._infer_feature_token(f"{issue_phrase}\n{current_title}\n{selected.title}\n{final_html[:800]}")
        summary = self._normalize_excerpt(final_html)[:380]
        if not summary:
            summary = f"This guide explains {issue_phrase} and gives step-by-step software fixes with expected results and fallback actions."
        must_terms = [
            issue_phrase,
            device_family,
            feature,
            "fix",
            "after update",
        ]
        dedup: list[str] = []
        seen: set[str] = set()
        for term in must_terms:
            txt = re.sub(r"\s+", " ", str(term or "")).strip()
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            dedup.append(txt[:60])
            if len(dedup) >= 6:
                break
        return {
            "short_summary": summary,
            "primary_issue_phrase": issue_phrase or current_title or selected.title,
            "device_family": device_family or "windows",
            "feature": feature or "network",
            "must_include_terms": dedup[:6],
            "source": "fallback",
        }

    def title_summary_payload(
        self,
        *,
        current_title: str,
        final_html: str,
        troubleshooting_plan: dict[str, Any],
        selected: TopicCandidate,
        is_ready: bool,
        reason: str
    ) -> dict[str, Any]:
        fallback = self._fallback_title_summary_payload(
            current_title=current_title,
            final_html=final_html,
            troubleshooting_plan=troubleshooting_plan,
            selected=selected,
        )
        if not is_ready:
            self.log_event(
                "ollama_title_summary_skipped_unavailable",
                {"purpose": "title_summary", "success": False, "fallback_used": True, "reason": reason},
            )
            return fallback

        started = time.perf_counter()
        prompt_len_est = len(str(current_title or "")) + len(str(final_html or "")) + len(json.dumps(troubleshooting_plan or {}, ensure_ascii=False))
        try:
            payload = self.ollama_client.summarize_for_title(
                title=current_title or selected.title,
                html=final_html,
                plan=troubleshooting_plan,
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            normalized = self._fallback_title_summary_payload(
                current_title=str(payload.get("primary_issue_phrase", "") or current_title),
                final_html=final_html,
                troubleshooting_plan={
                    **dict(troubleshooting_plan or {}),
                    "primary_keyword": str(payload.get("primary_issue_phrase", "") or (troubleshooting_plan or {}).get("primary_keyword", "")),
                    "device_family": str(payload.get("device_family", "") or (troubleshooting_plan or {}).get("device_family", "")),
                },
                selected=selected,
            )
            normalized.update(
                {
                    "short_summary": str(payload.get("short_summary", "") or normalized.get("short_summary", ""))[:400],
                    "primary_issue_phrase": str(payload.get("primary_issue_phrase", "") or normalized.get("primary_issue_phrase", ""))[:140],
                    "device_family": str(payload.get("device_family", "") or normalized.get("device_family", "windows")).lower(),
                    "feature": str(payload.get("feature", "") or normalized.get("feature", "network")).lower(),
                    "must_include_terms": list(payload.get("must_include_terms", []) or normalized.get("must_include_terms", []))[:6],
                    "source": "ollama",
                }
            )
            self.log_event(
                "ollama_title_summary_ok",
                {
                    "purpose": "title_summary",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": True,
                    "fallback_used": False,
                    "prompt_len": int(prompt_len_est),
                    "response_len": int(len(json.dumps(normalized, ensure_ascii=False))),
                },
            )
            return normalized
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            self.log_event(
                "ollama_title_summary_failed",
                {
                    "purpose": "title_summary",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": False,
                    "fallback_used": True,
                    "reason": reason,
                    "error": str(exc),
                    "prompt_len": int(prompt_len_est),
                    "response_len": 0,
                },
            )
            return fallback

    def _fallback_image_prompt_plan(self, draft: DraftPost, selected: TopicCandidate) -> dict[str, Any]:
        keyword = re.sub(r"\s+", " ", str(getattr(selected, "title", "") or "")).strip()
        device = self._infer_device_type(f"{draft.title}\n{selected.title}")
        banner = (
            f"minimal troubleshooting flow diagram for {device} software issue, "
            "3 to 5 boxes, pastel vector, no text, no letters, no numbers, no logos, no watermark"
        )
        inline = (
            f"minimal checklist diagram for {device} configuration settings, "
            "3 to 7 steps, pastel vector, no text, no letters, no numbers, no logos, no watermark"
        )
        return {
            "banner_prompt": banner,
            "inline_prompt": inline,
            "alt_suggestions": [f"{device} software fix steps", f"{device} error resolution guide", "troubleshooting illustration"],
            "style_tags": ["minimal", "pastel", "flat vector", "UI element"],
            "source": "fallback",
        }

    def _normalize_image_prompt_plan(self, plan: dict[str, Any], draft: DraftPost, selected: TopicCandidate) -> dict[str, Any]:
        safe = dict(plan or {})
        device = self._infer_device_type(f"{draft.title}\n{selected.title}")
        banner = re.sub(r"\s+", " ", str(safe.get("banner_prompt", "") or "")).strip()
        inline = re.sub(r"\s+", " ", str(safe.get("inline_prompt", "") or "")).strip()
        if not banner:
            banner = (
                f"minimal troubleshooting flow diagram for {device} software issue, "
                "3 to 5 boxes, pastel vector, no text, no letters, no numbers, no logos, no watermark"
            )
        if not inline:
            inline = (
                f"minimal checklist diagram for {device} microphone not working, "
                "3 to 7 steps, pastel vector, no text, no letters, no numbers, no logos, no watermark"
            )
        for blocked in self._banned_image_words():
            banner = re.sub(re.escape(blocked), "software", banner, flags=re.IGNORECASE)
            inline = re.sub(re.escape(blocked), "software", inline, flags=re.IGNORECASE)
        safe["banner_prompt"] = re.sub(r"\s+", " ", banner).strip()
        safe["inline_prompt"] = re.sub(r"\s+", " ", inline).strip()
        return safe

    def image_prompt_plan(self, draft: DraftPost, selected: TopicCandidate, sections: dict[str, str], is_ready: bool, reason: str, calls_in_post: int) -> tuple[dict[str, Any], int]:
        max_calls = max(0, int(getattr(self.settings.local_llm, "max_calls_per_post", 2) or 2))
        if calls_in_post >= max_calls:
            self.log_event(
                "ollama_prompt_plan_skipped_budget",
                {"purpose": "image_plan", "success": False, "fallback_used": True},
            )
            return self._fallback_image_prompt_plan(draft, selected), calls_in_post

        if not is_ready:
            self.log_event(
                "ollama_prompt_plan_skipped_unavailable",
                {"purpose": "image_plan", "success": False, "fallback_used": True, "reason": reason},
            )
            return self._fallback_image_prompt_plan(draft, selected), calls_in_post

        keyword = re.sub(r"\s+", " ", str(getattr(selected, "title", "") or draft.title or "").strip())
        device = self._infer_device_type(f"{draft.title}\n{selected.title}")
        cluster = self._infer_cluster_id_from_keyword(" ".join(getattr(selected, "long_tail_keywords", [])[:2]) or keyword)
        prompt_len_est = len(keyword) + len(device) + len(cluster) + sum(len(str(v or "")) for v in sections.values())
        started = time.perf_counter()
        try:
            plan = self.ollama_client.build_image_prompt_plan(
                keyword=keyword,
                device_type=device,
                cluster_id=cluster,
                section_texts=sections,
            )
            calls_in_post += 1
            latency_ms = int((time.perf_counter() - started) * 1000)
            plan_payload = {
                "banner_prompt": str(plan.banner_prompt or "").strip(),
                "inline_prompt": str(plan.inline_prompt or "").strip(),
                "alt_suggestions": list(plan.alt_suggestions or []),
                "style_tags": list(plan.style_tags or []),
                "source": "ollama",
                "ollama_reason": reason,
            }
            # (Hazard detection logic simplified here, relies on normalize to sanitize)
            self.log_event(
                "ollama_prompt_plan_ok",
                {
                    "purpose": "image_plan",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": True,
                    "fallback_used": False,
                    "prompt_len": int(prompt_len_est),
                },
            )
            return self._normalize_image_prompt_plan(plan_payload, draft, selected), calls_in_post
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            self.log_event(
                "ollama_prompt_plan_failed",
                {
                    "purpose": "image_plan",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": False,
                    "fallback_used": True,
                    "error": str(exc),
                    "reason": reason,
                    "prompt_len": int(prompt_len_est),
                },
            )
            return self._normalize_image_prompt_plan(self._fallback_image_prompt_plan(draft, selected), draft, selected), calls_in_post
    def _build_troubleshooting_context(self, selected: TopicCandidate) -> dict[str, Any]:
        return {
            "title": str(getattr(selected, "title", "") or ""),
            "excerpt": str(getattr(selected, "excerpt", "") or "")[:500],
            "keywords": list(getattr(selected, "long_tail_keywords", []) or []),
        }

    def troubleshooting_plan(
        self,
        selected: TopicCandidate,
        is_ready: bool,
        reason: str,
        calls_in_post: int
    ) -> tuple[dict[str, Any], int]:
        fallback_plan = self._fallback_troubleshooting_plan(selected)
        max_calls = max(0, int(getattr(self.settings.local_llm, "max_calls_per_post", 2) or 2))
        plan_enabled = bool(getattr(self.settings.local_llm, "plan_json_enabled", True))
        
        if not plan_enabled:
            self.log_event(
                "ollama_plan_json_skipped_disabled",
                {"purpose": "plan_json", "success": False, "fallback_used": True},
            )
            return fallback_plan, calls_in_post
            
        if calls_in_post >= max_calls:
            self.log_event(
                "ollama_plan_json_skipped_budget",
                {"purpose": "plan_json", "success": False, "fallback_used": True},
            )
            return fallback_plan, calls_in_post
            
        if not is_ready:
            self.log_event(
                "ollama_plan_json_skipped_unavailable",
                {"purpose": "plan_json", "success": False, "fallback_used": True, "reason": reason},
            )
            return fallback_plan, calls_in_post

        long_tails = [
            re.sub(r"\s+", " ", str(x or "")).strip()
            for x in (getattr(selected, "long_tail_keywords", []) or [])
            if str(x or "").strip()
        ]
        keyword = long_tails[0] if long_tails else re.sub(r"\s+", " ", str(getattr(selected, "title", "") or "")).strip()
        keyword = keyword or "windows update error fix"
        device_type = self._infer_device_type(f"{keyword}\n{selected.title}")
        cluster_id = self._infer_cluster_id_from_keyword(keyword)
        context = self._build_troubleshooting_context(selected)
        
        prompt_len_est = len(keyword) + len(device_type) + len(cluster_id) + sum(len(str(v or "")) for v in context.values())
        started = time.perf_counter()
        try:
            plan = self.ollama_client.build_troubleshooting_plan(
                keyword=keyword,
                device_type=device_type,
                cluster_id=cluster_id,
                context=context,
            )
            calls_in_post += 1
            latency_ms = int((time.perf_counter() - started) * 1000)
            payload = asdict(plan)
            payload["source"] = "ollama"
            self.log_event(
                "ollama_plan_json_ok",
                {
                    "purpose": "plan_json",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": True,
                    "fallback_used": False,
                    "prompt_len": int(prompt_len_est),
                    "response_len": int(len(json.dumps(payload, ensure_ascii=False))),
                },
            )
            return payload, calls_in_post
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            self.log_event(
                "ollama_plan_json_failed",
                {
                    "purpose": "plan_json",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": False,
                    "fallback_used": True,
                    "error": str(exc),
                    "reason": reason,
                    "prompt_len": int(prompt_len_est),
                    "response_len": 0,
                },
            )
            return fallback_plan, calls_in_post

    def qa_review(
        self,
        title: str,
        html: str,
        intro_text: str,
        alt_texts: list[str],
        is_ready: bool,
        reason: str,
        calls_in_post: int
    ) -> tuple[dict[str, Any], int]:
        fallback_result = {"issues": [], "remove_phrases": [], "rewrite_needed": False, "summary": ""}
        max_calls = max(0, int(getattr(self.settings.local_llm, "max_calls_per_post", 2) or 2))
        qa_enabled = bool(getattr(self.settings.local_llm, "qa_review_enabled", True))
        
        if not qa_enabled:
            self.log_event(
                "ollama_qa_review_skipped_disabled",
                {"purpose": "qa_review", "success": False, "fallback_used": True},
            )
            return fallback_result, calls_in_post
            
        if calls_in_post >= max_calls:
            self.log_event(
                "ollama_qa_review_skipped_budget",
                {"purpose": "qa_review", "success": False, "fallback_used": True},
            )
            return fallback_result, calls_in_post

        if not is_ready:
            self.log_event(
                "ollama_qa_review_skipped_unavailable",
                {"purpose": "qa_review", "success": False, "fallback_used": True, "reason": reason},
            )
            return fallback_result, calls_in_post

        started = time.perf_counter()
        prompt_len_est = len(title) + len(html) + len(intro_text) + sum(len(x) for x in alt_texts)
        try:
            result = self.ollama_client.review_article_quality(
                title=title,
                html=html,
                intro_text=intro_text,
                alt_texts=alt_texts
            )
            calls_in_post += 1
            latency_ms = int((time.perf_counter() - started) * 1000)
            self.log_event(
                "ollama_qa_review_ok",
                {
                    "purpose": "qa_review",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": True,
                    "fallback_used": False,
                    "issue_count": len(list((result or {}).get("issues", []) or [])),
                    "prompt_len": int(prompt_len_est),
                    "response_len": int(len(json.dumps(result or {}, ensure_ascii=False))),
                },
            )
            return (result if isinstance(result, dict) else fallback_result), calls_in_post
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            self.log_event(
                "ollama_qa_review_failed",
                {
                    "purpose": "qa_review",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": False,
                    "fallback_used": True,
                    "error": str(exc),
                    "reason": reason,
                    "prompt_len": int(prompt_len_est),
                    "response_len": 0,
                },
            )
            return fallback_result, calls_in_post
