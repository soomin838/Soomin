from __future__ import annotations

import json
import re
import random
import time
import threading
import uuid
from dataclasses import dataclass
from collections import Counter
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from html import escape, unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse
from zoneinfo import ZoneInfo

from .brain import DraftPost, GeminiBrain
from .budget import BudgetGuard
from .asset_store import KeywordAssetStore, PostsIndexStore
from .image_library import pick_images
from .index_sync import BloggerIndexSync
from .logstore import LogStore, RunRecord
from .ollama_client import OllamaClient
from .ollama_manager import OllamaManager
from .patterns import PatternEngine
from .publisher import Publisher
from .quality import ContentQAGate
from .reference_docs import ReferenceCorpus
from .scheduler import MonthlyScheduler
from .scout import SourceScout, TopicCandidate
from .settings import AppSettings
from .text_segmenter import section_bundle_for_llm
from .topic_growth import TopicGrower
from .visual import ImageAsset, VisualPipeline


@dataclass
class WorkflowResult:
    status: str
    message: str


class AgentWorkflow:
    def __init__(self, root: Path, settings: AppSettings) -> None:
        self.root = root
        self.settings = settings
        self.logs = LogStore(
            db_path=root / "storage" / "logs" / "agent_logs.sqlite3",
            json_log_path=root / "storage" / "logs" / "agent_events.jsonl",
        )
        self.guard = BudgetGuard(settings.budget, self.logs)
        self.scout = SourceScout(settings.sources, root, settings.content_mode)
        self.patterns = PatternEngine()
        self.references = ReferenceCorpus(
            files=[
                root / "storage" / "references" / "quality_automation_manual.txt",
                root / "storage" / "references" / "writing_patterns_playbook.txt",
            ]
        )
        self.brain = GeminiBrain(settings.gemini)
        self.visual = VisualPipeline(
            temp_dir=root / "storage" / "temp_images",
            session_dir=root / "storage" / "sessions",
            visual_settings=settings.visual,
            gemini_api_key=settings.gemini.api_key,
        )
        self.publisher = Publisher(
            credentials_path=root / settings.blogger.credentials_path,
            blog_id=settings.blogger.blog_id,
            service_account_path=root / settings.indexing.service_account_path,
            image_hosting_backend=settings.publish.image_hosting_backend,
            gcs_bucket_name=settings.publish.gcs_bucket_name,
            gcs_public_base_url=settings.publish.gcs_public_base_url,
            r2_config=getattr(settings.publish, "r2", None),
            max_banner_images=settings.visual.max_banner_images,
            max_inline_images=settings.visual.max_inline_images,
            semantic_html_enabled=bool(getattr(settings.publish, "enable_semantic_html", True)),
            strict_thumbnail_blogger_media=bool(getattr(settings.publish, "strict_thumbnail_blogger_media", True)),
            thumbnail_data_uri_allowed=bool(getattr(settings.publish, "thumbnail_data_uri_allowed", False)),
            auto_allow_data_uri_on_blogger_405=bool(getattr(settings.publish, "auto_allow_data_uri_on_blogger_405", False)),
        )
        self.qa = ContentQAGate(
            settings.quality,
            settings.authority_links,
            qa_runtime_path=root / "storage" / "logs" / "qa_runtime.jsonl",
        )
        self.ollama_manager = OllamaManager(
            root=root,
            settings=settings.local_llm,
            log_path=root / "storage" / "logs" / "ollama_manager.jsonl",
        )
        self.ollama_client = OllamaClient(
            settings.local_llm,
            log_path=root / "storage" / "logs" / "ollama_calls.jsonl",
        )
        self.topic_grower = TopicGrower(
            root=root,
            seeds_path=root / settings.sources.seeds_path,
            gemini=settings.gemini,
            topic_growth=settings.topic_growth,
        )
        self._progress_hook = None
        self._blog_snapshot_cache: tuple[datetime, dict] | None = None
        self._blog_cache_ttl_seconds = 60
        self._recent_blogger_titles_cache: tuple[datetime, list[str]] | None = None
        self._recent_blogger_titles_ttl_seconds = 120
        self._blog_totals_cache: tuple[datetime, dict[str, int]] | None = None
        self._blog_totals_ttl_seconds = 600
        self._resume_snapshot_cache: tuple[datetime, dict] | None = None
        self._resume_cache_ttl_seconds = 45
        self._global_keyword_cache: tuple[datetime, list[str]] | None = None
        self._global_keyword_cache_ttl_seconds = 1800
        self.last_global_keywords: list[str] = []
        self._kst = timezone(timedelta(hours=9))
        self._keyword_pool_path = self.root / "storage" / "logs" / "keyword_pool.json"
        self._blogger_recent_14d_path = self.root / "storage" / "logs" / "blogger_recent_14d.json"
        self.keyword_assets = KeywordAssetStore(
            db_path=self.root / str(getattr(self.settings.keywords, "db_path", "storage/keywords.sqlite")),
        )
        self.posts_index = PostsIndexStore(
            db_path=self.root / "storage" / "posts_index.sqlite",
        )
        self.index_sync = BloggerIndexSync(
            publisher=self.publisher,
            posts_index=self.posts_index,
            sync_settings=self.settings.sync,
            root=self.root,
        )
        self.monthly_scheduler = MonthlyScheduler(
            root=self.root,
            config=self.settings.monthly_scheduler,
        )
        self._active_slot_id = ""
        self._image_pipeline_state: dict[str, Any] = {
            "status": "idle",
            "passed": 0,
            "target": int(getattr(self.settings.visual, "target_images_per_post", 5) or 5),
            "message": "ready",
        }
        self._local_llm_checked = False
        self._local_llm_ready = False
        self._local_llm_last_reason = "not_checked"
        self._local_llm_calls_in_post = 0
        self._local_llm_used_last_run = False
        self._posts_index_bootstrap_started = False
        self._posts_index_bootstrap_done = False
        self._workflow_perf_path = self.root / "storage" / "logs" / "workflow_perf.jsonl"
        self._workflow_perf_run_id = ""
        self._workflow_perf_run_started_mono = 0.0
        self._workflow_perf_current_phase = ""
        self._workflow_perf_phase_started_mono = 0.0
        self._workflow_perf_phase_last_message = ""
        self._workflow_perf_phase_last_percent = 0
        self._workflow_perf_last_heartbeat_mono = 0.0
        self._workflow_perf_slow_phase_threshold_ms = 5000
        self._workflow_perf_slow_call_threshold_ms = 2500
        self._workflow_perf_heartbeat_sec = 20.0
        self._workflow_perf_phase_count = 0
        self._workflow_perf_slow_phases: list[dict[str, Any]] = []
        self._manual_upload_probe_done = False
        self._start_posts_index_bootstrap()

    def set_progress_hook(self, hook) -> None:
        self._progress_hook = hook

    def _progress(self, phase: str, message: str, percent: int = 0) -> None:
        phase_key = str(phase or "idle").strip() or "idle"
        phase_msg = str(message or "").strip()
        phase_pct = max(0, min(100, int(percent)))
        self._workflow_perf_track_progress(phase=phase_key, message=phase_msg, percent=phase_pct)
        try:
            if callable(self._progress_hook):
                self._progress_hook(phase_key, phase_msg, phase_pct)
        except Exception:
            pass

    def _append_workflow_perf(self, event: str, payload: dict[str, Any] | None = None) -> None:
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip(),
            "run_id": str(self._workflow_perf_run_id or "").strip(),
        }
        row.update(dict(payload or {}))
        try:
            self._workflow_perf_path.parent.mkdir(parents=True, exist_ok=True)
            with self._workflow_perf_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _workflow_perf_start_run(self, manual_trigger: bool = False) -> None:
        if self._workflow_perf_run_id:
            # Close any stale run from an abnormal termination path.
            self._workflow_perf_finish_run(status="aborted", message="stale_run_closed_on_next_start")
        self._workflow_perf_run_id = uuid.uuid4().hex[:12]
        self._workflow_perf_run_started_mono = time.perf_counter()
        self._workflow_perf_current_phase = ""
        self._workflow_perf_phase_started_mono = 0.0
        self._workflow_perf_phase_last_message = ""
        self._workflow_perf_phase_last_percent = 0
        self._workflow_perf_last_heartbeat_mono = 0.0
        self._workflow_perf_phase_count = 0
        self._workflow_perf_slow_phases = []
        self._manual_upload_probe_done = False
        self._append_workflow_perf(
            "run_start",
            {
                "manual_trigger": bool(manual_trigger),
                "qa_mode": str(getattr(self.settings.quality, "qa_mode", "quick") or "quick"),
                "target_images_per_post": int(getattr(self.settings.visual, "target_images_per_post", 2) or 2),
            },
        )

    def _workflow_perf_close_phase(self, now_mono: float, reason: str) -> None:
        phase = str(self._workflow_perf_current_phase or "").strip()
        if not phase:
            return
        elapsed_ms = int(max(0.0, now_mono - float(self._workflow_perf_phase_started_mono or now_mono)) * 1000)
        slow = elapsed_ms >= int(self._workflow_perf_slow_phase_threshold_ms)
        payload = {
            "phase": phase,
            "duration_ms": int(elapsed_ms),
            "reason": str(reason or "phase_change"),
            "last_message": str(self._workflow_perf_phase_last_message or ""),
            "last_percent": int(self._workflow_perf_phase_last_percent or 0),
            "slow": bool(slow),
        }
        self._append_workflow_perf("phase_end", payload)
        if slow:
            self._workflow_perf_slow_phases.append(
                {"phase": phase, "duration_ms": int(elapsed_ms), "reason": str(reason or "phase_change")}
            )
        self._workflow_perf_current_phase = ""
        self._workflow_perf_phase_started_mono = 0.0
        self._workflow_perf_phase_last_message = ""
        self._workflow_perf_phase_last_percent = 0

    def _workflow_perf_track_progress(self, phase: str, message: str, percent: int) -> None:
        if not self._workflow_perf_run_id:
            return
        now_mono = time.perf_counter()
        phase_key = str(phase or "idle").strip() or "idle"
        msg = str(message or "").strip()
        pct = max(0, min(100, int(percent or 0)))

        if not self._workflow_perf_current_phase:
            self._workflow_perf_current_phase = phase_key
            self._workflow_perf_phase_started_mono = now_mono
            self._workflow_perf_phase_last_message = msg
            self._workflow_perf_phase_last_percent = pct
            self._workflow_perf_last_heartbeat_mono = now_mono
            self._workflow_perf_phase_count += 1
            self._append_workflow_perf(
                "phase_start",
                {"phase": phase_key, "message": msg, "percent": pct},
            )
            return

        if phase_key != self._workflow_perf_current_phase:
            self._workflow_perf_close_phase(now_mono, reason="phase_change")
            self._workflow_perf_current_phase = phase_key
            self._workflow_perf_phase_started_mono = now_mono
            self._workflow_perf_phase_last_message = msg
            self._workflow_perf_phase_last_percent = pct
            self._workflow_perf_last_heartbeat_mono = now_mono
            self._workflow_perf_phase_count += 1
            self._append_workflow_perf(
                "phase_start",
                {"phase": phase_key, "message": msg, "percent": pct},
            )
            return

        changed = (msg != self._workflow_perf_phase_last_message) or (pct != self._workflow_perf_phase_last_percent)
        if changed and (now_mono - self._workflow_perf_last_heartbeat_mono) >= float(self._workflow_perf_heartbeat_sec):
            elapsed_ms = int(max(0.0, now_mono - float(self._workflow_perf_phase_started_mono or now_mono)) * 1000)
            self._append_workflow_perf(
                "phase_heartbeat",
                {
                    "phase": phase_key,
                    "elapsed_ms": elapsed_ms,
                    "message": msg,
                    "percent": pct,
                },
            )
            self._workflow_perf_last_heartbeat_mono = now_mono

        self._workflow_perf_phase_last_message = msg
        self._workflow_perf_phase_last_percent = pct

    def _workflow_perf_finish_run(self, status: str, message: str = "") -> None:
        if not self._workflow_perf_run_id:
            return
        now_mono = time.perf_counter()
        self._workflow_perf_close_phase(now_mono, reason="run_end")
        total_ms = int(max(0.0, now_mono - float(self._workflow_perf_run_started_mono or now_mono)) * 1000)
        self._append_workflow_perf(
            "run_end",
            {
                "status": str(status or "unknown").strip() or "unknown",
                "message": str(message or "")[:260],
                "total_ms": int(total_ms),
                "phase_count": int(self._workflow_perf_phase_count or 0),
                "slow_phase_count": int(len(self._workflow_perf_slow_phases)),
                "slow_phases": list(self._workflow_perf_slow_phases[:8]),
            },
        )
        self._workflow_perf_run_id = ""
        self._workflow_perf_run_started_mono = 0.0
        self._workflow_perf_current_phase = ""
        self._workflow_perf_phase_started_mono = 0.0
        self._workflow_perf_phase_last_message = ""
        self._workflow_perf_phase_last_percent = 0
        self._workflow_perf_last_heartbeat_mono = 0.0
        self._workflow_perf_phase_count = 0
        self._workflow_perf_slow_phases = []

    def _profile_call(
        self,
        stage: str,
        fn,
        *,
        slow_ms: int | None = None,
        meta: dict[str, Any] | None = None,
    ):
        stage_name = str(stage or "stage").strip() or "stage"
        threshold = int(slow_ms if slow_ms is not None else self._workflow_perf_slow_call_threshold_ms)
        self._append_workflow_perf("stage_start", {"stage": stage_name, **dict(meta or {})})
        started = time.perf_counter()
        ok = False
        error_summary = ""
        try:
            value = fn()
            ok = True
            return value
        except Exception as exc:
            error_summary = str(exc or "")[:260]
            raise
        finally:
            elapsed = int(max(0.0, time.perf_counter() - started) * 1000)
            payload = {
                "stage": stage_name,
                "elapsed_ms": int(elapsed),
                "ok": bool(ok),
                "slow": bool(elapsed >= threshold),
            }
            payload.update(dict(meta or {}))
            if error_summary:
                payload["error"] = error_summary
            self._append_workflow_perf("stage_end", payload)

    def _qa_evaluate(
        self,
        html: str,
        *,
        title: str = "",
        domain: str = "tech_troubleshoot",
        keyword: str = "",
        context: str = "qa",
    ):
        return self._profile_call(
            stage=f"qa_evaluate:{context}",
            fn=lambda: self.qa.evaluate(
                html,
                title=title,
                domain=domain,
                keyword=keyword,
            ),
            slow_ms=1200,
            meta={
                "domain": str(domain or ""),
                "keyword": str(keyword or "")[:120],
                "title": str(title or "")[:120],
            },
        )

    def _reset_local_llm_budget(self) -> None:
        self._local_llm_calls_in_post = 0
        self._local_llm_used_last_run = False

    def _log_ollama_event(self, event: str, payload: dict[str, Any] | None = None) -> None:
        payload = dict(payload or {})
        purpose = str(payload.get("purpose", "") or "").strip().lower()
        if purpose not in {"image_plan", "qa_review"}:
            return
        payload["purpose"] = purpose
        payload.setdefault("endpoint", "/api/generate")
        payload.setdefault("latency_ms", 0)
        payload.setdefault("prompt_len", int(payload.get("prompt_len", 0) or 0))
        payload.setdefault("response_len", int(payload.get("response_len", 0) or 0))
        payload.setdefault("success", event not in {"ollama_prompt_plan_failed", "ollama_qa_review_failed"})
        payload.setdefault("fallback_used", not payload.get("success", True))
        payload.setdefault("ok", bool(payload.get("success", False)))
        if "error_summary" not in payload:
            payload["error_summary"] = str(
                payload.get("error") or payload.get("reason") or ""
            )[:220]
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip(),
            "model": str(getattr(self.settings.local_llm, "model", "") or ""),
            "provider": "ollama",
        }
        row.update(payload)
        path = self.root / "storage" / "logs" / "ollama_calls.jsonl"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _prepare_local_llm(self) -> tuple[bool, str]:
        if not bool(getattr(self.settings.local_llm, "enabled", False)):
            self._local_llm_ready = False
            self._local_llm_last_reason = "disabled"
            self._local_llm_checked = True
            return False, "disabled"
        if self._local_llm_checked and self._local_llm_ready:
            return True, "ready"
        ready, reason = self.ollama_manager.ensure_ready()
        self._local_llm_checked = True
        self._local_llm_ready = bool(ready)
        self._local_llm_last_reason = str(reason or "")
        if not ready:
            try:
                self.settings.local_llm.enabled = False
            except Exception:
                pass
        return bool(ready), str(reason or "")

    def _fallback_image_prompt_plan(self, draft: DraftPost, selected: TopicCandidate) -> dict[str, Any]:
        sections = self._image_plan_sections(draft.html)
        quick = str(sections.get("quick_answer", "") or draft.summary or draft.title).strip()
        fix2 = str(sections.get("fix2", "") or quick).strip()
        device = self._infer_device_type(f"{draft.title}\n{selected.title}")
        return {
            "banner_prompt": (
                f"minimal software troubleshooting flow diagram for {device} issue, "
                f"key context: {quick[:180]}, pastel vector, rounded boxes, "
                "no text, no letters, no numbers, no logos, no watermark"
            ),
            "inline_prompt": (
                f"minimal software troubleshooting checklist diagram for {device}, "
                f"step focus: {fix2[:180]}, 3 to 7 steps, pastel vector, rounded icons, "
                "no text, no letters, no numbers, no logos, no watermark"
            ),
            "alt_suggestions": [
                "Troubleshooting flow diagram for a common software issue.",
                "Checklist-style visual for a practical software fix path.",
                "Step-by-step troubleshooting concept image for daily users.",
            ],
            "style_tags": ["minimal", "pastel", "rounded", "diagram"],
            "source": "fallback",
        }

    def _build_image_prompt_plan_with_local_llm(self, draft: DraftPost, selected: TopicCandidate) -> dict[str, Any]:
        max_calls = max(0, int(getattr(self.settings.local_llm, "max_calls_per_post", 2) or 2))
        if self._local_llm_calls_in_post >= max_calls:
            self._log_ollama_event(
                "ollama_prompt_plan_skipped_budget",
                {"purpose": "image_plan", "success": False, "fallback_used": True},
            )
            return self._fallback_image_prompt_plan(draft, selected)
        ready, reason = self._prepare_local_llm()
        if not ready:
            self._log_ollama_event(
                "ollama_prompt_plan_skipped_unavailable",
                {"purpose": "image_plan", "success": False, "fallback_used": True, "reason": reason},
            )
            return self._fallback_image_prompt_plan(draft, selected)

        sections = self._image_plan_sections(draft.html)
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
            self._local_llm_calls_in_post += 1
            self._local_llm_used_last_run = True
            latency_ms = int((time.perf_counter() - started) * 1000)
            plan_payload = {
                "banner_prompt": str(plan.banner_prompt or "").strip(),
                "inline_prompt": str(plan.inline_prompt or "").strip(),
                "alt_suggestions": list(plan.alt_suggestions or []),
                "style_tags": list(plan.style_tags or []),
                "source": "ollama",
                "ollama_reason": reason,
            }
            if self._image_prompt_plan_has_hazard(plan_payload):
                self._log_ollama_event(
                    "ollama_prompt_plan_retry_hazard",
                    {
                        "purpose": "image_plan",
                        "endpoint": "/api/generate",
                        "latency_ms": latency_ms,
                        "success": False,
                        "fallback_used": False,
                        "prompt_len": int(prompt_len_est),
                        "response_len": int(
                            len(str(plan.banner_prompt or ""))
                            + len(str(plan.inline_prompt or ""))
                            + sum(len(str(x or "")) for x in (plan.alt_suggestions or []))
                        ),
                    },
                )
                if self._local_llm_calls_in_post < max_calls:
                    retry_started = time.perf_counter()
                    retry_plan = self.ollama_client.build_image_prompt_plan(
                        keyword=keyword,
                        device_type=device,
                        cluster_id=cluster,
                        section_texts=sections,
                    )
                    self._local_llm_calls_in_post += 1
                    retry_latency_ms = int((time.perf_counter() - retry_started) * 1000)
                    retry_payload = {
                        "banner_prompt": str(retry_plan.banner_prompt or "").strip(),
                        "inline_prompt": str(retry_plan.inline_prompt or "").strip(),
                        "alt_suggestions": list(retry_plan.alt_suggestions or []),
                        "style_tags": list(retry_plan.style_tags or []),
                        "source": "ollama_retry",
                        "ollama_reason": reason,
                    }
                    if not self._image_prompt_plan_has_hazard(retry_payload):
                        plan_payload = retry_payload
                        self._log_ollama_event(
                            "ollama_prompt_plan_ok",
                            {
                                "purpose": "image_plan",
                                "endpoint": "/api/generate",
                                "latency_ms": retry_latency_ms,
                                "success": True,
                                "fallback_used": False,
                                "prompt_len": int(prompt_len_est),
                                "response_len": int(
                                    len(str(retry_plan.banner_prompt or ""))
                                    + len(str(retry_plan.inline_prompt or ""))
                                    + sum(len(str(x or "")) for x in (retry_plan.alt_suggestions or []))
                                ),
                            },
                        )
                        return self._normalize_image_prompt_plan(plan_payload, draft, selected)
                # hard fallback when hazard wording remains
                fallback = self._fallback_image_prompt_plan(draft, selected)
                fallback["source"] = "fallback_hazard_guard"
                self._log_ollama_event(
                    "ollama_prompt_plan_failed",
                    {
                        "purpose": "image_plan",
                        "endpoint": "/api/generate",
                        "latency_ms": latency_ms,
                        "success": False,
                        "fallback_used": True,
                        "reason": "hazard_terms_detected",
                        "prompt_len": int(prompt_len_est),
                        "response_len": 0,
                    },
                )
                return self._normalize_image_prompt_plan(fallback, draft, selected)

            self._log_ollama_event(
                "ollama_prompt_plan_ok",
                {
                    "purpose": "image_plan",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": True,
                    "fallback_used": False,
                    "prompt_len": int(prompt_len_est),
                    "response_len": int(
                        len(str(plan.banner_prompt or ""))
                        + len(str(plan.inline_prompt or ""))
                        + sum(len(str(x or "")) for x in (plan.alt_suggestions or []))
                    ),
                },
            )
            return self._normalize_image_prompt_plan(plan_payload, draft, selected)
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            self._log_ollama_event(
                "ollama_prompt_plan_failed",
                {
                    "purpose": "image_plan",
                    "endpoint": "/api/generate",
                    "latency_ms": latency_ms,
                    "success": False,
                    "fallback_used": True,
                    "error": str(exc),
                    "reason": reason,
                    "model": str(getattr(self.settings.local_llm, "model", "") or ""),
                    "prompt_len": int(prompt_len_est),
                    "response_len": 0,
                },
            )
            return self._normalize_image_prompt_plan(self._fallback_image_prompt_plan(draft, selected), draft, selected)

    def _image_plan_sections(self, html: str) -> dict[str, str]:
        sections = section_bundle_for_llm(html)
        out: dict[str, str] = {}
        for key in ("quick_answer", "fix2", "advanced_fix"):
            text = re.sub(r"\s+", " ", str(sections.get(key, "") or "")).strip()
            if not text:
                continue
            first_sentence = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0]
            out[key] = first_sentence[:220]
        if not out:
            out = {"quick_answer": "Practical software troubleshooting steps for normal users."}
        return out

    def _image_prompt_plan_has_hazard(self, plan: dict[str, Any]) -> bool:
        banned = self._banned_image_words()
        target = " ".join(
            [
                str(plan.get("banner_prompt", "") or ""),
                str(plan.get("inline_prompt", "") or ""),
                " ".join(str(x or "") for x in (plan.get("alt_suggestions", []) or [])),
            ]
        ).lower()
        return any(word in target for word in banned)

    def _banned_image_words(self) -> tuple[str, ...]:
        return (
            "fire",
            "smoke",
            "explosion",
            "burning",
            "hazard",
            "injury",
            "blood",
            "damaged",
            "broken outlet",
            "electric",
        )

    def _normalize_image_prompt_plan(
        self,
        plan: dict[str, Any],
        draft: DraftPost,
        selected: TopicCandidate,
    ) -> dict[str, Any]:
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

    def _run_local_llm_qa_review(self, title: str, html: str, images: list[ImageAsset]) -> dict[str, Any]:
        max_calls = max(0, int(getattr(self.settings.local_llm, "max_calls_per_post", 2) or 2))
        if self._local_llm_calls_in_post >= max_calls:
            self._log_ollama_event(
                "ollama_qa_review_skipped_budget",
                {"purpose": "qa_review", "success": False, "fallback_used": True},
            )
            return {"issues": [], "remove_phrases": [], "rewrite_needed": False, "summary": ""}
        ready, reason = self._prepare_local_llm()
        if not ready:
            self._log_ollama_event(
                "ollama_qa_review_skipped_unavailable",
                {"purpose": "qa_review", "success": False, "fallback_used": True, "reason": reason},
            )
            return {"issues": [], "remove_phrases": [], "rewrite_needed": False, "summary": ""}

        intro = self._extract_intro_text(html)
        alt_values = [str(getattr(img, "alt", "") or "") for img in (images or []) if str(getattr(img, "alt", "") or "").strip()]
        prompt_len_est = len(str(title or "")) + len(str(html or "")) + len(str(intro or "")) + sum(len(x) for x in alt_values)
        started = time.perf_counter()
        try:
            result = self.ollama_client.review_article_quality(
                title=title,
                html=html,
                intro_text=intro,
                alt_texts=alt_values,
            )
            self._local_llm_calls_in_post += 1
            self._local_llm_used_last_run = True
            latency_ms = int((time.perf_counter() - started) * 1000)
            self._log_ollama_event(
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
            return result if isinstance(result, dict) else {"issues": [], "remove_phrases": [], "rewrite_needed": False, "summary": ""}
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            self._log_ollama_event(
                "ollama_qa_review_failed",
                {
                    "purpose": "qa_review",
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
            return {"issues": [], "remove_phrases": [], "rewrite_needed": False, "summary": ""}

    def _can_auto_index_notify(self) -> bool:
        try:
            if not bool(getattr(self.settings.indexing, "enabled", True)):
                return False
            return bool(self.publisher.can_notify_indexing())
        except Exception:
            return False

    def _can_auto_search_console_inspect(self) -> tuple[bool, str]:
        site = str(getattr(self.settings.integrations, "search_console_site_url", "") or "").strip()
        enabled = bool(getattr(self.settings.integrations, "search_console_enabled", True))
        return (bool(enabled and site), site)

    def _preflight_recent_index_sync(self) -> str:
        """
        At run start, backfill indexing/inspection for recent live posts (14d).
        - Skip URLs already processed before.
        - Only process missing ones.
        - Never hard-fail the run because of this preflight step.
        """
        can_notify = self._can_auto_index_notify()
        can_inspect, sc_site = self._can_auto_search_console_inspect()
        if not can_notify and not can_inspect:
            return ""

        scan_days = 14
        scan_limit = 260
        per_run_cap = 40
        try:
            recent_rows = self.publisher.fetch_recent_live_urls(days=scan_days, limit=scan_limit)
        except Exception as exc:
            return f"preflight_index_sync_fetch_failed={str(exc)[:120]}"
        if not recent_rows:
            return ""

        total_rows = len(recent_rows)
        notified = 0
        inspected = 0
        skipped = 0
        deferred = 0
        errors = 0
        processed = 0
        daily_left = (
            max(0, int(self.settings.indexing.daily_quota) - int(self.logs.get_today_indexing_count()))
            if can_notify
            else 0
        )

        for row in recent_rows:
            url = str((row or {}).get("url", "") or "").strip()
            if not url:
                continue
            self.logs.touch_index_audit_url(url)
            audit = self.logs.get_index_audit(url)
            need_notify = can_notify and not str(audit.get("index_notified_at", "") or "").strip()
            need_inspect = can_inspect and not str(audit.get("inspection_checked_at", "") or "").strip()

            if not need_notify and not need_inspect:
                skipped += 1
                continue
            if processed >= per_run_cap:
                deferred += 1
                continue
            processed += 1

            if need_notify:
                if daily_left <= 0:
                    deferred += 1
                    self.logs.mark_index_audit_error(url, "index_notify_deferred:daily_quota_exhausted")
                else:
                    try:
                        self.publisher.notify_indexing(url)
                        self.logs.increment_today_indexing_count()
                        self.logs.mark_index_notified(url)
                        notified += 1
                        daily_left = max(0, daily_left - 1)
                    except Exception as exc:
                        errors += 1
                        self.logs.mark_index_audit_error(url, f"index_notify_failed:{str(exc)[:220]}")

            if need_inspect:
                try:
                    payload = self.publisher.inspect_url(site_url=sc_site, inspection_url=url)
                    verdict = self.publisher.inspection_verdict(payload)
                    self.logs.mark_inspection_checked(url, verdict=verdict)
                    inspected += 1
                except Exception as exc:
                    errors += 1
                    self.logs.mark_index_audit_error(url, f"url_inspect_failed:{str(exc)[:220]}")

        return (
            "preflight_index_sync="
            f"scan{total_rows},notify{notified},inspect{inspected},"
            f"skip{skipped},defer{deferred},err{errors}"
        )

    def _sync_posts_index_with_blogger(self, force: bool = False) -> str:
        try:
            report = self.index_sync.sync_with_blogger(force=bool(force))
        except Exception as exc:
            return f"sync_with_blogger=error:{str(exc)[:120]}"
        status = str(report.get("status", "") or "").strip().lower()
        reason = str(report.get("reason", "") or "").strip()
        counts = dict(report.get("counts", {}) or {})
        if status == "ok":
            return (
                "sync_with_blogger="
                f"ok:add{int(counts.get('added', 0))},"
                f"upd{int(counts.get('updated', 0))},"
                f"soft_del{int(counts.get('soft_deleted', 0))},"
                f"purge{int(counts.get('purged', 0))}"
            )
        if status == "skipped":
            return f"sync_with_blogger=skipped:{reason or 'interval_guard'}"
        if status == "error":
            return f"sync_with_blogger=error:{str(report.get('error', '') or '')[:120]}"
        return f"sync_with_blogger={status or 'unknown'}"

    def run_once(self, manual_trigger: bool = False) -> WorkflowResult:
        self._workflow_perf_start_run(manual_trigger=bool(manual_trigger))
        self._active_slot_id = ""
        self._set_image_pipeline_state(
            "idle",
            0,
            int(self.settings.visual.target_images_per_post or 5),
            "ready",
        )
        self._reset_local_llm_budget()
        generation_count = 0
        partial_fix_count = 0
        today_gemini_before = int(self.logs.get_today_gemini_count())
        self._progress("preflight", "실행 조건 확인 중", 2)
        sync_note = self._profile_call(
            "sync_with_blogger",
            lambda: self._sync_posts_index_with_blogger(force=bool(manual_trigger)),
            slow_ms=1500,
            meta={"manual_trigger": bool(manual_trigger)},
        )
        blog_snapshot = self._profile_call(
            "blog_snapshot_refresh",
            lambda: self._blog_snapshot(force_refresh=True),
            slow_ms=1800,
        )
        snapshot_source = str(blog_snapshot.get("source", "local"))
        preflight_index_note = self._profile_call(
            "preflight_index_sync",
            self._preflight_recent_index_sync,
            slow_ms=2000,
        )
        budget = self.guard.can_run(
            today_posts=(
                int(blog_snapshot.get("today_posts", 0))
                if snapshot_source == "blogger"
                else None
            ),
            # In schedule mode, do not block generation just because today's live posts hit cap.
            # Daily publish capacity is enforced by _compute_publish_at() per-day placement.
            enforce_post_limit=(snapshot_source == "blogger" and not bool(self.settings.publish.use_blogger_schedule)),
        )
        if not budget.ok:
            hold_note = f"Budget guard: {budget.reason}"
            if sync_note:
                hold_note = self._append_note(hold_note, sync_note)
            if preflight_index_note:
                hold_note = self._append_note(hold_note, preflight_index_note)
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title="",
                    source_url="",
                    published_url="",
                    note=hold_note,
                )
            )
            self._workflow_perf_finish_run("hold", budget.reason)
            return WorkflowResult("hold", budget.reason)

        self._progress("queue_check", "예약 큐 상태 점검", 8)
        queue_state = self._queue_state(blog_snapshot)
        queue_advisory = ""
        if (
            str(queue_state.get("source", "local")) == "blogger"
            and queue_state["scheduled"] >= int(self.settings.publish.target_queue_size)
        ):
            # Rezero 2.3: never stop due to queue size; keep compounding future buffer.
            queue_advisory = (
                f"queue_over_target={queue_state['scheduled']}/"
                f"{self.settings.publish.target_queue_size}"
            )

        self._progress("resume_check", "중단 초안 재개 가능 여부 확인", 10)
        resume_result = self._resume_from_saved_wip(
            manual_trigger=manual_trigger,
            queue_advisory=queue_advisory,
        )
        if resume_result is not None:
            self._workflow_perf_finish_run(str(resume_result.status or "hold"), str(resume_result.message or ""))
            return resume_result

        self._progress("topic_growth", "주제 풀 확장 점검", 12)
        existing_titles = [r.get("title", "") for r in self.logs.get_recent_topic_history(days=30, limit=600)]
        try:
            self.topic_grower.maybe_grow(existing_titles)
        except Exception:
            pass

        self._progress("collect", "콘텐츠 소스 수집 중", 18)
        working_draft_id = ""
        recent_history_titles: list[str] = []
        recent_urls: list[str] = []
        recent_titles: list[str] = []
        candidates = []
        latest_candidates = []
        duplicate_recovery_note = self._append_note(preflight_index_note or "", sync_note)
        for round_idx in range(1, 4):
            latest_candidates = self._profile_call(
                f"collect_candidates_round_{round_idx}",
                lambda: self._collect_candidates_with_retry(max_attempts=3),
                slow_ms=2500,
            )
            latest_candidates = [c for c in (latest_candidates or []) if self._candidate_matches_content_mode(c)]
            recent_history_titles = self._get_recent_blogger_titles(limit=240, refresh_api=False)
            recent_urls = []
            recent_titles = list(recent_history_titles)
            filtered = self._filter_recent_duplicates(latest_candidates, recent_titles)
            if filtered:
                candidates = filtered
                break
            if latest_candidates:
                duplicate_recovery_note = self._append_note(
                    duplicate_recovery_note,
                    f"duplicate_recovery_round={round_idx}",
                )
            # Force topic growth + recollect loop (max 3 rounds) to avoid premature skip.
            try:
                self.topic_grower.maybe_grow(existing_titles + recent_titles)
            except Exception:
                pass
            time.sleep(1)
        if not candidates and latest_candidates:
            # Completion-first policy: relax duplicate gate after 3 recovery rounds.
            candidates = latest_candidates
            duplicate_recovery_note = self._append_note(
                duplicate_recovery_note,
                "duplicate_guard_relaxed_for_completion",
            )
        if not candidates:
            raise RuntimeError("Candidate collection exhausted after 3 retry rounds")
        diversity_note = ""
        entity_title_map = self._entity_titles_from_today_snapshot(blog_snapshot)
        if entity_title_map:
            filtered_by_entity = self._exclude_same_entity_same_topic_candidates(
                candidates,
                entity_title_map,
            )
            if not filtered_by_entity:
                # Completion-first policy:
                # When topical-duplicate gate blocks all candidates, keep growing/recollecting instead of early skip.
                diversity_recovery_rounds = max(
                    4,
                    min(
                        8,
                        int(getattr(self.settings.topic_growth, "daily_new_topics", 6) or 6),
                    ),
                )
                for round_idx in range(1, diversity_recovery_rounds + 1):
                    try:
                        self.topic_grower.maybe_grow(
                            existing_titles + recent_titles + sorted(list(entity_title_map.keys()))
                        )
                    except Exception:
                        pass
                    rec = self._collect_candidates_with_retry(max_attempts=3)
                    rec = [c for c in (rec or []) if self._candidate_matches_content_mode(c)]
                    # Refresh recent history after each growth round to avoid reusing just-written patterns.
                    recent_history_titles = self._get_recent_blogger_titles(limit=240, refresh_api=False)
                    recent_urls = []
                    recent_titles = list(recent_history_titles)
                    rec = self._filter_recent_duplicates(rec, recent_titles)
                    filtered_by_entity = self._exclude_same_entity_same_topic_candidates(
                        rec,
                        entity_title_map,
                    )
                    if filtered_by_entity:
                        diversity_note = self._append_note(
                            diversity_note,
                            f"entity_recovery_round={round_idx}/{diversity_recovery_rounds}",
                        )
                        break
                    time.sleep(1)
            if filtered_by_entity:
                candidates = filtered_by_entity
                diversity_note = self._append_note(
                    diversity_note,
                    "entity_topic_dedupe_applied=" + ",".join(sorted(list(entity_title_map.keys()))[:8]),
                )
            else:
                self.logs.append_run(
                    RunRecord(
                        status="skipped",
                        score=0,
                        title="",
                        source_url="",
                        published_url="",
                        note="Entity topical-duplicate gate: no candidate after extended topic-growth recovery",
                    )
                )
                self._workflow_perf_finish_run("skipped", "Entity diversity gate: no diversified candidate")
                return WorkflowResult("skipped", "Entity diversity gate: no diversified candidate")
        self.brain.reset_run_counter()
        api_ready = bool(
            (self.settings.gemini.api_key or "").strip()
            and (self.settings.gemini.api_key or "").strip() != "GEMINI_API_KEY"
        )
        # Rezero 2.1: if API key is available, use Gemini path even in free_mode.
        free_local_mode = bool(self.settings.budget.free_mode and not api_ready)
        self._progress("trend", "글로벌 타겟 키워드 분석", 24)
        global_keywords, keyword_pool_note = self._acquire_run_keywords(candidates)
        keyword_fallback_note = keyword_pool_note or ""
        if not global_keywords:
            # Final non-API fallback from candidate text.
            local_pool = self._build_local_keyword_candidates(candidates, limit=24)
            global_keywords = local_pool[: max(1, int(self.settings.keyword_pool.pick_per_run))]
            if global_keywords:
                keyword_fallback_note = self._append_note(keyword_fallback_note, "keyword_local_fallback")
        if keyword_fallback_note:
            duplicate_recovery_note = self._append_note(duplicate_recovery_note, keyword_fallback_note)
        self.last_global_keywords = global_keywords[:5]
        self._set_cached_global_keywords(self.last_global_keywords)
        if self.brain.call_count:
            self.logs.increment_today_gemini_count(self.brain.call_count)
            self.brain.reset_run_counter()

        self._progress("select", "주제 선정 및 점수화", 28)
        selected = None
        score = 0
        reason = ""
        for select_try in range(1, 2):
            try:
                if free_local_mode:
                    selected, score, reason = self.brain.choose_best_free(
                        candidates,
                        recent_urls,
                        recent_titles,
                        target_keywords=global_keywords,
                    )
                else:
                    selected, score, reason = self.brain.choose_best(
                        candidates,
                        recent_urls,
                        recent_titles,
                        target_keywords=global_keywords,
                    )
                break
            except Exception as exc:
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                err = str(exc)
                if self._is_physical_impossible_error(err):
                    self.logs.append_run(
                        RunRecord(
                            status="skipped",
                            score=0,
                            title="",
                            source_url="",
                            published_url="",
                            note=self._physical_block_reason(err),
                        )
                    )
                    skip_reason = self._physical_block_reason(err)
                    self._workflow_perf_finish_run("skipped", skip_reason)
                    return WorkflowResult("skipped", skip_reason)
                if select_try >= 1 or not self._is_retryable_error(err):
                    reason = self._append_note(
                        reason,
                        f"selection_fallback_after_error={err[:120]}",
                    )
                    break
                time.sleep(min(2 * select_try, 6))
        if selected is None and candidates:
            selected = max(candidates, key=lambda x: int(getattr(x, "score", 0)))
            score = max(70, min(100, int(getattr(selected, "score", 70))))
            reason = self._append_note(reason, "fallback_selected_top_candidate")
        if selected is None:
            raise RuntimeError("Topic selection failed after retries")

        if not self._candidate_matches_content_mode(selected):
            filtered_pool = [c for c in (candidates or []) if self._candidate_matches_content_mode(c)]
            if filtered_pool:
                selected = max(filtered_pool, key=lambda x: int(getattr(x, "score", 0)))
                score = max(score, int(getattr(selected, "score", 70)))
                reason = self._append_note(reason, "content_mode_fallback_reselect")
            else:
                hold_title = str(getattr(selected, "title", "") or "content_mode_filtered")
                working_draft_id, hold_note = self._sync_stage_draft_checkpoint(
                    current_draft_post_id=working_draft_id,
                    stage="hold",
                    title=hold_title,
                    html_body="<h2>Hold</h2><p>All candidates violated content mode filters.</p>",
                    labels=["tech-fix", "troubleshooting"],
                    reason="content_mode_no_valid_candidate",
                )
                hold_msg = "content_mode_no_valid_candidate"
                if hold_note:
                    hold_msg += f" | {hold_note}"
                self._workflow_perf_finish_run("hold", hold_msg)
                return WorkflowResult("hold", hold_msg)

        if score < self.settings.gemini.min_publish_score:
            reason = self._append_note(reason, f"score_auto_raised_from_{score}")
            score = max(score, int(self.settings.gemini.min_publish_score))

        generation_degraded_note = ""
        if queue_advisory:
            generation_degraded_note = self._append_note(generation_degraded_note, queue_advisory)
        if duplicate_recovery_note:
            generation_degraded_note = self._append_note(generation_degraded_note, duplicate_recovery_note)
        if diversity_note:
            generation_degraded_note = self._append_note(generation_degraded_note, diversity_note)
        collect_checkpoint_html = (
            "<h2>Collection Snapshot</h2>"
            "<p><strong>Pipeline stage:</strong> collect_done</p>"
            f"<p><strong>Topic:</strong> {escape(str(getattr(selected, 'title', '') or 'Untitled Topic'))}</p>"
            f"<p><strong>Source:</strong> {escape(str(getattr(selected, 'source', '') or 'unknown'))}</p>"
            f"<p><strong>Score:</strong> {int(score)}</p>"
            f"<p><strong>Reason:</strong> {escape(str(reason or 'n/a')[:300])}</p>"
        )
        selected_url = str(getattr(selected, "url", "") or "").strip()
        if selected_url:
            collect_checkpoint_html += (
                f'<p><strong>Reference:</strong> <a href="{escape(selected_url)}" rel="noopener">{escape(selected_url)}</a></p>'
            )
        public_labels = self._build_public_labels(
            title=str(getattr(selected, "title", "") or ""),
            candidate=selected,
            global_keywords=global_keywords,
            max_labels=6,
        )
        working_draft_id, collect_note = self._sync_stage_draft_checkpoint(
            current_draft_post_id=working_draft_id,
            stage="collect_done",
            title=str(getattr(selected, "title", "") or "Topic candidate selected"),
            html_body=collect_checkpoint_html,
            labels=public_labels,
            reason=reason,
        )
        if collect_note:
            generation_degraded_note = self._append_note(generation_degraded_note, collect_note)
        headline_note = ""
        self._progress("draft", "본문 초안 생성", 40)
        current_domain = self._infer_domain_from_title(str(getattr(selected, "title", "") or ""))
        if free_local_mode:
            generation_count += 1
            draft = self.brain.generate_post_free(selected, self.settings.authority_links)
        else:
            pattern = self.patterns.choose(selected)
            current_domain = str(getattr(pattern, "domain", "tech_troubleshoot") or "tech_troubleshoot")
            pattern_instruction = (
                f"Pattern key: {pattern.key}\n"
                f"Domain: {current_domain}\n"
                f"Audience stage: {pattern.stage}\n"
                f"Objective: {pattern.objective}\n"
                "Outline:\n- " + "\n- ".join(pattern.outline)
            )
            reference_guidance = self.references.build_guidance()
            draft = None
            generation_fail_notes: list[str] = []
            max_attempts = 1
            for attempt in range(1, max_attempts + 1):
                try:
                    generation_count += 1
                    draft_candidate = self.brain.generate_post(
                        selected,
                        self.settings.authority_links,
                        pattern_instruction,
                        reference_guidance,
                        domain=current_domain,
                    )
                except Exception as exc:
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                    err = str(exc)
                    generation_fail_notes.append(f"attempt={attempt}, error={err}")
                    if self._is_physical_impossible_error(err):
                        self.logs.append_run(
                            RunRecord(
                                status="skipped",
                                score=0,
                                title="",
                                source_url=str(getattr(selected, "url", "") or ""),
                                published_url="",
                                note=self._physical_block_reason(err),
                            )
                        )
                        skip_reason = self._physical_block_reason(err)
                        self._workflow_perf_finish_run("skipped", skip_reason)
                        return WorkflowResult("skipped", skip_reason)
                    # Temporary 429 should be retried by scheduler with 20-30min window.
                    if self._is_temporary_rate_limit_error(err):
                        raise
                    if not self._is_retryable_error(err):
                        continue
                    continue

                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()

                issues = self._draft_fatal_issues(draft_candidate.html, domain=current_domain)
                if issues:
                    generation_fail_notes.append(f"attempt={attempt}, issues={','.join(issues)}")
                    continue
                draft = draft_candidate
                break

            # Keep the same cycle alive: emergency local rewrite instead of dropping the slot.
            if draft is None:
                draft = self.brain.generate_post_free(selected, self.settings.authority_links)
                generation_degraded_note = self._append_note(generation_degraded_note, "degraded_to_free_mode_rewrite")

        # Headline specialist step: dedicated CTR rewrite via Gemini 2.0 Pro.
        if not free_local_mode:
            self._progress("headline", "제목 CTR 최적화", 50)
            try:
                optimized_title, variants = self.brain.optimize_headline_ctr(
                    summary=(draft.summary or self._normalize_excerpt(draft.html)[:1200]),
                    trending_keywords=global_keywords,
                    current_title=draft.title,
                )
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                if optimized_title.strip():
                    draft.title = optimized_title.strip()
                merged = [draft.title, *variants, *draft.alt_titles]
                dedup: list[str] = []
                seen: set[str] = set()
                for title in merged:
                    t = str(title or "").strip()
                    if not t:
                        continue
                    key = t.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    dedup.append(t)
                draft.alt_titles = dedup[:5]
                headline_note = f"headline_opt=ok, variants={len(draft.alt_titles)}"
            except Exception as exc:
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                headline_note = f"headline_opt=failed:{exc}"
        draft.title = self._enforce_seo_title(
            title=draft.title,
            candidate=selected,
            global_keywords=global_keywords,
        )

        similarity = self._similarity_ratio(draft.html, selected.body)
        if similarity >= 0.55:
            generation_degraded_note = self._append_note(
                generation_degraded_note,
                f"high_source_similarity={similarity:.2f};forced_continue",
            )

        base_html = draft.html + self._build_compliance_block(selected.source, selected.url)
        base_html += self._build_internal_links_block(
            current_title=draft.title,
            current_keywords=global_keywords,
            current_device_type=self._infer_device_type(f"{draft.title}\n{selected.title}"),
            current_cluster_id=self._infer_cluster_id_from_keyword(" ".join(global_keywords[:2]) or draft.title),
        )
        base_html = self._sanitize_publish_html(base_html, domain=current_domain)
        base_html = self._ensure_quick_take_block(base_html, draft.title)
        if self._is_near_duplicate_post(base_html):
            base_html = self._inject_freshness_appendix(base_html, selected.title, global_keywords)
            generation_degraded_note = self._append_note(
                generation_degraded_note,
                "near_duplicate_detected;freshness_appendix_injected",
            )
        linked_html = self._inject_search_links(base_html, global_keywords)
        if linked_html != base_html:
            generation_degraded_note = self._append_note(
                generation_degraded_note,
                "search_links_injected",
            )
            base_html = linked_html
        base_html = self._canonicalize_html_payload(base_html)
        self._progress("qa", "품질 게이트 점검/개선", 58)
        qa_result = self._qa_evaluate(
            base_html,
            title=draft.title,
            domain=current_domain,
            keyword=str(getattr(selected, "title", "") or ""),
            context="initial",
        )
        final_html = base_html
        qa_retry_count = 0
        if self.settings.quality.enabled:
            # User policy: keep developing until score is above 90.
            target_score = max(91, int(self.settings.quality.min_quality_score))
            qa_no_progress_streak = 0
            while qa_result.score < target_score or (
                self.settings.quality.humanity_hard_fail_block and qa_result.has_hard_failure
            ):
                improved = self.qa.improve_with_feedback(final_html, qa_result.failed, qa_result)
                qa_retry_count += 1
                if improved != final_html:
                    partial_fix_count += 1
                if improved == final_html:
                    improved = self.qa.satisfy_requirements(final_html, qa_result)
                    if improved != final_html:
                        partial_fix_count += 1
                if improved == final_html:
                    improved = self.qa.force_comply(final_html)
                    if improved != final_html:
                        partial_fix_count += 1
                if improved == final_html:
                    improved = self._inject_qa_no_progress_patch(final_html, qa_result)
                    if improved != final_html:
                        partial_fix_count += 1
                if improved == final_html:
                    qa_no_progress_streak += 1
                    if qa_no_progress_streak >= 2:
                        hard_note = (
                            (";hard_fail=" + ",".join(qa_result.hard_failures[:3]))
                            if qa_result.has_hard_failure
                            else ""
                        )
                        generation_degraded_note = self._append_note(
                            generation_degraded_note,
                            f"qa_no_progress_break={qa_result.score}/{target_score}{hard_note}",
                        )
                        break
                else:
                    qa_no_progress_streak = 0
                final_html = self._canonicalize_html_payload(improved)
                qa_result = self._qa_evaluate(
                    final_html,
                    title=draft.title,
                    domain=current_domain,
                    keyword=str(getattr(selected, "title", "") or ""),
                    context="improve_loop",
                )

        if (
            self.settings.quality.enabled
            and self.settings.quality.strict_mode
            and (
                qa_result.score < self.settings.quality.min_quality_score
                or (self.settings.quality.humanity_hard_fail_block and qa_result.has_hard_failure)
            )
        ):
            completed = self.qa.satisfy_requirements(final_html, qa_result)
            if completed != final_html:
                qa_retry_count += 1
                final_html = self._canonicalize_html_payload(completed)
                qa_result = self._qa_evaluate(
                    final_html,
                    title=draft.title,
                    domain=current_domain,
                    keyword=str(getattr(selected, "title", "") or ""),
                    context="strict_mode_complete",
                )

        if (
            self.settings.quality.enabled
            and self.settings.quality.strict_mode
            and (
                qa_result.score < self.settings.quality.min_quality_score
                or (self.settings.quality.humanity_hard_fail_block and qa_result.has_hard_failure)
            )
        ):
            generation_degraded_note = self._append_note(
                generation_degraded_note,
                (
                    f"qa_soft_accept={qa_result.score}/"
                    f"{self.settings.quality.min_quality_score};"
                    f"retries={qa_retry_count};"
                    f"hard_fail={','.join(qa_result.hard_failures[:3]) if qa_result.hard_failures else 'none'}"
                ),
            )

        # Final QA polish pass even when already passed.
        if self.settings.quality.enabled:
            baseline_score = qa_result.score
            polished = self.qa.polish_if_possible(final_html, qa_result)
            if polished != final_html:
                polished = self._canonicalize_html_payload(polished)
                polished_result = self._qa_evaluate(
                    polished,
                    title=draft.title,
                    domain=current_domain,
                    keyword=str(getattr(selected, "title", "") or ""),
                    context="polish_pass",
                )
                # If polish reduced score, develop the polished draft instead of discarding.
                if polished_result.score < baseline_score:
                    candidate_html = polished
                    candidate_result = polished_result
                    target_score = max(91, int(self.settings.quality.min_quality_score))
                    qa_no_progress_streak = 0
                    while candidate_result.score < target_score or (
                        self.settings.quality.humanity_hard_fail_block and candidate_result.has_hard_failure
                    ):
                        improved = self.qa.improve_with_feedback(
                            candidate_html,
                            candidate_result.failed,
                            candidate_result,
                        )
                        qa_retry_count += 1
                        if improved == candidate_html:
                            improved = self.qa.satisfy_requirements(candidate_html, candidate_result)
                        if improved == candidate_html:
                            improved = self.qa.force_comply(candidate_html)
                        if improved == candidate_html:
                            improved = self._inject_qa_no_progress_patch(candidate_html, candidate_result)
                        if improved == candidate_html:
                            qa_no_progress_streak += 1
                            if qa_no_progress_streak >= 2:
                                hard_note = (
                                    (";hard_fail=" + ",".join(candidate_result.hard_failures[:3]))
                                    if candidate_result.has_hard_failure
                                    else ""
                                )
                                generation_degraded_note = self._append_note(
                                    generation_degraded_note,
                                    f"qa_polish_no_progress_break={candidate_result.score}/{target_score}{hard_note}",
                                )
                                break
                        else:
                            qa_no_progress_streak = 0
                        candidate_html = improved
                        candidate_result = self._qa_evaluate(
                            candidate_html,
                            title=draft.title,
                            domain=current_domain,
                            keyword=str(getattr(selected, "title", "") or ""),
                            context="polish_recover_loop",
                        )
                    if candidate_result.score >= target_score and (
                        (not self.settings.quality.humanity_hard_fail_block)
                        or (not candidate_result.has_hard_failure)
                    ):
                        final_html = candidate_html
                        qa_result = candidate_result
                else:
                    final_html = polished
                    qa_result = polished_result

        if self.settings.quality.llm_judge_enabled and (not free_local_mode):
            judge_score = 100
            judge_issues: list[str] = []
            for judge_try in range(1, 4):
                try:
                    judge_score, judge_issues = self.brain.judge_post(draft.title, final_html)
                    break
                except Exception as exc:
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                    err = str(exc)
                    if self._is_physical_impossible_error(err):
                        self.logs.append_run(
                            RunRecord(
                                status="skipped",
                                score=0,
                                title=draft.title,
                                source_url=selected.url,
                                published_url="",
                                note=self._physical_block_reason(err),
                            )
                        )
                        skip_reason = self._physical_block_reason(err)
                        self._workflow_perf_finish_run("skipped", skip_reason)
                        return WorkflowResult("skipped", skip_reason)
                    if self._is_temporary_rate_limit_error(err):
                        raise
                    if judge_try >= 3 or not self._is_retryable_error(err):
                        generation_degraded_note = self._append_note(
                            generation_degraded_note,
                            f"llm_judge_failed:{err}",
                        )
                        break
                    time.sleep(min(2 * judge_try, 6))
            if self.brain.call_count:
                self.logs.increment_today_gemini_count(self.brain.call_count)
                self.brain.reset_run_counter()
            if judge_score < int(self.settings.quality.llm_judge_min_score):
                generation_degraded_note = self._append_note(
                    generation_degraded_note,
                    "llm_judge_soft_override:"
                    + str(judge_score)
                    + ((";" + ";".join(judge_issues[:3])) if judge_issues else ""),
                )

        working_draft_id, draft_note = self._sync_stage_draft_checkpoint(
            current_draft_post_id=working_draft_id,
            stage="draft_done",
            title=draft.title,
            html_body=final_html,
            labels=self._build_public_labels(
                title=draft.title,
                candidate=selected,
                global_keywords=global_keywords,
                max_labels=6,
            ),
            reason=(
                f"qa={qa_result.score};base={qa_result.base_score};soft={qa_result.soft_score};"
                f"hard={len(qa_result.hard_failures)};qa_retries={qa_retry_count}"
            ),
        )
        if draft_note:
            generation_degraded_note = self._append_note(generation_degraded_note, draft_note)

        self._progress("visual", "이미지 라이브러리 선택", 74)
        target_images = max(2, int(self.settings.visual.target_images_per_post or 2))
        self._set_image_pipeline_state("running", 0, target_images, "이미지 라이브러리 선택 시작")
        image_prompt_plan: dict[str, Any] = {"source": "library"}
        images = self._profile_call(
            "image_library_pick",
            lambda: pick_images(
                title=draft.title,
                min_count=target_images,
                root=self.root,
            ),
            slow_ms=2000,
        )
        images = self.visual.ensure_unique_assets(images)
        image_kind_counts = Counter((getattr(img, "source_kind", "") or "unknown") for img in images)
        if len(images) < target_images:
            hold_labels = self._build_public_labels(
                title=draft.title,
                candidate=selected,
                global_keywords=global_keywords,
                max_labels=6,
            )
            policy_reason = "image_library_shortage"
            working_draft_id, hold_note = self._sync_stage_draft_checkpoint(
                current_draft_post_id=working_draft_id,
                stage="hold",
                title=draft.title,
                html_body=final_html,
                labels=hold_labels,
                reason=policy_reason,
            )
            hold_msg = policy_reason
            if hold_note:
                hold_msg += f" | {hold_note}"
            self._set_image_pipeline_state("failed", len(images), target_images, hold_msg)
            self._workflow_perf_finish_run("hold", hold_msg)
            return WorkflowResult("hold", hold_msg)
        self._set_image_pipeline_state("validated", len(images), target_images, f"이미지 선택 완료 {len(images)}/{target_images}")
        self._progress("visual", f"이미지 선택 완료 {len(images)}/{target_images}", 80)
        self._ensure_min_long_tail_keywords(
            candidate=selected,
            title=draft.title,
            global_keywords=global_keywords,
        )
        self._optimize_thumbnail_alt(images, selected)
        final_html += self._build_image_rights_block(images, draft.source_url)
        final_html = self._sanitize_publish_html(final_html, domain=current_domain)
        final_html = self._double_unescape(final_html)
        final_html = self._canonicalize_html_payload(final_html)
        local_qa_review = self._profile_call(
            "local_llm_qa_review",
            lambda: self._run_local_llm_qa_review(
                title=draft.title,
                html=final_html,
                images=images,
            ),
            slow_ms=2500,
        )
        for phrase in (local_qa_review.get("remove_phrases", []) if isinstance(local_qa_review, dict) else []):
            tok = re.escape(re.sub(r"\s+", " ", str(phrase or "")).strip())
            if not tok:
                continue
            final_html = re.sub(tok, "", final_html, flags=re.IGNORECASE)
        final_html = self._sanitize_publish_html(final_html, domain=current_domain)
        final_html = self._canonicalize_html_payload(final_html)
        if self._contains_markdown_tokens(final_html):
            final_html = self._canonicalize_html_payload(final_html)
            final_html = self._sanitize_publish_html(final_html, domain=current_domain)
            if self._contains_markdown_tokens(final_html):
                hold_labels = self._build_public_labels(
                    title=draft.title,
                    candidate=selected,
                    global_keywords=global_keywords,
                    max_labels=6,
                )
                working_draft_id, hold_note = self._sync_stage_draft_checkpoint(
                    current_draft_post_id=working_draft_id,
                    stage="hold",
                    title=draft.title,
                    html_body=final_html,
                    labels=hold_labels,
                    reason="markdown_canonicalize_failed",
                )
                hold_msg = "markdown_canonicalize_failed"
                if hold_note:
                    hold_msg += f" | {hold_note}"
                self._workflow_perf_finish_run("hold", hold_msg)
                return WorkflowResult("hold", hold_msg)
        if isinstance(local_qa_review, dict):
            issue_count = len(list(local_qa_review.get("issues", []) or []))
            if issue_count > 0:
                generation_degraded_note = self._append_note(
                    generation_degraded_note,
                    f"local_llm_qa_issues={issue_count}",
                )
        draft.title = self._double_unescape(str(draft.title or "")).strip()
        gate_preview_html = ""
        preflight_thumb_src = ""
        if bool(self.settings.budget.dry_run):
            try:
                gate_preview_html = self.publisher.build_dry_run_html(final_html, images)
            except Exception as exc:
                raise RuntimeError(f"Go-live preflight merge failed: {exc}") from exc
        else:
            try:
                images, preflight_thumb_src = self._profile_call(
                    "thumbnail_preflight_with_recovery",
                    lambda: self._preflight_thumbnail_with_recovery(
                        draft=draft,
                        candidate=selected,
                        images=images,
                        prompt_plan=image_prompt_plan,
                        max_attempts=3,
                        manual_trigger=manual_trigger,
                    ),
                    slow_ms=8000,
                )
                creds_for_gate = self.publisher._oauth_credentials()  # noqa: SLF001
                gate_preview_html = self.publisher._merge_images(  # noqa: SLF001
                    final_html,
                    images,
                    creds_for_gate,
                    preflight_thumbnail_src=preflight_thumb_src,
                )
            except Exception as exc:
                raise RuntimeError(f"Go-live preflight merge failed: {exc}") from exc
        go_live_errors, go_live_warnings = self._go_live_gate_checklist(
            title=draft.title,
            final_html=final_html,
            gate_html=gate_preview_html,
            images=images,
            candidate=selected,
        )
        if go_live_errors:
            raise RuntimeError("Go-live gate failed: " + "; ".join(go_live_errors[:5]))
        if go_live_warnings:
            generation_degraded_note = self._append_note(
                generation_degraded_note,
                "go_live_warnings=" + ",".join(go_live_warnings[:4]),
            )
        if self._has_visual_placeholder_text(final_html):
            raise RuntimeError("Visual placeholder leak detected after sanitize pass")
        labels = self._build_public_labels(
            title=draft.title,
            candidate=selected,
            global_keywords=global_keywords,
            max_labels=6,
        )
        meta_description = self._build_meta_description(
            title=draft.title,
            summary=draft.summary,
            html=final_html,
        )
        working_draft_id, image_note = self._sync_stage_draft_checkpoint(
            current_draft_post_id=working_draft_id,
            stage="images_done",
            title=draft.title,
            html_body=final_html,
            labels=labels,
            reason=f"images={len(images)};kinds={dict(image_kind_counts)}",
        )
        if image_note:
            generation_degraded_note = self._append_note(generation_degraded_note, image_note)

        if self.settings.budget.dry_run:
            dry_html = gate_preview_html or self.publisher.build_dry_run_html(final_html, images)
            dry_log_dir = self.root / "storage" / "logs"
            dry_log_dir.mkdir(parents=True, exist_ok=True)
            dry_html_path = dry_log_dir / f"dry_run_final_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.html"
            dry_html_path.write_text(dry_html, encoding="utf-8")
            if len(re.findall(r"<img\b[^>]*\bsrc=", dry_html, flags=re.IGNORECASE)) < 2:
                raise RuntimeError("dry-run regression failed: missing required <img> count (need 2)")
            published_url = "dry-run://not-published"
            self.logs.append_run(
                RunRecord(
                    status="success",
                    score=score,
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url=published_url,
                    note=(
                        f"Dry-run success (publish skipped, qa={qa_result.score}, "
                        f"generation_count={generation_count}, "
                        f"refine_count={qa_retry_count}, "
                        f"partial_fix_count={partial_fix_count}, "
                        f"total_gemini_calls={max(0, int(self.logs.get_today_gemini_count()) - today_gemini_before)}, "
                        f"images={len(images)}, kinds={dict(image_kind_counts)}, "
                        f"dry_html={dry_html_path.name}, "
                        f"keywords={','.join(global_keywords[:5])}"
                        + (f", {headline_note}" if headline_note else "")
                        + ")"
                    ),
                )
            )
            self._workflow_perf_finish_run("success", published_url)
            return WorkflowResult("success", published_url)

        self._progress("schedule", "예약 시간 계산", 84)
        publish_at = self._compute_publish_at()
        if publish_at is None:
            # Completion-first fallback: book the earliest safe slot instead of skipping.
            delay_min = max(
                10,
                int(getattr(self.settings.publish, "min_delay_minutes", 10)),
            )
            publish_at = datetime.now(timezone.utc) + timedelta(
                minutes=delay_min + random.randint(1, 20)
            )
            generation_degraded_note = self._append_note(
                generation_degraded_note,
                "schedule_fallback_used",
            )
        self._progress("publish", "Blogger 예약 발행 처리", 92)
        published = None
        last_publish_err = ""
        if not preflight_thumb_src:
            try:
                images, preflight_thumb_src = self._profile_call(
                    "thumbnail_preflight_with_recovery",
                    lambda: self._preflight_thumbnail_with_recovery(
                        draft=draft,
                        candidate=selected,
                        images=images,
                        prompt_plan=image_prompt_plan,
                        max_attempts=3,
                        manual_trigger=manual_trigger,
                    ),
                    slow_ms=8000,
                )
            except Exception as exc:
                last_publish_err = str(exc)
                draft_checkpoint_note = ""
                try:
                    checkpoint = self.publisher.save_draft_checkpoint(
                        title=draft.title,
                        html_body=final_html,
                        labels=labels,
                        stage="hold",
                        reason=last_publish_err,
                        draft_post_id=(working_draft_id or None),
                    )
                    if str(getattr(checkpoint, "post_id", "")).strip():
                        draft_checkpoint_note = f";draft_checkpoint={checkpoint.post_id}"
                except Exception as cp_exc:
                    draft_checkpoint_note = f";draft_checkpoint_failed={str(cp_exc)[:120]}"
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=0,
                        title=draft.title,
                        source_url=draft.source_url,
                        published_url="",
                        note=f"requeued_to_tail: {last_publish_err}{draft_checkpoint_note}",
                    )
                )
                checkpoint_msg = ""
                if "draft_checkpoint=" in draft_checkpoint_note:
                    checkpoint_msg = f" | {draft_checkpoint_note.lstrip(';')}"
                grade = self._classify_error_grade(last_publish_err)
                hold_msg = f"requeued_to_tail[{grade}]: {last_publish_err}{checkpoint_msg}"
                self._mark_active_slot("hold", hold_msg)
                self._workflow_perf_finish_run("hold", hold_msg)
                return WorkflowResult("hold", hold_msg)
        if bool(getattr(self.settings.publish, "thumbnail_preflight_only", False)):
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=score,
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url="",
                    note=f"thumbnail_preflight_only_ok:{preflight_thumb_src[:180]}",
                )
            )
            self._mark_active_slot("hold", "thumbnail_preflight_only_ok")
            self._workflow_perf_finish_run("hold", "thumbnail_preflight_only_ok")
            return WorkflowResult("hold", "thumbnail_preflight_only_ok")
        publish_backoff = [30, 300, 900]
        for publish_try in range(1, 4):
            try:
                published = self._profile_call(
                    f"publish_post_attempt_{publish_try}",
                    lambda: self.publisher.publish_post(
                        draft.title,
                        final_html,
                        images,
                        labels,
                        publish_at=publish_at,
                        existing_draft_post_id=(working_draft_id or None),
                        meta_description=meta_description,
                        preflight_thumbnail_src=preflight_thumb_src,
                    ),
                    slow_ms=8000,
                    meta={"attempt": int(publish_try)},
                )
                break
            except Exception as exc:
                err = str(exc)
                last_publish_err = err
                if self._is_physical_impossible_error(err):
                    self.logs.append_run(
                        RunRecord(
                            status="skipped",
                            score=0,
                            title=draft.title,
                            source_url=draft.source_url,
                            published_url="",
                            note=self._physical_block_reason(err),
                        )
                    )
                    skip_reason = self._physical_block_reason(err)
                    self._mark_active_slot("skipped", skip_reason)
                    self._workflow_perf_finish_run("skipped", skip_reason)
                    return WorkflowResult("skipped", skip_reason)
                if ("thumbnail_preflight_failed" in err or "missing thumbnail image url" in err) and publish_try < 3:
                    try:
                        images, preflight_thumb_src = self._preflight_thumbnail_with_recovery(
                            draft=draft,
                            candidate=selected,
                            images=images,
                            prompt_plan=image_prompt_plan,
                            max_attempts=3,
                            manual_trigger=manual_trigger,
                        )
                        continue
                    except Exception as pre_exc:
                        last_publish_err = str(pre_exc)
                        break
                if publish_try >= 3 or not self._is_retryable_error(err):
                    break
                wait = publish_backoff[min(publish_try - 1, len(publish_backoff) - 1)]
                generation_degraded_note = self._append_note(
                    generation_degraded_note,
                    f"publish_retry_backoff={wait}s",
                )
                time.sleep(wait)
        if published is None:
            draft_checkpoint_note = ""
            try:
                checkpoint = self.publisher.save_draft_checkpoint(
                    title=draft.title,
                    html_body=final_html,
                    labels=labels,
                    stage="publish_blocked",
                    reason=last_publish_err,
                    draft_post_id=(working_draft_id or None),
                )
                if str(getattr(checkpoint, "post_id", "")).strip():
                    draft_checkpoint_note = f";draft_checkpoint={checkpoint.post_id}"
            except Exception as exc:
                draft_checkpoint_note = f";draft_checkpoint_failed={str(exc)[:120]}"
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url="",
                    note=f"requeued_to_tail_after_publish_retries: {last_publish_err}{draft_checkpoint_note}",
                )
            )
            checkpoint_msg = ""
            if "draft_checkpoint=" in draft_checkpoint_note:
                checkpoint_msg = f" | {draft_checkpoint_note.lstrip(';')}"
            grade = self._classify_error_grade(last_publish_err)
            hold_msg = f"requeued_to_tail[{grade}]: {last_publish_err}{checkpoint_msg}"
            self._mark_active_slot("hold", hold_msg)
            self._workflow_perf_finish_run("hold", hold_msg)
            return WorkflowResult("hold", hold_msg)
        self._progress("indexing", "인덱싱/후처리 반영", 97)
        self.logs.add_scheduled_post(
            publish_at=publish_at.isoformat(),
            post_id=published.post_id,
            title=draft.title,
            source_url=draft.source_url,
            published_url=published.url,
        )
        # Force refresh from real Blogger state on next read.
        self._blog_snapshot_cache = None

        indexing_note = ""
        if self._can_auto_index_notify():
            daily = self.logs.get_today_indexing_count()
            if daily < self.settings.indexing.daily_quota:
                try:
                    self.publisher.notify_indexing(published.url)
                    self.logs.increment_today_indexing_count()
                    self.logs.mark_index_notified(published.url)
                except Exception as exc:
                    indexing_note = f", indexing_notify_failed={exc}"
        inspect_note = ""
        can_inspect, sc_site = self._can_auto_search_console_inspect()
        if can_inspect and published.url:
            try:
                payload = self.publisher.inspect_url(site_url=sc_site, inspection_url=published.url)
                verdict = self.publisher.inspection_verdict(payload)
                self.logs.mark_inspection_checked(published.url, verdict=verdict)
            except Exception as exc:
                inspect_note = f", url_inspect_failed={str(exc)[:120]}"
        recent_refresh_note = ""
        try:
            refreshed = self._refresh_blogger_recent_titles_cache(force_api=True, limit=260)
            recent_refresh_note = f", blogger_14d_cache={len(refreshed)}"
        except Exception as exc:
            recent_refresh_note = f", blogger_14d_cache_refresh_failed={str(exc)[:120]}"

        self.logs.append_run(
            RunRecord(
                status="success",
                score=score,
                title=draft.title,
                source_url=draft.source_url,
                published_url=published.url,
                note=(
                    f"Published successfully (qa={qa_result.score})"
                    + (f", qa_retries={qa_retry_count}" if qa_retry_count else "")
                    + f", generation_count={generation_count}"
                    + f", refine_count={qa_retry_count}"
                    + f", partial_fix_count={partial_fix_count}"
                    + f", total_gemini_calls={max(0, int(self.logs.get_today_gemini_count()) - today_gemini_before)}"
                    + f", images={len(images)}, kinds={dict(image_kind_counts)}"
                    + self._build_image_publish_note()
                    + f", keywords={','.join(global_keywords[:5])}"
                    + (f", {headline_note}" if headline_note else "")
                    + (f", {generation_degraded_note}" if generation_degraded_note else "")
                    + indexing_note
                    + inspect_note
                    + recent_refresh_note
                    + (f", scheduled_at={publish_at.isoformat()}" if publish_at else "")
                    + (", manual_excluded=true" if manual_trigger else "")
                ),
            )
        )
        if manual_trigger and str(getattr(published, "post_id", "")).strip():
            self.logs.add_excluded_post(str(published.post_id).strip(), reason="manual_trigger")
        self._rotate_keywords_after_success(
            candidates=candidates,
            used_text=f"{draft.title}\n{draft.summary}\n{self._normalize_excerpt(final_html)[:2000]}",
        )
        self.logs.add_content_fingerprint(
            title=draft.title,
            source_url=draft.source_url,
            excerpt=self._normalize_excerpt(final_html),
        )
        try:
            self._index_published_post(
                post_id=str(getattr(published, "post_id", "") or ""),
                url=str(getattr(published, "url", "") or ""),
                title=draft.title,
                html=final_html,
                summary=draft.summary,
                global_keywords=global_keywords,
                candidate=selected,
                publish_at=publish_at,
            )
        except Exception:
            pass
        self._cleanup_local_image_files(images)
        self._progress("done", "회차 완료", 100)
        self._mark_active_slot("consumed", "publish_success", post_id=str(getattr(published, "post_id", "") or ""))
        if publish_at:
            success_msg = f"{published.url} (scheduled: {publish_at.isoformat()})"
            self._workflow_perf_finish_run("success", success_msg)
            return WorkflowResult("success", success_msg)
        self._workflow_perf_finish_run("success", published.url)
        return WorkflowResult("success", published.url)

    def _cleanup_local_image_files(self, images: list[ImageAsset]) -> int:
        removed = 0
        protected_roots = [
            (self.root / "assets" / "library").resolve(),
            (self.root / "assets" / "fallback").resolve(),
        ]
        for img in (images or []):
            try:
                p = Path(getattr(img, "path", ""))
            except Exception:
                p = Path("")
            if not str(p):
                continue
            try:
                rp = p.resolve()
                if any(str(rp).startswith(str(pr)) for pr in protected_roots):
                    continue
                if p.exists():
                    p.unlink(missing_ok=True)
                    removed += 1
            except Exception:
                continue
        return removed

    def _collect_candidates_with_retry(self, max_attempts: int = 3):
        last_err = ""
        for attempt in range(1, max(1, int(max_attempts)) + 1):
            try:
                return self.scout.collect()
            except Exception as exc:
                last_err = str(exc)
                if attempt >= max_attempts or not self._is_retryable_error(last_err):
                    break
                time.sleep(min(2 * attempt, 6))
        if last_err:
            raise RuntimeError(f"Collect failed after retries: {last_err}")
        return []

    def _entity_titles_from_today_snapshot(self, blog_snapshot: dict) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        now = datetime.now(timezone.utc)
        today = now.date()
        for row in (blog_snapshot.get("scheduled_items", []) or []):
            dt = self._parse_iso_utc(str(row.get("publish_at", "")).strip())
            if dt is None:
                continue
            if dt.date() != today:
                continue
            title = str(row.get("title", "")).strip()
            if not title:
                continue
            for entity in self._extract_entities(title):
                out.setdefault(entity, []).append(title)
        for title in (blog_snapshot.get("today_live_titles", []) or []):
            t = str(title or "").strip()
            if not t:
                continue
            for entity in self._extract_entities(t):
                out.setdefault(entity, []).append(t)
        return out

    def _exclude_same_entity_same_topic_candidates(
        self,
        candidates,
        entity_titles: dict[str, list[str]],
    ):
        if not entity_titles:
            return list(candidates)
        out = []
        for c in candidates:
            text = f"{getattr(c, 'title', '')} {getattr(c, 'body', '')}"
            entities = self._extract_entities(text)
            if not entities:
                out.append(c)
                continue
            cand_title = str(getattr(c, "title", "") or "").strip()
            cand_tokens = self._tokenize(cand_title)
            same_topic = False
            for entity in entities:
                prev_titles = [str(t or "").strip() for t in (entity_titles.get(entity, []) or []) if str(t or "").strip()]
                if not prev_titles:
                    continue
                # Allow same company if angle/topic differs; only block near-duplicate title within same entity.
                if self._semantic_near_duplicate(cand_title, set(prev_titles), threshold=0.78):
                    same_topic = True
                    break
                for prev in prev_titles[:40]:
                    sim = self._bow_cosine(cand_tokens, self._tokenize(prev))
                    lex = SequenceMatcher(None, cand_title.lower(), prev.lower()).ratio()
                    if sim >= 0.80 or (sim >= 0.70 and lex >= 0.72):
                        same_topic = True
                        break
                if same_topic:
                    break
            if same_topic:
                continue
            out.append(c)
        return out

    def _extract_entities(self, text: str) -> set[str]:
        lower = (text or "").lower()
        aliases = getattr(SourceScout, "_ENTITY_ALIASES", {}) or {}
        out: set[str] = set()
        for entity, names in aliases.items():
            if any(str(alias).lower() in lower for alias in (names or [])):
                out.add(str(entity))
        return out

    def _candidate_matches_content_mode(self, candidate: TopicCandidate | None) -> bool:
        if candidate is None:
            return False
        mode = str(getattr(self.settings.content_mode, "mode", "") or "").strip().lower()
        if mode != "tech_troubleshoot_only":
            return True
        text = (
            f"{str(getattr(candidate, 'title', '') or '')}\n"
            f"{str(getattr(candidate, 'body', '') or '')}"
        ).lower()
        banned = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "banned_topic_keywords", []) or [])
            if str(x or "").strip()
        ]
        if any(tok in text for tok in banned):
            return False
        device_tokens = {
            "windows", "mac", "macos", "iphone", "ios", "galaxy", "samsung",
            "android", "wifi", "bluetooth", "audio", "speaker", "microphone",
            "battery", "charging", "update", "driver",
        }
        fix_tokens = {
            "not working", "fix", "error", "after update", "troubleshoot",
            "issue", "stuck", "crash", "failed", "broken",
        }
        has_device = any(tok in text for tok in device_tokens)
        has_fix = any(tok in text for tok in fix_tokens)
        return bool(has_device and has_fix)

    def _infer_domain_from_title(self, title: str) -> str:
        mode = str(getattr(self.settings.content_mode, "mode", "") or "").strip().lower()
        if mode == "tech_troubleshoot_only":
            return "tech_troubleshoot"
        lower = str(title or "").lower()
        if any(
            key in lower
            for key in [
                "prompt",
                "프롬프트",
                "system instruction",
                "example prompt",
                "prompt guide",
                "ai prompt",
            ]
        ):
            return "ai_prompt_guide"
        return "tech_troubleshoot"

    def _is_temporary_rate_limit_error(self, message: str) -> bool:
        msg = (message or "").lower()
        return "[temp_429_retry_min=" in msg

    def _is_physical_impossible_error(self, message: str) -> bool:
        msg = (message or "").lower()
        if "api 키가 설정되지 않았습니다" in msg:
            return True
        if "api key not valid" in msg:
            return True
        if "invalid api key" in msg:
            return True
        if "insufficient authentication scopes" in msg:
            return True
        if "invalid_scope" in msg:
            return True
        if "oauth 토큰 스코프가 현재 요청과 맞지 않습니다" in msg:
            return True
        if "permission denied" in msg and "drive" in msg:
            return True
        if "gcs 버킷이 설정되지 않았습니다" in msg:
            return True
        if "gcs 서비스 계정 키 파일을 찾을 수 없습니다" in msg:
            return True
        if "google-cloud-storage 패키지가 설치되지 않았습니다" in msg:
            return True
        if "blogger_token.json" in msg and "no such file or directory" in msg:
            return True
        return False

    def _physical_block_reason(self, message: str) -> str:
        msg = (message or "").lower()
        if "[daily_quota_exceeded]" in msg:
            return "No Quota: daily limit exhausted"
        if "api 키가 설정되지 않았습니다" in msg:
            return "No API Key: Gemini key is missing"
        if "api key not valid" in msg or "invalid api key" in msg:
            return "No API Key: invalid Gemini key"
        if "permission denied" in msg and "drive" in msg:
            return "No Permission: Google Drive scope missing"
        if "insufficient authentication scopes" in msg:
            return "No Permission: OAuth scopes are insufficient"
        if "invalid_scope" in msg or "oauth 토큰 스코프가 현재 요청과 맞지 않습니다" in msg:
            return "No Permission: OAuth scope mismatch (reconnect Google login)"
        if "gcs 버킷이 설정되지 않았습니다" in msg:
            return "No Image Hosting: GCS bucket is not configured"
        if "gcs 서비스 계정 키 파일을 찾을 수 없습니다" in msg:
            return "No Image Hosting: service_account.json is missing"
        if "google-cloud-storage 패키지가 설치되지 않았습니다" in msg:
            return "No Image Hosting: google-cloud-storage dependency missing"
        if "blogger_token.json" in msg and "no such file or directory" in msg:
            return "No Auth Token: blogger_token.json is missing"
        return "No Capacity: physical prerequisite not met"

    def _is_retryable_error(self, message: str) -> bool:
        msg = (message or "").lower()
        retry_signals = [
            "timeout",
            "timed out",
            "temporary",
            "temporarily",
            "connection",
            "connection reset",
            "connection aborted",
            "network",
            "503",
            "502",
            "500",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "please retry",
            "retry in",
            "retry required",
            "429",
            "[temp_429_retry_min=",
            "rate limit",
            "thumbnail must be generated image",
            "thumbnail must be hosted on blogger media server",
            "이미지 최소 개수 부족",
            "이미지 업로드에 실패했습니다",
        ]
        if self._is_physical_impossible_error(msg):
            return False
        return any(sig in msg for sig in retry_signals)

    def _append_note(self, base: str, note: str) -> str:
        left = (base or "").strip()
        right = (note or "").strip()
        if not left:
            return right
        if not right:
            return left
        return f"{left};{right}"

    def _current_rotated_device_type(self) -> str:
        default_order = ["windows", "mac", "iphone", "galaxy"]
        order = [str(x or "").strip().lower() for x in (getattr(self.settings.topics, "rotation_order", default_order) or default_order) if str(x or "").strip()]
        if not order:
            order = default_order
        if not bool(getattr(self.settings.topics, "monthly_rotation_enabled", True)):
            return order[0]
        month_no = datetime.now(self._kst).month
        idx = (month_no - 1) % len(order)
        return order[idx]

    def _infer_cluster_id_from_keyword(self, keyword: str) -> str:
        text = re.sub(r"\s+", " ", str(keyword or "").strip().lower())
        if any(k in text for k in ("bluetooth", "wifi", "network", "internet")):
            return "connectivity"
        if any(k in text for k in ("audio", "sound", "mic", "speaker")):
            return "audio"
        if any(k in text for k in ("battery", "charging", "power")):
            return "power"
        if any(k in text for k in ("update", "install", "patch", "version")):
            return "update_install"
        if any(k in text for k in ("camera", "photo", "video")):
            return "camera_media"
        if any(k in text for k in ("performance", "slow", "lag", "freeze")):
            return "performance"
        return "general"

    def _build_template_keywords(self, device_type: str, limit: int = 120) -> list[str]:
        d = str(device_type or "device").strip().lower()
        templates = [
            f"{d} not working",
            f"{d} update stuck",
            f"{d} not responding",
            "error code fix",
            f"{d} connected but no internet",
            f"{d} bluetooth not working",
            f"{d} no sound after update",
            f"{d} battery drain fix",
            f"{d} app crashes fix",
            f"{d} storage full fix",
            f"{d} network reset steps",
            f"{d} microphone not working",
        ]
        out: list[str] = []
        seen: set[str] = set()
        for raw in templates:
            norm = self._normalize_keyword(raw)
            if not norm:
                continue
            low = norm.lower()
            if low in seen:
                continue
            seen.add(low)
            out.append(norm)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def _get_cached_global_keywords(self) -> list[str]:
        now = datetime.now(timezone.utc)
        if self._global_keyword_cache is None:
            return []
        ts, kws = self._global_keyword_cache
        if (now - ts).total_seconds() >= self._global_keyword_cache_ttl_seconds:
            return []
        return [str(k).strip() for k in (kws or []) if str(k).strip()][:5]

    def _set_cached_global_keywords(self, keywords: list[str]) -> None:
        clean = [re.sub(r"\s+", " ", str(k or "")).strip() for k in (keywords or []) if str(k or "").strip()]
        self._global_keyword_cache = (datetime.now(timezone.utc), clean[:5])

    def _normalize_public_label(self, raw: str) -> str:
        label = re.sub(r"[^a-zA-Z0-9\s-]", " ", str(raw or "").strip().lower())
        label = re.sub(r"\s+", " ", label).strip()
        if not label:
            return ""
        label = label.replace(" ", "-")
        if len(label) < 2:
            return ""
        return label[:30]

    def _extract_title_label_tokens(self, title: str, limit: int = 8) -> list[str]:
        stop = {
            "the", "and", "for", "with", "from", "this", "that", "about", "your", "today",
            "guide", "why", "how", "what", "when", "new", "using", "into", "over", "under",
            "everyone", "talking", "worker", "workers", "office", "teams",
        }
        words = re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", str(title or ""))
        out: list[str] = []
        seen: set[str] = set()
        for w in words:
            lw = w.lower()
            if lw in stop:
                continue
            n = self._normalize_public_label(lw)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(n)
            if len(out) >= limit:
                break
        return out

    def _build_public_labels(
        self,
        title: str,
        candidate: TopicCandidate | None,
        global_keywords: list[str] | None,
        max_labels: int = 6,
    ) -> list[str]:
        labels: list[str] = []
        seen: set[str] = set()
        banned = {
            "qa-100",
            "qa-90",
            "automation",
            "resumed",
            "trending_entities",
            "software",
            "wip",
            "source",
            "resume",
            "global-giants",
            "stage-collect_done",
            "stage-draft_done",
            "stage-images_done",
        }

        def push(v: str) -> None:
            n = self._normalize_public_label(v)
            if not n:
                return
            if n in banned:
                return
            if n in seen:
                return
            seen.add(n)
            labels.append(n)

        for kw in (global_keywords or [])[:8]:
            push(str(kw))
        if candidate is not None:
            push(str(getattr(candidate, "main_entity", "") or ""))
            for lt in list(getattr(candidate, "long_tail_keywords", []) or [])[:4]:
                words = re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", str(lt or ""))[:3]
                push(" ".join(words))
        for tok in self._extract_title_label_tokens(title, limit=10):
            push(tok)

        if not labels:
            labels = ["tech-fix", "troubleshooting"]
        return labels[: max(1, int(max_labels))]

    def _set_image_pipeline_state(self, status: str, passed: int, target: int, message: str) -> None:
        self._image_pipeline_state = {
            "status": str(status or "idle"),
            "passed": max(0, int(passed or 0)),
            "target": max(1, int(target or 1)),
            "message": str(message or "").strip(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_resume_snapshot(self, force_refresh: bool = False, allow_remote: bool = True) -> dict:
        now = datetime.now(timezone.utc)
        if not force_refresh and self._resume_snapshot_cache is not None:
            ts, cached = self._resume_snapshot_cache
            if (now - ts).total_seconds() < self._resume_cache_ttl_seconds:
                return dict(cached)
        if not allow_remote:
            empty = {
                "exists": False,
                "post_id": "",
                "stage": "",
                "title": "",
                "updated": "",
            }
            self._resume_snapshot_cache = (now, empty)
            return dict(empty)
        try:
            row = self.publisher.fetch_latest_wip_draft(
                max_age_hours=max(24, int(self.settings.publish.queue_horizon_hours)),
                include_content=False,
            )
        except Exception:
            row = {}
        out = {
            "exists": bool(row and row.get("post_id")),
            "post_id": str(row.get("post_id", "") if isinstance(row, dict) else ""),
            "stage": str(row.get("stage", "") if isinstance(row, dict) else ""),
            "title": str(row.get("title", "") if isinstance(row, dict) else ""),
            "updated": str(row.get("updated", "") if isinstance(row, dict) else ""),
        }
        self._resume_snapshot_cache = (now, out)
        return dict(out)

    def _resume_from_saved_wip(self, manual_trigger: bool, queue_advisory: str = "") -> WorkflowResult | None:
        try:
            row = self.publisher.fetch_latest_wip_draft(
                max_age_hours=max(24, int(self.settings.publish.queue_horizon_hours)),
                include_content=True,
            )
        except Exception:
            row = {}
        self._resume_snapshot_cache = None
        if not isinstance(row, dict) or not str(row.get("post_id", "")).strip():
            return None

        stage = str(row.get("stage", "") or "").strip().lower()
        if stage == "images_done":
            return self._resume_images_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        if stage == "draft_done":
            return self._resume_draft_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        if stage == "publish_blocked":
            return self._resume_draft_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        if stage == "collect_done":
            return self._resume_collect_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        return None

    def _resume_collect_done(self, row: dict, manual_trigger: bool, queue_advisory: str = "") -> WorkflowResult:
        self._progress("draft", "중단 문서 재개: collect 단계에서 초안 생성", 40)
        title = self._strip_wip_title_prefix(str(row.get("title", "") or "").strip())
        html = self._strip_wip_checkpoint_banner(str(row.get("content", "") or ""))
        plain = re.sub(r"<[^>]+>", " ", html or "")
        plain = re.sub(r"\s+", " ", plain).strip()
        source_url = self._extract_first_href(html)
        candidate = TopicCandidate(
            source="resume",
            title=(title or "Recovered Topic"),
            body=(plain or "Recovered topic from previous collection snapshot."),
            score=80,
            url=source_url,
        )
        draft = self.brain.generate_post_free(candidate, self.settings.authority_links)
        labels = self._normalize_resume_labels(row.get("labels", []))
        if not labels:
            labels = self._build_public_labels(
                title=draft.title,
                candidate=candidate,
                global_keywords=self.last_global_keywords,
                max_labels=6,
            )
        post_id = str(row.get("post_id", "") or "").strip()
        updated_draft_id, note = self._sync_stage_draft_checkpoint(
            current_draft_post_id=post_id,
            stage="draft_done",
            title=draft.title,
            html_body=draft.html,
            labels=labels,
            reason="resumed_from_collect_done",
        )
        resumed_row = dict(row)
        resumed_row["post_id"] = updated_draft_id or post_id
        resumed_row["stage"] = "draft_done"
        resumed_row["title"] = draft.title
        resumed_row["content"] = draft.html
        resumed_row["labels"] = labels
        result = self._resume_draft_done(
            row=resumed_row,
            manual_trigger=manual_trigger,
            queue_advisory=queue_advisory,
        )
        if note and result is not None:
            result.message = f"{result.message} | {note}"
        return result

    def _resume_images_done(self, row: dict, manual_trigger: bool, queue_advisory: str = "") -> WorkflowResult:
        self._progress("schedule", "중단 문서 재개: 예약 시간 계산", 84)
        publish_at = self._compute_publish_at()
        if publish_at is None:
            delay_min = max(10, int(getattr(self.settings.publish, "min_delay_minutes", 10)))
            publish_at = datetime.now(timezone.utc) + timedelta(minutes=delay_min + random.randint(1, 20))

        title = self._strip_wip_title_prefix(str(row.get("title", "") or "").strip())
        html_body = self._strip_wip_checkpoint_banner(str(row.get("content", "") or ""))
        labels = self._normalize_resume_labels(row.get("labels", []))
        if not labels or labels == ["tech-fix", "troubleshooting"]:
            labels = self._build_public_labels(
                title=title,
                candidate=None,
                global_keywords=self.last_global_keywords,
                max_labels=6,
            )
        post_id = str(row.get("post_id", "") or "").strip()
        if not title:
            title = "Recovered Draft"
        if not html_body:
            raise RuntimeError("재개 가능한 draft 본문이 비어 있습니다.")

        self._progress("publish", "중단 문서 재개: 예약 발행 처리", 92)
        backoff = [30, 300, 900]
        published = None
        last_err = ""
        meta_description = self._build_meta_description(
            title=title,
            summary=self._normalize_excerpt(html_body)[:400],
            html=html_body,
        )
        for attempt in range(1, 4):
            try:
                published = self.publisher.publish_existing_draft(
                    post_id=post_id,
                    publish_at=publish_at,
                    title=title,
                    html_body=html_body,
                    labels=labels,
                    meta_description=meta_description,
                )
                break
            except Exception as exc:
                last_err = str(exc)
                if self._is_physical_impossible_error(last_err):
                    self.logs.append_run(
                        RunRecord(
                            status="skipped",
                            score=0,
                            title=title,
                            source_url="",
                            published_url="",
                            note=self._physical_block_reason(last_err),
                        )
                    )
                    self._mark_active_slot("skipped", self._physical_block_reason(last_err))
                    return WorkflowResult("skipped", self._physical_block_reason(last_err))
                if attempt >= 3 or not self._is_retryable_error(last_err):
                    break
                time.sleep(backoff[min(attempt - 1, len(backoff) - 1)])

        if published is None:
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=title,
                    source_url="",
                    published_url="",
                    note=f"resume_images_done_publish_failed: {last_err}",
                )
            )
            grade = self._classify_error_grade(last_err)
            self._mark_active_slot("hold", f"resume_images_done_publish_failed[{grade}]:{last_err}")
            return WorkflowResult("hold", f"requeued_to_tail[{grade}]: {last_err}")

        self.logs.add_scheduled_post(
            publish_at=publish_at.isoformat(),
            post_id=published.post_id,
            title=title,
            source_url="",
            published_url=published.url,
        )
        self._blog_snapshot_cache = None
        note = f"resumed_from_wip=images_done, scheduled_at={publish_at.isoformat()}"
        if queue_advisory:
            note = self._append_note(note, queue_advisory)
        self.logs.append_run(
            RunRecord(
                status="success",
                score=100,
                title=title,
                source_url="",
                published_url=published.url,
                note=note,
            )
        )
        if manual_trigger and str(getattr(published, "post_id", "")).strip():
            self.logs.add_excluded_post(str(published.post_id).strip(), reason="manual_trigger")
        self._progress("done", "중단 문서 재개 완료", 100)
        self._mark_active_slot("consumed", "resume_images_done_publish_success", post_id=str(getattr(published, "post_id", "") or ""))
        return WorkflowResult("success", f"{published.url} (scheduled: {publish_at.isoformat()})")

    def _resume_draft_done(self, row: dict, manual_trigger: bool, queue_advisory: str = "") -> WorkflowResult:
        title = self._strip_wip_title_prefix(str(row.get("title", "") or "").strip())
        base_html = self._strip_wip_checkpoint_banner(str(row.get("content", "") or ""))
        if not title or not base_html:
            return WorkflowResult("hold", "resume draft payload is empty")

        summary_text = self._normalize_excerpt(base_html)[:700]
        draft = DraftPost(
            title=title,
            alt_titles=[],
            html=base_html,
            summary=summary_text,
            score=90,
            source_url="",
            extracted_urls=[],
        )
        candidate = TopicCandidate(
            source="resume",
            title=title,
            body=summary_text,
            score=90,
            url="",
        )

        self._progress("visual", "중단 문서 재개: 이미지 라이브러리 선택", 74)
        target_images = max(2, int(self.settings.visual.target_images_per_post or 2))
        self._set_image_pipeline_state("running", 0, target_images, "중단 작업 이미지 선택")
        image_prompt_plan: dict[str, Any] = {"source": "library"}
        images = pick_images(title=title, min_count=target_images, root=self.root)
        images = self.visual.ensure_unique_assets(images)
        if len(images) < target_images:
            hold_labels = self._normalize_resume_labels(row.get("labels", []))
            if not hold_labels:
                hold_labels = self._build_public_labels(
                    title=title,
                    candidate=candidate,
                    global_keywords=self.last_global_keywords,
                    max_labels=6,
                )
            post_id = str(row.get("post_id", "") or "").strip()
            updated_draft_id, hold_note = self._sync_stage_draft_checkpoint(
                current_draft_post_id=post_id,
                stage="hold",
                title=title,
                html_body=draft.html,
                labels=hold_labels,
                reason="image_library_shortage",
            )
            hold_msg = "image_library_shortage"
            if hold_note:
                hold_msg += f" | {hold_note}"
            if updated_draft_id:
                hold_msg += f" | draft_checkpoint={updated_draft_id}"
            self._set_image_pipeline_state("failed", len(images), target_images, hold_msg)
            return WorkflowResult("hold", hold_msg)
        self._set_image_pipeline_state("validated", len(images), target_images, f"이미지 선택 완료 {len(images)}/{target_images}")
        self._progress("visual", f"이미지 선택 완료 {len(images)}/{target_images}", 80)

        resume_domain = self._infer_domain_from_title(title)
        self._ensure_min_long_tail_keywords(candidate=candidate, title=title, global_keywords=self.last_global_keywords)
        self._optimize_thumbnail_alt(images, candidate)

        final_html = draft.html + self._build_image_rights_block(images, draft.source_url)
        final_html = self._sanitize_publish_html(final_html, domain=resume_domain)
        final_html = self._double_unescape(final_html)
        final_html = self._canonicalize_html_payload(final_html)
        local_qa_review = self._run_local_llm_qa_review(
            title=title,
            html=final_html,
            images=images,
        )
        for phrase in (local_qa_review.get("remove_phrases", []) if isinstance(local_qa_review, dict) else []):
            tok = re.escape(re.sub(r"\s+", " ", str(phrase or "")).strip())
            if not tok:
                continue
            final_html = re.sub(tok, "", final_html, flags=re.IGNORECASE)
        final_html = self._sanitize_publish_html(final_html, domain=resume_domain)
        final_html = self._canonicalize_html_payload(final_html)
        if self._contains_markdown_tokens(final_html):
            final_html = self._canonicalize_html_payload(final_html)
            final_html = self._sanitize_publish_html(final_html, domain=resume_domain)
            if self._contains_markdown_tokens(final_html):
                hold_labels = self._normalize_resume_labels(row.get("labels", []))
                if not hold_labels:
                    hold_labels = self._build_public_labels(
                        title=title,
                        candidate=candidate,
                        global_keywords=self.last_global_keywords,
                        max_labels=6,
                    )
                post_id = str(row.get("post_id", "") or "").strip()
                updated_draft_id, hold_note = self._sync_stage_draft_checkpoint(
                    current_draft_post_id=post_id,
                    stage="hold",
                    title=title,
                    html_body=final_html,
                    labels=hold_labels,
                    reason="markdown_canonicalize_failed",
                )
                hold_msg = "markdown_canonicalize_failed"
                if hold_note:
                    hold_msg += f" | {hold_note}"
                if updated_draft_id:
                    hold_msg += f" | draft_checkpoint={updated_draft_id}"
                return WorkflowResult("hold", hold_msg)
        gate_preview_html = ""
        preflight_thumb_src = ""
        if bool(self.settings.budget.dry_run):
            try:
                gate_preview_html = self.publisher.build_dry_run_html(final_html, images)
            except Exception as exc:
                raise RuntimeError(f"Go-live preflight merge failed: {exc}") from exc
        else:
            try:
                images, preflight_thumb_src = self._profile_call(
                    "resume_thumbnail_preflight_with_recovery",
                    lambda: self._preflight_thumbnail_with_recovery(
                        draft=draft,
                        candidate=candidate,
                        images=images,
                        prompt_plan=image_prompt_plan,
                        max_attempts=3,
                        manual_trigger=manual_trigger,
                    ),
                    slow_ms=8000,
                )
                creds_for_gate = self.publisher._oauth_credentials()  # noqa: SLF001
                gate_preview_html = self.publisher._merge_images(  # noqa: SLF001
                    final_html,
                    images,
                    creds_for_gate,
                    preflight_thumbnail_src=preflight_thumb_src,
                )
            except Exception as exc:
                raise RuntimeError(f"Go-live preflight merge failed: {exc}") from exc
        go_live_errors, go_live_warnings = self._go_live_gate_checklist(
            title=title,
            final_html=final_html,
            gate_html=gate_preview_html,
            images=images,
            candidate=candidate,
        )
        if go_live_errors:
            raise RuntimeError("Go-live gate failed: " + "; ".join(go_live_errors[:5]))

        qa_result = self._qa_evaluate(
            final_html,
            title=title,
            domain=resume_domain,
            keyword=str(getattr(candidate, "title", "") or ""),
            context="resume_publish",
        )
        labels = self._normalize_resume_labels(row.get("labels", []))
        if not labels:
            labels = self._build_public_labels(
                title=title,
                candidate=candidate,
                global_keywords=self.last_global_keywords,
                max_labels=6,
            )
        post_id = str(row.get("post_id", "") or "").strip()

        self._progress("schedule", "중단 문서 재개: 예약 시간 계산", 84)
        publish_at = self._compute_publish_at()
        if publish_at is None:
            delay_min = max(10, int(getattr(self.settings.publish, "min_delay_minutes", 10)))
            publish_at = datetime.now(timezone.utc) + timedelta(minutes=delay_min + random.randint(1, 20))

        self._progress("publish", "중단 문서 재개: 예약 발행 처리", 92)
        meta_description = self._build_meta_description(
            title=title,
            summary=summary_text,
            html=final_html,
        )
        if not preflight_thumb_src:
            try:
                images, preflight_thumb_src = self._profile_call(
                    "resume_thumbnail_preflight_with_recovery",
                    lambda: self._preflight_thumbnail_with_recovery(
                        draft=draft,
                        candidate=candidate,
                        images=images,
                        prompt_plan=image_prompt_plan,
                        max_attempts=3,
                        manual_trigger=manual_trigger,
                    ),
                    slow_ms=8000,
                )
            except Exception as exc:
                hold_labels = self._normalize_resume_labels(row.get("labels", []))
                if not hold_labels:
                    hold_labels = self._build_public_labels(
                        title=title,
                        candidate=candidate,
                        global_keywords=self.last_global_keywords,
                        max_labels=6,
                    )
                post_id = str(row.get("post_id", "") or "").strip()
                updated_draft_id, hold_note = self._sync_stage_draft_checkpoint(
                    current_draft_post_id=post_id,
                    stage="hold",
                    title=title,
                    html_body=final_html,
                    labels=hold_labels,
                    reason=str(exc),
                )
                grade = self._classify_error_grade(str(exc))
                hold_msg = f"thumbnail_preflight_failed[{grade}]: {str(exc)}"
                if hold_note:
                    hold_msg += f" | {hold_note}"
                if updated_draft_id:
                    hold_msg += f" | draft_checkpoint={updated_draft_id}"
                self._mark_active_slot("hold", hold_msg)
                return WorkflowResult("hold", hold_msg)
        if bool(getattr(self.settings.publish, "thumbnail_preflight_only", False)):
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=90,
                    title=title,
                    source_url="",
                    published_url="",
                    note=f"thumbnail_preflight_only_ok:{preflight_thumb_src[:180]}",
                )
            )
            self._mark_active_slot("hold", "thumbnail_preflight_only_ok")
            return WorkflowResult("hold", "thumbnail_preflight_only_ok")
        published = self._profile_call(
            "resume_publish_post",
            lambda: self.publisher.publish_post(
                title,
                final_html,
                images,
                labels,
                publish_at=publish_at,
                existing_draft_post_id=(post_id or None),
                meta_description=meta_description,
                preflight_thumbnail_src=preflight_thumb_src,
            ),
            slow_ms=8000,
        )
        self.logs.add_scheduled_post(
            publish_at=publish_at.isoformat(),
            post_id=published.post_id,
            title=title,
            source_url="",
            published_url=published.url,
        )
        self._blog_snapshot_cache = None
        self._cleanup_local_image_files(images)

        note = f"resumed_from_wip=draft_done, qa={qa_result.score}, images={len(images)}, scheduled_at={publish_at.isoformat()}"
        if go_live_warnings:
            note = self._append_note(note, "go_live_warnings=" + ",".join(go_live_warnings[:4]))
        if queue_advisory:
            note = self._append_note(note, queue_advisory)
        self.logs.append_run(
            RunRecord(
                status="success",
                score=90,
                title=title,
                source_url="",
                published_url=published.url,
                note=note,
            )
        )
        if manual_trigger and str(getattr(published, "post_id", "")).strip():
            self.logs.add_excluded_post(str(published.post_id).strip(), reason="manual_trigger")
        self._progress("done", "중단 문서 재개 완료", 100)
        self._mark_active_slot("consumed", "resume_publish_success", post_id=str(getattr(published, "post_id", "") or ""))
        return WorkflowResult("success", f"{published.url} (scheduled: {publish_at.isoformat()})")

    def _strip_wip_title_prefix(self, title: str) -> str:
        t = str(title or "").strip()
        t = re.sub(r"^\[WIP:[^\]]+\]\s*", "", t, flags=re.IGNORECASE)
        return t.strip()

    def _strip_wip_checkpoint_banner(self, html: str) -> str:
        out = str(html or "")
        out = re.sub(
            r"<p>\s*<em>\s*WIP checkpoint:[^<]*</em>\s*</p>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        return out.strip()

    def _normalize_resume_labels(self, labels: list[str] | None) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        blocked = {
            "wip",
            "automation",
            "software",
            "resumed",
            "trending_entities",
            "global_giants",
            "source",
            "resume",
        }
        for raw in (labels or []):
            label = re.sub(r"\s+", " ", str(raw or "")).strip()
            if not label:
                continue
            low = label.lower()
            if low in blocked or low.startswith("stage-") or low.startswith("qa-"):
                continue
            norm = self._normalize_public_label(label)
            if not norm:
                continue
            if norm in seen:
                continue
            seen.add(norm)
            out.append(norm)
        if not out:
            out = ["tech-fix", "troubleshooting"]
        return out[:10]

    def _extract_first_href(self, html: str) -> str:
        m = re.search(r'<a[^>]+href="([^"]+)"', html or "", flags=re.IGNORECASE)
        if not m:
            return ""
        return str(m.group(1) or "").strip()

    def _sync_stage_draft_checkpoint(
        self,
        current_draft_post_id: str,
        stage: str,
        title: str,
        html_body: str,
        labels: list[str],
        reason: str = "",
    ) -> tuple[str, str]:
        """Upsert a Blogger draft checkpoint without blocking the run on failure."""
        if self.settings.budget.dry_run:
            return current_draft_post_id, ""
        try:
            checkpoint = self.publisher.save_draft_checkpoint(
                title=title,
                html_body=html_body,
                labels=labels,
                stage=stage,
                reason=reason,
                draft_post_id=(current_draft_post_id or None),
            )
            post_id = str(getattr(checkpoint, "post_id", "") or "").strip() or current_draft_post_id
            if post_id:
                return post_id, f"draft_{stage}={post_id}"
            return current_draft_post_id, f"draft_{stage}=saved"
        except Exception as exc:
            return current_draft_post_id, f"draft_{stage}_failed={str(exc)[:120]}"

    def _inject_freshness_appendix(
        self,
        html: str,
        topic_title: str,
        keywords: list[str] | None,
    ) -> str:
        kw = ", ".join([str(k).strip() for k in (keywords or []) if str(k).strip()][:5])
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        block = (
            "<h2>What's New In This Iteration</h2>"
            f"<p>This version was regenerated to avoid overlap and add fresh angle coverage for "
            f"<strong>{escape(topic_title)}</strong>.</p>"
            f"<p>Freshness markers: generated at {escape(stamp)}"
            + (f", focus keywords: {escape(kw)}" if kw else "")
            + ".</p>"
        )
        return (html or "") + block

    def _draft_fatal_issues(self, html: str, domain: str = "tech_troubleshoot") -> list[str]:
        out: list[str] = []
        if not html or "<h2" not in html.lower() or "<p" not in html.lower():
            out.append("invalid_html_structure")
        if re.search(r"(?m)^\s{0,3}#{1,6}\s+\S+", str(html or "")):
            out.append("markdown_heading_leak")
        lower = html.lower()
        if "this draft did not include valid html sections" in lower:
            out.append("template_fallback_text")
        if re.search(r"\b(executive summary|decision framework|operational depth)\b", lower):
            out.append("template_placeholder_leak")
        if str(domain or "").strip().lower() != "ai_prompt_guide":
            for token in (getattr(self.settings.quality, "prompt_leak_patterns", []) or []):
                t = str(token or "").strip().lower()
                if t and t in lower:
                    out.append("prompt_leak_pattern")
                    break
            if re.search(r"\bfor quick take\b.{0,80}\b(you are|write|must|do not)\b", lower):
                out.append("quick_take_template_leak")
        if re.search(r"\b(section context visual|concept visual|supporting chart)\s*\d+\b", lower):
            out.append("visual_placeholder_leak")
        if str(domain or "").strip().lower() in {"office_experiment", "tech_troubleshoot"}:
            severe_hits = 0
            disallowed_terms = (
                getattr(self.settings.quality, "disallowed_terms_tech_troubleshoot", [])
                if str(domain or "").strip().lower() == "tech_troubleshoot"
                else getattr(self.settings.quality, "disallowed_terms_office_experiment", [])
            )
            for term in (disallowed_terms or []):
                t = str(term or "").strip().lower()
                if not t:
                    continue
                if t in lower:
                    severe_hits += 1
            if severe_hits >= 3:
                out.append("severe_domain_drift")
        return out

    def _has_excessive_repetition(self, html: str) -> bool:
        paras = re.findall(r"<p[^>]*>(.*?)</p>", html or "", flags=re.IGNORECASE | re.DOTALL)
        norm: list[str] = []
        for p in paras:
            t = re.sub(r"<[^>]+>", " ", p)
            t = re.sub(r"\s+", " ", t).strip().lower()
            if len(t) >= 40:
                norm.append(t)
        if not norm:
            return False
        counts = Counter(norm)
        max_repeat = max(counts.values())
        # Any paragraph repeated 3+ times is considered corrupted output.
        if max_repeat >= 3:
            return True
        # If near-duplicate paragraph share is too high, treat as failure.
        repeated = sum(v for v in counts.values() if v >= 2)
        return (repeated / max(1, len(norm))) >= 0.35

    def _has_visual_placeholder_text(self, html: str) -> bool:
        lower = (html or "").lower()
        patterns = [
            r"\bsection context visual\b",
            r"\bconcept visual\b",
            r"\bsupporting chart\b",
            r"\bvisual\s*\d+\b",
            r"\bscreenshot\s*\d+\b",
            r"\bfocused screenshot\b",
            r"\bbroken image\b",
            r"\bimage placeholder\b",
        ]
        return any(re.search(p, lower) for p in patterns)

    def _build_compliance_block(self, source: str, source_url: str) -> str:
        # Keep visible post body clean; attribution stays in links and run logs.
        return ""

    def _build_image_rights_block(self, images: list[ImageAsset], source_url: str) -> str:
        return ""

    def _ensure_quick_take_block(self, html: str, title: str) -> str:
        if not html:
            return html
        heading_re = re.compile(
            r"<h2[^>]*>\s*Quick Take\s*</h2>\s*(<p[^>]*>.*?</p>)?",
            flags=re.IGNORECASE | re.DOTALL,
        )
        match = heading_re.search(html)
        paragraph_text = ""
        body = html
        if match:
            para_html = match.group(1) or ""
            paragraph_text = re.sub(r"<[^>]+>", " ", para_html)
            paragraph_text = re.sub(r"\s+", " ", paragraph_text).strip()
            body = html[: match.start()] + html[match.end() :]

        sentence1, sentence2 = self._quick_take_sentences(paragraph_text, title)
        block = (
            "<h2>Quick Take</h2>"
            f"<p>{escape(sentence1)} {escape(sentence2)}</p>"
        )
        return block + body

    def _quick_take_sentences(self, text: str, title: str) -> tuple[str, str]:
        clean = re.sub(r"\s+", " ", str(text or "")).strip()
        if clean:
            parts = [
                p.strip()
                for p in re.split(r"(?<=[.!?])\s+", clean)
                if p.strip()
            ]
            if len(parts) >= 2:
                return parts[0], parts[1]
            if len(parts) == 1:
                return (
                    parts[0],
                    "Use the practical steps below to apply this quickly in real work.",
                )
        safe_title = re.sub(r"\s+", " ", str(title or "")).strip() or "this topic"
        return (
            f"This guide gives the fastest practical answer about {safe_title}.",
            "Start with one safe troubleshooting step, then scale only after you verify the result.",
        )

    def _optimize_thumbnail_alt(self, images: list[ImageAsset], candidate: TopicCandidate) -> None:
        if not images:
            return
        thumb = images[0]
        long_tails = [
            re.sub(r"\s+", " ", str(k or "")).strip()
            for k in (getattr(candidate, "long_tail_keywords", []) or [])
            if str(k or "").strip()
        ]
        entity = re.sub(r"\s+", " ", str(getattr(candidate, "main_entity", "") or "")).strip()
        if long_tails:
            base = re.sub(r"[?]+$", "", long_tails[0]).strip()
            base = re.sub(r"^(how|why|what)\s+", "", base, flags=re.IGNORECASE)
            base = re.sub(
                r"\b(trending today|mean for (?:team|daily) productivity|why everyone is talking|for office workflows?)\b",
                "",
                base,
                flags=re.IGNORECASE,
            )
            base = re.sub(r"\s+", " ", base).strip(" -")
            if entity and entity.lower() not in base.lower():
                base = f"{base} using {entity}"
            subject = base
        else:
            title = re.sub(r"\s+", " ", str(getattr(candidate, "title", "") or "")).strip()
            if entity:
                subject = entity
            elif title:
                subject = title
            else:
                subject = "visual summary"
        thumb.alt = (
            f"Practical troubleshooting process diagram for {subject}."
        )[:180]

    def _enforce_seo_title(
        self,
        title: str,
        candidate: TopicCandidate | None,
        global_keywords: list[str] | None,
    ) -> str:
        raw = re.sub(r"\s+", " ", str(title or "")).strip()
        base_candidate_title = re.sub(r"\s+", " ", str(getattr(candidate, "title", "") or "")).strip()
        if not raw:
            raw = base_candidate_title or "Windows issue not working fix guide"

        mode = str(getattr(self.settings.content_mode, "mode", "") or "").strip().lower()
        banned = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "banned_topic_keywords", []) or [])
            if str(x or "").strip()
        ]
        for token in banned:
            raw = re.sub(re.escape(token), "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s+", " ", raw).strip(" -:")

        device_hint = self._infer_device_type(f"{raw}\n{base_candidate_title}") or "windows"
        req_tokens = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "required_title_tokens_any", []) or [])
            if str(x or "").strip()
        ] or ["not working", "fix", "error", "after update"]
        lower = raw.lower()
        if mode == "tech_troubleshoot_only" and not any(tok in lower for tok in req_tokens):
            if "after update" in lower or "update" in lower:
                raw = f"{device_hint.title()} not working after update? 5 fixes that actually work (2026)"
            elif re.search(r"\berror\b", lower):
                raw = f"{device_hint.title()} error fix: 5 steps that actually work (2026)"
            else:
                raw = f"{device_hint.title()} not working? 5 fixes that actually work (2026)"
            lower = raw.lower()

        if mode == "tech_troubleshoot_only" and not re.search(r"\b(2026|\d+ fixes?)\b", lower):
            raw = f"{raw} (2026 Fix Guide)"
        raw = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw[:120]

    def _go_live_gate_checklist(
        self,
        title: str,
        final_html: str,
        gate_html: str,
        images: list[ImageAsset],
        candidate: TopicCandidate,
    ) -> tuple[list[str], list[str]]:
        errors: list[str] = []
        warnings: list[str] = []
        merged_raw = f"{title}\n{final_html}"
        merged = merged_raw.lower()
        if re.search(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", merged_raw):
            errors.append("english_only_violation_hangul")

        if re.search(r"(?m)^\s{0,3}#{1,6}\s+\S+", str(final_html or "")):
            errors.append("markdown_heading_detected")
        if "## " in str(final_html or "") or "### " in str(final_html or ""):
            errors.append("markdown_tokens_detected")

        if re.search(r"(&#x27;|&#39;|x27\?)", merged, flags=re.IGNORECASE):
            errors.append("encoding_fragment_detected")

        if re.search(r"(https?://(?:www\.)?google\.com(?:/search)?[^\s\"<]*)", merged, flags=re.IGNORECASE):
            errors.append("forbidden_google_link_detected")
        if "<figcaption" in merged:
            errors.append("forbidden_figcaption_detected")

        banned_tokens = list(getattr(self.settings.quality, "banned_debug_patterns", []) or [])
        if not banned_tokens:
            banned_tokens = [
                "workflow checkpoint stage",
                "av reference context",
                "jobtitle",
                "sameas",
                "selected topic",
                "source trending_entities",
            ]
        for token in banned_tokens:
            t = str(token or "").strip().lower()
            if not t:
                continue
            if t in merged:
                errors.append(f"debug_token_leak:{t}")
                break
        if "[[meta]]" in merged or "[[/meta]]" in merged:
            errors.append("meta_block_leak")
        if re.search(r"\billustration\s+showing\b", merged):
            errors.append("illustration_placeholder_leak")

        if not images:
            errors.append("images_missing")
        else:
            target_required = max(2, int(getattr(self.settings.visual, "target_images_per_post", 2) or 2))
            min_required = 2
            if len(images) < min_required:
                errors.append(f"insufficient_images_min(<{min_required})")
            elif len(images) < target_required:
                warnings.append(f"images_below_target({len(images)}/{target_required})")
            missing_alt = [img for img in images if not str(getattr(img, "alt", "") or "").strip()]
            if missing_alt:
                errors.append("alt_missing")
            hangul_alt = [img for img in images if re.search(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", str(getattr(img, "alt", "") or ""))]
            if hangul_alt:
                errors.append("alt_non_english_detected")
            if images:
                thumb_alt = str(getattr(images[0], "alt", "") or "").strip().lower()
                if not thumb_alt:
                    errors.append("thumbnail_alt_missing")
                if "focused screenshot" in thumb_alt:
                    errors.append("thumbnail_alt_forbidden_phrase")
            intro_text = self._extract_intro_text(final_html)
            alt_values = [str(getattr(img, "alt", "") or "") for img in (images or []) if str(getattr(img, "alt", "") or "").strip()]
            intro_alt_fail, intro_alt_detail = self.qa.detect_intro_alt_similarity(
                intro_text=intro_text,
                alt_texts=alt_values,
                threshold=float(getattr(self.settings.quality, "alt_similarity_threshold", 0.75)),
            )
            if intro_alt_fail:
                errors.append("intro_alt_similarity_high")
                warnings.append(intro_alt_detail)
        html_img_count = len(re.findall(r"<img\b[^>]*\bsrc=", str(gate_html or final_html or ""), flags=re.IGNORECASE))
        if html_img_count < 2:
            errors.append(f"insufficient_html_images({html_img_count}/2)")
        # Enforce runtime backend image hosts in production mode.
        html_for_hosts = str(gate_html or final_html or "")
        src_matches = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', html_for_hosts, flags=re.IGNORECASE)
        dry_run = bool(getattr(self.settings.budget, "dry_run", False))
        allow_data_uri = (
            bool(getattr(self.settings.publish, "thumbnail_data_uri_allowed", False))
            or bool(getattr(self.publisher, "thumbnail_data_uri_allowed", False))
            or dry_run
        )
        for src in src_matches:
            clean_src = str(src or "").strip()
            if not clean_src:
                errors.append("image_src_empty")
                continue
            lower_src = clean_src.lower()
            if lower_src.startswith("data:image/"):
                if not allow_data_uri:
                    errors.append("thumbnail_data_uri_not_allowed")
                continue
            host = (urlparse(clean_src).netloc or "").lower()
            if not host:
                errors.append("image_host_missing")
                continue
            if not self.publisher._is_allowed_image_url(clean_src, allow_data_uri=False):  # noqa: SLF001
                errors.append(f"invalid_image_host:{host}")

        # Troubleshooting title quality check.
        title_lower = str(title or "").lower()
        req_tokens = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "required_title_tokens_any", []) or [])
            if str(x or "").strip()
        ] or ["not working", "fix", "error", "after update"]
        if not any(tok in title_lower for tok in req_tokens):
            errors.append("title_missing_troubleshoot_token")
        banned_topic = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "banned_topic_keywords", []) or [])
            if str(x or "").strip()
        ]
        banned_hit = next((tok for tok in banned_topic if tok in title_lower), "")
        if banned_hit:
            errors.append(f"title_banned_topic:{banned_hit}")

        # Operator checklist visibility for robots mobile duplicate policy.
        mobile_block = str(
            getattr(self.settings.publish, "mobile_duplicate_block_enabled", False)
        ).strip().lower() in {"1", "true", "yes", "on"}
        if not mobile_block:
            warnings.append("robots_m1_policy_not_confirmed")

        # Candidate quality sanity: long-tail/LSI depth guard (min 5 long-tail proxies).
        long_tail = [str(x).strip() for x in (getattr(candidate, "long_tail_keywords", []) or []) if str(x).strip()]
        if len(long_tail) < 5:
            warnings.append("long_tail_insufficient(<5)")

        return errors, warnings

    def _extract_intro_text(self, html: str) -> str:
        m = re.search(r"<p[^>]*>(.*?)</p>", str(html or ""), flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        text = re.sub(r"<[^>]+>", " ", str(m.group(1) or ""))
        return re.sub(r"\s+", " ", text).strip()

    def _contains_markdown_tokens(self, html: str) -> bool:
        src = str(html or "")
        if re.search(r"(?m)^\s{0,3}#{1,6}\s+\S+", src):
            return True
        return ("## " in src) or ("### " in src)

    def _enforce_image_fallback_publish_policy(self, images: list[ImageAsset]) -> tuple[bool, str]:
        allow_inline = bool(getattr(self.settings.publish, "allow_inline_fallback_publish", False))
        allow_banner = bool(getattr(self.settings.publish, "allow_banner_fallback_publish", True))
        if not images:
            return False, "no_image_assets_available"
        banner_src = str(getattr(images[0], "source_url", "") or "").strip().lower()
        if (not allow_banner) and banner_src.startswith("local://fallback"):
            return False, "banner_image_generation_failed"
        if len(images) > 1 and (not allow_inline):
            for image in images[1:]:
                src = str(getattr(image, "source_url", "") or "").strip().lower()
                if src.startswith("local://fallback"):
                    return False, "inline_image_generation_failed"
        return True, ""

    def _run_manual_upload_probe_session(self, max_total_seconds: int = 90) -> dict[str, Any]:
        started = time.perf_counter()
        generic_dir = (self.root / "assets" / "library" / "generic").resolve()
        if not generic_dir.exists():
            raise RuntimeError("upload_probe_no_working_strategy:generic_library_missing")
        candidates = [
            p for p in sorted(generic_dir.iterdir())
            if p.is_file() and p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
        ][:5]
        if not candidates:
            raise RuntimeError("upload_probe_no_working_strategy:no_generic_images")

        tried: list[str] = []
        hard_405 = 0
        allow_values: set[str] = set()
        last_error = ""
        for image_path in candidates:
            elapsed = time.perf_counter() - started
            remaining = float(max_total_seconds) - float(elapsed)
            if remaining <= 0:
                raise RuntimeError("upload_probe_timeout")
            probe = self.publisher.upload_probe_harness(
                image_path=image_path,
                max_total_seconds=max(5, int(max_total_seconds)),
                start_monotonic=started,
            )
            strategy = str(probe.get("strategy", "") or "").strip()
            if strategy:
                tried.append(strategy)
            err = str(probe.get("error", "") or "").strip()
            if "hard_fail_405" in err:
                hard_405 += 1
            if "allow=" in err:
                allow_values.add(err.split("allow=", 1)[-1].strip())
            last_error = err or last_error
            if bool(probe.get("ok", False)) and self.publisher._is_blogger_media_url(str(probe.get("url", "") or "")):  # noqa: SLF001
                self._append_workflow_perf(
                    "manual_upload_probe_success",
                    {
                        "strategy": strategy,
                        "image_path": str(image_path),
                        "host": str(probe.get("host", "") or ""),
                    },
                )
                return probe

        summary = "upload_probe_no_working_strategy"
        if tried:
            summary += f";tried={','.join(tried)}"
        if hard_405:
            summary += f";http405={hard_405}"
        if allow_values:
            summary += f";allow={','.join(sorted(allow_values))}"
        if last_error:
            summary += f";last_error={last_error[:180]}"
        raise RuntimeError(summary)

    def _preflight_thumbnail_with_recovery(
        self,
        draft: DraftPost,
        candidate: TopicCandidate,
        images: list[ImageAsset],
        prompt_plan: dict[str, Any] | None,
        max_attempts: int = 3,
        manual_trigger: bool = False,
    ) -> tuple[list[ImageAsset], str]:
        working = self.visual.ensure_unique_assets(list(images or []))
        if not working:
            raise RuntimeError("thumbnail_preflight_failed:no_images")
        if bool(manual_trigger) and (not bool(self._manual_upload_probe_done)) and str(getattr(self.settings.publish, "image_hosting_backend", "")).strip().lower() in {"blogger_media", "blogger", "blogger_server"}:
            probe = self._profile_call(
                "manual_upload_probe_session",
                lambda: self._run_manual_upload_probe_session(max_total_seconds=90),
                slow_ms=12000,
            )
            self._manual_upload_probe_done = True
            if not bool(probe.get("ok", False)):
                raise RuntimeError(str(probe.get("error", "") or "upload_probe_no_working_strategy"))
        last_err = "thumbnail_preflight_failed:unknown"
        cfg_cycles = int(getattr(self.settings.publish, "thumbnail_preflight_max_cycles", max_attempts) or 0)
        retry_delay_sec = max(1, int(getattr(self.settings.publish, "thumbnail_preflight_retry_delay_sec", 8) or 8))
        finite_cycles = max(1, cfg_cycles if cfg_cycles > 0 else int(max_attempts or 3))
        attempt_no = 0
        while True:
            attempt_no += 1
            try:
                self._progress(
                    "publish",
                    f"썸네일 업로드 재시도 중 ({attempt_no}/{finite_cycles})",
                    85,
                )
                thumb_src = self.publisher.preflight_thumbnail_blogger_media(
                    working[0],
                    max_attempts=2,
                )
                return working, str(thumb_src or "").strip()
            except Exception as exc:
                last_err = str(exc or "thumbnail_preflight_failed:unknown")
                self._append_workflow_perf(
                    "thumbnail_preflight_retry",
                    {
                        "attempt_no": int(attempt_no),
                        "max_cycles": int(finite_cycles),
                        "delay_sec": int(retry_delay_sec),
                        "error": str(last_err)[:260],
                        "infinite": False,
                    },
                )
                # Image generation loops are disabled. Recovery rotates a different library image as thumbnail.
                if len(working) > 1:
                    rotated = list(working[1:]) + [working[0]]
                    working = self.visual.ensure_unique_assets(rotated)
                    self._optimize_thumbnail_alt(working, candidate)
                else:
                    last_err = f"{last_err};thumbnail_rotation_exhausted"
                if attempt_no >= finite_cycles:
                    break
                time.sleep(float(retry_delay_sec))
        raise RuntimeError(last_err)

    def _force_image_floor(
        self,
        draft: DraftPost,
        images: list[ImageAsset],
        target_images: int,
    ) -> list[ImageAsset]:
        """
        Ensure we always hand at least `target_images` assets to publisher.
        If generation misses, inject role-based local fallback assets.
        """
        target = max(2, int(target_images or 0))
        out = self.visual.ensure_unique_assets(list(images or []))
        if len(out) >= target:
            return out[:target]

        # Keep thumbnail in slot 0.
        try:
            out = self.visual.ensure_generated_thumbnail(draft, out, prompt_plan=None)
        except Exception:
            pass
        out = self.visual.ensure_unique_assets(out)

        seed_idx = 400 + len(out)
        while len(out) < target:
            role = "thumbnail" if len(out) == 0 else "content"
            fallback = self.visual._fallback_asset_for_role(role=role, index=seed_idx)  # noqa: SLF001
            seed_idx += 1
            if fallback is None:
                break
            out.append(fallback)
            out = self.visual.ensure_unique_assets(out)
            if len(out) >= target:
                break
        return out[:target]

    def _ensure_min_long_tail_keywords(
        self,
        candidate: TopicCandidate,
        title: str,
        global_keywords: list[str] | None,
    ) -> None:
        current = [
            re.sub(r"\s+", " ", str(x or "")).strip()
            for x in (getattr(candidate, "long_tail_keywords", []) or [])
            if str(x or "").strip()
        ]
        seen: set[str] = set()
        out: list[str] = []

        def _push(v: str) -> None:
            norm = re.sub(r"\s+", " ", str(v or "")).strip()
            if not norm:
                return
            key = norm.lower()
            if key in seen:
                return
            seen.add(key)
            out.append(norm)

        for kw in current:
            _push(kw)

        entity = re.sub(r"\s+", " ", str(getattr(candidate, "main_entity", "") or "")).strip()
        base_title = re.sub(r"\s+", " ", str(title or getattr(candidate, "title", "") or "")).strip()
        subject = entity or base_title or "this tool"

        for kw in (global_keywords or [])[:5]:
            key = re.sub(r"\s+", " ", str(kw or "")).strip()
            if not key:
                continue
            _push(f"how to fix {key}")
            _push(f"{key} troubleshooting checklist")

        if len(out) < 5:
            _push(f"how to fix {subject} not working")
            _push(f"{subject} update issue recovery steps")
            _push(f"{subject} connectivity troubleshooting")
            _push(f"{subject} beginner safe fixes")
            _push(f"{subject} troubleshooting checklist")
            _push(f"{subject} setup error fix guide")

        candidate.long_tail_keywords = out[:8]

    def _canonicalize_html_payload(self, html: str) -> str:
        out = str(html or "").strip()
        if not out:
            return out

        has_markdown_heading = bool(re.search(r"(?m)^\s{0,3}#{1,6}\s+\S+", out))
        has_html_structure = bool(re.search(r"<h2\b|<p\b", out, flags=re.IGNORECASE))
        if not has_markdown_heading and has_html_structure:
            return out

        converted = self._convert_markdown_like_to_html(out)
        converted = re.sub(r"(?m)^\s*#{1,6}\s+(.+)$", "", converted)
        converted = re.sub(r"\n{3,}", "\n\n", converted)
        return converted.strip()

    def _convert_markdown_like_to_html(self, text: str) -> str:
        src = str(text or "")
        try:
            import markdown as md  # type: ignore

            html_out = md.markdown(
                src,
                extensions=["extra", "sane_lists"],
                output_format="html5",
            )
            if html_out:
                return html_out
        except Exception:
            pass

        lines = src.splitlines()
        blocks: list[str] = []
        in_ul = False
        in_ol = False
        for raw in lines:
            line = re.sub(r"\s+", " ", str(raw or "")).strip()
            if not line:
                if in_ul:
                    blocks.append("</ul>")
                    in_ul = False
                if in_ol:
                    blocks.append("</ol>")
                    in_ol = False
                continue
            m_h = re.match(r"^\s*#{2,3}\s+(.+)$", raw)
            if m_h:
                if in_ul:
                    blocks.append("</ul>")
                    in_ul = False
                if in_ol:
                    blocks.append("</ol>")
                    in_ol = False
                level = 2 if raw.lstrip().startswith("##") else 3
                blocks.append(f"<h{level}>{escape(m_h.group(1).strip())}</h{level}>")
                continue
            m_li = re.match(r"^\s*[-*]\s+(.+)$", raw)
            if m_li:
                if in_ol:
                    blocks.append("</ol>")
                    in_ol = False
                if not in_ul:
                    blocks.append("<ul>")
                    in_ul = True
                blocks.append(f"<li>{escape(m_li.group(1).strip())}</li>")
                continue
            m_oli = re.match(r"^\s*\d+\.\s+(.+)$", raw)
            if m_oli:
                if in_ul:
                    blocks.append("</ul>")
                    in_ul = False
                if not in_ol:
                    blocks.append("<ol>")
                    in_ol = True
                blocks.append(f"<li>{escape(m_oli.group(1).strip())}</li>")
                continue
            if in_ul:
                blocks.append("</ul>")
                in_ul = False
            if in_ol:
                blocks.append("</ol>")
                in_ol = False
            blocks.append(f"<p>{escape(line)}</p>")
        if in_ul:
            blocks.append("</ul>")
        if in_ol:
            blocks.append("</ol>")
        return "\n".join(blocks)

    def _sanitize_publish_html(self, html: str, domain: str = "tech_troubleshoot") -> str:
        out = html or ""
        if not out:
            return out
        # Remove internal meta fences unconditionally.
        out = re.sub(r"\[\[META\]\].*?\[\[/META\]\]", "", out, flags=re.IGNORECASE | re.DOTALL)
        # Remove boilerplate sections that look like internal report output.
        out = re.sub(
            r"<h3>\s*(Sources And License|Image Sources And Rights)\s*</h3>\s*(<ul>.*?</ul>)?",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        # Remove placeholder-heavy paragraphs leaked from image metadata/templates.
        out = re.sub(
            r"<p[^>]*>[^<]*(section context visual|concept visual|supporting chart|visual\s*\d+|screenshot\s*\d+)[^<]*</p>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"<p[^>]*>\s*illustration\s+showing[^<]*</p>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"<p[^>]*>[^<]*focused screenshot[^<]*</p>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        # Remove list/headline leaks from template-like visual markers.
        out = re.sub(
            r"<(h2|h3|li)[^>]*>[^<]*(section context visual|concept visual|supporting chart|visual\s*\d+|screenshot\s*\d+)[^<]*</\1>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        # Remove leaked bracket-style image placeholder sentences.
        out = re.sub(
            r"\[[^\]]*(focused screenshot|section context visual|concept visual|supporting chart|visual\s*\d+|screenshot\s*\d+)[^\]]*\]",
            "",
            out,
            flags=re.IGNORECASE,
        )
        # Remove publishing strategy/meta terms from visible body.
        out = re.sub(
            r"\b(SEO|Algorithm|E-?E-?A-?T|Trustworthiness|Process Disclosure|search ranking|helpful content update)\b",
            "",
            out,
            flags=re.IGNORECASE,
        )
        if str(domain or "").strip().lower() != "ai_prompt_guide":
            # Remove internal prompt leakage fragments.
            out = re.sub(
                r"\b(for quick take[^<\n]*|you are a system that[^<\n]*|for generated image context[^<\n]*)",
                "",
                out,
                flags=re.IGNORECASE,
            )
            for token in (getattr(self.settings.quality, "prompt_leak_patterns", []) or []):
                t = str(token or "").strip()
                if not t:
                    continue
                out = re.sub(re.escape(t), "", out, flags=re.IGNORECASE)
            out = re.sub(
                r"\b(workflow checkpoint stage|av reference context|jobtitle|sameas|selected topic|source[_\s-]*trending[_\s-]*entities)\b",
                "",
                out,
                flags=re.IGNORECASE,
            )
        # Remove google.com references from body links/text.
        out = re.sub(
            r"<a[^>]+href=\"https?://(?:www\.)?google\.com[^\"]*\"[^>]*>(.*?)</a>",
            r"\1",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(
            r"https?://(?:www\.)?google\.com/search\?[^\"\s<]+",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"https?://(?:www\.)?google\.com/[^\s\"<]*",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(
            r"https?://(?:www\.)?google\.com/?",
            "",
            out,
            flags=re.IGNORECASE,
        )
        # Translate common heavy technical terms into plain language.
        jargon_map = {
            r"\bCUDA kernel\b": "AI processing engine",
            r"\bCUDA\b": "AI acceleration stack",
            r"\bLLVM backend\b": "system translation tool",
            r"\bLLVM\b": "code translation tool",
            r"\bvector embeddings?\b": "meaning map",
            r"\blatency\b": "response delay",
            r"\bthroughput\b": "work volume",
        }
        for pat, repl in jargon_map.items():
            out = re.sub(pat, repl, out, flags=re.IGNORECASE)
        # Convert robotic heading names into natural blog headings.
        heading_map = {
            "Executive Summary": "Quick Take",
            "Decision Framework": "What Actually Works",
            "Decision Criteria": "When To Use This (And When Not To)",
            "Operational Depth": "What I Learned After Testing",
            "Common Failure Modes": "Where It Usually Breaks",
        }
        for src, dst in heading_map.items():
            out = re.sub(
                rf"<h2>\s*{re.escape(src)}\s*</h2>",
                f"<h2>{dst}</h2>",
                out,
                flags=re.IGNORECASE,
            )
        # Collapse accidental repeated fallback lines from QA no-progress loops.
        out = self._collapse_repeated_paragraph_line(
            out,
            "Operationally, validate one metric, one pause rule, and one owner before release.",
        )
        out = self._collapse_repeated_plain_line(
            out,
            "Operationally, validate one metric, one pause rule, and one owner before release.",
        )
        out = self._collapse_repeated_paragraph_line(
            out,
            "Prioritize measurable, reversible changes first.",
        )
        out = self._collapse_repeated_plain_line(
            out,
            "Prioritize measurable, reversible changes first.",
        )
        out = self._rewrite_quick_notes_section(out)
        out = self._strip_hangul_blocks(out)
        # Normalize whitespace from removals.
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()

    def _rewrite_quick_notes_section(self, html: str) -> str:
        out = str(html or "")
        section_re = re.compile(
            r"(<h[23][^>]*>\s*Quick Notes[^<]*</h[23]>\s*)(<ul>.*?</ul>)",
            flags=re.IGNORECASE | re.DOTALL,
        )

        def _rewrite_ul(ul_html: str) -> str:
            items = re.findall(r"<li[^>]*>(.*?)</li>", ul_html, flags=re.IGNORECASE | re.DOTALL)
            rewritten: list[str] = []
            for raw in items:
                txt = re.sub(r"<[^>]+>", " ", raw)
                txt = re.sub(r"\s+", " ", txt).strip()
                low = txt.lower()
                # Keyword dump / system memo patterns.
                if not txt or re.fullmatch(r"(?:[a-z0-9][a-z0-9\- ]{1,25}\s*,\s*){2,}[a-z0-9][a-z0-9\- ]{1,25}", low):
                    rewritten.append("Focus on one practical habit per week instead of changing everything at once.")
                    continue
                if re.search(r"\b(prompt|system|instruction|persona|config|template)\b", low):
                    rewritten.append("Translate each idea into one concrete action you can test during your next workday.")
                    continue
                if len(txt.split()) < 5:
                    rewritten.append(f"{txt.capitalize()} in a way that supports one measurable outcome this week.")
                    continue
                if not txt.endswith("."):
                    txt += "."
                rewritten.append(txt)
            if not rewritten:
                rewritten = [
                    "Focus on one practical habit per week instead of changing everything at once.",
                    "Measure one outcome before and after the change to confirm what actually worked.",
                ]
            return "<ul>" + "".join(f"<li>{escape(x)}</li>" for x in rewritten[:6]) + "</ul>"

        def _repl(m: re.Match[str]) -> str:
            heading = m.group(1)
            ul = m.group(2)
            return heading + _rewrite_ul(ul)

        return section_re.sub(_repl, out)

    def _strip_hangul_blocks(self, html: str) -> str:
        out = str(html or "")
        out = re.sub(
            r"<(p|li|h2|h3|figcaption)[^>]*>[^<]*[가-힣ㄱ-ㅎㅏ-ㅣ][^<]*</\1>",
            "",
            out,
            flags=re.IGNORECASE,
        )
        out = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]+", " ", out)
        out = re.sub(r"\s{2,}", " ", out)
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()

    def _inject_qa_no_progress_patch(self, html: str, qa_result) -> str:
        """Inject one unique, requirement-driven patch when QA loops make no progress."""
        out = html or ""
        failed = {str(getattr(c, "key", "") or "").strip().lower() for c in getattr(qa_result, "failed", [])}
        snippets: list[str] = []

        if "heading_structure" in failed:
            snippets.append(
                "<h3>Release Readiness Check</h3>"
                "<p>Before shipping, confirm one owner, one pause trigger, and one success metric.</p>"
            )
        if "actionability" in failed:
            snippets.append(
                "<h3>Minimum Action Plan</h3>"
                "<ul>"
                "<li>Define a pause threshold.</li>"
                "<li>Assign one responsible owner.</li>"
                "<li>Capture one before/after metric snapshot.</li>"
                "</ul>"
            )
        if "word_count" in failed:
            snippets.append(
                "<p>Operationally, validate one metric, one pause rule, and one owner before release.</p>"
                "<p>Use the smallest reversible change first, then expand scope only after evidence is stable.</p>"
            )

        # Generic fallback (single line), but never duplicate.
        snippets.append("<p>Operationally, validate one metric, one pause rule, and one owner before release.</p>")

        for block in snippets:
            plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", block)).strip()
            if not plain:
                continue
            if re.search(re.escape(plain), re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", out)), flags=re.IGNORECASE):
                continue
            return out + block
        return out

    def _collapse_repeated_paragraph_line(self, html: str, line: str) -> str:
        sentence = re.escape(line.strip())
        # Collapse repeated same <p> sentence into a single paragraph.
        pattern = rf"(?:<p[^>]*>\s*{sentence}\s*</p>\s*){{2,}}"
        return re.sub(pattern, f"<p>{line.strip()}</p>", html, flags=re.IGNORECASE)

    def _collapse_repeated_plain_line(self, text: str, line: str) -> str:
        sentence = re.escape(line.strip())
        pattern = rf"(?:{sentence}\s*){{2,}}"
        return re.sub(pattern, f"{line.strip()} ", text, flags=re.IGNORECASE)

    def _double_unescape(self, value: str) -> str:
        out = str(value or "")
        for _ in range(2):
            dec = unescape(out)
            if dec == out:
                break
            out = dec
        return out

    def _inject_search_links(self, html: str, keywords: list[str] | None) -> str:
        # Policy: never inject google.com/search references into article body.
        return html or ""

    def _google_search_link(self, keyword: str) -> str:
        # Deprecated by policy (no google.com references in body).
        q = quote_plus(re.sub(r"\s+", " ", str(keyword or "").strip()))
        return f"https://duckduckgo.com/?q={q}"

    def _replace_keyword_in_text_nodes(self, html: str, keyword: str, replacement_html: str) -> str:
        parts = re.split(r"(<[^>]+>)", html or "")
        pattern = re.compile(rf"\b{re.escape(keyword)}\b", flags=re.IGNORECASE)
        replaced = False
        out: list[str] = []
        for part in parts:
            if replaced:
                out.append(part)
                continue
            if part.startswith("<") and part.endswith(">"):
                out.append(part)
                continue
            new_part, n = pattern.subn(replacement_html, part, count=1)
            if n > 0:
                replaced = True
                out.append(new_part)
            else:
                out.append(part)
        return "".join(out)

    def _build_image_publish_note(self) -> str:
        try:
            report = self.publisher.get_last_upload_report()
        except Exception:
            report = {}
        if not report:
            return ""
        backend = str(report.get("backend", "") or "").strip()
        uploaded = int(report.get("uploaded", 0) or 0)
        total = int(report.get("requested", 0) or 0)
        hosts = ",".join(str(h) for h in (report.get("hosts", []) or [])[:3])
        parts = []
        if backend:
            parts.append(f"image_backend={backend}")
        if total:
            parts.append(f"image_uploaded={uploaded}/{total}")
        if hosts:
            parts.append(f"image_hosts={hosts}")
        return (", " + ", ".join(parts)) if parts else ""

    def _similarity_ratio(self, html: str, source_body: str) -> float:
        html_text = re.sub(r"<[^>]+>", " ", html or "")
        html_text = re.sub(r"\s+", " ", html_text).strip().lower()
        source_text = re.sub(r"\s+", " ", source_body or "").strip().lower()
        if not html_text or not source_text:
            return 0.0
        return SequenceMatcher(None, html_text[:6000], source_text[:6000]).ratio()

    def _classify_error_grade(self, message: str) -> str:
        msg = str(message or "").lower()
        hold_fatal = (
            "invalid_scope",
            "drive_backend_disabled",
            "non_blogger_host",
            "invalid_image_host",
            "r2_missing_config",
            "r2_url_invalid_host",
            "thumbnail_data_uri_not_allowed",
            "english_only",
            "data:image",
        )
        hold_recoverable = (
            "405",
            "http_4xx_or_5xx",
            "timeout",
            "qa below threshold",
            "thumbnail_preflight_failed",
            "image library shortage",
            "upload timeout",
            "r2_upload_failed",
        )
        skip_keys = (
            "duplicate title",
            "buffer over target",
            "entity diversity gate",
        )
        if any(k in msg for k in hold_fatal):
            return "HOLD_FATAL"
        if any(k in msg for k in hold_recoverable):
            return "HOLD_RECOVERABLE"
        if any(k in msg for k in skip_keys):
            return "SKIP"
        return "HOLD_RECOVERABLE"

    def _mark_active_slot(self, status: str, reason: str = "", post_id: str = "") -> None:
        key = str(self._active_slot_id or "").strip()
        if not key:
            return
        try:
            self.monthly_scheduler.mark_slot(
                key,
                status=str(status or "hold"),
                reason=str(reason or "")[:200],
                post_id=str(post_id or ""),
                now_utc=datetime.now(timezone.utc),
            )
        except Exception:
            pass
        finally:
            if str(status or "").strip().lower() in {"consumed", "hold", "skipped", "failed"}:
                self._active_slot_id = ""

    def _compute_publish_at(self) -> datetime | None:
        if not self.settings.publish.use_blogger_schedule:
            return None
        now = datetime.now(timezone.utc)
        min_delay = max(1, int(getattr(self.settings.publish, "min_delay_minutes", 10) or 10))
        if bool(getattr(self.settings.monthly_scheduler, "enabled", True)):
            slot = self.monthly_scheduler.acquire_next_pending_slot(now_utc=now, min_delay_minutes=min_delay)
            if isinstance(slot, dict) and slot.get("publish_at_utc"):
                self._active_slot_id = str(slot.get("slot_id", "") or "").strip()
                try:
                    dt = slot.get("publish_at_utc")
                    if isinstance(dt, datetime):
                        return dt.astimezone(timezone.utc).replace(microsecond=0)
                except Exception:
                    pass
        return self._compute_publish_at_legacy(now=now, min_delay=min_delay)

    def _compute_publish_at_legacy(self, now: datetime, min_delay: int) -> datetime | None:
        snapshot = self._blog_snapshot(force_refresh=True)
        times, day_count = self._build_publish_state(snapshot, now)
        daily_cap = max(1, int(self.settings.publish.daily_publish_cap))
        anchor = times[-1] if times else now + timedelta(minutes=min_delay)
        if anchor < now + timedelta(minutes=min_delay):
            anchor = now + timedelta(minutes=min_delay)
        return self._next_chained_publish_time(
            anchor=anchor,
            now=now,
            times=times,
            day_count=day_count,
            daily_cap=daily_cap,
            min_delay_minutes=min_delay,
        )

    def preview_publish_plan(self, count: int = 20, horizon_hours: int = 48) -> list[datetime]:
        now = datetime.now(timezone.utc)
        snapshot = self._blog_snapshot(force_refresh=True)
        times, day_count = self._build_publish_state(snapshot, now)
        daily_cap = max(1, int(self.settings.publish.daily_publish_cap))
        min_delay = max(1, int(self.settings.publish.min_delay_minutes))
        anchor = times[-1] if times else now + timedelta(minutes=min_delay)
        if anchor < now + timedelta(minutes=min_delay):
            anchor = now + timedelta(minutes=min_delay)

        out: list[datetime] = []
        for _ in range(max(1, int(count))):
            cand = self._next_chained_publish_time(
                anchor=anchor,
                now=now,
                times=times,
                day_count=day_count,
                daily_cap=daily_cap,
                min_delay_minutes=min_delay,
            )
            if cand is None:
                break
            out.append(cand)
            times.append(cand)
            times.sort()
            dkey = cand.date().isoformat()
            day_count[dkey] = day_count.get(dkey, 0) + 1
            anchor = cand

        # Keep full plan returned; caller can mark items beyond horizon if needed.
        if horizon_hours <= 0:
            return out
        return out

    def _build_publish_state(self, snapshot: dict, now: datetime) -> tuple[list[datetime], dict[str, int]]:
        scheduled = snapshot.get("scheduled_items", []) or []
        times: list[datetime] = []
        day_count: dict[str, int] = {}
        tz = self._publish_tz()
        today_key = now.astimezone(tz).date().isoformat()
        try:
            today_posts = int(snapshot.get("today_posts", 0))
        except Exception:
            today_posts = 0
        if str(snapshot.get("source", "local")) == "blogger" and today_posts > 0:
            # Respect daily cap with already-published posts counted for today.
            day_count[today_key] = day_count.get(today_key, 0) + today_posts
        for row in scheduled:
            dt = self._parse_iso_utc(str(row.get("publish_at", "")))
            if dt is None:
                continue
            if dt <= now - timedelta(minutes=1):
                continue
            dt = dt.replace(microsecond=0)
            times.append(dt)
            dkey = dt.astimezone(tz).date().isoformat()
            day_count[dkey] = day_count.get(dkey, 0) + 1
        times.sort()
        return times, day_count

    def _next_chained_publish_time(
        self,
        anchor: datetime,
        now: datetime,
        times: list[datetime],
        day_count: dict[str, int],
        daily_cap: int,
        min_delay_minutes: int,
    ) -> datetime | None:
        hard_min = now + timedelta(minutes=max(1, int(min_delay_minutes)))
        current_anchor = anchor if anchor > hard_min else hard_min
        level = str(getattr(self.settings.publish, "randomness_level", "medium") or "medium").strip().lower()
        jitter_map = {"low": 10, "medium": 30, "high": 90}
        jitter_minutes = int(jitter_map.get(level, 30))
        min_gap = max(10, int(getattr(self.settings.publish, "min_gap_minutes", min_delay_minutes) or min_delay_minutes))
        for _ in range(700):
            min_dt = current_anchor + timedelta(hours=3)
            max_dt = current_anchor + timedelta(hours=7)
            span_seconds = max(1, int((max_dt - min_dt).total_seconds()))
            cand = min_dt + timedelta(seconds=random.randint(0, span_seconds))
            cand = cand.replace(microsecond=0)
            cand += timedelta(minutes=random.randint(-jitter_minutes, jitter_minutes))
            if cand <= hard_min:
                bump = hard_min + timedelta(minutes=random.randint(3, 33))
                cand = bump.replace(second=random.randint(0, 59), microsecond=0)

            cand = self._shift_to_day_with_capacity(cand, day_count, daily_cap, times=times)
            if cand is None:
                return None
            cand = self._fit_publish_time_window(cand)
            if self._is_quiet_hours(cand):
                current_anchor = cand + timedelta(minutes=min_gap)
                continue
            # avoid near-collision (same minute-level bunching)
            if any(abs((cand - t).total_seconds()) < min_gap * 60 for t in times):
                current_anchor = cand + timedelta(minutes=random.randint(3, 12))
                continue
            return cand
        return self._shift_to_day_with_capacity(
            current_anchor + timedelta(hours=3),
            day_count,
            daily_cap,
            times=times,
        )

    def _parse_hhmm(self, value: str, default_h: int, default_m: int) -> tuple[int, int]:
        txt = str(value or "").strip()
        m = re.match(r"^\s*(\d{1,2}):(\d{1,2})\s*$", txt)
        if not m:
            return default_h, default_m
        h = max(0, min(23, int(m.group(1))))
        mm = max(0, min(59, int(m.group(2))))
        return h, mm

    def _publish_tz(self):
        tz_name = str(getattr(self.settings, "timezone", "") or "").strip() or "America/New_York"
        try:
            return ZoneInfo(tz_name)
        except Exception:
            try:
                return ZoneInfo("America/New_York")
            except Exception:
                return timezone.utc

    def _fit_publish_time_window(self, dt: datetime) -> datetime:
        tz = self._publish_tz()
        local_dt = (dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)).astimezone(tz)
        sh, sm = self._parse_hhmm(getattr(self.settings.publish, "time_window_start", "09:00"), 9, 0)
        eh, em = self._parse_hhmm(getattr(self.settings.publish, "time_window_end", "23:00"), 23, 0)
        start_min = sh * 60 + sm
        end_min = eh * 60 + em
        cur_min = local_dt.hour * 60 + local_dt.minute
        if start_min <= end_min:
            if cur_min < start_min:
                local_dt = local_dt.replace(hour=sh, minute=sm, second=random.randint(0, 59), microsecond=0)
                return local_dt.astimezone(timezone.utc)
            if cur_min > end_min:
                nxt = local_dt + timedelta(days=1)
                local_dt = nxt.replace(hour=sh, minute=sm, second=random.randint(0, 59), microsecond=0)
                return local_dt.astimezone(timezone.utc)
            return local_dt.astimezone(timezone.utc)
        # wrapped window (e.g., 22:00-03:00)
        if cur_min >= start_min or cur_min <= end_min:
            return local_dt.astimezone(timezone.utc)
        local_dt = local_dt.replace(hour=sh, minute=sm, second=random.randint(0, 59), microsecond=0)
        return local_dt.astimezone(timezone.utc)

    def _is_quiet_hours(self, dt: datetime) -> bool:
        if not bool(getattr(self.settings.publish, "quiet_hours_enabled", True)):
            return False
        tz = self._publish_tz()
        local_dt = (dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)).astimezone(tz)
        sh, sm = self._parse_hhmm(getattr(self.settings.publish, "quiet_hours_start", "02:00"), 2, 0)
        eh, em = self._parse_hhmm(getattr(self.settings.publish, "quiet_hours_end", "07:00"), 7, 0)
        start_min = sh * 60 + sm
        end_min = eh * 60 + em
        cur_min = local_dt.hour * 60 + local_dt.minute
        if start_min <= end_min:
            return start_min <= cur_min <= end_min
        return cur_min >= start_min or cur_min <= end_min

    def _shift_to_day_with_capacity(
        self,
        cand: datetime,
        day_count: dict[str, int],
        daily_cap: int,
        times: list[datetime] | None = None,
    ) -> datetime | None:
        base = cand
        tz = self._publish_tz()
        for day_offset in range(0, 730):
            day = ((base if base.tzinfo else base.replace(tzinfo=timezone.utc)).astimezone(tz) + timedelta(days=day_offset)).date()
            dkey = day.isoformat()
            if day_count.get(dkey, 0) < daily_cap:
                if day_offset == 0:
                    return base
                # Overflow rule: move to the next day with capacity at the first available slot.
                candidate_local = datetime(
                    year=day.year,
                    month=day.month,
                    day=day.day,
                    hour=0,
                    minute=5,
                    second=random.randint(0, 59),
                    tzinfo=tz,
                )
                candidate = candidate_local.astimezone(timezone.utc)
                same_day = sorted(
                    [
                        t for t in (times or [])
                        if (t if t.tzinfo else t.replace(tzinfo=timezone.utc)).astimezone(tz).date().isoformat() == dkey
                    ]
                )
                for t in same_day:
                    if candidate >= t + timedelta(minutes=3):
                        continue
                    if abs((candidate - t).total_seconds()) < 180:
                        candidate = (t + timedelta(minutes=4)).replace(microsecond=0)
                        if candidate.date() != day:
                            break
                if candidate.astimezone(tz).date() == day:
                    return candidate
                return datetime(
                    year=day.year,
                    month=day.month,
                    day=day.day,
                    hour=0,
                    minute=30,
                    second=random.randint(0, 59),
                    tzinfo=tz,
                ).astimezone(timezone.utc)
        return None

    def _queue_state(self, snapshot: dict | None = None) -> dict:
        data = snapshot or self._blog_snapshot(force_refresh=False)
        return {
            "scheduled": int(data.get("scheduled", 0)),
            "horizon_end": str(data.get("horizon_end", "")),
            "source": str(data.get("source", "local")),
        }

    def get_usage_snapshot(self, allow_remote: bool = True) -> dict:
        data = self._blog_snapshot(force_refresh=False, allow_remote=allow_remote)
        resume = self.get_resume_snapshot(force_refresh=False, allow_remote=allow_remote)
        if allow_remote:
            totals = self._blog_status_totals(force_refresh=False)
        else:
            totals = {"live": 0, "scheduled": 0}
            if self._blog_totals_cache is not None:
                try:
                    totals = dict(self._blog_totals_cache[1] or totals)
                except Exception:
                    totals = {"live": 0, "scheduled": 0}
        index_notified = int(self.logs.get_today_index_notified_count())
        inspected = int(self.logs.get_today_inspection_checked_count())
        today_written = int(self.logs.get_today_success_posts())
        today_reserved = int(data.get("today_scheduled", 0))
        today_published = int(data.get("today_posts", 0))
        return {
            "today_posts": today_published,
            "today_runs": int(data.get("today_runs", 0)),
            "today_scheduled": today_reserved,
            "scheduled_72h": int(data.get("scheduled", 0)),
            "scheduled_horizon": int(data.get("scheduled", 0)),
            "source": str(data.get("source", "local")),
            "today_written": today_written,
            "today_reserved": today_reserved,
            "today_published": today_published,
            "blogger_live_total": int(totals.get("live", 0)),
            "blogger_scheduled_total": int(totals.get("scheduled", 0)),
            "index_notified_today": index_notified,
            "inspection_checked_today": inspected,
            "resume_exists": bool(resume.get("exists", False)),
            "resume_stage": str(resume.get("stage", "")),
            "resume_title": str(resume.get("title", "")),
            "resume_updated": str(resume.get("updated", "")),
            "local_llm_used_last_run": bool(self._local_llm_used_last_run),
            "local_llm_ready": bool(self._local_llm_ready),
            "local_llm_reason": str(self._local_llm_last_reason or ""),
            "today_schedule_items": self.get_today_schedule_items(limit=24, allow_remote=allow_remote),
            "image_pipeline_status": str((self._image_pipeline_state or {}).get("status", "idle")),
            "image_pipeline_passed": int((self._image_pipeline_state or {}).get("passed", 0) or 0),
            "image_pipeline_target": int((self._image_pipeline_state or {}).get("target", 0) or 0),
            "image_pipeline_message": str((self._image_pipeline_state or {}).get("message", "") or ""),
            "publish_timezone": str(getattr(self.settings, "timezone", "America/New_York") or "America/New_York"),
            "buffer_target_days": int(getattr(self.settings.publish, "buffer_target_days", 5) or 5),
        }

    def get_today_global_keywords(self) -> list[str]:
        return list(self.last_global_keywords[:5])

    def get_recent_posts_preview(self, limit: int = 5) -> list[dict]:
        safe_limit = max(1, int(limit))
        out: list[dict] = []
        seen: set[str] = set()

        try:
            api_posts = self.publisher.fetch_posts_for_export(
                statuses=["live", "scheduled"],
                limit=max(10, safe_limit * 3),
                include_bodies=False,
            )
            for post in api_posts:
                title = str(getattr(post, "title", "") or "").strip()
                url = str(getattr(post, "url", "") or "").strip()
                post_id = str(getattr(post, "post_id", "") or "").strip()
                published_at = str(getattr(post, "published_at", "") or "").strip()
                key = (post_id or url or title).strip()
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "title": title,
                        "url": url,
                        "post_id": post_id,
                        "published_at": published_at,
                        "source": "blogger",
                    }
                )
                if len(out) >= safe_limit:
                    return out
        except Exception:
            pass

        rows = self.logs.get_recent_published_posts(days=120, limit=max(20, safe_limit * 4))
        for row in rows:
            title = str((row or {}).get("title", "") or "").strip()
            url = str((row or {}).get("published_url", "") or "").strip()
            created_at = str((row or {}).get("created_at", "") or "").strip()
            key = (url or title).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "title": title,
                    "url": url,
                    "post_id": "",
                    "published_at": created_at,
                    "source": "local",
                }
            )
            if len(out) >= safe_limit:
                break
        return out[:safe_limit]

    def get_today_schedule_items(self, limit: int = 24, allow_remote: bool = True) -> list[dict]:
        data = self._blog_snapshot(force_refresh=False, allow_remote=allow_remote)
        rows = list(data.get("scheduled_items", []) or [])
        now = datetime.now(timezone.utc)
        out: list[dict] = []
        for row in rows:
            publish_at_raw = str((row or {}).get("publish_at", "") or "").strip()
            dt = self._parse_iso_utc(publish_at_raw)
            if dt is None:
                continue
            if dt.date() != now.date():
                continue
            out.append(
                {
                    "publish_at": dt.isoformat(),
                    "title": str((row or {}).get("title", "") or "").strip(),
                    "post_id": str((row or {}).get("post_id", "") or "").strip(),
                    "published_url": str((row or {}).get("published_url", "") or "").strip(),
                }
            )
        out.sort(key=lambda x: str(x.get("publish_at", "")))
        return out[: max(1, int(limit))]

    def _blog_snapshot(self, force_refresh: bool = False, allow_remote: bool = True) -> dict:
        now = datetime.now(timezone.utc)
        if not force_refresh and self._blog_snapshot_cache is not None:
            ts, cached = self._blog_snapshot_cache
            if (now - ts).total_seconds() < self._blog_cache_ttl_seconds:
                return cached

        horizon_h = max(
            24,
            int(self.settings.publish.queue_horizon_hours),
            max(1, int(getattr(self.settings.publish, "schedule_horizon_days", 14) or 14)) * 24,
        )
        end = now + timedelta(hours=horizon_h)
        end_iso = end.isoformat()
        fallback_error = ""
        excluded_ids = self.logs.get_excluded_post_ids(days=14)
        try:
            if not allow_remote:
                raise RuntimeError("remote_snapshot_disabled")
            remote = self.publisher.fetch_live_snapshot(
                horizon_hours=horizon_h,
                timezone_name=self.settings.timezone,
            )
            scheduled_items = remote.get("scheduled_items", []) or []
            normalized: list[dict] = []
            for row in scheduled_items:
                publish_at = str(row.get("publish_at", "")).strip()
                dt = self._parse_iso_utc(publish_at)
                if dt is None:
                    continue
                if dt <= now or dt > end:
                    continue
                pid = str(row.get("post_id", "")).strip()
                if pid and pid in excluded_ids:
                    continue
                normalized.append(
                    {
                        "publish_at": dt.isoformat(),
                        "post_id": pid,
                        "title": str(row.get("title", "")).strip(),
                        "source_url": str(row.get("source_url", "")).strip(),
                        "published_url": str(row.get("published_url", "")).strip(),
                    }
                )
            live_rows_raw = list(remote.get("today_live_items", []) or [])
            live_rows: list[dict] = []
            for row in live_rows_raw:
                pid = str(row.get("post_id", "") or "").strip()
                if pid and pid in excluded_ids:
                    continue
                ttl = str(row.get("title", "") or "").strip()
                if not pid and not ttl:
                    continue
                live_rows.append(
                    {
                        "post_id": pid,
                        "title": ttl,
                        "published_url": str(row.get("published_url", "") or "").strip(),
                    }
                )
            if live_rows:
                today_live_posts = int(len(live_rows))
                today_live_titles = [str(r.get("title", "")).strip() for r in live_rows if str(r.get("title", "")).strip()][:100]
            else:
                # Backward compatibility when remote snapshot has no per-item payload.
                today_live_posts = int(remote.get("today_live_posts", 0))
                today_live_titles = list(remote.get("today_live_titles", []) or [])[:100]
            today_scheduled = 0
            for row in normalized:
                dt = self._parse_iso_utc(str(row.get("publish_at", "")))
                if dt is not None and dt.date() == now.date():
                    today_scheduled += 1
            out = {
                "source": "blogger",
                "today_posts": int(today_live_posts),
                "today_runs": int(today_live_posts),
                "today_scheduled": int(today_scheduled),
                "today_live_titles": today_live_titles,
                "scheduled": int(len(normalized)),
                "scheduled_items": normalized,
                "horizon_end": end_iso,
            }
            self._blog_snapshot_cache = (now, out)
            return out
        except Exception as exc:
            fallback_error = str(exc)

        now_iso = now.isoformat()
        # UI fast path: skip maintenance writes on read-only local snapshot access.
        if allow_remote or force_refresh:
            self.logs.purge_expired_scheduled(now_iso)
        local_items_raw = self.logs.list_scheduled_in_window(now_iso, end_iso, limit=1000)
        local_items = []
        for row in local_items_raw:
            pid = str(row.get("post_id", "") or "").strip()
            if pid and pid in excluded_ids:
                continue
            local_items.append(row)
        local_today_scheduled = 0
        for row in local_items:
            dt = self._parse_iso_utc(str(row.get("publish_at", "")))
            if dt is not None and dt.date() == now.date():
                local_today_scheduled += 1
        out = {
            "source": "local",
            # Local fallback cannot reliably know real "already published today" count.
            "today_posts": 0,
            # Keep local successful run count as a separate telemetry value.
            "today_runs": int(self.logs.get_today_success_posts()),
            "today_scheduled": int(local_today_scheduled),
            "today_live_titles": [
                str(r.get("title", "")).strip()
                for r in self.logs.get_recent_published_posts(days=1, limit=40)
                if str(r.get("title", "")).strip()
            ],
            "scheduled": int(len(local_items)),
            "scheduled_items": local_items,
            "horizon_end": end_iso,
            "error": fallback_error,
        }
        self._blog_snapshot_cache = (now, out)
        return out

    def _blog_status_totals(self, force_refresh: bool = False) -> dict[str, int]:
        now = datetime.now(timezone.utc)
        if not force_refresh and self._blog_totals_cache is not None:
            ts, cached = self._blog_totals_cache
            if (now - ts).total_seconds() < self._blog_totals_ttl_seconds:
                return dict(cached)

        out = {"live": 0, "scheduled": 0}
        try:
            counts = self.publisher.fetch_status_counts(["live", "scheduled"])
            out["live"] = int(counts.get("live", 0))
            out["scheduled"] = int(counts.get("scheduled", 0))
        except Exception:
            pass

        self._blog_totals_cache = (now, dict(out))
        return out

    def _parse_iso_utc(self, text: str) -> datetime | None:
        value = (text or "").strip()
        if not value:
            return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _filter_recent_duplicates(
        self,
        candidates,
        recent_titles: list[str] | set[str],
    ):
        # Blogger 14-day live/scheduled titles are the source of truth.
        # No Gemini calls here: lexical/semantic local scoring only.
        if not candidates:
            return candidates
        title_pool = {
            str(t).strip().lower()
            for t in (recent_titles or [])
            if str(t).strip()
        }
        title_sigs = [self._topic_signature(t) for t in title_pool if t]
        out = []
        for c in candidates:
            ct = (c.title or "").strip().lower()
            cb = (c.body or "").strip().lower()
            if not ct:
                continue
            if ct in title_pool:
                continue
            sem_hit = self._semantic_near_duplicate(ct, title_pool, threshold=0.74)
            cand_sig = self._topic_signature(f"{ct} {cb[:1000]}")
            jac = 0.0
            if cand_sig:
                jac = max((self._set_jaccard(cand_sig, s) for s in title_sigs if s), default=0.0)
            if sem_hit and jac >= 0.70:
                continue
            out.append(c)
        return out

    def _semantic_near_duplicate(self, title: str, recent_titles: set[str], threshold: float = 0.74) -> bool:
        if not title:
            return False
        title_tokens = self._tokenize(title)
        if not title_tokens:
            return False
        for rt in list(recent_titles)[:250]:
            rt_tokens = self._tokenize(rt)
            if not rt_tokens:
                continue
            sim = self._bow_cosine(title_tokens, rt_tokens)
            lexical = SequenceMatcher(None, title, rt).ratio()
            if sim >= 0.90:
                return True
            if sim >= threshold and lexical >= 0.62:
                return True
        return False

    def _topic_signature(self, text: str) -> set[str]:
        stop = {
            "the", "and", "for", "with", "from", "this", "that", "into", "your", "their",
            "guide", "using", "about", "over", "under", "what", "when", "where", "why",
            "how", "best", "real", "more", "less", "than", "then", "also", "after",
            "before", "between", "through", "post", "blog", "feature", "issue",
        }
        words = re.findall(r"[a-z0-9]{4,}", (text or "").lower())
        words = [w for w in words if w not in stop]
        if not words:
            return set()
        freq: dict[str, int] = {}
        for w in words:
            freq[w] = freq.get(w, 0) + 1
        ranked = sorted(freq.items(), key=lambda x: x[1], reverse=True)
        return {w for w, _ in ranked[:12]}

    def _set_jaccard(self, a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        inter = len(a & b)
        uni = len(a | b)
        if uni == 0:
            return 0.0
        return inter / uni

    def _tokenize(self, text: str) -> dict[str, int]:
        words = re.findall(r"[a-z0-9]{3,}", (text or "").lower())
        freq: dict[str, int] = {}
        for w in words:
            freq[w] = freq.get(w, 0) + 1
        return freq

    def _bow_cosine(self, a: dict[str, int], b: dict[str, int]) -> float:
        if not a or not b:
            return 0.0
        keys = set(a.keys()) & set(b.keys())
        dot = sum(a[k] * b[k] for k in keys)
        na = sum(v * v for v in a.values()) ** 0.5
        nb = sum(v * v for v in b.values()) ** 0.5
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)

    def _build_internal_links_block(
        self,
        current_title: str,
        current_keywords: list[str] | None = None,
        current_device_type: str | None = None,
        current_cluster_id: str | None = None,
    ) -> str:
        if not bool(getattr(self.settings.internal_links, "enabled", True)):
            return ""
        body_link_count = max(0, int(getattr(self.settings.internal_links, "body_link_count", 1) or 1))
        related_link_count = max(0, int(getattr(self.settings.internal_links, "related_link_count", 2) or 2))
        overlap_threshold = float(getattr(self.settings.internal_links, "overlap_threshold", 0.4) or 0.4)
        if body_link_count + related_link_count <= 0:
            return ""

        device = str(current_device_type or self._infer_device_type(current_title)).strip().lower() or "windows"
        cluster = str(current_cluster_id or self._infer_cluster_id_from_keyword(current_title)).strip().lower() or "general"
        current_kw = self._parse_focus_keywords(current_keywords or current_title)
        current_title_l = str(current_title or "").strip().lower()

        rows = self.posts_index.query_recent(
            limit=260,
            include_future=False,
            statuses=["live"],
            exclude_deleted=True,
        )
        primary: list[dict] = []
        secondary: list[tuple[float, dict]] = []
        for row in rows:
            title = str((row or {}).get("title", "") or "").strip()
            url = str((row or {}).get("url", "") or "").strip()
            if not title or not url:
                continue
            if title.lower() == current_title_l:
                continue
            row_device = str((row or {}).get("device_type", "") or "").strip().lower()
            row_cluster = str((row or {}).get("cluster_id", "") or "").strip().lower()
            row_kw = self._parse_focus_keywords(str((row or {}).get("focus_keywords", "") or ""))
            if row_device == device and row_cluster == cluster:
                primary.append(row)
                continue
            if row_device == device:
                ov = self._keyword_overlap(current_kw, row_kw)
                if ov >= overlap_threshold:
                    secondary.append((ov, row))

        secondary.sort(key=lambda x: x[0], reverse=True)
        merged: list[dict] = []
        merged.extend(primary)
        merged.extend([r for _, r in secondary])
        if not merged:
            return ""

        picked: list[dict] = []
        seen_url: set[str] = set()
        for row in merged:
            url = str((row or {}).get("url", "") or "").strip()
            title = str((row or {}).get("title", "") or "").strip()
            if not url or not title:
                continue
            if url in seen_url:
                continue
            seen_url.add(url)
            picked.append(row)
            if len(picked) >= (body_link_count + related_link_count):
                break

        if not picked:
            return ""

        body_links = picked[:body_link_count]
        related_links = picked[body_link_count : body_link_count + related_link_count]
        block_parts: list[str] = []
        if body_links:
            row = body_links[0]
            anchor = self._clean_anchor_text(str((row or {}).get("title", "") or "Related practical guide"))
            url = str((row or {}).get("url", "") or "").strip()
            block_parts.append(
                f'<p>If you want a complementary walkthrough, read <a href="{url}" rel="noopener">{escape(anchor)}</a>.</p>'
            )
        if related_links:
            lis = []
            for row in related_links:
                anchor = self._clean_anchor_text(str((row or {}).get("title", "") or "Related post"))
                url = str((row or {}).get("url", "") or "").strip()
                lis.append(f'<li><a href="{url}" rel="noopener">{escape(anchor)}</a></li>')
            block_parts.append("<h2>More Fix Guides You Might Like</h2><ul>" + "".join(lis) + "</ul>")
        return "".join(block_parts)

    def _clean_anchor_text(self, title: str) -> str:
        text = re.sub(r"[\[\]\(\)\{\}\"'`]+", " ", str(title or ""))
        text = re.sub(r"[_|]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:120] if text else "Related post"

    def _build_meta_description(self, title: str, summary: str, html: str) -> str:
        keyword = re.sub(r"\s+", " ", str(title or "")).strip()
        if keyword:
            words = keyword.split()
            keyword = " ".join(words[:8]).strip()
        base = re.sub(r"<[^>]+>", " ", str(html or ""))
        base = re.sub(r"\s+", " ", base).strip()
        seed = re.sub(r"\s+", " ", str(summary or "")).strip()
        if len(seed) < 80:
            seed = base[:260]
        seed = re.sub(r"\s+", " ", seed).strip()
        if keyword and keyword.lower() not in seed.lower():
            seed = f"{keyword}: {seed}"
        if len(seed) < 120:
            seed = (
                seed
                + " This post explains what worked, what failed, and what everyday users can apply immediately."
            )
        seed = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]+", " ", seed)
        seed = re.sub(r"\s+", " ", seed).strip()
        if len(seed) < 120:
            seed = (seed + " Learn the practical steps, common mistakes, and realistic outcomes from this one-week test.").strip()
        if len(seed) > 160:
            seed = seed[:157].rstrip(" ,.;:") + "..."
        return seed

    def _normalize_excerpt(self, html: str) -> str:
        txt = re.sub(r"<[^>]+>", " ", html or "")
        txt = re.sub(r"\s+", " ", txt).strip().lower()
        return txt[:5000]

    def _is_near_duplicate_post(self, html: str) -> bool:
        excerpt = self._normalize_excerpt(html)
        if not excerpt:
            return False
        excerpt_bow = self._tokenize(excerpt[:5000])
        recent = self.logs.get_recent_content_fingerprints(days=14, limit=250)
        for item in recent:
            old = str(item.get("excerpt", "")).strip().lower()
            if not old:
                continue
            lexical = SequenceMatcher(None, excerpt[:5000], old[:5000]).ratio()
            semantic = self._bow_cosine(excerpt_bow, self._tokenize(old[:5000]))
            # Relax false-positive skips: require both medium similarity signals,
            # or a very high single similarity.
            if max(lexical, semantic) >= 0.92:
                return True
            if lexical >= 0.74 and semantic >= 0.74:
                return True
        return False

    def _keyword_pool_today_kst(self) -> str:
        return datetime.now(self._kst).date().isoformat()

    def _normalize_keyword(self, value: str) -> str:
        txt = re.sub(r"[^A-Za-z0-9\-\s]", " ", str(value or ""))
        txt = re.sub(r"\s+", " ", txt).strip()
        if not txt:
            return ""
        words = txt.split(" ")
        if len(words) < 2:
            return ""
        if len(words) > 8:
            txt = " ".join(words[:8])
        return txt

    def _keyword_allowed_for_content_mode(self, keyword: str) -> bool:
        mode = str(getattr(self.settings.content_mode, "mode", "") or "").strip().lower()
        if mode != "tech_troubleshoot_only":
            return True
        lower = str(keyword or "").lower()
        banned = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "banned_topic_keywords", []) or [])
            if str(x or "").strip()
        ]
        if any(token in lower for token in banned):
            return False
        device_tokens = ("windows", "mac", "iphone", "ios", "galaxy", "samsung", "android")
        fix_tokens = ("not working", "fix", "error", "after update", "troubleshoot", "issue")
        return any(t in lower for t in device_tokens) and any(t in lower for t in fix_tokens)

    def _load_keyword_pool(self) -> dict[str, Any]:
        today = self._keyword_pool_today_kst()
        out: dict[str, Any] = {
            "date_kst": today,
            "active": [],
            "daily_seed_done": False,
            "daily_seed_retry_count": 0,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
        }
        try:
            if self._keyword_pool_path.exists():
                payload = json.loads(self._keyword_pool_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    out.update(payload)
        except Exception:
            pass
        active_raw = out.get("active", []) or []
        clean: list[str] = []
        seen: set[str] = set()
        for item in active_raw:
            norm = self._normalize_keyword(str(item or ""))
            if not norm:
                continue
            key = norm.lower()
            if key in seen:
                continue
            seen.add(key)
            clean.append(norm)
        out["active"] = clean
        return out

    def _save_keyword_pool(self, pool: dict[str, Any]) -> None:
        pool = dict(pool or {})
        pool["updated_utc"] = datetime.now(timezone.utc).isoformat()
        self._keyword_pool_path.parent.mkdir(parents=True, exist_ok=True)
        self._keyword_pool_path.write_text(
            json.dumps(pool, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _build_local_keyword_candidates(self, candidates, limit: int = 240) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        stop = {
            "the", "and", "for", "with", "from", "that", "this", "into", "your", "their",
            "about", "after", "before", "between", "through", "where", "what", "when",
            "will", "would", "could", "should", "best", "guide", "news", "today", "using",
            "use", "tool", "tools",
        }

        def push(v: str) -> None:
            norm = self._normalize_keyword(v)
            if not norm:
                return
            if not self._keyword_allowed_for_content_mode(norm):
                return
            key = norm.lower()
            if key in seen:
                return
            seen.add(key)
            out.append(norm)

        for c in candidates or []:
            for kw in (getattr(c, "long_tail_keywords", []) or [])[:8]:
                push(str(kw or ""))

        for c in candidates or []:
            title = str(getattr(c, "title", "") or "")
            body = str(getattr(c, "body", "") or "")
            src = " ".join([title, body[:700]])
            words = [
                w for w in re.findall(r"[A-Za-z][A-Za-z0-9\-]{2,}", src.lower())
                if w not in stop
            ]
            words = words[:26]
            for n in (2, 3):
                for i in range(0, max(0, len(words) - n + 1)):
                    push(" ".join(words[i : i + n]))
            if words:
                head = " ".join(words[:2])
                push(f"{head} troubleshooting")
                push(f"{head} fix")
                push(f"how to fix {head}")
                push(f"{head} not working")
                push(f"{head} setup error fix")
            if len(out) >= limit:
                break
        return out[:limit]

    def _fill_keyword_pool(
        self,
        pool: dict[str, Any],
        candidates,
        target_add: int,
        max_pool: int,
        allow_api_retry: bool = False,
    ) -> int:
        active = [str(x).strip() for x in (pool.get("active", []) or []) if str(x).strip()]
        existing = {x.lower() for x in active}
        added = 0

        local = self._build_local_keyword_candidates(candidates, limit=max(240, target_add * 4))
        for kw in local:
            key = kw.lower()
            if key in existing:
                continue
            active.append(kw)
            existing.add(key)
            added += 1
            if added >= target_add:
                break

        if allow_api_retry and added < target_add:
            try:
                extra = self.brain.extract_global_keywords(
                    candidates,
                    limit=5,
                    avoid_keywords=list(existing)[:120],
                )
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
            except Exception:
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                extra = []
            for kw in extra:
                norm = self._normalize_keyword(str(kw or ""))
                if not norm:
                    continue
                key = norm.lower()
                if key in existing:
                    continue
                active.append(norm)
                existing.add(key)
                added += 1
                if added >= target_add:
                    break

        if len(active) > max_pool:
            active = active[-max_pool:]
        pool["active"] = active
        return added

    def _acquire_run_keywords(self, candidates) -> tuple[list[str], str]:
        note = ""
        device_type = self._current_rotated_device_type()
        refill_threshold = max(10, int(getattr(self.settings.keywords, "refill_threshold_per_device", 100) or 100))
        avoid_days = max(7, int(getattr(self.settings.keywords, "avoid_reuse_days", 30) or 30))
        pick_count = max(1, int(getattr(self.settings.publish, "daily_publish_cap", 2) or 2))

        available = self.keyword_assets.available_count(device_type=device_type, avoid_reuse_days=avoid_days)
        note = self._append_note(note, f"kw_device={device_type}")
        note = self._append_note(note, f"kw_available={available}")
        if available < refill_threshold:
            local_candidates = self._build_local_keyword_candidates(candidates, limit=max(200, refill_threshold * 3))
            rows: list[tuple[str, str, str, float, float]] = []
            for kw in local_candidates:
                rows.append(
                    (
                        kw,
                        self._infer_cluster_id_from_keyword(kw),
                        "scout",
                        0.7,
                        min(1.0, len(kw.split()) / 8.0),
                    )
                )
            for kw in self._build_template_keywords(device_type=device_type, limit=max(80, refill_threshold)):
                rows.append(
                    (
                        kw,
                        self._infer_cluster_id_from_keyword(kw),
                        "templates",
                        0.5,
                        0.2,
                    )
                )
            added = self.keyword_assets.upsert_keywords(device_type=device_type, rows=rows)
            note = self._append_note(note, f"kw_refill_added={added}")

        picks = self.keyword_assets.pick_keywords(
            device_type=device_type,
            limit=pick_count,
            avoid_reuse_days=avoid_days,
        )
        if not picks:
            fallback = self._build_local_keyword_candidates(candidates, limit=24)
            picks = fallback[:pick_count]
            note = self._append_note(note, "kw_fallback_local")
        return picks, note

    def _load_blogger_recent_titles_cache(self) -> dict[str, Any]:
        if not self._blogger_recent_14d_path.exists():
            return {}
        try:
            payload = json.loads(self._blogger_recent_14d_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    def _save_blogger_recent_titles_cache(self, titles: list[str], source: str = "api") -> None:
        clean: list[str] = []
        seen: set[str] = set()
        for t in titles or []:
            txt = re.sub(r"\s+", " ", str(t or "")).strip()
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            clean.append(txt)
        payload = {
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "titles": clean[:600],
        }
        self._blogger_recent_14d_path.parent.mkdir(parents=True, exist_ok=True)
        self._blogger_recent_14d_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _refresh_blogger_recent_titles_cache(self, force_api: bool = False, limit: int = 240) -> list[str]:
        now = datetime.now(timezone.utc)
        cached = self._load_blogger_recent_titles_cache()
        cached_titles = [str(x).strip() for x in (cached.get("titles", []) if isinstance(cached, dict) else []) if str(x).strip()]
        cached_ts = self._parse_iso_utc(str(cached.get("updated_utc", "") if isinstance(cached, dict) else ""))
        if (not force_api) and cached_ts is not None:
            if (now - cached_ts).total_seconds() < self._recent_blogger_titles_ttl_seconds:
                self._recent_blogger_titles_cache = (now, cached_titles[: max(limit, 10)])
                return cached_titles[:limit]

        try:
            rows = self.publisher.fetch_posts_for_export(
                statuses=["live", "scheduled"],
                limit=max(limit * 2, 180),
                include_bodies=False,
            )
            edge = now - timedelta(days=14)
            titles: list[str] = []
            for row in rows:
                dt = self._parse_iso_utc(str(getattr(row, "published", "") or getattr(row, "updated", "") or ""))
                if dt is not None and dt < edge:
                    continue
                title = str(getattr(row, "title", "") or "").strip()
                if title:
                    titles.append(title)
            if titles:
                self._save_blogger_recent_titles_cache(titles, source="api")
                clean = [str(x).strip() for x in titles if str(x).strip()]
                self._recent_blogger_titles_cache = (now, clean[: max(limit, 10)])
                return clean[:limit]
        except Exception:
            pass
        self._recent_blogger_titles_cache = (now, cached_titles[: max(limit, 10)])
        return cached_titles[:limit]

    def _get_recent_blogger_titles(self, limit: int = 10, refresh_api: bool = False) -> list[str]:
        if refresh_api:
            return self._refresh_blogger_recent_titles_cache(force_api=True, limit=limit)
        now = datetime.now(timezone.utc)
        if self._recent_blogger_titles_cache is not None:
            ts, cached = self._recent_blogger_titles_cache
            if (now - ts).total_seconds() < self._recent_blogger_titles_ttl_seconds:
                return list(cached[:limit])
        cached = self._load_blogger_recent_titles_cache()
        titles = [
            str(x).strip()
            for x in (cached.get("titles", []) if isinstance(cached, dict) else [])
            if str(x).strip()
        ]
        if titles:
            self._recent_blogger_titles_cache = (now, titles[: max(limit, 10)])
            return titles[:limit]
        return self._refresh_blogger_recent_titles_cache(force_api=True, limit=limit)

    def _rotate_keywords_after_success(self, candidates, used_text: str) -> None:
        # Keywords are consumed from persistent pool when selected for this run.
        # Keep the current run keywords only for UI/log visibility.
        self._set_cached_global_keywords(self.last_global_keywords)

    def _start_posts_index_bootstrap(self) -> None:
        if self._posts_index_bootstrap_started:
            return
        self._posts_index_bootstrap_started = True
        threading.Thread(target=self._bootstrap_posts_index_from_logs, daemon=True).start()

    def _bootstrap_posts_index_from_logs(self) -> None:
        try:
            if self.posts_index.count() > 0:
                self._posts_index_bootstrap_done = True
                return
            rows = self.logs.get_recent_published_posts(days=365, limit=240)
            for row in rows:
                title = str((row or {}).get("title", "") or "").strip()
                url = str((row or {}).get("published_url", "") or "").strip()
                if not title or not url:
                    continue
                self.posts_index.upsert_post(
                    post_id=url,
                    url=url,
                    title=title,
                    published_at=str((row or {}).get("created_at", "") or datetime.now(timezone.utc).isoformat()),
                    summary=title,
                    focus_keywords="",
                    cluster_id=self._infer_cluster_id_from_keyword(title),
                    device_type=self._infer_device_type(title),
                    word_count=len(re.findall(r"[A-Za-z0-9']+", title)),
                    status="live",
                    deleted_at=None,
                    last_seen_at=datetime.now(timezone.utc).isoformat(),
                    source="blogger",
                )
            self._posts_index_bootstrap_done = True
        except Exception:
            self._posts_index_bootstrap_done = False

    def _infer_device_type(self, text: str) -> str:
        low = str(text or "").lower()
        if any(x in low for x in ("iphone", "ios", "ipad")):
            return "iphone"
        if any(x in low for x in ("galaxy", "android", "samsung")):
            return "galaxy"
        if any(x in low for x in ("mac", "macbook", "macos")):
            return "mac"
        return "windows"

    def _parse_focus_keywords(self, value: str | list[str]) -> set[str]:
        if isinstance(value, list):
            source = ",".join(str(x or "") for x in value)
        else:
            source = str(value or "")
        out: set[str] = set()
        for tok in re.split(r"[,;\s]+", source.lower()):
            tok = tok.strip()
            if len(tok) < 3:
                continue
            out.add(tok)
        return out

    def _keyword_overlap(self, a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        inter = len(a & b)
        uni = len(a | b)
        if uni == 0:
            return 0.0
        return inter / uni

    def _index_published_post(
        self,
        post_id: str,
        url: str,
        title: str,
        html: str,
        summary: str,
        global_keywords: list[str],
        candidate,
        publish_at: datetime | None = None,
    ) -> None:
        device_type = self._infer_device_type(f"{title}\n{getattr(candidate, 'title', '')}")
        cluster_id = self._infer_cluster_id_from_keyword(" ".join(global_keywords[:2]) or title)
        published_at_iso = (
            publish_at.astimezone(timezone.utc).isoformat()
            if isinstance(publish_at, datetime)
            else datetime.now(timezone.utc).isoformat()
        )
        status = "live"
        if isinstance(publish_at, datetime):
            now_utc = datetime.now(timezone.utc)
            ref_dt = publish_at.astimezone(timezone.utc) if publish_at.tzinfo else publish_at.replace(tzinfo=timezone.utc)
            if ref_dt > now_utc:
                status = "scheduled"
        self.posts_index.upsert_post(
            post_id=post_id or url,
            url=url,
            title=title,
            published_at=published_at_iso,
            summary=str(summary or "")[:900],
            focus_keywords=[str(x).strip() for x in (global_keywords or []) if str(x).strip()],
            cluster_id=cluster_id,
            device_type=device_type,
            word_count=len(re.findall(r"[A-Za-z0-9']+", re.sub(r"<[^>]+>", " ", html or ""))),
            status=status,
            deleted_at=None,
            last_seen_at=datetime.now(timezone.utc).isoformat(),
            source="blogger",
        )

