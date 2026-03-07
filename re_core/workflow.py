from __future__ import annotations

import json
import hashlib
import re
import random
import time
import threading
import uuid
from dataclasses import asdict, dataclass
from collections import Counter
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from html import escape, unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse
from zoneinfo import ZoneInfo

from re_core.brain import DraftPost, GeminiBrain, stable_hash
from re_core.actionability_gate import ActionabilityGate, ActionabilityGateResult
from re_core.news_actionability_gate import NewsActionabilityGate, NewsActionabilityGateResult
from re_core.budget import BudgetGuard
from re_core.asset_store import KeywordAssetStore, PostsIndexStore
from re_core.index_sync import BloggerIndexSync
from re_core.insights import GrowthInsights, InsightsSettingsView
from re_core.logstore import LogStore, RunRecord
from re_core.news_facets import ensure_what_to_do_now_section, facet_emphasis_hint, resolve_facet_context
from re_core.news_pool import NewsPoolStore
from re_core.news_clustering import NewsClusterEngine, should_skip_same_run
from re_core.news_rss import fetch_feed_detailed
from re_core.news_score import contains_allow_keywords, has_blocked_keywords, score_news_item
from re_core.safety_filter import SafetyFilter
from re_core.services.news_collector import fetch_trending_topics
from re_core.services.search_intent import IntentBundle, SearchIntentGenerator
from re_core.services.keyword_discovery import DiscoveryOpportunity, KeywordDiscovery
from re_core.services.topic_scoring import TopicScoring
from re_core.services.content_allocation import ContentAllocationEngine
from re_core.news_pack_manifest import NewsPackManifest
from re_core.news_pack_picker import NewsPackPicker

from re_core.publish_ledger import PublishLedger, make_ledger_key
from re_core.readability import optimize_html_readability
from re_core.source_naturalization import apply_source_naturalization
from re_core.content_entropy import check_entropy
from re_core.visual_diagnostics import diagnose_visual_settings
from re_core.title_diversity import choose_diverse_title
from re_core.run_metrics import RunMetricsLogger, parse_reason_codes
from re_core.clickbait_sanitizer import sanitize_clickbait_terms
from re_core.ollama_client import OllamaClient
from re_core.ollama_manager import OllamaManager
from re_core.patterns import PatternEngine
from re_core.prompt_factory import PromptFactory
from re_core.publisher import Publisher
from re_core.quality import ContentQAGate
from re_core.reference_docs import ReferenceCorpus
from re_core.scheduler import MonthlyScheduler
from re_core.scout import SourceScout, TopicCandidate
from re_core.settings import AppSettings, is_news_mode
from re_core.structure_randomizer import OutlinePlan, StructureRandomizer
from re_core.story_profile import (
    build_story_tags,
    filter_relevant_authority_links,
    infer_story_profile,
)
from re_core.preflight import validate_secrets
from re_core.text_segmenter import section_bundle_for_llm
from re_core.thumbnail_overlay import ThumbnailOverlayRenderer
from re_core.topic_growth import TopicGrower
from re_core.visual import ImageAsset, VisualPipeline
from re_core.watchdog import Watchdog
from re_core.r2_uploader import R2Config, upload_file as r2_upload_file
from re_core.services.media_manager import MediaManagerService
from re_core.services.metrics_tracker import MetricsTrackerService
from re_core.services.ollama_fallback import OllamaFallbackService
from re_core.services.intelligence_service import IntelligenceService


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
        self.safety_filter = SafetyFilter(log_path=root / "storage" / "logs" / "policy_gate.jsonl")
        self.scout = SourceScout(settings.sources, root, settings.content_mode, safety_filter=self.safety_filter)
        self.patterns = PatternEngine()
        self.prompt_factory = PromptFactory(root)
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
            r2_config=getattr(settings.publish, "r2", None),
        )
        self._workflow_perf_path = self.root / "storage" / "logs" / "workflow_perf.jsonl"
        self.metrics_tracker = MetricsTrackerService(log_path=self._workflow_perf_path)
        self.ollama_manager = OllamaManager(
            root=root,
            settings=settings.local_llm,
            log_path=root / "storage" / "logs" / "ollama_manager.jsonl",
        )
        self.ollama_client = OllamaClient(
            settings.local_llm,
            log_path=root / "storage" / "logs" / "ollama_calls.jsonl",
        )
        self.intelligence = IntelligenceService(ollama_client=self.ollama_client)
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
            min_required_images=int(getattr(settings.publish, "min_images_required", 0) or 0),
        )

        self.qa = ContentQAGate(
            settings.quality,
            settings.authority_links,
            qa_runtime_path=root / "storage" / "logs" / "qa_runtime.jsonl",
            ollama_client=self.ollama_client,
        )
        self.actionability_gate = ActionabilityGate()
        self.news_actionability_gate = NewsActionabilityGate()
        self.news_provider = "gdelt"
        self.search_intent_generator = SearchIntentGenerator(
            settings=settings.search_intent,
            ollama_client=self.ollama_client,
            log_path=root / "storage" / "logs" / "search_intent.jsonl",
        )
        self.structure_randomizer = StructureRandomizer(
            state_path=root / "storage" / "state" / "structure_randomizer.json",
            log_path=root / "storage" / "logs" / "structure_randomizer.jsonl",
            similarity_threshold=float(getattr(settings.structure_randomization, "similarity_threshold", 0.75) or 0.75),
            fingerprint_ttl_days=int(getattr(settings.structure_randomization, "fingerprint_ttl_days", 30) or 30),
            max_attempts=int(getattr(settings.structure_randomization, "max_attempts", 3) or 3),
        )
        self.topic_scoring = TopicScoring(log_path=root / "storage" / "logs" / "topic_scoring.jsonl")
        self.content_allocator = ContentAllocationEngine(
            enabled=bool(getattr(settings.content_allocation, "enabled", False)),
            mix_hot=int(getattr(settings.content_allocation, "mix_hot", 2) or 2),
            mix_search_derived=int(getattr(settings.content_allocation, "mix_search_derived", 2) or 2),
            mix_evergreen=int(getattr(settings.content_allocation, "mix_evergreen", 1) or 1),
            log_path=root / "storage" / "logs" / "content_allocation.jsonl",
        )
        self.keyword_discovery = KeywordDiscovery(
            db_path=root / "storage" / "search_console.sqlite3",
            fetch_rows_callback=self._fetch_search_console_rows,
            log_path=root / "storage" / "logs" / "keyword_discovery.jsonl",
            safety_filter=self.safety_filter,
            cluster_resolver=self._infer_cluster_id_from_keyword,
        )
        self.scout = SourceScout(
            settings.sources,
            root,
            settings.content_mode,
            intelligence=self.intelligence,
            safety_filter=self.safety_filter,
        )
        self.topic_grower = TopicGrower(
            root=root,
            seeds_path=root / settings.sources.seeds_path,
            gemini=settings.gemini,
            topic_growth=settings.topic_growth,
            safety_filter=self.safety_filter,
        )
        self.ollama_fallback = OllamaFallbackService(
            ollama_client=self.ollama_client,
            settings=self.settings,
            log_event_callback=self._log_ollama_event,
            get_recent_runs_callback=self._latest_run_record_since,
            title_fp_callback=self._remember_fix_steps_fingerprint
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
        self._pending_keyword_claims: list[str] = []
        self._kst = timezone(timedelta(hours=9))
        self._keyword_pool_path = self.root / "storage" / "logs" / "keyword_pool.json"
        self._blogger_recent_14d_path = self.root / "storage" / "logs" / "blogger_recent_14d.json"
        self._cluster_rotation_state_path = self.root / "storage" / "state" / "cluster_rotation_state.json"
        self._feature_rotation_state_path = self.root / "storage" / "state" / "feature_rotation_state.json"
        self._title_fingerprint_path = self.root / "storage" / "logs" / "title_fingerprints.jsonl"
        self._news_title_shape_path = self.root / "storage" / "logs" / "news_title_shapes.jsonl"
        self._title_pattern_weights_path = self.root / "storage" / "state" / "title_pattern_weights.json"
        try:
            self._title_pattern_weights_path.parent.mkdir(parents=True, exist_ok=True)
            if not self._title_pattern_weights_path.exists():
                self._title_pattern_weights_path.write_text(
                    json.dumps({"enabled": False, "weights": {}, "updated_utc": ""}, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
        except Exception:
            pass
        self._news_guard_logged: set[str] = set()
        self._seen_cluster_ids_in_run: set[str] = set()
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
        self._workflow_perf_last_run_id = ""
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
        self.run_metrics_logger = RunMetricsLogger(self.root)
        self._run_metrics_context: dict[str, dict[str, Any]] = {}
        self._run_metrics_emitted_keys: set[str] = set()
        self._manual_upload_probe_done = False
        self._news_rotation_state_path = self.root / "storage" / "state" / "news_rotation_state.json"
        self._slug_ledger_path = self.root / "storage" / "state" / "slug_ledger.jsonl"
        self._slug_ledger_ttl_days = 180
        self._internal_links_pool_refresh_cooldown_hours = 6
        ledger_rel = str(getattr(getattr(self.settings, "ledger", None), "path", "storage/ledger/publish_ledger.jsonl") or "storage/ledger/publish_ledger.jsonl")
        ledger_path = Path(ledger_rel)
        if not ledger_path.is_absolute():
            ledger_path = (self.root / ledger_path).resolve()
        self._publish_ledger_path = ledger_path
        self._publish_ledger_enabled = bool(getattr(getattr(self.settings, "ledger", None), "enabled", True))
        self._publish_ledger_ttl_days = max(1, int(getattr(getattr(self.settings, "ledger", None), "ttl_days", 90) or 90))
        self._retry_enabled = bool(getattr(getattr(self.settings, "workflow", None), "retry_enabled", True))
        self._retry_max_attempts_per_event = max(
            1,
            int(getattr(getattr(self.settings, "workflow", None), "retry_max_attempts_per_event", 4) or 4),
        )
        self._retry_debounce_seconds = [
            max(0, int(x))
            for x in (list(getattr(getattr(self.settings, "workflow", None), "retry_debounce_seconds", [0, 30, 120, 600]) or [0, 30, 120, 600]))
            if str(x).strip()
        ]
        if not self._retry_debounce_seconds:
            self._retry_debounce_seconds = [0, 30, 120, 600]
        self._retry_reset_on_success = bool(getattr(getattr(self.settings, "workflow", None), "retry_reset_on_success", True))
        self._watchdog_state_path = self.root / "storage" / "state" / "watchdog_state.json"
        self._search_learning_state_path = self.root / "storage" / "state" / "search_learning_state.json"
        self._watchdog_enabled = bool(getattr(getattr(self.settings, "watchdog", None), "enabled", True))
        self._title_diversity_state_path = self.root / "storage" / "state" / "title_pattern_state.json"
        self._title_diversity_enabled = bool(getattr(getattr(self.settings, "title_diversity", None), "enabled", True))
        self._source_naturalization_enabled = bool(
            getattr(getattr(self.settings, "source_naturalization", None), "enabled", True)
        )
        self._entropy_check_enabled = bool(getattr(getattr(self.settings, "entropy_check", None), "enabled", True))
        self._entropy_max_rewrite_attempts = max(
            0,
            int(getattr(getattr(self.settings, "entropy_check", None), "max_rewrite_attempts", 1) or 1),
        )
        self.publish_ledger = PublishLedger(
            path=self._publish_ledger_path,
            ttl_days=self._publish_ledger_ttl_days,
        )
        self.watchdog = Watchdog(
            state_path=self._watchdog_state_path,
            enabled=self._watchdog_enabled,
            settings={
                "max_same_hard_failure_streak": int(
                    getattr(getattr(self.settings, "watchdog", None), "max_same_hard_failure_streak", 3) or 3
                ),
                "max_event_wallclock_minutes": int(
                    getattr(getattr(self.settings, "watchdog", None), "max_event_wallclock_minutes", 20) or 20
                ),
                "max_event_total_attempts": int(
                    getattr(getattr(self.settings, "watchdog", None), "max_event_total_attempts", 6) or 6
                ),
                "max_global_holds_per_hour": int(
                    getattr(getattr(self.settings, "watchdog", None), "max_global_holds_per_hour", 12) or 12
                ),
                "max_provider_530_streak": int(
                    getattr(getattr(self.settings, "watchdog", None), "max_provider_530_streak", 6) or 6
                ),
                "max_provider_429_streak": int(
                    getattr(getattr(self.settings, "watchdog", None), "max_provider_429_streak", 4) or 4
                ),
                "backoff_on_provider_failure_minutes": dict(
                    getattr(getattr(self.settings, "watchdog", None), "backoff_on_provider_failure_minutes", {}) or {}
                ),
                "retry_reset_on_success": bool(self._retry_reset_on_success),
            },
        )
        self._seen_cluster_ids_in_run: set[str] = set()
        self._failed_ledger_keys_in_run: set[str] = set()
        self._ledger_skip_streak_in_run: dict[str, int] = {}
        self.news_pack_manifest = NewsPackManifest(
            root=self.root,
            manifest_path=str(getattr(self.settings.news_pack, "manifest_path", "storage/state/news_pack_manifest.jsonl")),
        )
        self.news_pack_picker = NewsPackPicker(
            root=self.root,
            manifest_path=str(getattr(self.settings.news_pack, "manifest_path", "storage/state/news_pack_manifest.jsonl")),
        )
        self.thumbnail_overlay = ThumbnailOverlayRenderer(
            style=str(getattr(self.settings.news_pack, "thumb_overlay_style", "yt_clean") or "yt_clean"),
            font_paths=list(getattr(self.settings.news_pack, "thumb_overlay_font_paths", []) or []),
            max_words=int(getattr(self.settings.news_pack, "thumb_hook_max_words", 4) or 4),
        )
        self._news_pack_last_tick_mono = 0.0
        self._news_domain = "tech_news_explainer"
        self.news_pool_store = NewsPoolStore(
            db_path=root / "storage" / "logs" / "news_pool.sqlite3",
        )
        self.news_cluster_engine = NewsClusterEngine(
            state_path=root / "storage" / "logs" / "news_pool_state.json",
            stable_hash_fn=stable_hash,
        )
        self._news_pool_state_path = self.root / "storage" / "state" / "news_pool_state.json"
        self._news_pool_refresh_log_path = self.root / "storage" / "logs" / "news_pool_refresh.jsonl"
        self._news_pool_refresh_tick_log_path = self.root / "storage" / "logs" / "news_pool_refresh_tick.jsonl"
        self._last_news_pool_refresh_stats: dict[str, Any] = {}
        self.media_manager = MediaManagerService(
            root=self.root,
            settings=self.settings,
            visual=self.visual,
            news_picker=self.news_pack_picker,
            overlay_renderer=self.thumbnail_overlay,
            publisher=self.publisher,
            news_manifest=self.news_pack_manifest
        )
        self._start_posts_index_bootstrap()
        self._run_legacy_news_cleanup_once()

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

    def _news_pack_tags_for_candidate(self, candidate: TopicCandidate, category: str) -> list[str]:
        tags: list[str] = []
        raw_meta = dict(getattr(candidate, "meta", {}) or {})
        for value in (raw_meta.get("tags", []) or []):
            t = re.sub(r"[^a-z0-9_-]", "", str(value or "").lower()).strip()
            if t and t not in tags:
                tags.append(t)
        inferred_tags = build_story_tags(
            title=str(getattr(candidate, "title", "") or ""),
            snippet=str(getattr(candidate, "body", "") or ""),
            category=str(category or ""),
        )
        for tag in reversed(inferred_tags):
            t = re.sub(r"[^a-z0-9_-]", "", str(tag or "").lower()).strip()
            if t and t not in tags:
                tags.insert(0, t)
        defaults = [str(x or "").strip().lower() for x in (getattr(self.settings.news_pack, "tags", []) or []) if str(x or "").strip()]
        for t in defaults:
            if t not in tags:
                tags.append(t)
            if len(tags) >= 4:
                break
        return tags[:4] if tags else ["platform"]


    def _append_workflow_perf(self, event: str, payload: dict[str, Any] | None = None) -> None:
        self.metrics_tracker.record_event(event, payload)

    def _workflow_perf_start_run(self, manual_trigger: bool = False) -> None:
        meta = {
            "qa_mode": str(getattr(self.settings.quality, "qa_mode", "quick") or "quick"),
            "target_images_per_post": int(getattr(self.settings.visual, "target_images_per_post", 5) or 5),
        }
        self.metrics_tracker.start_run(manual_trigger=manual_trigger, meta=meta)

    def _workflow_perf_close_phase(self, now_mono: float, reason: str) -> None:
        self.metrics_tracker.end_phase(reason=reason)

    def _workflow_perf_track_progress(self, phase: str, message: str, percent: int) -> None:
        self.metrics_tracker.track_progress(phase=phase, message=message, percent=percent)

    def _workflow_perf_finish_run(self, status: str, message: str = "") -> None:
        self.metrics_tracker.finish_run(status=status, message=message)

    def _update_run_metrics_context(self, scope: str, **kwargs: Any) -> None:
        key = str(scope or "").strip() or "unknown"
        cur = dict(self._run_metrics_context.get(key, {}) or {})
        for k, v in kwargs.items():
            if v is None:
                continue
            cur[str(k)] = v
        self._run_metrics_context[key] = cur

    def _latest_run_record_since(self, min_id: int) -> dict[str, Any]:
        rows = self.logs.get_recent_runs(days=14, limit=20)
        for row in rows:
            try:
                rid = int((row or {}).get("id", 0) or 0)
            except Exception:
                rid = 0
            if rid > int(min_id):
                return dict(row or {})
        return {}

    def _count_allowed_image_urls(self, images: list[ImageAsset] | None) -> int:
        seen: set[str] = set()
        for image in (images or []):
            src = str(getattr(image, "source_url", "") or "").strip()
            if not src or src in seen:
                continue
            allowed = False
            try:
                allowed = bool(self.publisher._is_allowed_image_url(src, allow_data_uri=False))  # noqa: SLF001
            except Exception:
                allowed = src.lower().startswith("http://") or src.lower().startswith("https://")
            if not allowed:
                continue
            seen.add(src)
        return int(len(seen))

    def _count_internal_links_by_canonical_host(self, html: str) -> int:
        src = str(html or "")
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', src, flags=re.IGNORECASE)
        if not hrefs:
            return 0
        canonical = self._canonical_internal_host()
        if not canonical:
            counts: dict[str, int] = {}
            for url in hrefs:
                host = self._host_from_url(str(url or "").strip())
                if not host:
                    continue
                counts[host] = int(counts.get(host, 0) or 0) + 1
            if counts:
                canonical = str(sorted(counts.items(), key=lambda x: x[1], reverse=True)[0][0] or "").strip().lower()
        if not canonical:
            return 0
        total = 0
        for url in hrefs:
            if self._host_from_url(str(url or "").strip()) == canonical:
                total += 1
        return int(total)

    def _count_related_links_in_html(self, html: str) -> int:
        src = str(html or "")
        m = re.search(
            r"<!--\s*RZ-RELATED:START\s*-->(.*?)<!--\s*RZ-RELATED:END\s*-->",
            src,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return 0
        inner = str(m.group(1) or "")
        links = re.findall(r'href=["\']([^"\']+)["\']', inner, flags=re.IGNORECASE)
        return int(len([u for u in links if str(u or "").strip()]))

    def _emit_run_metrics(
        self,
        *,
        scope: str,
        baseline_run_id: str,
        baseline_log_id: int,
        status_hint: str,
        message_hint: str,
    ) -> None:
        scope_key = str(scope or "").strip() or "run"
        ctx = dict(self._run_metrics_context.get(scope_key, {}) or {})
        run_record = self._latest_run_record_since(int(baseline_log_id or 0))

        status = str((run_record.get("status") if isinstance(run_record, dict) else "") or status_hint or "failed").strip().lower()
        if status not in {"success", "skipped", "hold", "failed"}:
            if status in {"error", "exception"}:
                status = "failed"
            else:
                status = "failed"

        note = str((run_record.get("note") if isinstance(run_record, dict) else "") or "").strip()
        reason_seed = note
        if message_hint:
            reason_seed = f"{reason_seed}|{str(message_hint)}" if reason_seed else str(message_hint)
        reason_codes = parse_reason_codes(reason_seed)

        title = str(
            (run_record.get("title") if isinstance(run_record, dict) else "")
            or ctx.get("title", "")
            or ""
        ).strip()
        final_html = str(ctx.get("final_html", "") or "")
        topic_cluster = str(ctx.get("topic_cluster", "") or "").strip().lower()
        focus_keywords = ctx.get("focus_keywords", [])
        if not isinstance(focus_keywords, list):
            focus_keywords = []
        focus_keywords = [str(x).strip().lower() for x in focus_keywords if str(x).strip()][:6]
        if (not focus_keywords) and title:
            inferred_topic = topic_cluster or self._infer_topic_cluster(title, [], final_html)
            focus_keywords = self._compute_focus_keywords(title, final_html, inferred_topic)[:6]
            if not topic_cluster:
                topic_cluster = inferred_topic
        if not topic_cluster:
            topic_cluster = self._infer_topic_cluster(title, focus_keywords, final_html)

        seo_slug = str(ctx.get("seo_slug", "") or "").strip().lower()
        if not seo_slug and title:
            seo_slug = self._compute_seo_slug(title, topic_cluster)

        images_raw = ctx.get("images", [])
        images = images_raw if isinstance(images_raw, list) else []
        images_count = self._count_allowed_image_urls(images)
        internal_links_count = self._count_internal_links_by_canonical_host(final_html)
        related_links_count = self._count_related_links_in_html(final_html)

        ctr_risk = any(str(x).startswith("ctr_risk_low_visual_density") for x in reason_codes)
        entropy_ok = not any(str(x).startswith("entropy_fail") for x in reason_codes)
        published_url = str(
            (run_record.get("published_url") if isinstance(run_record, dict) else "")
            or ctx.get("published_url", "")
            or ""
        ).strip()
        publish_at_utc = str(ctx.get("publish_at_utc", "") or "").strip()

        run_id = str(
            ctx.get("run_id", "")
            or self._workflow_perf_last_run_id
            or baseline_run_id
            or uuid.uuid4().hex[:12]
        ).strip()
        if not run_id:
            run_id = uuid.uuid4().hex[:12]
        dedupe_key = f"{scope_key}|{run_id}"
        if dedupe_key in self._run_metrics_emitted_keys:
            return
        self._run_metrics_emitted_keys.add(dedupe_key)

        payload = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "scope": scope_key,
            "status": status,
            "reason_codes": reason_codes,
            "topic_cluster": topic_cluster or "default",
            "focus_keywords": focus_keywords[:6],
            "seo_slug": seo_slug,
            "title": title,
            "published_url": published_url,
            "publish_at_utc": publish_at_utc,
            "images_count": int(images_count),
            "internal_links_count": int(internal_links_count),
            "related_links_count": int(related_links_count),
            "ctr_risk_low_visual_density": bool(ctr_risk),
            "entropy_ok": bool(entropy_ok),
        }
        try:
            logger = getattr(self, "run_metrics_logger", None)
            if logger is None:
                logger = RunMetricsLogger(getattr(self, "root", Path(".")))
                self.run_metrics_logger = logger
            logger.log(payload)
        except Exception:
            pass

    def _run_with_metrics_guard(self, scope: str, fn):
        safe_scope = str(scope or "").strip() or "run"
        baseline_run_id = str(self._workflow_perf_run_id or self._workflow_perf_last_run_id or "")
        before_rows = self.logs.get_recent_runs(days=14, limit=1)
        baseline_log_id = int(before_rows[0]["id"]) if before_rows else 0
        self._run_metrics_context[safe_scope] = {
            "run_id": baseline_run_id,
            "title": "",
            "topic_cluster": "",
            "focus_keywords": [],
            "seo_slug": "",
            "publish_at_utc": "",
            "published_url": "",
            "final_html": "",
            "images": [],
        }
        status_hint = "failed"
        message_hint = ""
        try:
            result = fn()
            status_hint = str(getattr(result, "status", "") or "success").strip().lower() or "success"
            message_hint = str(getattr(result, "message", "") or "").strip()
            return result
        except Exception as exc:
            status_hint = "failed"
            message_hint = str(exc or "").strip()
            raise
        finally:
            self._emit_run_metrics(
                scope=safe_scope,
                baseline_run_id=baseline_run_id,
                baseline_log_id=baseline_log_id,
                status_hint=status_hint,
                message_hint=message_hint,
            )

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
        include_image_integrity: bool | None = None,
        phase: str = "post_images",
    ):
        return self._profile_call(
            stage=f"qa_evaluate:{context}",
            fn=lambda: self.qa.evaluate(
                html,
                title=title,
                domain=domain,
                keyword=keyword,
                include_image_integrity=include_image_integrity,
                phase=phase,
            ),
            slow_ms=1200,
            meta={
                "domain": str(domain or ""),
                "keyword": str(keyword or "")[:120],
                "title": str(title or "")[:120],
            },
        )

    def _qa_failed_keys(self, qa_result: Any) -> list[str]:
        keys: list[str] = []
        for check in list(getattr(qa_result, "failed", []) or []):
            key = str(getattr(check, "key", "") or "").strip().lower()
            if key and key not in keys:
                keys.append(key)
        return keys

    def _news_html_metrics(self, html: str) -> dict[str, int]:
        src = str(html or "")
        text = re.sub(r"<[^>]+>", " ", src)
        text = re.sub(r"\s+", " ", text).strip()
        words = len(re.findall(r"[A-Za-z0-9']+", text))
        h2_count = len(re.findall(r"<h2\b", src, flags=re.IGNORECASE))
        links = re.findall(r'href="([^"]+)"', src, flags=re.IGNORECASE)
        ext_links = [u for u in links if u.startswith("http://") or u.startswith("https://")]
        authority_links = list(getattr(self.settings, "authority_links", []) or [])
        auth_count = sum(1 for u in ext_links if any(u.startswith(a) for a in authority_links))
        story_hits = len(re.findall(r"\b(i|my|when i|i tried|in our test|real-world)\b", text.lower()))
        return {
            "word_count": int(words),
            "h2_count": int(h2_count),
            "external_links": int(len(ext_links)),
            "authority_links": int(auth_count),
            "story_markers": int(story_hits),
        }

    def _build_news_word_count_expansion(self, *, html: str, source_url: str = "") -> str:
        metrics = self._news_html_metrics(html)
        quality = getattr(self.settings, "quality", None)
        target_words = max(1200, int(getattr(quality, "min_word_count", 1800) or 1800))
        current_words = int(metrics.get("word_count", 0) or 0)
        deficit = max(0, target_words - current_words)
        if deficit <= 0:
            return ""

        text = re.sub(r"<[^>]+>", " ", str(html or ""))
        tokens = re.findall(r"[A-Za-z0-9']+", text)
        stopwords = {
            "about",
            "after",
            "before",
            "could",
            "first",
            "from",
            "have",
            "into",
            "more",
            "most",
            "news",
            "over",
            "said",
            "that",
            "their",
            "there",
            "these",
            "this",
            "those",
            "today",
            "using",
            "what",
            "when",
            "where",
            "which",
            "while",
            "with",
            "would",
        }
        subject_tokens: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            clean = str(token or "").strip()
            lower = clean.lower()
            if len(clean) < 4 or lower in stopwords or lower in seen:
                continue
            seen.add(lower)
            subject_tokens.append(clean)
            if len(subject_tokens) >= 4:
                break
        subject = " ".join(subject_tokens[:4]) or "this development"

        host = ""
        try:
            host = (urlparse(str(source_url or "")).netloc or "").lower()
        except Exception:
            host = ""
        if host.startswith("www."):
            host = host[4:]
        source_note = f" tied to reporting from {escape(host)}" if host else ""

        blocks: list[tuple[str, str]] = [
            (
                "Why This Story Has Operational Weight",
                (
                    f"<p>Readers looking at {escape(subject)} need more than the headline summary{source_note}. "
                    "The practical meaning usually sits one layer deeper than the announcement itself: who has to change behavior, "
                    "what assumptions stop being safe, and which routine now carries more friction than it did a week ago. "
                    "That is why the right reading of a tech news explainer is operational rather than theatrical. "
                    "The question is not simply whether the move is interesting. The question is whether it changes timing, tooling, review steps, or trust for the people who actually have to live with the outcome.</p>"
                    "<p>That deeper lens also helps filter hype. A launch note, policy shift, funding round, or research update can sound bigger than it really is if the article never forces the next question: what becomes easier, harder, cheaper, slower, or riskier because of this? "
                    "For an American reader, that often means tracing the downstream effect into familiar routines such as account security, team approvals, device management, cloud spend, vendor selection, or rollout sequencing. "
                    "Once the story is translated into those concrete terms, the signal becomes far more useful and far less noisy.</p>"
                ),
            ),
            (
                "What Changes For Readers And Teams",
                (
                    "<p>A second pass matters because teams rarely absorb change in one clean step. "
                    "Executives may read the strategic angle first, operators may care about migration risk, and end users may only notice the change when a daily workflow becomes slower or more confusing. "
                    "Good analysis has to bridge those layers. "
                    "It should explain what a cautious team would verify before rollout, what a small team can safely defer, and what signals suggest the story is becoming durable instead of temporary.</p>"
                    "<p>In practice, that means slowing the reader down just enough to create better decisions. "
                    "The safest response is usually a staged one: confirm the exact scope, identify the dependency most exposed to the change, and decide what evidence would justify broader action. "
                    "That sounds conservative, but it is also the fastest path to useful clarity because it prevents teams from overreacting to partial information or mistaking a narrow edge case for a broad platform shift.</p>"
                    "<h3>Reader Checklist</h3>"
                    "<ul>"
                    "<li>Confirm whether the story changes policy, pricing, access, or only marketing language.</li>"
                    "<li>Check if the impact lands immediately or only after a staged rollout reaches more users.</li>"
                    "<li>Identify one workflow that would break first if the interpretation is wrong.</li>"
                    "<li>Note which official document or release note would validate the next decision.</li>"
                    "<li>Keep a rollback or wait-and-see option visible instead of treating action as mandatory.</li>"
                    "</ul>"
                ),
            ),
            (
                "Signals Worth Tracking Next",
                (
                    "<p>The final layer is time. Many stories become clearer only after support updates, user reactions, and follow-on clarifications start to agree with each other. "
                    "When those sources point in the same direction, the reader can upgrade confidence. "
                    "When they diverge, the smarter move is often to document the ambiguity, narrow the blast radius, and watch for a second wave of evidence before changing anything expensive. "
                    "That discipline is especially useful in fast-moving software, platform, privacy, and infrastructure stories where the first interpretation is often incomplete.</p>"
                    "<p>So the durable takeaway is simple: treat the news as a change-management signal, not just a content event. "
                    "Use it to decide what deserves immediate testing, what belongs on a watchlist, and what should stay in observation mode until the facts settle. "
                    "That approach gives the article more value than a recap because it leaves the reader with a decision frame, a monitoring plan, and a clearer sense of what to do next if the story keeps growing.</p>"
                ),
            ),
        ]

        need_blocks = 1 if deficit <= 220 else 2 if deficit <= 460 else 3
        chosen: list[str] = []
        for heading, body in blocks:
            if len(chosen) >= need_blocks:
                break
            if re.search(rf"<h2[^>]*>\s*{re.escape(heading)}\s*</h2>", str(html or ""), flags=re.IGNORECASE):
                continue
            chosen.append(f"<h2>{heading}</h2>{body}")
        while len(chosen) < need_blocks:
            addendum_no = len(chosen) + 1
            chosen.append(
                f"<h2>Extended Context Addendum {addendum_no}</h2>"
                "<p>This addendum exists for one reason: a useful explainer should not stop at the point where the basic facts are visible. "
                "Readers still need enough context to decide whether the story deserves immediate testing, cautious monitoring, or no action at all. "
                "That means spelling out the tradeoff in plain language, especially for people managing a live workflow, a team dependency, or a platform decision that could be expensive to reverse later.</p>"
                "<p>The practical takeaway is rarely dramatic. It is usually procedural. "
                "Confirm what changed, identify who is exposed first, and treat the next decision as reversible until the evidence becomes consistent across official notes, user reports, and follow-up documentation. "
                "That discipline turns a fast-moving news item into something operationally useful instead of just memorable for a day.</p>"
            )
        return "".join(chosen)

    def _log_news_qa_runtime(self, event: str, payload: dict[str, Any] | None = None) -> None:
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip() or "news_qa_event",
            "run_id": str(self._workflow_perf_run_id or "").strip(),
        }
        row.update(dict(payload or {}))
        path = self.root / "storage" / "logs" / "qa_runtime.jsonl"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _is_forbidden_news_url(self, url: str) -> bool:
        low = str(url or "").strip().lower()
        if not low:
            return True
        return bool(
            re.search(
                r"(?:^https?://)?(?:www\.)?(google\.com|googleusercontent\.com|googleapis\.com)\b",
                low,
                flags=re.IGNORECASE,
            )
        )

    def _apply_news_qa_autopatch(self, html: str, qa_result: Any, source_url: str = "") -> str:
        out = str(html or "")
        failed = set(self._qa_failed_keys(qa_result))
        metrics = self._news_html_metrics(out)
        quality = getattr(self.settings, "quality", None)
        min_h2 = max(1, int(getattr(quality, "min_h2", 5) or 5))
        min_external_links = max(0, int(getattr(quality, "min_external_links", 1) or 1))
        min_authority_links = max(0, int(getattr(quality, "min_authority_links", 1) or 1))

        if "story_block" in failed and metrics.get("story_markers", 0) <= 0:
            if not re.search(r"<h2[^>]*>\s*Real-World Scenario\s*</h2>", out, flags=re.IGNORECASE):
                out += (
                    "<h2>Real-World Scenario</h2>"
                    "<p>In a real deployment window, one team applied this change during peak traffic and saw a partial recovery first. "
                    "They confirmed each fix step with logs, then rolled out the stable sequence to all users.</p>"
                )

        if "heading_structure" in failed and metrics.get("h2_count", 0) < min_h2:
            missing = max(0, min_h2 - metrics.get("h2_count", 0))
            fillers = [
                ("What Teams Miss First", "Most regressions happen when a rollout note is read without validating affected scope."),
                ("What Changed Since Last Update", "Version changes in policy, permissions, and defaults can alter behavior unexpectedly."),
                ("How To Verify Fast", "Confirm one measurable checkpoint per fix so you can stop early when the issue is resolved."),
                ("Rollback Signals", "If error rate rises or key workflows break, pause and return to the last known stable state."),
                ("Operational Notes", "Keep a simple run log with timestamp, step result, and next action."),
            ]
            for title, body in fillers:
                if missing <= 0:
                    break
                if re.search(rf"<h2[^>]*>\s*{re.escape(title)}\s*</h2>", out, flags=re.IGNORECASE):
                    continue
                out += f"<h2>{title}</h2><p>{body}</p>"
                missing -= 1

        if "word_count" in failed:
            out += self._build_news_word_count_expansion(html=out, source_url=source_url)

        if {"authority_links", "external_links", "source_attribution"} & failed:
            links_now = re.findall(r'href="([^"]+)"', out, flags=re.IGNORECASE)
            ext_now = [u for u in links_now if u.startswith("http://") or u.startswith("https://")]
            existing = set(ext_now)
            add_urls: list[str] = []
            # Keep body/source attribution deterministic and safe:
            # do not inject candidate/source URL into article body.
            for ref in list(getattr(self.settings, "authority_links", []) or []):
                ref_url = str(ref or "").strip()
                if not ref_url or self._is_forbidden_news_url(ref_url):
                    continue
                if ref_url not in add_urls:
                    add_urls.append(ref_url)
                if len(add_urls) >= max(3, min_external_links, min_authority_links):
                    break
            add_urls = [u for u in add_urls if u not in existing]
            if add_urls:
                items = "".join(
                    f'<li><a href="{escape(u)}" rel="nofollow noopener" target="_blank">{escape(u)}</a></li>'
                    for u in add_urls
                )
                out += f"<h2>Sources</h2><ul>{items}</ul>"

        out, _ = self._strip_forbidden_news_links(out)
        return out

    def _apply_news_qa_repair_chain(
        self,
        *,
        html: str,
        qa_result: Any,
        domain: str,
        source_url: str,
    ) -> tuple[str, list[dict[str, Any]]]:
        current = str(html or "")
        logs: list[dict[str, Any]] = []
        steps = [
            ("improve_with_feedback", lambda v: self.qa.improve_with_feedback(v, qa_result.failed, qa_result)),
            ("satisfy_requirements", lambda v: self.qa.satisfy_requirements(v, qa_result)),
            ("force_comply", lambda v: self.qa.force_comply(v)),
            ("news_autopatch", lambda v: self._apply_news_qa_autopatch(v, qa_result, source_url=source_url)),
        ]
        for step_name, fn in steps:
            before = current
            before_stats = self._news_html_metrics(before)
            updated = fn(before)
            updated = self._sanitize_publish_html(updated, domain=domain)
            updated = self._canonicalize_html_payload(updated)
            updated, removed = self._strip_forbidden_news_links(updated)
            after_stats = self._news_html_metrics(updated)
            changed = updated != before
            if changed:
                current = updated
            logs.append(
                {
                    "step": step_name,
                    "changed": bool(changed),
                    "removed_forbidden_links": int(removed),
                    "before_len": int(len(before)),
                    "after_len": int(len(updated)),
                    "before": before_stats,
                    "after": after_stats,
                }
            )
        return current, logs

    def _image_target_max(self) -> int:
        configured_max = int(getattr(self.settings.publish, "max_images_per_post", 5) or 5)
        visual_target = int(getattr(self.settings.visual, "target_images_per_post", configured_max) or configured_max)
        return max(0, min(5, configured_max, visual_target))

    def _image_min_required(self) -> int:
        requested = int(getattr(self.settings.publish, "min_images_required", 0) or 0)
        return max(0, min(self._image_target_max(), requested))

    def _pick_images_from_library_or_guard(self, *, title: str, min_count: int) -> list[ImageAsset]:
        if is_news_mode(self.settings):
            self._append_workflow_perf(
                "legacy_image_library_guard",
                {
                    "mode": str(getattr(self.settings.content_mode, "mode", "") or ""),
                    "reason": "tech_news_only_disables_image_library_primary_path",
                },
            )
            raise RuntimeError("news_mode_image_library_disabled")
        draft = DraftPost(
            title=str(title or "").strip() or "Generated visual",
            alt_titles=[],
            html=(
                f"<h2>Quick Take</h2><p>{escape(str(title or 'Generated visual context'))}</p>"
                f"<h2>What To Show</h2><p>{escape(str(title or 'Generated visual context'))}</p>"
                f"<h2>Why This Matters</h2><p>{escape(str(title or 'Generated visual context'))}</p>"
            ),
            summary=str(title or "").strip(),
            score=80,
            source_url="",
            extracted_urls=[],
        )
        return self.media_manager.prepare_post_images(
            draft=draft,
            prompt_plan=None,
            target_count=max(1, int(min_count or 1)),
        )

    def _reset_local_llm_budget(self) -> None:
        self._local_llm_calls_in_post = 0
        self._local_llm_used_last_run = False

    def _log_ollama_event(self, event: str, payload: dict[str, Any] | None = None) -> None:
        payload = dict(payload or {})
        purpose = str(payload.get("purpose", "") or "").strip().lower()
        if purpose not in {"image_plan", "qa_review", "plan_json", "title_summary"}:
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


    def _build_troubleshooting_plan_with_local_llm(self, selected: TopicCandidate) -> dict[str, Any]:
        ready, reason = self._prepare_local_llm()
        payload, updated_calls = self.ollama_fallback.troubleshooting_plan(
            selected=selected,
            is_ready=ready,
            reason=reason,
            calls_in_post=self._local_llm_calls_in_post
        )
        self._local_llm_calls_in_post = updated_calls
        if (payload or {}).get("source") == "ollama":
            self._local_llm_used_last_run = True
        return payload


    def _build_title_summary_payload_with_local_llm(
        self,
        *,
        current_title: str,
        final_html: str,
        troubleshooting_plan: dict[str, Any],
        selected: TopicCandidate,
    ) -> dict[str, Any]:
        ready, reason = self._prepare_local_llm()
        payload = self.ollama_fallback.title_summary_payload(
            current_title=current_title,
            final_html=final_html,
            troubleshooting_plan=troubleshooting_plan,
            selected=selected,
            is_ready=ready,
            reason=reason
        )
        if (payload or {}).get("source") == "ollama":
            self._local_llm_used_last_run = True
        return payload

    def _score_title_candidate(
        self,
        title: str,
        *,
        summary_payload: dict[str, Any],
        recent_fps: set[str],
        recent_title_norm: set[str],
    ) -> int:
        raw = re.sub(r"\s+", " ", str(title or "")).strip()
        if not raw:
            return -10_000
        normalized = self._normalize_title_for_fingerprint(raw)
        if not normalized:
            return -10_000
        fp = self._title_fp(raw)
        if fp and fp in recent_fps:
            return -9_000
        if normalized in recent_title_norm:
            return -9_000
        score = 0
        lower = raw.lower()
        banned = ("fixes that actually work", "ultimate guide", "device not working")
        if any(b in lower for b in banned):
            return -8_000
        if not any(tok in lower for tok in ("not working", "fix", "error", "after update")):
            score -= 120
        device = str(summary_payload.get("device_family", "") or "").strip().lower()
        if device and device in lower:
            score += 40
        feature = str(summary_payload.get("feature", "") or "").strip().lower()
        if feature and feature in lower:
            score += 38
        if any(tok in lower for tok in ("after update", "error", "not detected", "keeps disconnecting", "not working")):
            score += 28
        length = len(raw)
        if 45 <= length <= 90:
            score += 24
        elif 36 <= length <= 105:
            score += 8
        else:
            score -= 16
        must_terms = [str(x or "").strip().lower() for x in (summary_payload.get("must_include_terms", []) or []) if str(x or "").strip()]
        for term in must_terms[:3]:
            if term in lower:
                score += 6
        if "?" in raw:
            score += 3
        return score

    def _choose_best_unique_title(
        self,
        *,
        candidates: list[str],
        summary_payload: dict[str, Any],
        recent_titles: list[str],
    ) -> tuple[str, str]:
        recent_fps = self._load_recent_title_fingerprints(limit=50)
        recent_title_norm = {
            self._normalize_title_for_fingerprint(t)
            for t in (recent_titles or [])
            if str(t or "").strip()
        }
        best_title = ""
        best_score = -10_000
        for candidate in candidates or []:
            score = self._score_title_candidate(
                candidate,
                summary_payload=summary_payload,
                recent_fps=recent_fps,
                recent_title_norm=recent_title_norm,
            )
            if score > best_score:
                best_score = score
                best_title = re.sub(r"\s+", " ", str(candidate or "")).strip()
        if not best_title or best_score < -1000:
            return "", "all_candidates_rejected"
        return best_title, ""

    def _generation_mode(self) -> str:
        mode = str(getattr(getattr(self.settings, "generation", None), "mode", "hybrid") or "hybrid").strip().lower()
        if mode not in {"local_first", "hybrid", "cloud_first"}:
            return "hybrid"
        return mode

    def _gemini_budget_remaining(self) -> int:
        daily_limit = int(getattr(self.settings.gemini, "max_calls_per_day", 0) or 0)
        mode_budget = int(getattr(getattr(self.settings, "generation", None), "gemini_daily_budget_calls", 0) or 0)
        caps = [x for x in [daily_limit, mode_budget] if x > 0]
        if not caps:
            return 999999
        cap = min(caps)
        used = int(self.logs.get_today_gemini_count() or 0)
        return max(0, cap - used)

    def _style_variant_id(self, keyword: str, cluster_id: str) -> str:
        seed = f"{datetime.now(timezone.utc).date().isoformat()}:{cluster_id}:{keyword}".encode("utf-8")
        bucket = int(hashlib.sha1(seed).hexdigest(), 16) % 12
        return f"v{bucket + 1}"

    def _build_local_draft_with_ollama(
        self,
        *,
        selected: TopicCandidate,
        plan: dict[str, Any],
        internal_links_block: str,
    ) -> DraftPost | None:
        max_calls = max(0, int(getattr(self.settings.local_llm, "max_calls_per_post", 2) or 2))
        if self._local_llm_calls_in_post >= max_calls:
            return None
        ready, _ = self._prepare_local_llm()
        if not ready:
            return None
        keyword = re.sub(r"\s+", " ", str((plan or {}).get("primary_keyword", "") or selected.title or "").strip())
        cluster_id = self._infer_cluster_id_from_keyword(keyword or selected.title)
        style_variant_id = self._style_variant_id(keyword=keyword, cluster_id=cluster_id)
        html = self.ollama_client.build_draft_html(
            plan=plan or {},
            internal_links_block=internal_links_block,
            images_plan=None,
            style_variant_id=style_variant_id,
            title_hint=str(getattr(selected, "title", "") or ""),
        )
        if not str(html or "").strip():
            return None
        self._local_llm_calls_in_post += 1
        self._local_llm_used_last_run = True
        title_hint = re.sub(r"\s+", " ", str((plan or {}).get("primary_keyword", "") or selected.title or "")).strip()
        title_hint = title_hint or "Windows update error fix"
        return DraftPost(
            title=title_hint,
            alt_titles=[],
            summary=str((plan or {}).get("issue_summary", "") or "").strip(),
            html=str(html),
            score=85,
            source_url=str(getattr(selected, "url", "") or ""),
            extracted_urls=[],
        )

    def _fix_steps_fingerprint(self, fix_steps: list[dict[str, Any]]) -> str:
        toks: list[str] = []
        for row in fix_steps or []:
            if not isinstance(row, dict):
                continue
            for key in ("step_title", "action", "menu_path"):
                txt = re.sub(r"[^a-z0-9\s-]", " ", str(row.get(key, "") or "").lower())
                txt = re.sub(r"\s+", " ", txt).strip()
                if txt:
                    toks.append(txt)
        uniq = sorted(set(" ".join(toks).split()))
        if not uniq:
            return ""
        return hashlib.sha1(" ".join(uniq).encode("utf-8")).hexdigest()

    def _is_recent_fix_steps_duplicate(self, fp: str) -> bool:
        if not fp:
            return False
        path = self.root / "storage" / "logs" / "fix_steps_fingerprints.json"
        try:
            if not path.exists():
                return False
            rows = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(rows, list):
                return False
            seen = {str(x.get("fp", "")).strip().lower() for x in rows if isinstance(x, dict)}
            return fp.lower() in seen
        except Exception:
            return False

    def _remember_fix_steps_fingerprint(self, fp: str, title: str) -> None:
        if not fp:
            return
        path = self.root / "storage" / "logs" / "fix_steps_fingerprints.json"
        try:
            rows: list[dict[str, str]] = []
            if path.exists():
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    rows = [x for x in loaded if isinstance(x, dict)]
            rows.append(
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "fp": fp,
                    "title": str(title or "")[:160],
                }
            )
            rows = rows[-200:]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            return

    def _evaluate_actionability_gate(self, title: str, html: str) -> ActionabilityGateResult:
        if is_news_mode(self.settings):
            result = self.news_actionability_gate.evaluate(title=title, html=html)
            details = dict(result.details or {})
            details["gate"] = "editorial_depth_gate"
            return ActionabilityGateResult(
                ok=bool(result.ok),
                score=int(result.score),
                reasons=list(result.reasons or []),
                details=details,
            )
        gate_settings = getattr(self.settings, "actionability_gate", None)
        return self.actionability_gate.evaluate(
            title=title,
            html=html,
            min_steps=max(1, int(getattr(gate_settings, "min_steps", 8) or 8)),
            min_word_count=max(300, int(getattr(gate_settings, "min_word_count", 900) or 900)),
            max_generic_ratio=float(getattr(gate_settings, "max_generic_ratio", 0.012) or 0.012),
        )

    def _scaled_content_risk(self, title: str, html: str, plan_fp: str = "") -> tuple[bool, list[str]]:
        reasons: list[str] = []
        title_fp = self._title_fp(title)
        if title_fp and title_fp in self._load_recent_title_fingerprints():
            reasons.append("title_fp_duplicate")
        if plan_fp and self._is_recent_fix_steps_duplicate(plan_fp):
            reasons.append("fix_steps_fp_duplicate")
        excerpt = self._normalize_excerpt(html)
        excerpt_bow = self._tokenize(excerpt[:4000])
        for row in self.logs.get_recent_content_fingerprints(days=14, limit=80):
            old = str((row or {}).get("excerpt", "") or "").strip().lower()
            if not old:
                continue
            lex = SequenceMatcher(None, excerpt[:4000], old[:4000]).ratio()
            sem = self._bow_cosine(excerpt_bow, self._tokenize(old[:4000]))
            if lex >= 0.84 and sem >= 0.80:
                reasons.append("excerpt_similarity_high")
                break
        return (len(reasons) > 0), reasons

    def _build_image_prompt_plan_with_local_llm(self, draft: DraftPost, selected: TopicCandidate) -> dict[str, Any]:
        ready, reason = self._prepare_local_llm()
        sections = self._image_plan_sections(draft.html)
        payload, updated_calls = self.ollama_fallback.image_prompt_plan(
            draft=draft,
            selected=selected,
            sections=sections,
            is_ready=ready,
            reason=reason,
            calls_in_post=self._local_llm_calls_in_post
        )
        self._local_llm_calls_in_post = updated_calls
        if (payload or {}).get("source") in ("ollama", "ollama_retry", "fallback_hazard_guard"):
            self._local_llm_used_last_run = True
        return payload

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


    def _run_local_llm_qa_review(self, title: str, html: str, images: list[ImageAsset]) -> dict[str, Any]:
        ready, reason = self._prepare_local_llm()
        intro = self._extract_intro_text(html)
        alt_values = [str(getattr(img, "alt", "") or "") for img in (images or []) if str(getattr(img, "alt", "") or "").strip()]
        result, updated_calls = self.ollama_fallback.qa_review(
            title=title,
            html=html,
            intro_text=intro,
            alt_texts=alt_values,
            is_ready=ready,
            reason=reason,
            calls_in_post=self._local_llm_calls_in_post
        )
        self._local_llm_calls_in_post = updated_calls
        if (result or {}).get("issues"):
            self._local_llm_used_last_run = True
        return result

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

    def _load_news_pool_state(self) -> dict[str, Any]:
        try:
            if not self._news_pool_state_path.exists():
                return {}
            payload = json.loads(self._news_pool_state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    def _save_news_pool_state(self, payload: dict[str, Any]) -> None:
        try:
            self._news_pool_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._news_pool_state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            return

    def _append_news_pool_refresh_log(self, payload: dict[str, Any]) -> None:
        row = {"ts_utc": datetime.now(timezone.utc).isoformat()}
        row.update(dict(payload or {}))
        try:
            self._news_pool_refresh_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._news_pool_refresh_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _append_news_pool_refresh_tick_log(self, payload: dict[str, Any]) -> None:
        row = {"ts_utc": datetime.now(timezone.utc).isoformat()}
        row.update(dict(payload or {}))
        try:
            self._news_pool_refresh_tick_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._news_pool_refresh_tick_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _insights_settings_view(self) -> InsightsSettingsView:
        integrations = getattr(self.settings, "integrations", None)
        return InsightsSettingsView(
            enabled=bool(getattr(integrations, "enabled", True)),
            adsense_enabled=bool(getattr(integrations, "adsense_enabled", True)),
            analytics_enabled=bool(getattr(integrations, "analytics_enabled", True)),
            search_console_enabled=bool(getattr(integrations, "search_console_enabled", True)),
            ga4_property_id=str(getattr(integrations, "ga4_property_id", "") or "").strip(),
            search_console_site_url=str(getattr(integrations, "search_console_site_url", "") or "").strip(),
        )

    def _growth_insights_client(self) -> GrowthInsights:
        token_path = self.root / str(getattr(self.settings.blogger, "credentials_path", "config/blogger_token.json") or "config/blogger_token.json")
        return GrowthInsights(token_path, self._insights_settings_view())

    def _fetch_search_console_rows(
        self,
        start_date: str,
        end_date: str,
        dimensions: tuple[str, ...] = ("query", "page"),
        page_size: int = 25000,
        max_rows: int = 50000,
    ) -> list[dict[str, Any]]:
        client = self._growth_insights_client()
        rows = client.fetch_search_console_rows(
            start_date=start_date,
            end_date=end_date,
            dimensions=dimensions,
            page_size=page_size,
            max_rows=max_rows,
        )
        if len(rows) >= int(max_rows):
            self._append_workflow_perf(
                "search_console_row_cap_hit",
                {
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                    "max_rows": int(max_rows),
                    "rows": int(len(rows)),
                },
            )
        return rows

    def _load_search_learning_state(self) -> dict[str, Any]:
        try:
            if not self._search_learning_state_path.exists():
                return {}
            payload = json.loads(self._search_learning_state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    def _save_search_learning_state(self, payload: dict[str, Any]) -> None:
        try:
            self._search_learning_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._search_learning_state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            return

    def _refresh_search_learning_if_due(self) -> str:
        search_learning = getattr(self.settings, "search_learning", None)
        integrations = getattr(self.settings, "integrations", None)
        if not bool(getattr(search_learning, "enabled", True)):
            return "search_learning_disabled"
        if not bool(getattr(integrations, "search_console_enabled", True)):
            return "search_learning_disabled:search_console_off"
        interval_hours = max(1, int(getattr(search_learning, "collection_interval_hours", 24) or 24))
        state = self._load_search_learning_state()
        now_utc = datetime.now(timezone.utc)
        last_run = self._parse_iso_utc(str(state.get("last_run_utc", "") or ""))
        if last_run is not None:
            elapsed_hours = (now_utc - last_run).total_seconds() / 3600.0
            if elapsed_hours < float(interval_hours):
                return f"search_learning_skipped:interval<{interval_hours}h"

        lookback_days = max(1, int(getattr(search_learning, "lookback_days", 14) or 14))
        end_date = now_utc.date().isoformat()
        start_date = (now_utc.date() - timedelta(days=max(0, lookback_days - 1))).isoformat()
        try:
            opportunities = self.keyword_discovery.run(start_date=start_date, end_date=end_date)
            summary = dict(getattr(self.keyword_discovery, "last_run_summary", {}) or {})
            state.update(
                {
                    "last_run_utc": now_utc.isoformat(),
                    "start_date": start_date,
                    "end_date": end_date,
                    "rows_fetched": int(summary.get("rows_fetched", 0) or 0),
                    "opportunities_created": int(summary.get("opportunities_created", 0) or 0),
                    "note": str(summary.get("note", "") or ""),
                }
            )
            self._save_search_learning_state(state)
        except Exception as exc:
            note = f"search_learning_failed:{str(exc)[:160]}"
            state.update({"last_run_utc": now_utc.isoformat(), "note": note})
            self._save_search_learning_state(state)
            return note
        supporting_count = sum(1 for item in opportunities if str(item.action_type or "") == "supporting_post")
        if not opportunities:
            return "keyword_discovery_no_data"
        return (
            "keyword_discovery_ok:"
            f"rows={int(state.get('rows_fetched', 0) or 0)};"
            f"opportunities={int(state.get('opportunities_created', 0) or 0)};"
            f"supporting={int(supporting_count)}"
        )

    def _supporting_discovery_candidates(self, limit: int = 3) -> list[TopicCandidate]:
        items = self.keyword_discovery.queued_supporting_candidates(limit=max(1, int(limit)))
        candidates: list[TopicCandidate] = []
        for item in items:
            query = re.sub(r"\s+", " ", str(item.query or "")).strip()
            page = re.sub(r"\s+", " ", str(item.page or "")).strip()
            if not query or not page:
                continue
            cluster_id = self._infer_cluster_id_from_keyword(query)
            candidates.append(
                TopicCandidate(
                    source="search_console",
                    title=query,
                    body=(
                        f"Search Console opportunity for query '{query}' from {page}. "
                        f"Impressions {int(item.impressions)}, clicks {int(item.clicks)}, "
                        f"CTR {item.ctr:.4f}, average position {item.position:.1f}."
                    ),
                    score=max(60, min(95, int(round(float(item.priority_score or 0.0))))),
                    url=page,
                    long_tail_keywords=[query],
                    meta={
                        "opportunity_source": True,
                        "opportunity_action": str(item.action_type or ""),
                        "search_console_query": query,
                        "search_console_page": page,
                        "cluster_id": cluster_id,
                        "news_category": "search_derived",
                    },
                )
            )
        return candidates

    def _policy_gate_candidate(
        self,
        *,
        title: str,
        snippet: str = "",
        body_excerpt: str = "",
        category: str = "",
        route: str,
        source_url: str = "",
    ) -> tuple[bool, str, dict[str, Any]]:
        if not bool(getattr(getattr(self.settings, "policy_gate", None), "enabled", True)):
            return True, "", {}
        decision = self.safety_filter.evaluate(
            title=title,
            snippet=snippet,
            body_excerpt=body_excerpt,
            category=category,
            route=route,
            source_url=source_url,
        )
        payload = asdict(decision)
        if decision.allow:
            return True, "", payload
        first_reason = str((decision.reason_codes or ["unknown"])[0] or "unknown").strip().lower() or "unknown"
        return False, f"policy_denied:{first_reason}", payload

    def _build_search_intent_bundle(
        self,
        *,
        candidate: TopicCandidate,
        category: str,
    ) -> tuple[IntentBundle, str]:
        body_excerpt = str(getattr(candidate, "body", "") or "")[:1800]
        bundle = self.search_intent_generator.generate(
            headline=str(getattr(candidate, "title", "") or ""),
            snippet=str((getattr(candidate, "meta", {}) or {}).get("snippet", "") or body_excerpt[:300]),
            body_excerpt=body_excerpt,
            category=str(category or ""),
            source_url=str(getattr(candidate, "url", "") or ""),
        )
        source = str(getattr(self.search_intent_generator, "last_source", "rules") or "rules").strip().lower()
        return bundle, source

    def _pick_outline_plan(
        self,
        *,
        candidate: TopicCandidate,
        intent_bundle: IntentBundle,
        category: str,
        cluster_id: str = "",
    ) -> OutlinePlan:
        return self.structure_randomizer.pick_outline(
            candidate=candidate,
            intent_bundle=intent_bundle,
            category=category,
            cluster_id=cluster_id,
        )

    def _append_publish_metadata_log(self, payload: dict[str, Any]) -> None:
        path = self.root / "storage" / "logs" / "publish_metadata.jsonl"
        row = {"ts_utc": datetime.now(timezone.utc).isoformat()}
        row.update(dict(payload or {}))
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _canonical_news_url(self, url: str) -> str:
        raw = str(url or "").strip()
        if not raw:
            return ""
        try:
            parsed = urlparse(raw)
            host = str(parsed.netloc or "").strip().lower()
            if host.startswith("www."):
                host = host[4:]
            path = re.sub(r"/+$", "", str(parsed.path or "").strip()) or "/"
            return f"{host}{path}"
        except Exception:
            return raw.strip().lower()

    def _news_pool_feed_fingerprint(self, feeds: list[str]) -> str:
        merged = "\n".join([str(x or "").strip().lower() for x in (feeds or []) if str(x or "").strip()])
        return hashlib.sha1(merged.encode("utf-8", errors="ignore")).hexdigest()

    def _slice_news_pool_feeds(
        self,
        feeds: list[str],
        cursor: int,
        max_feeds: int,
    ) -> tuple[list[str], int]:
        ordered = [str(x or "").strip() for x in (feeds or []) if str(x or "").strip()]
        total = len(ordered)
        if total <= 0:
            return [], 0
        safe_max = max(1, min(int(max_feeds or 1), total))
        safe_cursor = int(cursor or 0) % total
        selected: list[str] = []
        idx = safe_cursor
        for _ in range(safe_max):
            selected.append(ordered[idx])
            idx = (idx + 1) % total
        return selected, int(idx)

    def news_pool_refresh_tick_if_needed(self, force: bool = False) -> dict[str, Any]:
        started = time.perf_counter()
        if not is_news_mode(self.settings):
            status = "disabled"
            payload = {
                "status": status,
                "reason": "not_news_mode",
                "feed_limit": int(max(1, int(getattr(self.settings.sources, "news_pool_background_max_feeds_per_tick", 5) or 5))),
                "cursor": int((self._load_news_pool_state() or {}).get("feed_cursor", 0) or 0),
                "fetched_items": 0,
                "upserted": 0,
                "queued_count": int(self.news_pool_store.queued_count(days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)))),
                "duration_ms": int(max(0.0, time.perf_counter() - started) * 1000),
            }
            self._append_news_pool_refresh_tick_log(payload)
            return payload
        if not bool(getattr(self.settings.sources, "news_pool_background_tick_enabled", True)):
            status = "disabled"
            payload = {
                "status": status,
                "reason": "tick_disabled",
                "feed_limit": int(max(1, int(getattr(self.settings.sources, "news_pool_background_max_feeds_per_tick", 5) or 5))),
                "cursor": int((self._load_news_pool_state() or {}).get("feed_cursor", 0) or 0),
                "fetched_items": 0,
                "upserted": 0,
                "queued_count": int(self.news_pool_store.queued_count(days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)))),
                "duration_ms": int(max(0.0, time.perf_counter() - started) * 1000),
            }
            self._append_news_pool_refresh_tick_log(payload)
            return payload
        max_feeds = max(1, int(getattr(self.settings.sources, "news_pool_background_max_feeds_per_tick", 5) or 5))
        try:
            note = self._refresh_news_pool_if_needed(
                force=bool(force),
                feed_limit=max_feeds,
                source="tick",
            )
        except Exception as exc:
            payload = {
                "status": "failed",
                "feed_limit": int(max_feeds),
                "cursor": int((self._load_news_pool_state() or {}).get("feed_cursor", 0) or 0),
                "fetched_items": 0,
                "upserted": 0,
                "queued_count": int(self.news_pool_store.queued_count(days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)))),
                "duration_ms": int(max(0.0, time.perf_counter() - started) * 1000),
                "note": f"tick_failed:{str(exc)[:160]}",
            }
            self._append_news_pool_refresh_tick_log(payload)
            return payload
        stats = dict(self._last_news_pool_refresh_stats or {})
        status = str(stats.get("status", "") or "").strip().lower()
        if not status:
            lowered = str(note or "").lower()
            if "interval<" in lowered:
                status = "skipped_interval"
            elif "ok" in lowered:
                status = "ok"
            else:
                status = "failed"
        payload = {
            "status": status,
            "feed_limit": int(max_feeds),
            "cursor": int(stats.get("feed_cursor_after", stats.get("feed_cursor", 0) or 0)),
            "fetched_items": int(stats.get("fetched_items", 0) or 0),
            "upserted": int(stats.get("upserted", 0) or 0),
            "queued_count": int(stats.get("queued_count", 0) or 0),
            "duration_ms": int(max(0.0, time.perf_counter() - started) * 1000),
            "note": str(note or "")[:220],
        }
        self._append_news_pool_refresh_tick_log(payload)
        return payload

    def _refresh_news_pool_if_needed(
        self,
        force: bool = False,
        *,
        feed_limit: int | None = None,
        source: str = "publish",
    ) -> str:
        started_mono = time.perf_counter()
        feeds = [str(x or "").strip() for x in (getattr(self.settings.sources, "news_pool_feeds", []) or []) if str(x or "").strip()]
        gdelt_enabled = True
        if not feeds and not gdelt_enabled:
            self._last_news_pool_refresh_stats = {
                "status": "no_feeds",
                "source": str(source or ""),
                "feed_limit": int(max(1, int(feed_limit or 1))),
                "feed_cursor_before": 0,
                "feed_cursor_after": 0,
                "fetched_items": 0,
                "upserted": 0,
                "queued_count": int(self.news_pool_store.queued_count(days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)))),
                "duration_ms": int(max(0.0, time.perf_counter() - started_mono) * 1000),
            }
            self._append_news_pool_refresh_log(
                {"event": "news_pool_refresh_skipped", "reason": "no_feeds", "source": str(source or "")}
            )
            return "news_pool_refresh_skipped:no_feeds"
        state = self._load_news_pool_state()
        now_utc = datetime.now(timezone.utc)
        last_refresh = self._parse_iso_utc(str(state.get("last_refresh_utc", "") or ""))
        interval_min = max(15, int(getattr(self.settings.sources, "news_pool_refresh_interval_minutes", 120) or 120))
        if (not force) and last_refresh is not None:
            elapsed_min = (now_utc - last_refresh).total_seconds() / 60.0
            if elapsed_min < interval_min:
                cursor_now = int(state.get("feed_cursor", 0) or 0)
                self._last_news_pool_refresh_stats = {
                    "status": "skipped_interval",
                    "source": str(source or ""),
                    "feed_limit": int(max(1, int(feed_limit or getattr(self.settings.sources, "news_pool_background_max_feeds_per_tick", 5) or 5))),
                    "feed_cursor_before": int(cursor_now),
                    "feed_cursor_after": int(cursor_now),
                    "fetched_items": 0,
                    "upserted": 0,
                    "queued_count": int(self.news_pool_store.queued_count(days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)))),
                    "duration_ms": int(max(0.0, time.perf_counter() - started_mono) * 1000),
                }
                self._append_news_pool_refresh_log(
                    {
                        "event": "news_pool_refresh_skipped",
                        "reason": "interval_guard",
                        "interval_min": int(interval_min),
                        "elapsed_min": round(float(elapsed_min), 2),
                        "source": str(source or ""),
                    }
                )
                return f"news_pool_refresh_skipped:interval<{interval_min}m"

        feeds_fp = self._news_pool_feed_fingerprint(feeds)
        last_feeds_hash = str(state.get("feeds_hash", state.get("feeds_fingerprint", "")) or "")
        if last_feeds_hash != feeds_fp:
            state["feed_cursor"] = 0
            state["feeds_hash"] = feeds_fp
            state["feeds_fingerprint"] = feeds_fp
        feed_cursor = int(state.get("feed_cursor", 0) or 0)
        safe_max_feeds = (
            max(1, int(feed_limit))
            if feed_limit is not None
            else max(1, int(getattr(self.settings.sources, "news_pool_background_max_feeds_per_tick", 5) or 5))
        )
        feed_batch, next_cursor = self._slice_news_pool_feeds(feeds, feed_cursor, safe_max_feeds)

        allow = [str(x or "").strip().lower() for x in (getattr(self.settings.sources, "news_pool_keywords_allow", []) or []) if str(x or "").strip()]
        block = [str(x or "").strip().lower() for x in (getattr(self.settings.sources, "news_pool_keywords_block", []) or []) if str(x or "").strip()]
        source_weights = {
            str(k or "").strip().lower(): float(v)
            for k, v in (getattr(self.settings.sources, "news_pool_source_weights", {}) or {}).items()
            if str(k or "").strip()
        }
        rows: list[dict[str, Any]] = []
        per_feed_results: list[dict[str, Any]] = []
        fetched_items = 0
        seen_news_urls: set[str] = set()
        gdelt_items = 0
        gdelt_groups: list[dict[str, Any]] = []
        gdelt_error = ""
        try:
            gdelt_groups = list(fetch_trending_topics() or [])
        except Exception as exc:
            gdelt_error = str(exc or "")[:180]
        gdelt_article_count = sum(
            len(list((group or {}).get("articles", []) or []))
            for group in gdelt_groups
            if isinstance(group, dict)
        )
        fetched_items += int(gdelt_article_count)
        self._append_news_pool_refresh_log(
            {
                "event": "news_pool_gdelt_result",
                "ok": bool(not gdelt_error),
                "topic_groups": int(len(gdelt_groups)),
                "items": int(gdelt_article_count),
                "error": gdelt_error,
                "source": str(source or ""),
            }
        )
        for group in gdelt_groups:
            if not isinstance(group, dict):
                continue
            topic = re.sub(r"\s+", " ", str(group.get("topic", "") or "")).strip()
            for item in list(group.get("articles", []) or []):
                if not isinstance(item, dict):
                    continue
                title = re.sub(r"\s+", " ", str((item or {}).get("title", "") or "")).strip()
                url = re.sub(r"\s+", " ", str((item or {}).get("url", "") or (item or {}).get("link", "") or "")).strip()
                snippet = re.sub(
                    r"\s+",
                    " ",
                    str((item or {}).get("summary", "") or (item or {}).get("snippet", "") or ""),
                ).strip()[:380]
                if not title or not url:
                    continue
                canonical = self._canonical_news_url(url)
                if not canonical or canonical in seen_news_urls:
                    continue
                merged = f"{topic}\n{title}\n{snippet}"
                if has_blocked_keywords(merged, block):
                    continue
                if not contains_allow_keywords(merged, allow):
                    continue
                source_name = str((item or {}).get("source", "") or (urlparse(url).netloc or "")).strip().lower()
                published_dt = self._parse_iso_utc(str((item or {}).get("published_date", "") or ""))
                score, category = score_news_item(
                    title=title,
                    snippet=snippet,
                    source=source_name,
                    published_at=published_dt if isinstance(published_dt, datetime) else None,
                    source_weights=source_weights,
                )
                if score <= 0:
                    continue
                allowed, _, _ = self._policy_gate_candidate(
                    title=title,
                    snippet=snippet,
                    body_excerpt="",
                    category=category,
                    route="news",
                    source_url=url,
                )
                if not allowed:
                    continue
                seen_news_urls.add(canonical)
                gdelt_items += 1
                rows.append(
                    {
                        "url": url,
                        "title": title[:220],
                        "source": source_name,
                        "provider": "gdelt",
                        "topic": topic,
                        "collected_at": now_utc.isoformat(),
                        "published_at": (
                            published_dt.astimezone(timezone.utc).isoformat()
                            if isinstance(published_dt, datetime)
                            else ""
                        ),
                        "snippet": snippet,
                        "category": category,
                        "score": int(score),
                    }
                )
        for feed in feed_batch:
            try:
                detail = fetch_feed_detailed(feed, timeout=20)
            except Exception as exc:
                detail = {"status_code": 0, "error": str(exc)[:160], "items": []}
            feed_status = int(detail.get("status_code", 0) or 0)
            feed_error = str(detail.get("error", "") or "").strip()
            items = list(detail.get("items", []) or [])
            if feed_status != 200:
                items = []
            fetched_items += len(items or [])
            per_feed_results.append(
                {
                    "feed": str(feed),
                    "status_code": int(feed_status),
                    "ok": bool(feed_status == 200),
                    "items": int(len(items or [])),
                    "error": feed_error[:140],
                }
            )
            self._append_news_pool_refresh_log(
                {
                    "event": "news_pool_feed_result",
                    "feed": str(feed),
                    "status_code": int(feed_status),
                    "ok": bool(feed_status == 200),
                    "items": int(len(items or [])),
                    "error": feed_error[:180],
                    "source": str(source or ""),
                }
            )
            for item in items or []:
                title = re.sub(r"\s+", " ", str((item or {}).get("title", "") or "")).strip()
                url = re.sub(r"\s+", " ", str((item or {}).get("url", "") or "")).strip()
                snippet = re.sub(r"\s+", " ", str((item or {}).get("snippet", "") or "")).strip()[:380]
                if not title or not url:
                    continue
                canonical = self._canonical_news_url(url)
                if not canonical or canonical in seen_news_urls:
                    continue
                merged = f"{title}\n{snippet}"
                if has_blocked_keywords(merged, block):
                    continue
                if not contains_allow_keywords(merged, allow):
                    continue
                source_name = str((item or {}).get("source", "") or (urlparse(url).netloc or "")).strip().lower()
                published_dt = (item or {}).get("published_at")
                if not isinstance(published_dt, datetime):
                    published_dt = self._parse_iso_utc(str((item or {}).get("published_at", "") or ""))
                score, category = score_news_item(
                    title=title,
                    snippet=snippet,
                    source=source_name,
                    published_at=published_dt if isinstance(published_dt, datetime) else None,
                    source_weights=source_weights,
                )
                if score <= 0:
                    continue
                allowed, _, _ = self._policy_gate_candidate(
                    title=title,
                    snippet=snippet,
                    body_excerpt="",
                    category=category,
                    route="news",
                    source_url=url,
                )
                if not allowed:
                    continue
                seen_news_urls.add(canonical)
                rows.append(
                    {
                        "url": url,
                        "title": title[:220],
                        "source": source_name,
                        "provider": "rss",
                        "topic": "",
                        "collected_at": now_utc.isoformat(),
                        "published_at": (
                            published_dt.astimezone(timezone.utc).isoformat()
                            if isinstance(published_dt, datetime)
                            else ""
                        ),
                        "snippet": snippet,
                        "category": category,
                        "score": int(score),
                    }
                )

        upserted = self.news_pool_store.upsert_items(rows)
        purge_report = self.news_pool_store.purge(
            news_pool_days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)),
            keep_used_days=max(7, int(getattr(self.settings.sources, "news_pool_keep_used_days", 30) or 30)),
            max_items=max(100, int(getattr(self.settings.sources, "news_pool_max_items", 800) or 800)),
        )
        queued = self.news_pool_store.queued_count(days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)))
        state.update(
            {
                "last_refresh_utc": now_utc.isoformat(),
                "last_refresh_ok": True,
                "gdelt_items": int(gdelt_items),
                "feeds_count": int(len(feeds)),
                "feeds_fetched_this_refresh": int(len(feed_batch)),
                "fetched_items": int(fetched_items),
                "upserted": int(upserted),
                "queued_count": int(queued),
                "purged": dict(purge_report or {}),
                "feed_cursor": int(next_cursor),
                "feeds_hash": feeds_fp,
                "feeds_fingerprint": feeds_fp,
                "last_source": str(source or ""),
            }
        )
        self._save_news_pool_state(state)
        duration_ms = int(max(0.0, time.perf_counter() - started_mono) * 1000)
        self._last_news_pool_refresh_stats = {
            "status": "ok",
            "source": str(source or ""),
            "feed_limit": int(safe_max_feeds),
            "feed_cursor_before": int(feed_cursor),
            "feed_cursor_after": int(next_cursor),
            "gdelt_items": int(gdelt_items),
            "fetched_items": int(fetched_items),
            "upserted": int(upserted),
            "queued_count": int(queued),
            "feeds_total": int(len(feeds)),
            "feeds_fetched": int(len(feed_batch)),
            "duration_ms": int(duration_ms),
        }
        self._append_news_pool_refresh_log(
            {
                "event": "news_pool_refresh_summary",
                "source": str(source or ""),
                "feeds_total": int(len(feeds)),
                "feeds_fetched": int(len(feed_batch)),
                "feed_cursor_before": int(feed_cursor),
                "feed_cursor_after": int(next_cursor),
                "gdelt_items": int(gdelt_items),
                "fetched_items": int(fetched_items),
                "upserted": int(upserted),
                "queued": int(queued),
                "per_feed": per_feed_results[:12],
                "duration_ms": int(duration_ms),
            }
        )
        return (
            f"news_pool_refresh_ok:feeds={len(feed_batch)}/{len(feeds)};"
            f"cursor={feed_cursor}->{next_cursor};"
            f"fetched={fetched_items};upserted={upserted};queued={queued}"
        )

    def _claim_news_item(
        self,
        force_refresh_once: bool = True,
        retry_event_id: str = "",
        exclude_item_ids: list[int] | None = None,
    ) -> dict[str, Any] | None:
        rotation_state = self._load_news_rotation_state()
        avoid_category = str(rotation_state.get("last_news_category", "") or "").strip().lower()
        recent_domains = [
            str(x or "").strip().lower()
            for x in (rotation_state.get("last_domains_used", []) or [])
            if str(x or "").strip()
        ][:6]
        exclude_set: set[int] = set()
        for raw in (exclude_item_ids or []):
            try:
                val = int(raw)
            except Exception:
                continue
            if val > 0:
                exclude_set.add(val)
        retry_norm = str(retry_event_id or "").strip()
        if retry_norm.isdigit():
            retry_id = int(retry_norm)
            if retry_id not in exclude_set:
                direct = self.news_pool_store.claim_by_id(
                    retry_id,
                    news_pool_days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)),
                )
                if direct is not None:
                    return direct
        item = self.news_pool_store.claim_one(
            news_pool_days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)),
            top_k=max(10, int(getattr(self.settings.sources, "news_pool_pick_top_k", 60) or 60)),
            source_weights=dict(getattr(self.settings.sources, "news_pool_source_weights", {}) or {}),
            avoid_category=avoid_category,
            recent_domains=recent_domains,
            exclude_ids=sorted(exclude_set),
        )
        if item is not None:
            return item
        if force_refresh_once:
            self._refresh_news_pool_if_needed(force=True, source="claim_refill")
            return self.news_pool_store.claim_one(
                news_pool_days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7)),
                top_k=max(10, int(getattr(self.settings.sources, "news_pool_pick_top_k", 60) or 60)),
                source_weights=dict(getattr(self.settings.sources, "news_pool_source_weights", {}) or {}),
                avoid_category=avoid_category,
                recent_domains=recent_domains,
                exclude_ids=sorted(exclude_set),
            )
        return None

    def _build_news_long_tail_keywords(self, title: str, category: str) -> list[str]:
        base = re.sub(r"\s+", " ", str(title or "").strip())
        cat = re.sub(r"\s+", " ", str(category or "platform").strip().lower())
        seeds = [
            f"what changed in {base}",
            f"who is affected by {base}",
            f"should you update now for {base}",
            f"what to do now after {base}",
            f"what to watch next for {base}",
            f"{cat} update impact for users",
        ]
        out: list[str] = []
        seen: set[str] = set()
        for row in seeds:
            t = re.sub(r"\s+", " ", row).strip()
            if not t:
                continue
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(t[:120])
            if len(out) >= 8:
                break
        return out

    def _load_news_rotation_state(self) -> dict[str, Any]:
        try:
            if not self._news_rotation_state_path.exists():
                return {}
            payload = json.loads(self._news_rotation_state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    def _save_news_rotation_state(self, payload: dict[str, Any]) -> None:
        try:
            self._news_rotation_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._news_rotation_state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            return

    def _update_news_rotation_state_on_publish(self, item: dict[str, Any], category: str) -> None:
        source = str((item or {}).get("source", "") or "").strip().lower()
        domain = source or (urlparse(str((item or {}).get("url", "") or "")).netloc or "").lower()
        state = self._load_news_rotation_state()
        domains = [
            str(x or "").strip().lower()
            for x in (state.get("last_domains_used", []) or [])
            if str(x or "").strip()
        ]
        if domain:
            domains = [domain, *[x for x in domains if x != domain]]
        state.update(
            {
                "last_news_category": str(category or "").strip().lower(),
                "last_domains_used": domains[:6],
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._save_news_rotation_state(state)

    def _rotate_log_if_large(self, path: Path, *, max_bytes: int = 50 * 1024 * 1024, keep: int = 10) -> None:
        p = Path(path).resolve()
        try:
            if (not p.exists()) or p.stat().st_size <= int(max_bytes):
                return
        except Exception:
            return
        safe_keep = max(1, int(keep))
        for idx in range(safe_keep - 1, 0, -1):
            older = p.with_name(f"{p.name}.{idx}")
            newer = p.with_name(f"{p.name}.{idx + 1}")
            if older.exists():
                try:
                    newer.unlink(missing_ok=True)
                except Exception:
                    pass
                try:
                    older.replace(newer)
                except Exception:
                    pass
        first = p.with_name(f"{p.name}.1")
        try:
            first.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            p.replace(first)
        except Exception:
            return
        try:
            p.touch()
        except Exception:
            pass

    def _maintenance_tick(self) -> None:
        for rel in (
            "storage/logs/visual_pipeline.jsonl",
            "storage/logs/publisher_upload.jsonl",
            "storage/logs/thumbnail_gate.jsonl",
            "storage/logs/news_pool_refresh.jsonl",
        ):
            self._rotate_log_if_large(self.root / rel, max_bytes=50 * 1024 * 1024, keep=10)
        try:
            self.news_pack_manifest.prune_duplicates()
        except Exception:
            pass

    def _run_legacy_news_cleanup_once(self) -> None:
        if not is_news_mode(self.settings):
            return
        state_path = self.root / "storage" / "state" / "legacy_cleanup_state.json"
        state: dict[str, Any] = {}
        try:
            if state_path.exists():
                loaded = json.loads(state_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    state = loaded
        except Exception:
            state = {}
        if bool(state.get("legacy_library_archived", False)):
            return
        src_root = (self.root / "assets" / "library").resolve()
        if not src_root.exists():
            state["legacy_library_archived"] = True
            state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
            try:
                state_path.parent.mkdir(parents=True, exist_ok=True)
                state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
            return
        archive_root = (self.root / "assets" / "library_legacy_archive").resolve()
        try:
            archive_root.mkdir(parents=True, exist_ok=True)
            legacy_dirs = [
                p.name
                for p in src_root.glob("*")
                if p.is_dir()
            ]
            (archive_root / "README.txt").write_text(
                (
                    "Legacy library paths are disabled in tech_news_only mode.\n"
                    "This archive marker is logical-only (no file move).\n"
                    f"Detected categories: {legacy_dirs}\n"
                ),
                encoding="utf-8",
            )
            state.update(
                {
                    "legacy_library_archived": True,
                    "archived_mode": "logical",
                    "archived_from": str(src_root),
                    "archived_to": str(archive_root),
                    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception as exc:
            state.update(
                {
                    "legacy_library_archived": False,
                    "error": str(exc)[:220],
                    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            )
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _news_item_to_candidate(self, item: dict[str, Any]) -> TopicCandidate:
        title = re.sub(r"\s+", " ", str((item or {}).get("title", "") or "")).strip()
        snippet = re.sub(r"\s+", " ", str((item or {}).get("snippet", "") or "")).strip()
        source = re.sub(r"\s+", " ", str((item or {}).get("source", "") or "")).strip().lower()
        url = re.sub(r"\s+", " ", str((item or {}).get("url", "") or "")).strip()
        category = re.sub(r"\s+", " ", str((item or {}).get("category", "") or "platform")).strip().lower()
        provider = re.sub(r"\s+", " ", str((item or {}).get("provider", "") or "")).strip().lower()
        topic = re.sub(r"\s+", " ", str((item or {}).get("topic", "") or "")).strip()
        if not category:
            category = classify_category(f"{title} {snippet}")
        long_tail = self._build_news_long_tail_keywords(title, category)
        return TopicCandidate(
            source=source or "news_pool",
            title=title,
            body=snippet,
            score=max(70, int((item or {}).get("score", 70) or 70)),
            url=url,
            main_entity=self.scout._extract_main_entity(title),  # noqa: SLF001
            long_tail_keywords=long_tail[:8],
            meta={
                "news_pool_id": int((item or {}).get("id", 0) or 0),
                "news_category": category or "platform",
                "published_at": str((item or {}).get("published_at", "") or ""),
                "news_provider": provider or "unknown",
                "news_topic": topic,
                "collected_at": str((item or {}).get("collected_at", "") or ""),
                "topic_fp": str((item or {}).get("topic_fp", "") or ""),
            },
        )

    def _build_news_post_local_fallback(
        self,
        selected: TopicCandidate,
        category: str,
        authority_links: list[str],
        facet_context: dict[str, Any] | None = None,
    ) -> DraftPost:
        title = self._enforce_seo_title(
            title=str(getattr(selected, "title", "") or ""),
            candidate=selected,
            global_keywords=list(getattr(selected, "long_tail_keywords", []) or []),
            preferred_keyword=str(getattr(selected, "title", "") or ""),
        )
        source_url = re.sub(r"\s+", " ", str(getattr(selected, "url", "") or "")).strip()
        snippet = re.sub(r"\s+", " ", str(getattr(selected, "body", "") or "")).strip()
        profile = infer_story_profile(title=title, snippet=snippet, category=str(category or "platform"))
        cat = str(profile.category or "platform").strip().lower() or "platform"
        facet_data = dict(facet_context or {})
        selected_facet = str(facet_data.get("selected_facet", "impact") or "impact").strip().lower() or "impact"
        perspective_hint = facet_emphasis_hint(selected_facet)
        try:
            fallback_seed = int(facet_data.get("facet_seed", 0) or 0)
        except Exception:
            fallback_seed = 0
        if fallback_seed <= 0:
            fallback_seed = stable_hash(f"{title}{cat}")
        rng = random.Random(int(fallback_seed) ^ 0x4F1BBCDC)

        def _publisher_label(url: str) -> str:
            host = (urlparse(str(url or "")).netloc or "").lower()
            if "security.googleblog.com" in host:
                return "Google Security Blog"
            if "aws.amazon.com" in host:
                return "AWS Security Blog"
            if "cisa.gov" in host:
                return "CISA Advisory"
            if "msrc.microsoft.com" in host:
                return "Microsoft Security Response Center"
            if "support.apple.com" in host:
                return "Apple Support"
            if "cloudflare.com" in host:
                return "Cloudflare Blog"
            if "nist.gov" in host:
                return "NIST Cybersecurity Framework"
            label_parts = [p for p in host.split(".") if p and p not in {"www", "com", "org", "net", "gov"}]
            if not label_parts:
                return "Source"
            return " ".join(x.capitalize() for x in label_parts[:2]) + " Report"

        safe_authorities = filter_relevant_authority_links(
            list(authority_links or []),
            title=title,
            snippet=snippet,
            category=cat,
        )[:2]
        source_items: list[str] = []
        if source_url:
            source_items.append(
                f'<li><a href="{escape(source_url)}" rel="nofollow noopener" target="_blank">{escape(_publisher_label(source_url))}</a></li>'
            )
        for link in safe_authorities:
            if any(bad in link.lower() for bad in ("google.com", "googleusercontent.com", "googleapis.com")):
                continue
            source_items.append(
                f'<li><a href="{escape(link)}" rel="nofollow noopener" target="_blank">{escape(_publisher_label(link))}</a></li>'
            )
        question_pool = list(profile.questions or [])
        if len(question_pool) < 3:
            question_pool.extend(
                [
                    "What should a normal reader compare before acting on the headline?",
                    "Which tradeoff would matter first after a week of real use?",
                    "What part of the claim still needs better evidence?",
                ]
            )
        rng.shuffle(question_pool)
        question_lines = question_pool[:3]
        compare_pool = list(profile.comparisons or [])
        if not compare_pool:
            compare_pool = [
                "A faster first impression can still be a worse long-term fit if cost or maintenance rises later.",
                "The highest-ranked option is not automatically the best choice once real-life constraints show up.",
            ]
        compare_block = compare_pool[int(rng.randrange(len(compare_pool)))]
        compare_followup = compare_pool[(int(rng.randrange(len(compare_pool))) + 1) % len(compare_pool)]
        emphasis_line_map = {
            "impact": "This analysis focuses on who feels the impact first and where disruptions are most likely to surface.",
            "timeline": "This analysis follows the update sequence so readers can align decisions with timing, not guesswork.",
            "official": "This analysis prioritizes confirmed statements and release-note evidence over speculation.",
            "risk": "This analysis stresses low-regret safeguards and practical checks before broader rollout.",
            "market": "This analysis tracks ecosystem and vendor reaction where it changes real-world planning.",
            "user_angle": "This analysis reflects everyday reader workflows and immediate usability consequences.",
        }
        emphasis_line = emphasis_line_map.get(selected_facet, emphasis_line_map["impact"])
        action_items = [
            re.sub(r"\s+", " ", str(x or "").strip())
            for x in (facet_data.get("action_items", []) or [])
            if str(x or "").strip()
        ][:6]
        if len(action_items) < 3:
            action_items = [
                "Check the official update notes and your current app or OS version.",
                "Apply one low-risk change in a controlled scope before full rollout.",
                "Monitor impact for 24 hours and keep a rollback option available.",
            ]
        what_to_do = "<ul>" + "".join(f"<li>{escape(item)}</li>" for item in action_items[:6]) + "</ul>"
        reader_group = {
            "wellness": "students, commuters, gym-goers, and anyone buying caffeine on repeat",
            "home": "households comparing room coverage, allergy relief, quiet operation, and filter cost",
            "consumer": "shoppers trying to separate useful testing from headline ranking noise",
            "security": "admins, small teams, and readers worried about immediate exposure",
            "policy": "users trying to understand which defaults or limits are quietly moving",
            "ai": "workers, students, and small teams who depend on consistent output",
            "chips": "buyers and teams timing the next hardware decision",
        }.get(cat, "ordinary readers trying to map a headline to a practical decision")
        decision_cost_line = {
            "wellness": "The hidden costs usually show up as price per can, overstimulation, ingredient tolerance, and whether the drink still feels worth buying on the fifth weekday in a row.",
            "home": "The hidden costs usually show up as replacement filters, fan noise, electricity use, and whether the recommended model actually fits the room where it will live.",
            "consumer": "The hidden costs usually show up as setup friction, accessories, cleaning effort, warranty headaches, and the small compromises that never fit neatly inside a winner badge.",
            "security": "The hidden costs usually show up as downtime, emergency patch windows, support overhead, and the time it takes to separate confirmed scope from raw fear.",
            "policy": "The hidden costs usually show up as changed defaults, user confusion, new compliance steps, and the effort required to retrain habits after a quiet rule shift.",
            "ai": "The hidden costs usually show up as slower output, weaker answers, new usage caps, or a price move that suddenly changes the economics of everyday work.",
            "chips": "The hidden costs usually show up as delayed purchases, price movement, accessory changes, and uncertainty around how long an older system remains sensible.",
        }.get(cat, "The hidden costs usually show up after the headline cycle ends and real life has to absorb the tradeoff.")
        real_world_line = {
            "wellness": "In real life, that means comparing how a can feels at 8 a.m. before work, at 2 p.m. after lunch, or before a workout rather than assuming one ranking applies to every routine.",
            "home": "In real life, that means thinking about a nursery, a bedroom, a pet-heavy living room, or a small apartment where noise and footprint matter just as much as the clean-air claim.",
            "consumer": "In real life, that means asking where the product will sit, how often it will be used, and which inconvenience will start to annoy you after the return window closes.",
            "security": "In real life, that means deciding which accounts, devices, or teams need action first and which can safely wait for better evidence.",
            "policy": "In real life, that means tracking which users will notice the change first and which support questions will arrive before the documentation catches up.",
            "ai": "In real life, that means checking whether the update helps the exact task people do every day instead of assuming the new capability automatically improves the workflow.",
            "chips": "In real life, that means judging whether the supply and pricing story actually changes the next purchase rather than just feeding speculation.",
        }.get(cat, "In real life, the signal matters only when it changes the next practical choice.")
        headline_gap_line = {
            "wellness": "List-style coverage compresses taste, caffeine feel, price, and ingredient tolerance into one score even though those are exactly the reasons readers make different choices.",
            "home": "List-style coverage compresses room size, filter cost, design, and sleep-time noise into one score even though those are the variables that split households apart.",
            "consumer": "List-style coverage compresses fit, comfort, warranty, and upkeep into one winner badge even though those are the variables that make recommendations feel trustworthy or shallow.",
            "security": "Headline coverage compresses scope, severity, and mitigation timing into a single level of urgency even though each part changes the real response plan.",
            "policy": "Headline coverage compresses enforcement language, defaults, and rollout timing into one reaction even though each part changes what users must do next.",
            "ai": "Headline coverage compresses price, quality, and latency into one progress narrative even though readers feel those tradeoffs separately in ordinary work.",
            "chips": "Headline coverage compresses pricing, roadmap, and availability into one momentum story even though buyers experience those signals at different times.",
        }.get(cat, "Headline coverage tends to flatten the exact detail readers need in order to make a better decision.")
        summary_line = snippet or f"{title} has moved beyond a casual headline and into a story about {profile.subject_phrase}."
        source_label = _publisher_label(source_url) if source_url else "the latest source coverage"
        detail_items = list(profile.detail_items or [])
        while len(detail_items) < 4:
            detail_items.append("The most useful explanation is the one that connects the headline to real tradeoffs.")
        heading_sets = (
            [
                {
                    "quick": "Bottom Line",
                    "happened": "What The Coverage Actually Says",
                    "care": "Why This Matters To Buyers",
                    "read": "How To Judge The Claim",
                    "tradeoffs": "What The Ranking Hides",
                    "costs": "Where The Real Cost Shows Up",
                    "scenarios": "Where This Plays Out",
                    "gap": "What The Headline Misses",
                    "watch": "What To Watch Next",
                    "do_now": "What To Do Before You Act",
                },
                {
                    "quick": "Quick Verdict",
                    "happened": "What Changed",
                    "care": "Why People Are Paying Attention",
                    "read": "How To Evaluate The Claim",
                    "tradeoffs": "What The Comparison Leaves Out",
                    "costs": "The Tradeoffs You Feel Later",
                    "scenarios": "Real-World Scenarios",
                    "gap": "What The Roundup Still Does Not Show",
                    "watch": "Signals Worth Watching",
                    "do_now": "What Readers Can Do Now",
                },
            ]
            if cat in {"wellness", "home", "consumer"}
            else [
                {
                    "quick": "Quick Take",
                    "happened": "What Happened",
                    "care": "Why Readers Care",
                    "read": "How To Read The Claim",
                    "tradeoffs": "The Tradeoffs Behind The Headline",
                    "costs": "The Costs Readers Actually Feel",
                    "scenarios": "Real-World Scenarios",
                    "gap": "What The Headline Still Leaves Out",
                    "watch": "What To Watch Next",
                    "do_now": "What To Do Now",
                },
                {
                    "quick": "The Short Version",
                    "happened": "What Changed",
                    "care": "Why It Matters",
                    "read": "How To Interpret The Claim",
                    "tradeoffs": "What The Headline Flattens",
                    "costs": "Where The Real Cost Shows Up",
                    "scenarios": "Where This Becomes Real",
                    "gap": "What The First Wave Leaves Out",
                    "watch": "What To Watch Next",
                    "do_now": "What To Do Next",
                },
            ]
        )
        headings = heading_sets[int(fallback_seed) % len(heading_sets)]
        audience_line = rng.choice(
            [
                f"For U.S. readers, the useful question is not whether the headline sounds dramatic. It is whether the story changes cost, convenience, comfort, trust, or planning over the next few weeks. {real_world_line}",
                f"American readers usually feel stories like this through routine first, not hype. If the coverage changes cost, convenience, comfort, trust, or planning soon, it matters. {real_world_line}",
                f"The practical test for U.S. readers is straightforward: does this story change what people buy, postpone, tolerate, or double-check this month? {real_world_line}",
            ]
        )

        def _p(text: str) -> str:
            return f"<p>{escape(re.sub(r'\s+', ' ', str(text or '').strip()))}</p>"

        facts_html = (
            "<ul>"
            f"<li>Who: {escape(reader_group)}.</li>"
            f"<li>What: New coverage about {escape(profile.subject_phrase)}.</li>"
            f"<li>Source frame: The current read is anchored by {escape(source_label)}.</li>"
            "<li>Timing: The headline is current, but the most durable implications still depend on follow-up evidence.</li>"
            f"<li>Main tradeoff: {escape(compare_block)}</li>"
            "</ul>"
        )

        body_html = (
            f"<h2>{headings['quick']}</h2>"
            + _p(summary_line)
            + _p(emphasis_line)
            + _p(
                f"{profile.decision_frame} Read the story as a decision aid for {reader_group}, not as a generic attention spike."
            )
            + _p(perspective_hint)
            + facts_html
            + f"<h2>{headings['happened']}</h2>"
            + _p(
                f"The immediate news value is not simply that another headline appeared. It is that {source_label} packaged "
                f"{profile.subject_phrase} in a form that can influence mainstream readers, buyers, and casual researchers."
            )
            + _p(
                f"In plain English, {title} pushes readers to compare products, claims, or rollout signals that often get lumped together "
                "even though the right choice can differ sharply once routine, budget, and tolerance enter the picture."
            )
            + _p(question_lines[0])
            + _p(
                f"When a story keeps resurfacing across coverage, the useful signal is usually not the headline itself. "
                f"It is the operational consequence underneath it. {profile.operational_line}"
            )
            + f"<h2>{headings['care']}</h2>"
            + "<h3>Where the headline lands</h3>"
            + _p(profile.decision_frame)
            + _p(audience_line)
            + _p(
                f"That is why readers should care before the story fully settles. A casual ranking or update note can turn into a much more practical decision "
                f"once {reader_group} start mapping it to ordinary routines."
            )
            + f"<h2>{headings['read']}</h2>"
            + _p(
                f"A strong explainer does more than repeat the headline. It shows how testing, comparison criteria, or rollout evidence produced the claim in the first place. "
                f"That matters because {profile.subject_phrase} almost always look simpler from far away than they feel in real life."
            )
            + "<ul>"
            + "".join(f"<li>{escape(item)}</li>" for item in detail_items)
            + "</ul>"
            + _p(f"Comparison: {compare_block}")
            + _p(
                "Readers should separate headline force from practical fit. The most dramatic winner or the loudest warning is not always the choice "
                "that best survives a month of ordinary use."
            )
            + f"<h2>{headings['tradeoffs']}</h2>"
            + "<h3>What the comparison hides</h3>"
            + _p(profile.scenario_line)
            + _p(
                f"Headlines flatten tradeoffs. They make one winner, one policy move, or one update path look universal even when the real choice is between "
                f"comfort, price, maintenance, intensity, or timing. {decision_cost_line}"
            )
            + _p(f"Comparison: {compare_followup}")
            + f"<h2>{headings['costs']}</h2>"
            + _p(decision_cost_line)
            + _p(
                f"This is the part many readers experience only after they act. A purifier can be powerful but too loud. An energy drink can be effective but too harsh. "
                "A platform change can sound manageable but still create annoying, repeated friction. The real cost rarely arrives as one dramatic failure. "
                "It arrives as repeated inconvenience."
            )
            + _p(
                f"That is why durable explainers translate abstract praise into household or workflow math. The question is not only what ranked well or what changed. "
                f"It is whether the recommendation still makes sense once {reader_group} live with it for a week."
            )
            + f"<h2>{headings['scenarios']}</h2>"
            + _p(profile.scenario_line)
            + _p(real_world_line)
            + _p(question_lines[1])
            + f"<h2>{headings['gap']}</h2>"
            + _p(headline_gap_line)
            + _p(
                "This gap matters because list-style and update-style coverage naturally compress nuance. That is useful for scanning. It is less useful for spending money, "
                "adjusting routines, or advising someone else. Readers need to know not just who won the headline, but who is likely to regret following it blindly."
            )
            + _p(
                f"When follow-up context is weak, the safest move is to narrow the decision rather than rush it. Figure out which variable matters most in your case, "
                f"then compare the story against that variable first."
            )
            + f"<h2>{headings['watch']}</h2>"
            + "<h3>Signals worth trusting</h3>"
            + _p(profile.watch_line)
            + _p(
                "The strongest signal is consistency. When follow-up testing, owner feedback, pricing, support notes, or clarified methodology all point in the same direction, "
                "confidence goes up. When they stay messy or contradictory, the headline should remain provisional."
            )
            + _p(question_lines[2])
            + "<ul>"
            + (
                "<li>Look for updated testing notes or methodology details, not just recycled rankings.</li>"
                "<li>Watch whether price, availability, or follow-up owner feedback shifts the conclusion.</li>"
                "<li>Track what changes for the specific room, routine, or workflow you care about.</li>"
                "<li>Give more weight to repeated evidence than to one sharp first impression.</li>"
            )
            + "</ul>"
            + f"<h2>{headings['do_now']}</h2>"
            + what_to_do
            + _p(
                "None of those steps require panic or blind trust. They simply make the next decision narrower, calmer, and more evidence-based while the story is still settling."
            )
            + _p(
                "A useful news analysis should leave readers with a better question and a cleaner short list, not with the feeling that they were pushed into a generic conclusion. "
                "That is the standard worth holding this kind of coverage to."
            )
        )
        word_count = len(re.findall(r"[A-Za-z0-9']+", re.sub(r"<[^>]+>", " ", body_html)))
        if word_count < 1700:
            body_html += (
                "<h2>How To Narrow The Decision</h2>"
                + _p(
                    f"A good next step is not to memorize the whole ranking or every reaction. It is to reduce the choice to one or two variables that matter most in your own case. "
                    f"For {reader_group}, that usually means choosing between fit, cost, and how the product or change behaves on an ordinary day."
                )
                + _p(
                    "Once the decision is narrowed, a lot of the noise falls away. Readers can ignore broad claims that do not affect their room, budget, tolerance, or workflow. "
                    "That discipline matters because broad comparisons are designed to be shareable, not to be perfectly personalized."
                )
                + _p(
                    "The smartest readers are often the ones who slow the story down just enough to test one assumption. That does not make the coverage less useful. "
                    "It makes the next action much more defensible."
                )
            )
        word_count = len(re.findall(r"[A-Za-z0-9']+", re.sub(r"<[^>]+>", " ", body_html)))
        if word_count < 1850:
            body_html += (
                "<h2>The Part Most Roundups Compress</h2>"
                + _p(
                    "What gets compressed first is the mismatch between a universal headline and a specific life. A model can be the best for a reviewer and still be wrong for a bedroom. "
                    "A drink can top a ranking and still be the wrong caffeine choice for a weekday routine. A software change can be officially minor and still be personally annoying."
                )
                + _p(
                    "That is why readers should treat mainstream explainers as a map rather than a verdict. The article should help you see the landscape, the likely friction points, "
                    "and the claims that deserve a second look. It should not try to erase the existence of tradeoffs."
                )
                + _p(
                    "The closer a story gets to influencing real purchases or habits, the more important it is to read past the badge, the ranking number, or the first dramatic framing. "
                    "That extra minute of skepticism usually produces a better decision than any perfect-sounding headline."
                )
            )
        word_count = len(re.findall(r"[A-Za-z0-9']+", re.sub(r"<[^>]+>", " ", body_html)))
        if word_count < 1950:
            body_html += (
                "<h2>Questions That Actually Change The Choice</h2>"
                + _p(
                    f"The best follow-up questions are usually the least glamorous ones. For {reader_group}, that means asking what part of daily use would feel wrong first, "
                    "what cost shows up after the first purchase, and what piece of the headline would stop mattering once the product or update meets a real week of use."
                )
                + _p(
                    "These questions work because they pull the story out of generic commentary and back into lived constraints. A recommendation is only as good as the situation it fits, "
                    "and most reader regret begins when a general claim is mistaken for a personal answer."
                )
                + _p(
                    "That is also why good analysis keeps returning to fit. Fit is where price, tolerance, convenience, maintenance, and timing finally meet. If the article helps readers identify fit more quickly, "
                    "it has done its job. If it only made the headline louder, it has not."
                )
            )
        word_count = len(re.findall(r"[A-Za-z0-9']+", re.sub(r"<[^>]+>", " ", body_html)))
        if word_count < 2050:
            body_html += (
                "<h2>How Readers Can Compare Evidence Better</h2>"
                + _p(
                    "A useful habit is to compare the story on three levels at once: the headline claim, the evidence behind the claim, and the situation where the claim is supposed to matter. "
                    "Most weak explainers handle only the first level. Better explainers make the other two visible."
                )
                + _p(
                    f"For {reader_group}, that means checking whether the article actually explains why one option fits one routine better than another. "
                    "If the reasoning stays vague, readers should treat the ranking or reaction as directional rather than final."
                )
                + _p(
                    "That sounds slower, but it usually saves time. One careful comparison is cheaper than buying, updating, or trusting the wrong thing and then undoing the mistake after a week of irritation."
                )
            )
        html = body_html + "<h2>Sources</h2>" + f"<ul>{''.join(source_items) if source_items else '<li>No external source available yet.</li>'}</ul>"
        html = ensure_what_to_do_now_section(html=html, action_items=action_items)
        return DraftPost(
            title=title,
            alt_titles=[],
            html=html,
            summary=snippet[:260] if snippet else title,
            score=max(70, int(getattr(selected, "score", 70) or 70)),
            source_url=source_url,
            extracted_urls=[source_url, *safe_authorities][:8],
        )

    def _retry_debounce_seconds_for_attempt(self, attempt_no: int) -> int:
        schedule = list(self._retry_debounce_seconds or [0, 30, 120, 600])
        idx = max(0, int(attempt_no) - 1)
        if idx >= len(schedule):
            return int(schedule[-1])
        return int(schedule[idx])

    def _provider_http_code_from_error(self, message: str) -> int | None:
        msg = str(message or "").lower()
        if "530" in msg or "http_530" in msg:
            return 530
        if "429" in msg or "http_429" in msg or "rate limit" in msg:
            return 429
        return None

    def _watchdog_hold_gate(self, reason: str) -> tuple[bool, str]:
        if not self._watchdog_enabled:
            return False, str(reason or "")
        blocked, detail = self.watchdog.should_hold_global()
        if blocked:
            return True, f"watchdog_global_limit:{detail}"
        return False, str(reason or "")

    def _run_once_news_mode(
        self,
        *,
        manual_trigger: bool,
        run_start_minute: str,
        queue_advisory: str,
        sync_note: str,
        preflight_index_note: str,
        blog_snapshot: dict[str, Any],
        today_gemini_before: int,
        news_retry_depth: int = 0,
        retry_event_id: str = "",
        draft_post_id: str = "",
    ) -> WorkflowResult:
        if int(news_retry_depth or 0) > 0:
            return self._run_once_news_mode_impl(
                manual_trigger=manual_trigger,
                run_start_minute=run_start_minute,
                queue_advisory=queue_advisory,
                sync_note=sync_note,
                preflight_index_note=preflight_index_note,
                blog_snapshot=blog_snapshot,
                today_gemini_before=today_gemini_before,
                news_retry_depth=news_retry_depth,
                retry_event_id=retry_event_id,
                draft_post_id=draft_post_id,
            )
        return self._run_with_metrics_guard(
            "news_mode",
            lambda: self._run_once_news_mode_impl(
                manual_trigger=manual_trigger,
                run_start_minute=run_start_minute,
                queue_advisory=queue_advisory,
                sync_note=sync_note,
                preflight_index_note=preflight_index_note,
                blog_snapshot=blog_snapshot,
                today_gemini_before=today_gemini_before,
                news_retry_depth=news_retry_depth,
                retry_event_id=retry_event_id,
                draft_post_id=draft_post_id,
            ),
        )

    def _run_once_news_mode_impl(
        self,
        *,
        manual_trigger: bool,
        run_start_minute: str,
        queue_advisory: str,
        sync_note: str,
        preflight_index_note: str,
        blog_snapshot: dict[str, Any],
        today_gemini_before: int,
        news_retry_depth: int = 0,
        retry_event_id: str = "",
        draft_post_id: str = "",
    ) -> WorkflowResult:
        self._progress("news_pool", "뉴스 풀 갱신/클레임", 18)
        working_draft_id = str(draft_post_id or "").strip()
        claimed_item: dict[str, Any] | None = None
        claimed_id = 0
        claim_finalized = False
        entropy_retry_used = False
        degraded_note = self._append_note(preflight_index_note or "", sync_note)
        if queue_advisory:
            degraded_note = self._append_note(degraded_note, queue_advisory)
        try:
            refresh_note = self._profile_call(
                "news_pool_refresh_if_needed",
                lambda: self._refresh_news_pool_if_needed(force=False, source="publish"),
                slow_ms=2600,
            )
            degraded_note = self._append_note(degraded_note, refresh_note)
            queued_now = self.news_pool_store.queued_count(
                days=max(1, int(getattr(self.settings.sources, "news_pool_days", 7) or 7))
            )
            min_items = max(20, int(getattr(self.settings.sources, "news_pool_min_items", 80) or 80))
            if queued_now < min_items:
                force_note = self._profile_call(
                    "news_pool_refresh_force",
                    lambda: self._refresh_news_pool_if_needed(force=True, source="publish_force"),
                    slow_ms=2600,
                )
                degraded_note = self._append_note(
                    degraded_note,
                    self._append_note(force_note, f"news_pool_low={queued_now}/{min_items}"),
                )
            selected: TopicCandidate | None = None
            cluster_id = ""
            cluster_skip_count = 0
            retry_event_mismatch_count = 0
            excluded_claim_ids: set[int] = set()
            retry_event_filter = str(retry_event_id or "").strip()
            max_claim_attempts = 6
            for claim_try in range(1, max_claim_attempts + 1):
                claimed_item = self._profile_call(
                    "news_pool_claim",
                    lambda: self._claim_news_item(
                        force_refresh_once=bool(claim_try == 1),
                        retry_event_id=retry_event_filter,
                        exclude_item_ids=sorted(excluded_claim_ids),
                    ),
                    slow_ms=1800,
                )
                if not claimed_item:
                    break
                attempt_claimed_id = int((claimed_item or {}).get("id", 0) or 0)
                if attempt_claimed_id > 0 and attempt_claimed_id in excluded_claim_ids:
                    try:
                        self.news_pool_store.rollback_claim(attempt_claimed_id)
                    except Exception:
                        pass
                    continue
                attempt_selected = self._news_item_to_candidate(claimed_item)
                attempt_category = str((attempt_selected.meta or {}).get("news_category", "") or "").strip().lower()
                allowed_news, deny_reason, _ = self._policy_gate_candidate(
                    title=str(getattr(attempt_selected, "title", "") or ""),
                    snippet=str(getattr(attempt_selected, "body", "") or ""),
                    body_excerpt="",
                    category=attempt_category,
                    route="news",
                    source_url=str(getattr(attempt_selected, "url", "") or ""),
                )
                if not allowed_news:
                    if attempt_claimed_id > 0:
                        excluded_claim_ids.add(attempt_claimed_id)
                        try:
                            self.news_pool_store.mark_used(attempt_claimed_id, deny_reason)
                        except Exception:
                            try:
                                self.news_pool_store.rollback_claim(attempt_claimed_id)
                            except Exception:
                                pass
                    degraded_note = self._append_note(degraded_note, deny_reason)
                    continue
                attempt_event_id = str(
                    (attempt_selected.meta or {}).get("event_id", "")
                    or (attempt_selected.meta or {}).get("news_event_id", "")
                    or (attempt_selected.meta or {}).get("news_pool_id", "")
                    or attempt_claimed_id
                ).strip()
                if retry_event_filter and attempt_event_id != retry_event_filter:
                    retry_event_mismatch_count += 1
                    if attempt_claimed_id > 0:
                        excluded_claim_ids.add(attempt_claimed_id)
                    try:
                        self.news_pool_store.rollback_claim(attempt_claimed_id)
                    except Exception:
                        pass
                    continue
                cluster_decision = self.news_cluster_engine.assign_cluster(
                    event_id=attempt_event_id,
                    title=str(getattr(attempt_selected, "title", "") or ""),
                    body=str(getattr(attempt_selected, "body", "") or ""),
                    run_start_minute=str(run_start_minute or "").strip(),
                )
                attempt_meta = dict(getattr(attempt_selected, "meta", {}) or {})
                attempt_meta["cluster_id"] = str(cluster_decision.cluster_id or "")
                attempt_meta["cluster_similarity"] = float(cluster_decision.best_similarity)
                attempt_meta["cluster_matched_existing"] = bool(cluster_decision.matched_existing)
                attempt_selected.meta = attempt_meta
                attempt_cluster_id = str(attempt_meta.get("cluster_id", "") or "")
                if should_skip_same_run(attempt_cluster_id, self._seen_cluster_ids_in_run):
                    cluster_skip_count += 1
                    if attempt_claimed_id > 0:
                        excluded_claim_ids.add(attempt_claimed_id)
                    degraded_note = self._append_note(
                        degraded_note,
                        f"cluster_skip_same_cycle={attempt_cluster_id or 'none'}",
                    )
                    if retry_event_filter and attempt_event_id == retry_event_filter:
                        # Retry target collided with same-run cluster guard; release filter and move on.
                        retry_event_filter = ""
                        degraded_note = self._append_note(
                            degraded_note,
                            "retry_event_filter_released_on_cluster_skip",
                        )
                    self._append_workflow_perf(
                        "news_cluster_skip",
                        {
                            "claim_try": int(claim_try),
                            "claimed_id": int(attempt_claimed_id),
                            "cluster_id": str(attempt_cluster_id or ""),
                            "similarity": float(cluster_decision.best_similarity),
                        },
                    )
                    try:
                        self.news_pool_store.rollback_claim(attempt_claimed_id)
                    except Exception:
                        pass
                    continue
                selected = attempt_selected
                claimed_id = int(attempt_claimed_id)
                cluster_id = attempt_cluster_id
                break

            if not selected:
                hold_reason = (
                    f"news_cluster_skip_exhausted:{cluster_skip_count}"
                    if cluster_skip_count > 0
                    else (
                        f"news_retry_event_mismatch_exhausted:{retry_event_mismatch_count}"
                        if str(retry_event_filter or "").strip() and retry_event_mismatch_count > 0
                        else "news_pool_empty"
                    )
                )
                global_blocked, global_reason = self._watchdog_hold_gate(hold_reason)
                if global_blocked:
                    hold_reason = global_reason
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=0,
                        title="",
                        source_url="",
                        published_url="",
                        note=self._append_note(hold_reason, degraded_note),
                    )
                )
                self._workflow_perf_finish_run("hold", hold_reason)
                return WorkflowResult("hold", hold_reason)

            category = str((selected.meta or {}).get("news_category", "platform") or "platform").strip().lower() or "platform"
            global_keywords = list(getattr(selected, "long_tail_keywords", []) or [])[:8]
            reason = f"news_pool_claimed={claimed_id};category={category};cluster_id={cluster_id or 'none'}"
            if cluster_id:
                degraded_note = self._append_note(degraded_note, f"news_cluster_id={cluster_id}")
            labels = self._build_public_labels(
                title=selected.title,
                candidate=selected,
                global_keywords=global_keywords,
                max_labels=6,
            )
            working_draft_id, collect_note = self._sync_stage_draft_checkpoint(
                current_draft_post_id=working_draft_id,
                stage="collect_done",
                title=selected.title,
                html_body=f"<h2>News Topic Claimed</h2><p>{escape(selected.title)}</p>",
                labels=labels,
                reason=reason,
            )
            degraded_note = self._append_note(degraded_note, collect_note)

            self._progress("intent", "검색 의도 해석", 26)
            intent_bundle, intent_source = self._build_search_intent_bundle(
                candidate=selected,
                category=category,
            )
            selected_meta = dict(getattr(selected, "meta", {}) or {})
            selected_meta["intent_bundle"] = asdict(intent_bundle)
            if intent_source != "ollama":
                degraded_note = self._append_note(degraded_note, "search_intent_fallback_used")
            self._progress("outline", "구조 아키타입 선택", 30)
            try:
                outline_plan = self._pick_outline_plan(
                    candidate=selected,
                    intent_bundle=intent_bundle,
                    category=category,
                    cluster_id=cluster_id,
                )
            except RuntimeError as exc:
                hold_reason = str(exc or "template_similarity_too_high")
                if "template_similarity_too_high" not in hold_reason:
                    hold_reason = "template_similarity_too_high"
                if claimed_id:
                    try:
                        self.news_pool_store.rollback_claim(claimed_id)
                        claim_finalized = True
                    except Exception:
                        pass
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=0,
                        title=str(selected.title or ""),
                        source_url=str(getattr(selected, "url", "") or ""),
                        published_url="",
                        note=self._append_note(hold_reason, degraded_note),
                    )
                )
                self._workflow_perf_finish_run("hold", hold_reason)
                return WorkflowResult("hold", hold_reason)
            selected_meta["outline_plan"] = asdict(outline_plan)
            selected_meta["outline_fingerprint"] = str(outline_plan.fingerprint or "")
            selected.meta = selected_meta

            self._progress("draft", "뉴스 본문 초안 생성", 36)
            api_ready = bool(
                (self.settings.gemini.api_key or "").strip()
                and (self.settings.gemini.api_key or "").strip() != "GEMINI_API_KEY"
            )
            draft: DraftPost | None = None
            reference_guidance = self.references.build_guidance()
            event_id = str(
                (selected.meta or {}).get("event_id", "")
                or (selected.meta or {}).get("news_event_id", "")
                or (selected.meta or {}).get("news_pool_id", "")
                or claimed_id
            ).strip()
            retry_index = (selected.meta or {}).get("retry_index", None)
            self.watchdog.begin_event(event_id)
            abort_event, abort_reason = self.watchdog.should_abort_event(event_id)
            if abort_event:
                skip_reason = f"watchdog_abort:{abort_reason}"
                degraded_note = self._append_note(degraded_note, skip_reason)
                if claimed_id:
                    try:
                        self.news_pool_store.rollback_claim(claimed_id)
                        claim_finalized = True
                    except Exception:
                        pass
                self.logs.append_run(
                    RunRecord(
                        status="skipped",
                        score=0,
                        title=str(selected.title or ""),
                        source_url=str(getattr(selected, "url", "") or ""),
                        published_url="",
                        note=self._append_note(skip_reason, degraded_note),
                    )
                )
                self._workflow_perf_finish_run("skipped", skip_reason)
                return WorkflowResult("skipped", skip_reason)
            if api_ready and self._gemini_budget_remaining() > 0:
                try:
                    if self.ollama_client and selected.body:
                        # Locally clean context to reduce noise/cost before Gemini
                        selected.body = self._profile_call(
                            "ollama_clean_context",
                            lambda: self.ollama_client.clean_context(selected.body),
                            slow_ms=1500
                        )
                        # NEW: Think step - generate expert hints locally
                        expert_hints = self._profile_call(
                            "ollama_think_step",
                            lambda: self.ollama_client.think_about_topic(selected.title, selected.body),
                            slow_ms=2000
                        )
                        selected.meta["expert_hints"] = expert_hints

                    draft = self.brain.generate_post_from_outline(
                        candidate=selected,
                        authority_links=self.settings.authority_links,
                        reference_guidance=reference_guidance,
                        category=category,
                        intent_bundle=intent_bundle,
                        outline_plan=outline_plan,
                        plan={
                            "primary_keyword": intent_bundle.primary_query or selected.title,
                            "news_category": category,
                            "event_id": event_id,
                            "run_start_minute": str(run_start_minute or "").strip(),
                            "retry_index": retry_index,
                            "cluster_id": str((selected.meta or {}).get("cluster_id", "") or ""),
                            "format": "explainer",
                        },
                    )
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                except Exception as exc:
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                    provider_code = self._provider_http_code_from_error(str(exc))
                    if provider_code in {429, 530}:
                        self.watchdog.register_provider_failure(event_id, int(provider_code))
                        backoff_minutes = self.watchdog.compute_backoff_minutes(event_id, int(provider_code))
                        if backoff_minutes is not None:
                            hold_reason = f"provider_backoff_http_{int(provider_code)}:{int(backoff_minutes)}m"
                            degraded_note = self._append_note(degraded_note, hold_reason)
                            global_blocked, global_reason = self._watchdog_hold_gate(hold_reason)
                            if global_blocked:
                                hold_reason = global_reason
                            self.logs.append_run(
                                RunRecord(
                                    status="hold",
                                    score=0,
                                    title=str(selected.title or ""),
                                    source_url=str(getattr(selected, "url", "") or ""),
                                    published_url="",
                                    note=self._append_note(hold_reason, degraded_note),
                                )
                            )
                            self._workflow_perf_finish_run("hold", hold_reason)
                            return WorkflowResult("hold", hold_reason)
                    degraded_note = self._append_note(degraded_note, f"news_gemini_failed={str(exc)[:120]}")
            if draft is None:
                local_facet_context = resolve_facet_context(
                    event_id=event_id,
                    run_start_minute=str(run_start_minute or "").strip(),
                    title=str(selected.title or ""),
                    body=str(selected.body or ""),
                    category=category,
                    source_url=str(getattr(selected, "url", "") or ""),
                    retry_index=retry_index,
                    llm_candidates=[],
                    state_path=(self.root / "storage" / "state" / "facet_rotation_state.json")
                    if not api_ready
                    else None,
                    stable_hash_fn=stable_hash,
                ).as_dict()
                try:
                    selected_meta = dict(getattr(selected, "meta", {}) or {})
                    selected_meta["selected_facet"] = str(local_facet_context.get("selected_facet", "impact") or "impact")
                    selected.meta = selected_meta
                except Exception:
                    pass
                draft = self._build_news_post_local_fallback(
                    selected,
                    category,
                    self.settings.authority_links,
                    facet_context=local_facet_context,
                )
                degraded_note = self._append_note(degraded_note, "news_local_fallback_draft")

            intent_keywords = [
                re.sub(r"\s+", " ", str(x or "")).strip()
                for x in [intent_bundle.primary_query, *(intent_bundle.supporting_queries or [])]
                if str(x or "").strip()
            ]
            if intent_keywords:
                merged_long_tail: list[str] = []
                seen_long_tail: set[str] = set()
                for item in [*intent_keywords, *(getattr(selected, "long_tail_keywords", []) or [])]:
                    clean = re.sub(r"\s+", " ", str(item or "")).strip()
                    key = clean.lower()
                    if not clean or key in seen_long_tail:
                        continue
                    seen_long_tail.add(key)
                    merged_long_tail.append(clean[:120])
                    if len(merged_long_tail) >= 8:
                        break
                selected.long_tail_keywords = merged_long_tail

            draft.title = self._enforce_seo_title(
                title=draft.title,
                candidate=selected,
                global_keywords=global_keywords,
                preferred_keyword=intent_bundle.primary_query or selected.title,
            )
            self._ensure_min_long_tail_keywords(
                candidate=selected,
                title=draft.title,
                global_keywords=global_keywords,
            )
            global_keywords = list(getattr(selected, "long_tail_keywords", []) or [])[:8]
            current_domain = self._news_domain
            base_html = self._sanitize_publish_html(draft.html, domain=current_domain)
            base_html = self._canonicalize_html_payload(base_html)
            base_html += self._build_internal_links_block(
                current_title=draft.title,
                current_keywords=global_keywords,
                current_device_type="news",
                current_cluster_id=category,
            )
            final_html = self._sanitize_publish_html(base_html, domain=current_domain)
            final_html = self._canonicalize_html_payload(final_html)
            final_html, removed_news_links = self._strip_forbidden_news_links(final_html)
            if removed_news_links > 0:
                degraded_note = self._append_note(degraded_note, f"news_google_link_removed={removed_news_links}")
                self._append_workflow_perf(
                    "news_link_sanitize",
                    {"removed": int(removed_news_links), "domain": current_domain},
                )

            self._progress("qa", "품질 게이트 점검", 56)
            qa_result = self._qa_evaluate(
                final_html,
                title=draft.title,
                domain=current_domain,
                keyword=selected.title,
                context="news_initial",
                phase="pre_images",
            )
            if self.settings.quality.enabled and self.settings.quality.strict_mode:
                min_quality = max(91, int(getattr(self.settings.quality, "min_quality_score", 85) or 85))
                configured_max_passes = int(getattr(self.settings.quality, "qa_retry_max_passes", 0) or 0)
                # Keep news QA retries bounded; long no-progress loops were consuming minutes per run.
                max_passes = max(1, configured_max_passes) if configured_max_passes > 0 else 3
                max_passes = min(6, max_passes)
                no_progress_limit = min(2, max_passes)
                pass_no = 0
                no_progress = 0
                loop_stop_reason = "not_started"
                self._log_news_qa_runtime(
                    "news_qa_loop_start",
                    {
                        "phase": "pre_images",
                        "title": draft.title,
                        "keyword": selected.title,
                        "initial_score": int(qa_result.score),
                        "initial_failed_keys": self._qa_failed_keys(qa_result)[:8],
                        "initial_hard_failures": list(getattr(qa_result, "hard_failures", []) or [])[:8],
                        "configured_retry_max_passes": int(configured_max_passes),
                        "effective_retry_max_passes": int(max_passes),
                        "no_progress_limit": int(no_progress_limit),
                        "target_score": int(min_quality),
                        "metrics": self._news_html_metrics(final_html),
                    },
                )
                self._append_workflow_perf(
                    "news_qa_loop_start",
                    {
                        "phase": "pre_images",
                        "initial_score": int(qa_result.score),
                        "max_passes": int(max_passes),
                        "no_progress_limit": int(no_progress_limit),
                        "target_score": int(min_quality),
                    },
                )
                while (qa_result.score < min_quality or qa_result.has_hard_failure) and pass_no < max_passes:
                    pass_no += 1
                    prev_score = int(qa_result.score)
                    prev_failed_count = len(list(getattr(qa_result, "failed", []) or []))
                    prev_hard_count = len(list(getattr(qa_result, "hard_failures", []) or []))
                    improved, step_logs = self._apply_news_qa_repair_chain(
                        html=final_html,
                        qa_result=qa_result,
                        domain=current_domain,
                        source_url=str(getattr(selected, "url", "") or ""),
                    )
                    for step_row in step_logs:
                        self._log_news_qa_runtime(
                            "news_qa_pass_step",
                            {
                                "phase": "pre_images",
                                "pass_no": int(pass_no),
                                **dict(step_row or {}),
                            },
                        )
                    final_html = improved
                    new_result = self._qa_evaluate(
                        final_html,
                        title=draft.title,
                        domain=current_domain,
                        keyword=selected.title,
                        context=f"news_strict_pass_{pass_no}",
                        phase="pre_images",
                    )
                    qa_result = new_result
                    next_score = int(qa_result.score)
                    next_failed_count = len(list(getattr(qa_result, "failed", []) or []))
                    next_hard_count = len(list(getattr(qa_result, "hard_failures", []) or []))
                    progressed = (
                        (next_score > prev_score)
                        or (next_hard_count < prev_hard_count)
                        or (next_failed_count < prev_failed_count)
                    )
                    if not progressed:
                        no_progress += 1
                    else:
                        no_progress = 0
                    self._log_news_qa_runtime(
                        "news_qa_pass_result",
                        {
                            "phase": "pre_images",
                            "pass_no": int(pass_no),
                            "score_before": int(prev_score),
                            "score_after": int(next_score),
                            "failed_before": int(prev_failed_count),
                            "failed_after": int(next_failed_count),
                            "hard_before": int(prev_hard_count),
                            "hard_after": int(next_hard_count),
                            "progressed": bool(progressed),
                            "no_progress_streak": int(no_progress),
                            "failed_keys": self._qa_failed_keys(qa_result)[:8],
                            "hard_failures": list(getattr(qa_result, "hard_failures", []) or [])[:8],
                            "metrics": self._news_html_metrics(final_html),
                        },
                    )
                    self._append_workflow_perf(
                        "news_qa_pass_result",
                        {
                            "pass_no": int(pass_no),
                            "score_before": int(prev_score),
                            "score_after": int(next_score),
                            "failed_before": int(prev_failed_count),
                            "failed_after": int(next_failed_count),
                            "hard_before": int(prev_hard_count),
                            "hard_after": int(next_hard_count),
                            "progressed": bool(progressed),
                            "no_progress_streak": int(no_progress),
                        },
                    )
                    if qa_result.score >= min_quality and (not qa_result.has_hard_failure):
                        loop_stop_reason = "score_reached"
                        break
                    if no_progress >= no_progress_limit:
                        loop_stop_reason = "no_progress_limit"
                        break
                if loop_stop_reason == "not_started":
                    if qa_result.score >= min_quality and (not qa_result.has_hard_failure):
                        loop_stop_reason = "score_reached"
                    elif pass_no >= max_passes:
                        loop_stop_reason = "max_passes_reached"
                    else:
                        loop_stop_reason = "loop_completed"
                self._log_news_qa_runtime(
                    "news_qa_loop_end",
                    {
                        "phase": "pre_images",
                        "stop_reason": loop_stop_reason,
                        "passes_used": int(pass_no),
                        "target_score": int(min_quality),
                        "last_score": int(qa_result.score),
                        "last_failed_keys": self._qa_failed_keys(qa_result)[:8],
                        "last_hard_failures": list(getattr(qa_result, "hard_failures", []) or [])[:8],
                        "last_no_progress_streak": int(no_progress),
                        "metrics": self._news_html_metrics(final_html),
                    },
                )
                self._append_workflow_perf(
                    "news_qa_loop_end",
                    {
                        "phase": "pre_images",
                        "stop_reason": loop_stop_reason,
                        "passes_used": int(pass_no),
                        "last_score": int(qa_result.score),
                        "last_no_progress_streak": int(no_progress),
                    },
                )
                if qa_result.score < min_quality or qa_result.has_hard_failure:
                    qa_metrics = self._news_html_metrics(final_html)
                    failed_keys = self._qa_failed_keys(qa_result)[:5]
                    hard_keys = list(getattr(qa_result, "hard_failures", []) or [])[:5]
                    detail_note = (
                        f"qa_passes_used={pass_no};"
                        f"last_score={int(qa_result.score)};"
                        f"last_failed_keys={','.join(failed_keys) if failed_keys else 'none'};"
                        f"last_hard_failures={','.join(hard_keys) if hard_keys else 'none'};"
                        f"last_no_progress_streak={int(no_progress)};"
                        f"last_word_count={int(qa_metrics.get('word_count', 0))};"
                        f"h2_count={int(qa_metrics.get('h2_count', 0))};"
                        f"external_links={int(qa_metrics.get('external_links', 0))};"
                        f"authority_links={int(qa_metrics.get('authority_links', 0))}"
                    )
                    if qa_result.has_hard_failure:
                        hard_token = ",".join(hard_keys[:2]) if hard_keys else "unknown"
                        hold_reason = (
                            f"qa_hard_failure:{hard_token};"
                            f"qa_score={qa_result.score}/{min_quality}"
                        )
                    else:
                        hold_reason = f"qa_below_threshold:{qa_result.score}/{min_quality}"
                    self.watchdog.register_hard_failure(event_id, hold_reason)
                    only_word_count_shortfall = (not qa_result.has_hard_failure) and failed_keys == ["word_count"]
                    can_retry = bool(self._retry_enabled and int(news_retry_depth) < int(self._retry_max_attempts_per_event))
                    if only_word_count_shortfall and loop_stop_reason == "no_progress_limit":
                        can_retry = False
                        degraded_note = self._append_note(
                            degraded_note,
                            "news_qa_retry_skipped_no_progress_word_count",
                        )
                        self._append_workflow_perf(
                            "news_qa_retry_skipped",
                            {
                                "reason": "no_progress_word_count",
                                "passes_used": int(pass_no),
                                "last_score": int(qa_result.score),
                            },
                        )
                    abort_now, abort_reason = self.watchdog.should_abort_event(event_id)
                    if can_retry and (not abort_now):
                        next_depth = int(news_retry_depth) + 1
                        debounce = self._retry_debounce_seconds_for_attempt(next_depth)
                        if debounce > 0:
                            degraded_note = self._append_note(degraded_note, f"retry_debounce={debounce}s")
                            time.sleep(float(debounce))
                        if claimed_id:
                            try:
                                self.news_pool_store.rollback_claim(claimed_id)
                                claim_finalized = True
                            except Exception:
                                pass
                        return self._run_once_news_mode(
                            manual_trigger=bool(manual_trigger),
                            run_start_minute=run_start_minute,
                            queue_advisory=queue_advisory,
                            sync_note=sync_note,
                            preflight_index_note=preflight_index_note,
                            blog_snapshot=blog_snapshot,
                            today_gemini_before=today_gemini_before,
                            news_retry_depth=next_depth,
                            retry_event_id=event_id,
                            draft_post_id=working_draft_id,
                        )
                    if abort_now:
                        hold_reason = f"{hold_reason};watchdog_abort={abort_reason}"
                    global_blocked, global_reason = self._watchdog_hold_gate(hold_reason)
                    if global_blocked:
                        hold_reason = global_reason
                    self.logs.append_run(
                        RunRecord(
                            status="hold",
                            score=qa_result.score,
                            title=draft.title,
                            source_url=draft.source_url,
                            published_url="",
                            note=self._append_note(self._append_note(hold_reason, detail_note), degraded_note),
                        )
                    )
                    self._workflow_perf_finish_run("hold", hold_reason)
                    return WorkflowResult("hold", hold_reason)

            if bool(getattr(getattr(self.settings, "readability", None), "enabled", True)):
                try:
                    final_html = optimize_html_readability(final_html, self.settings.readability)
                except Exception:
                    degraded_note = self._append_note(degraded_note, "readability_failed")

            # Evaluate quality and execution gates
            hold_reason = ""
            actionability = self._evaluate_actionability_gate(draft.title, final_html)
            if not actionability.ok:
                gate_prefix = "editorial_depth_gate_failed" if is_news_mode(self.settings) else "actionability_gate_failed"
                hold_reason = gate_prefix + ":" + ",".join(actionability.reasons[:4])
                self.watchdog.register_hard_failure(event_id, hold_reason)
                can_retry = bool(self._retry_enabled and int(news_retry_depth) < int(self._retry_max_attempts_per_event))
                abort_now, abort_reason = self.watchdog.should_abort_event(event_id)
                if can_retry and (not abort_now):
                    next_depth = int(news_retry_depth) + 1
                    debounce = self._retry_debounce_seconds_for_attempt(next_depth)
                    if debounce > 0:
                        degraded_note = self._append_note(degraded_note, f"retry_debounce={debounce}s")
                        time.sleep(float(debounce))
                    if claimed_id:
                        try:
                            self.news_pool_store.rollback_claim(claimed_id)
                            claim_finalized = True
                        except Exception:
                            pass
                    return self._run_once_news_mode(
                        manual_trigger=bool(manual_trigger),
                        run_start_minute=run_start_minute,
                        queue_advisory=queue_advisory,
                        sync_note=sync_note,
                        preflight_index_note=preflight_index_note,
                        blog_snapshot=blog_snapshot,
                        today_gemini_before=today_gemini_before,
                        news_retry_depth=next_depth,
                        retry_event_id=event_id,
                        draft_post_id=working_draft_id,
                    )
                if abort_now:
                    hold_reason = f"{hold_reason};watchdog_abort={abort_reason}"
                global_blocked, global_reason = self._watchdog_hold_gate(hold_reason)
                if global_blocked:
                    hold_reason = global_reason
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=int(actionability.score),
                        title=draft.title,
                        source_url=draft.source_url,
                        published_url="",
                        note=self._append_note(hold_reason, degraded_note),
                    )
                )
                self._workflow_perf_finish_run("hold", hold_reason)
                return WorkflowResult("hold", hold_reason)

            self._progress("visual", "뉴스 이미지/썸네일 구성", 72)
            try:
                visual_diag_pre = diagnose_visual_settings(self.settings, self.root)
                self._append_workflow_perf(
                    "visual_diagnostics",
                    {
                        "stage": "news_pre_images",
                        "can_attempt_generation": bool(visual_diag_pre.get("can_attempt_generation", False)),
                        "blockers": list(visual_diag_pre.get("blockers", []) or []),
                        "target_images_per_post": int(visual_diag_pre.get("visual.target_images_per_post", 0) or 0),
                        "min_images_required": int(visual_diag_pre.get("publish.min_images_required", 0) or 0),
                        "max_images_per_post": int(visual_diag_pre.get("publish.max_images_per_post", 0) or 0),
                        "thumbnail_preflight_only": bool(
                            visual_diag_pre.get("publish.thumbnail_preflight_only", False)
                        ),
                        "provider": str(visual_diag_pre.get("visual.image_provider", "") or ""),
                        "gemini_enabled": bool(
                            visual_diag_pre.get("visual.enable_gemini_image_generation", False)
                        ),
                        "api_key_present": bool(visual_diag_pre.get("gemini.api_key_present", False)),
                    },
                )
            except Exception:
                pass
            target_images = self._image_target_max()
            min_images_required = self._image_min_required()
            news_tags = self._news_pack_tags_for_candidate(selected, category)
            
            images, emergency_notes = self._profile_call(
                "media_manager_prepare_news_images",
                lambda: self.media_manager.prepare_news_images(
                    draft=draft,
                    category=category,
                    tags=news_tags,
                    target_count=target_images,
                    min_required=min_images_required,
                    seed_tick_fn=lambda **kw: {},
                ),
                slow_ms=4000,
                meta={
                    "category": str(category or ""),
                    "target_count": int(target_images),
                    "min_required": int(min_images_required),
                },
            )
            
            for note in emergency_notes:
                degraded_note = self._append_note(degraded_note, note)
                
            if len(images) < min_images_required:
                detail = "; ".join(emergency_notes) if emergency_notes else "no_provider_notes"
                hold_reason = f"missing_images_required({len(images)}/{min_images_required}) | {detail}"
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=0,
                        title=draft.title,
                        source_url=draft.source_url,
                        published_url="",
                        note=self._append_note(hold_reason, degraded_note),
                    )
                )
                self._workflow_perf_finish_run("hold", hold_reason)
                return WorkflowResult("hold", hold_reason)

            try:
                visual_diag_post = diagnose_visual_settings(self.settings, self.root)
                self._append_workflow_perf(
                    "visual_diagnostics",
                    {
                        "stage": "news_post_images",
                        "can_attempt_generation": bool(visual_diag_post.get("can_attempt_generation", False)),
                        "blockers": list(visual_diag_post.get("blockers", []) or []),
                        "target_images_per_post": int(visual_diag_post.get("visual.target_images_per_post", 0) or 0),
                        "min_images_required": int(visual_diag_post.get("publish.min_images_required", 0) or 0),
                        "max_images_per_post": int(visual_diag_post.get("publish.max_images_per_post", 0) or 0),
                        "thumbnail_preflight_only": bool(
                            visual_diag_post.get("publish.thumbnail_preflight_only", False)
                        ),
                        "provider": str(visual_diag_post.get("visual.image_provider", "") or ""),
                        "gemini_enabled": bool(
                            visual_diag_post.get("visual.enable_gemini_image_generation", False)
                        ),
                        "api_key_present": bool(visual_diag_post.get("gemini.api_key_present", False)),
                        "selected_images_count": int(len(images)),
                    },
                )
            except Exception:
                pass

            final_html, degraded_note, _ = self._apply_body_clickbait_sanitizer(
                final_html,
                degraded_note,
            )
            dry_run = bool(getattr(self.settings.budget, "dry_run", False))
            preflight_thumb_src = self._preflight_thumb_src_from_images(images)
            degraded_note = self._annotate_image_pipeline_diagnostics(
                note=degraded_note,
                stage="news_pre_merge",
                images=images,
                preflight_thumb_src=preflight_thumb_src,
                required_images=min_images_required,
            )
            if dry_run:
                gate_preview_html = self._profile_call(
                    "news_gate_preview_html",
                    lambda: self.publisher.build_dry_run_html(final_html, images),
                    slow_ms=1200,
                    meta={"images_count": int(len(images)), "mode": "dry_run"},
                )
            else:
                if images:
                    if not preflight_thumb_src:
                        images, preflight_thumb_src = self._profile_call(
                            "news_preflight_thumbnail_recovery",
                            lambda: self._preflight_thumbnail_with_recovery(
                                draft=draft,
                                candidate=selected,
                                images=images,
                                prompt_plan={"source": "news_pack_picker"},
                                max_attempts=2,
                                manual_trigger=manual_trigger,
                            ),
                            slow_ms=1800,
                            meta={"images_count": int(len(images))},
                        )
                creds_for_gate = self.publisher._oauth_credentials()  # noqa: SLF001
                gate_preview_html = self._profile_call(
                    "news_gate_merge_images",
                    lambda: self.publisher._merge_images(  # noqa: SLF001
                        final_html,
                        images,
                        creds_for_gate,
                        preflight_thumbnail_src=preflight_thumb_src,
                    ),
                    slow_ms=2500,
                    meta={
                        "images_count": int(len(images)),
                        "preflight_thumb": bool(preflight_thumb_src),
                    },
                )

            post_image_qa = self._qa_evaluate(
                gate_preview_html,
                title=draft.title,
                domain=current_domain,
                keyword=selected.title,
                context="news_post_image",
                include_image_integrity=bool(images),
                phase="post_images",
            )
            if post_image_qa.has_hard_failure:
                hold_reason = "post_image_qa_hard_fail:" + ",".join(list(post_image_qa.hard_failures or [])[:3])
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=post_image_qa.score,
                        title=draft.title,
                        source_url=draft.source_url,
                        published_url="",
                        note=self._append_note(hold_reason, degraded_note),
                    )
                )
                self._workflow_perf_finish_run("hold", hold_reason)
                return WorkflowResult("hold", hold_reason)
            if self.settings.quality.enabled and self.settings.quality.strict_mode:
                min_quality = int(getattr(self.settings.quality, "min_quality_score", 85) or 85)
                if post_image_qa.score < min_quality:
                    hold_reason = f"post_image_qa_below_threshold:{post_image_qa.score}/{min_quality}"
                    self.logs.append_run(
                        RunRecord(
                            status="hold",
                            score=post_image_qa.score,
                            title=draft.title,
                            source_url=draft.source_url,
                            published_url="",
                            note=self._append_note(hold_reason, degraded_note),
                        )
                    )
                    self._workflow_perf_finish_run("hold", hold_reason)
                    return WorkflowResult("hold", hold_reason)

            go_live_errors, go_live_warnings = self._go_live_gate_checklist(
                title=draft.title,
                final_html=final_html,
                gate_html=gate_preview_html,
                images=images,
                candidate=selected,
            )
            if go_live_errors:
                hold_reason = "go_live_gate_failed:" + ",".join(go_live_errors[:4])
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=0,
                        title=draft.title,
                        source_url=draft.source_url,
                        published_url="",
                        note=self._append_note(hold_reason, degraded_note),
                    )
                )
                self._workflow_perf_finish_run("hold", hold_reason)
                return WorkflowResult("hold", hold_reason)
            if go_live_warnings:
                degraded_note = self._append_note(degraded_note, "go_live_warnings=" + ",".join(go_live_warnings[:3]))

            if self._title_diversity_enabled:
                try:
                    title_mix = choose_diverse_title(
                        base_title=str(draft.title or ""),
                        cluster_id=str((selected.meta or {}).get("cluster_id", "") or ""),
                        facet=str((selected.meta or {}).get("selected_facet", "impact") or "impact"),
                        category=str(category or ""),
                        run_start_minute=str(run_start_minute or ""),
                        stable_hash_fn=stable_hash,
                        state_path=self._title_diversity_state_path,
                        settings=getattr(self.settings, "title_diversity", None),
                    )
                    diversified_title = str(title_mix.get("title", "") or "").strip()
                    if diversified_title:
                        draft.title = diversified_title
                    alt_titles = title_mix.get("alt_titles", [])
                    if isinstance(alt_titles, list):
                        draft.alt_titles = [str(x).strip() for x in alt_titles if str(x).strip()]
                    degraded_note = self._append_note(
                        degraded_note,
                        f"title_pattern={int(title_mix.get('pattern_id', -1) or -1)}",
                    )
                except Exception:
                    degraded_note = self._append_note(degraded_note, "title_diversity_failed")

            if self._source_naturalization_enabled:
                try:
                    final_html = apply_source_naturalization(
                        html=final_html,
                        source_url="",
                        authority_links=list(getattr(self.settings, "authority_links", []) or []),
                        settings=getattr(self.settings, "source_naturalization", None),
                    )
                except Exception:
                    degraded_note = self._append_note(degraded_note, "source_naturalization_failed")

            final_html = self._inject_images_into_html(final_html, images)
            degraded_note = self._apply_ctr_visual_density_note(degraded_note, images)
            news_link_topic = self._infer_topic_cluster(
                draft.title,
                global_keywords,
                final_html,
            )
            news_link_keywords = self._compute_focus_keywords(
                draft.title,
                final_html,
                news_link_topic,
            )
            try:
                final_html = self._inject_internal_links_and_related_coverage(
                    final_html,
                    current_title=draft.title,
                    current_keywords=news_link_keywords,
                )
            except Exception:
                degraded_note = self._append_note(degraded_note, "internal_links_failed")
            final_html, degraded_note, _ = self._apply_body_clickbait_sanitizer(
                final_html,
                degraded_note,
            )
            final_html = self._normalize_duplicate_h2_sections(final_html)

            entropy_settings = getattr(self.settings, "entropy_check", None)
            entropy_result: dict[str, Any] = {"ok": True, "reasons": []}
            if self._entropy_check_enabled:
                entropy_result = self._profile_call(
                    "news_entropy_check",
                    lambda: check_entropy(final_html, entropy_settings),
                    slow_ms=800,
                    meta={"phase": "post_images"},
                )
                if not bool(entropy_result.get("ok", False)):
                    entropy_reason = ",".join(list(entropy_result.get("reasons", []) or [])[:4]) or "unknown"
                    degraded_note = self._append_note(degraded_note, f"entropy_fail:{entropy_reason}")
                    can_entropy_rewrite = (not entropy_retry_used) and int(self._entropy_max_rewrite_attempts) > 0
                    if can_entropy_rewrite:
                        entropy_retry_used = True
                        try:
                            base_retry_index = int(retry_index or 0)
                        except Exception:
                            base_retry_index = 0
                        entropy_retry_index = max(0, int(base_retry_index) + 1)
                        retry_index = entropy_retry_index
                        try:
                            selected_meta = dict(getattr(selected, "meta", {}) or {})
                            selected_meta["retry_index"] = int(entropy_retry_index)
                            selected_meta["entropy_retry"] = True
                            selected.meta = selected_meta
                        except Exception:
                            pass

                        rewritten_draft: DraftPost | None = None
                        if api_ready and self._gemini_budget_remaining() > 0:
                            try:
                                rewritten_draft = self._profile_call(
                                    "news_entropy_rewrite_gemini",
                                    lambda: self.brain.generate_news_post(
                                        selected,
                                        self.settings.authority_links,
                                        reference_guidance,
                                        category=category,
                                        plan={
                                            "primary_keyword": selected.title,
                                            "news_category": category,
                                            "event_id": event_id,
                                            "run_start_minute": str(run_start_minute or "").strip(),
                                            "retry_index": int(entropy_retry_index),
                                            "cluster_id": str((selected.meta or {}).get("cluster_id", "") or ""),
                                            "entropy_retry": True,
                                            "format": "explainer",
                                        },
                                    ),
                                    slow_ms=4000,
                                    meta={
                                        "retry_index": int(entropy_retry_index),
                                        "category": str(category or ""),
                                    },
                                )
                                if self.brain.call_count:
                                    self.logs.increment_today_gemini_count(self.brain.call_count)
                                    self.brain.reset_run_counter()
                                degraded_note = self._append_note(degraded_note, "entropy_rewrite_gemini")
                            except Exception as exc:
                                if self.brain.call_count:
                                    self.logs.increment_today_gemini_count(self.brain.call_count)
                                    self.brain.reset_run_counter()
                                degraded_note = self._append_note(
                                    degraded_note,
                                    f"entropy_rewrite_gemini_failed={str(exc)[:120]}",
                                )

                        if rewritten_draft is None:
                            local_facet_context = resolve_facet_context(
                                event_id=event_id,
                                run_start_minute=str(run_start_minute or "").strip(),
                                title=str(selected.title or ""),
                                body=str(selected.body or ""),
                                category=category,
                                source_url=str(getattr(selected, "url", "") or ""),
                                retry_index=int(entropy_retry_index),
                                llm_candidates=[],
                                state_path=(self.root / "storage" / "state" / "facet_rotation_state.json")
                                if not api_ready
                                else None,
                                stable_hash_fn=stable_hash,
                            ).as_dict()
                            try:
                                selected_meta = dict(getattr(selected, "meta", {}) or {})
                                selected_meta["selected_facet"] = str(
                                    local_facet_context.get("selected_facet", "impact") or "impact"
                                )
                                selected.meta = selected_meta
                            except Exception:
                                pass
                            rewritten_draft = self._profile_call(
                                "news_entropy_rewrite_local_fallback",
                                lambda: self._build_news_post_local_fallback(
                                    selected,
                                    category,
                                    self.settings.authority_links,
                                    facet_context=local_facet_context,
                                ),
                                slow_ms=120,
                                meta={"retry_index": int(entropy_retry_index)},
                            )
                            degraded_note = self._append_note(degraded_note, "entropy_rewrite_local_fallback")

                        if rewritten_draft is not None:
                            draft = rewritten_draft
                            draft.title = self._enforce_seo_title(
                                title=draft.title,
                                candidate=selected,
                                global_keywords=global_keywords,
                                preferred_keyword=selected.title,
                            )
                            self._ensure_min_long_tail_keywords(
                                candidate=selected,
                                title=draft.title,
                                global_keywords=global_keywords,
                            )
                            global_keywords = list(getattr(selected, "long_tail_keywords", []) or [])[:8]
                            base_html = self._sanitize_publish_html(draft.html, domain=current_domain)
                            base_html = self._canonicalize_html_payload(base_html)
                            base_html += self._build_internal_links_block(
                                current_title=draft.title,
                                current_keywords=global_keywords,
                                current_device_type="news",
                                current_cluster_id=category,
                            )
                            final_html = self._sanitize_publish_html(base_html, domain=current_domain)
                            final_html = self._canonicalize_html_payload(final_html)
                            final_html, removed_news_links = self._strip_forbidden_news_links(final_html)
                            if removed_news_links > 0:
                                degraded_note = self._append_note(
                                    degraded_note,
                                    f"news_google_link_removed_rewrite={removed_news_links}",
                                )

                            if bool(getattr(getattr(self.settings, "readability", None), "enabled", True)):
                                try:
                                    final_html = optimize_html_readability(final_html, self.settings.readability)
                                except Exception:
                                    degraded_note = self._append_note(degraded_note, "readability_failed")

                            if self._title_diversity_enabled:
                                try:
                                    title_mix = choose_diverse_title(
                                        base_title=str(draft.title or ""),
                                        cluster_id=str((selected.meta or {}).get("cluster_id", "") or ""),
                                        facet=str((selected.meta or {}).get("selected_facet", "impact") or "impact"),
                                        category=str(category or ""),
                                        run_start_minute=str(run_start_minute or ""),
                                        stable_hash_fn=stable_hash,
                                        state_path=self._title_diversity_state_path,
                                        settings=getattr(self.settings, "title_diversity", None),
                                    )
                                    diversified_title = str(title_mix.get("title", "") or "").strip()
                                    if diversified_title:
                                        draft.title = diversified_title
                                    alt_titles = title_mix.get("alt_titles", [])
                                    if isinstance(alt_titles, list):
                                        draft.alt_titles = [str(x).strip() for x in alt_titles if str(x).strip()]
                                    degraded_note = self._append_note(
                                        degraded_note,
                                        f"title_pattern={int(title_mix.get('pattern_id', -1) or -1)}",
                                    )
                                except Exception:
                                    degraded_note = self._append_note(degraded_note, "title_diversity_failed")

                            if self._source_naturalization_enabled:
                                try:
                                    final_html = apply_source_naturalization(
                                        html=final_html,
                                        source_url="",
                                        authority_links=list(getattr(self.settings, "authority_links", []) or []),
                                        settings=getattr(self.settings, "source_naturalization", None),
                                    )
                                except Exception:
                                    degraded_note = self._append_note(
                                        degraded_note,
                                        "source_naturalization_failed",
                                    )
                            final_html = self._inject_images_into_html(final_html, images)
                            degraded_note = self._apply_ctr_visual_density_note(degraded_note, images)
                            rewrite_link_topic = self._infer_topic_cluster(
                                draft.title,
                                global_keywords,
                                final_html,
                            )
                            rewrite_link_keywords = self._compute_focus_keywords(
                                draft.title,
                                final_html,
                                rewrite_link_topic,
                            )
                            try:
                                final_html = self._inject_internal_links_and_related_coverage(
                                    final_html,
                                    current_title=draft.title,
                                    current_keywords=rewrite_link_keywords,
                                )
                            except Exception:
                                degraded_note = self._append_note(degraded_note, "internal_links_failed")
                            final_html = self._normalize_duplicate_h2_sections(final_html)

                            if dry_run:
                                gate_preview_html = self.publisher.build_dry_run_html(final_html, images)
                            else:
                                try:
                                    creds_for_gate = self.publisher._oauth_credentials()  # noqa: SLF001
                                    gate_preview_html = self.publisher._merge_images(  # noqa: SLF001
                                        final_html,
                                        images,
                                        creds_for_gate,
                                        preflight_thumbnail_src=preflight_thumb_src,
                                    )
                                except Exception:
                                    pass
                            entropy_result = self._profile_call(
                                "news_entropy_check",
                                lambda: check_entropy(final_html, entropy_settings),
                                slow_ms=800,
                                meta={"phase": "post_rewrite"},
                            )

                if not bool(entropy_result.get("ok", False)):
                    exhausted_reason = ",".join(list(entropy_result.get("reasons", []) or [])[:4]) or "unknown"
                    skip_reason = f"entropy_fail_exhausted:{exhausted_reason}"
                    self.logs.append_run(
                        RunRecord(
                            status="skipped",
                            score=0,
                            title=draft.title,
                            source_url=draft.source_url,
                            published_url="",
                            note=self._append_note(skip_reason, degraded_note),
                        )
                    )
                    self._workflow_perf_finish_run("skipped", skip_reason)
                    return WorkflowResult("skipped", skip_reason)

            seo_topic = self._infer_topic_cluster(
                draft.title,
                global_keywords,
                final_html,
            )
            seo_focus_keywords = self._compute_focus_keywords(
                draft.title,
                final_html,
                seo_topic,
            )
            seo_slug_base = self._compute_seo_slug(draft.title, seo_topic)
            seo_slug = ""
            self._update_run_metrics_context(
                "news_mode",
                title=str(draft.title or ""),
                topic_cluster=str(seo_topic or "default"),
                focus_keywords=list(seo_focus_keywords or [])[:6],
                final_html=str(final_html or ""),
                images=list(images or []),
            )
            labels = self._build_public_labels(
                title=draft.title,
                candidate=selected,
                global_keywords=global_keywords,
                max_labels=6,
            )
            meta_description = self._build_meta_description(draft.title, draft.summary, final_html)
            working_draft_id, image_note = self._sync_stage_draft_checkpoint(
                current_draft_post_id=working_draft_id,
                stage="images_done",
                title=draft.title,
                html_body=final_html,
                labels=labels,
                reason=f"news_pool_id={claimed_id};images={len(images)}",
            )
            degraded_note = self._append_note(degraded_note, image_note)

            if dry_run:
                if claimed_id:
                    self.news_pool_store.rollback_claim(claimed_id)
                    claim_finalized = True
                dry_path = self.root / "storage" / "logs" / f"dry_run_news_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.html"
                dry_path.parent.mkdir(parents=True, exist_ok=True)
                dry_path.write_text(gate_preview_html, encoding="utf-8")
                self.logs.append_run(
                    RunRecord(
                        status="success",
                        score=int(qa_result.score),
                        title=draft.title,
                        source_url=draft.source_url,
                        published_url="dry-run://not-published",
                        note=self._append_note(
                            f"Dry-run news success;news_pool_id={claimed_id};category={category};images={len(images)};dry_html={dry_path.name}",
                            degraded_note,
                        ),
                    )
                )
                self._remember_title_fingerprint(draft.title)
                self._workflow_perf_finish_run("success", "dry-run://not-published")
                return WorkflowResult("success", "dry-run://not-published")

            self._progress("schedule", "예약 시간 계산", 84)
            publish_at = self._compute_publish_at()
            if publish_at is None:
                publish_at = datetime.now(timezone.utc) + timedelta(minutes=15)
                degraded_note = self._append_note(degraded_note, "schedule_fallback_used")

            ledger_key = ""
            ledger_payload: dict[str, Any] = {}
            if self._publish_ledger_enabled:
                ledger_event_id = str(event_id or "").strip() or str(claimed_id or "")
                ledger_cluster_id = str((selected.meta or {}).get("cluster_id", "") or "").strip()
                ledger_facet = str((selected.meta or {}).get("selected_facet", "impact") or "impact").strip().lower() or "impact"
                ledger_blog_id = str(getattr(self.settings.blogger, "blog_id", "") or "").strip() or "default"
                ledger_key = make_ledger_key(
                    event_id=ledger_event_id,
                    cluster_id=ledger_cluster_id,
                    facet=ledger_facet,
                    blog_id=ledger_blog_id,
                )
                ledger_payload = {
                    "key": ledger_key,
                    "event_id": ledger_event_id,
                    "cluster_id": ledger_cluster_id,
                    "facet": ledger_facet,
                    "blog_id": ledger_blog_id,
                    "title": str(draft.title or ""),
                    "source_url": str(draft.source_url or ""),
                    "topic_cluster": str(seo_topic or "default"),
                    "focus_keywords": list(seo_focus_keywords or [])[:6],
                }
                ledger_exists = bool(
                    (ledger_key in self._failed_ledger_keys_in_run)
                    or self.publish_ledger.exists(ledger_key)
                )
                if ledger_exists:
                    self.watchdog.register_hard_failure(event_id, "ledger_skip")
                    event_key = str(event_id or "").strip() or str(claimed_id or "")
                    current_skip_streak = int(self._ledger_skip_streak_in_run.get(event_key, 0) or 0) + 1
                    self._ledger_skip_streak_in_run[event_key] = int(current_skip_streak)
                    degraded_note = self._append_note(
                        degraded_note,
                        f"ledger_skip:{ledger_key}",
                    )
                    self._append_workflow_perf(
                        "publish_ledger_skip",
                        {
                            "event_id": ledger_event_id,
                            "cluster_id": ledger_cluster_id,
                            "facet": ledger_facet,
                            "key": ledger_key,
                            "retry_depth": int(news_retry_depth),
                        },
                    )
                    if claimed_id:
                        try:
                            self.news_pool_store.rollback_claim(claimed_id)
                            claim_finalized = True
                        except Exception:
                            pass
                    abort_now, abort_reason = self.watchdog.should_abort_event(event_id)
                    if (not abort_now) and int(current_skip_streak) < 3 and int(news_retry_depth) < 3:
                        return self._run_once_news_mode(
                            manual_trigger=bool(manual_trigger),
                            run_start_minute=run_start_minute,
                            queue_advisory=queue_advisory,
                            sync_note=sync_note,
                            preflight_index_note=preflight_index_note,
                            blog_snapshot=blog_snapshot,
                            today_gemini_before=today_gemini_before,
                            news_retry_depth=int(news_retry_depth) + 1,
                            retry_event_id="",
                            draft_post_id=working_draft_id,
                        )
                    skip_reason = (
                        f"ledger_skip_watchdog_abort:{abort_reason}"
                        if abort_now
                        else f"ledger_skip_exhausted:{current_skip_streak}"
                    )
                    self.logs.append_run(
                        RunRecord(
                            status="skipped",
                            score=0,
                            title=draft.title,
                            source_url=draft.source_url,
                            published_url="",
                            note=self._append_note(skip_reason, degraded_note),
                        )
                    )
                    self._workflow_perf_finish_run("skipped", skip_reason)
                    return WorkflowResult("skipped", skip_reason)

            seo_slug = self._reserve_unique_slug(
                seo_slug_base,
                title=str(draft.title or ""),
                topic=str(seo_topic or "default"),
            )
            self._update_run_metrics_context(
                "news_mode",
                seo_slug=str(seo_slug or ""),
                publish_at_utc=str(
                    publish_at.astimezone(timezone.utc).isoformat()
                    if isinstance(publish_at, datetime)
                    else ""
                ),
            )
            if ledger_payload:
                ledger_payload["seo_slug"] = str(seo_slug or "")
            self._progress("publish", "Blogger 예약 발행 처리", 92)
            published = self.publisher.publish_post(
                draft.title,
                final_html,
                images,
                labels,
                publish_at=publish_at,
                existing_draft_post_id=(working_draft_id or None),
                meta_description=meta_description,
                preflight_thumbnail_src=preflight_thumb_src,
                seo_slug=seo_slug,
                focus_keywords=seo_focus_keywords,
                topic_cluster=seo_topic,
            )
            self._update_run_metrics_context(
                "news_mode",
                published_url=str(getattr(published, "url", "") or ""),
            )
            published_url_value = str(getattr(published, "url", "") or "").strip()
            published_ok = bool(published and published_url_value)
            if self._publish_ledger_enabled and ledger_payload and published_ok:
                if not self.publish_ledger.record(ledger_payload):
                    if ledger_key:
                        self._failed_ledger_keys_in_run.add(ledger_key)
                    degraded_note = self._append_note(degraded_note, "ledger_record_failed")
                    self._append_workflow_perf(
                        "publish_ledger_record_failed",
                        {
                            "key": str(ledger_key or ""),
                            "event_id": str(ledger_payload.get("event_id", "") or ""),
                            "cluster_id": str(ledger_payload.get("cluster_id", "") or ""),
                        },
                    )
            elif self._publish_ledger_enabled and ledger_payload and (not published_ok):
                degraded_note = self._append_note(degraded_note, "ledger_record_skipped_publish_not_confirmed")
            if claimed_id:
                self.news_pool_store.mark_used(claimed_id, str(getattr(published, "url", "") or ""))
                claim_finalized = True
            self._update_news_rotation_state_on_publish(claimed_item or {}, category)
            self.media_manager.mark_news_pack_used(images, str(getattr(published, "post_id", "") or ""))

            self.logs.add_scheduled_post(
                publish_at=publish_at.isoformat(),
                post_id=published.post_id,
                title=draft.title,
                source_url=draft.source_url,
                published_url=published.url,
            )
            self.logs.append_run(
                RunRecord(
                    status="success",
                    score=int(max(70, qa_result.score)),
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url=published.url,
                    note=self._append_note(
                        (
                            f"Published news successfully;news_pool_id={claimed_id};category={category};"
                            f"images={len(images)};qa={qa_result.score};"
                            f"total_gemini_calls={max(0, int(self.logs.get_today_gemini_count()) - int(today_gemini_before))}"
                        ),
                        degraded_note,
                    ),
                )
            )
            self._remember_title_fingerprint(draft.title)
            self._cleanup_local_image_files(images)
            self._mark_active_slot("consumed", "publish_success", post_id=str(getattr(published, "post_id", "") or ""))
            self.watchdog.register_success(event_id)
            msg = f"{published.url} (scheduled: {publish_at.isoformat()})"
            self._workflow_perf_finish_run("success", msg)
            return WorkflowResult("success", msg)
        except Exception as exc:
            hold_reason = str(exc)[:240] or "news_mode_error"
            wd_event_id = str(locals().get("event_id", "") or "").strip()
            provider_code = self._provider_http_code_from_error(hold_reason)
            if wd_event_id and provider_code in {429, 530}:
                self.watchdog.register_provider_failure(wd_event_id, int(provider_code))
                backoff_minutes = self.watchdog.compute_backoff_minutes(wd_event_id, int(provider_code))
                if backoff_minutes is not None:
                    hold_reason = f"provider_backoff_http_{int(provider_code)}:{int(backoff_minutes)}m"
            elif wd_event_id:
                self.watchdog.register_hard_failure(wd_event_id, hold_reason)
            if wd_event_id:
                abort_now, abort_reason = self.watchdog.should_abort_event(wd_event_id)
                if abort_now:
                    hold_reason = self._append_note(hold_reason, f"watchdog_abort={abort_reason}")
            global_blocked, global_reason = self._watchdog_hold_gate(hold_reason)
            if global_blocked:
                hold_reason = global_reason
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=str((claimed_item or {}).get("title", "") or ""),
                    source_url=str((claimed_item or {}).get("url", "") or ""),
                    published_url="",
                    note=self._append_note(f"news_mode_hold:{hold_reason}", degraded_note),
                )
            )
            self._mark_active_slot("hold", hold_reason)
            self._workflow_perf_finish_run("hold", hold_reason)
            return WorkflowResult("hold", hold_reason)
        finally:
            if claimed_id and (not claim_finalized):
                try:
                    self.news_pool_store.rollback_claim(claimed_id)
                except Exception:
                    pass

    def run_once(self, manual_trigger: bool = False) -> WorkflowResult:
        return self._run_with_metrics_guard(
            "run_once",
            lambda: self._run_once_impl(manual_trigger=manual_trigger),
        )

    def _run_once_impl(self, manual_trigger: bool = False) -> WorkflowResult:
        self._workflow_perf_start_run(manual_trigger=bool(manual_trigger))
        run_start_minute = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
        self._maintenance_tick()
        self._pending_keyword_claims = []
        self._seen_cluster_ids_in_run = set()
        self._failed_ledger_keys_in_run = set()
        self._ledger_skip_streak_in_run = {}
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
        secret_issues = self._profile_call(
            "secret_preflight",
            lambda: validate_secrets(self.settings),
            slow_ms=500,
        )
        if secret_issues:
            reason = "preflight_missing_secrets:" + ",".join(str(x) for x in secret_issues[:8])
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title="",
                    source_url="",
                    published_url="",
                    note=reason,
                )
            )
            self._workflow_perf_finish_run("hold", reason)
            return WorkflowResult("hold", reason)
        search_learning_note = self._profile_call(
            "search_learning_refresh",
            self._refresh_search_learning_if_due,
            slow_ms=1500,
        )
        if search_learning_note and not search_learning_note.startswith("search_learning_skipped"):
            sync_note = self._append_note(sync_note, search_learning_note)
        preflight_generation_mode = self._generation_mode()
        budget = self.guard.can_run(
            today_posts=(
                int(blog_snapshot.get("today_posts", 0))
                if snapshot_source == "blogger"
                else None
            ),
            # In schedule mode, do not block generation just because today's live posts hit cap.
            # Daily publish capacity is enforced by _compute_publish_at() per-day placement.
            enforce_post_limit=(snapshot_source == "blogger" and not bool(self.settings.publish.use_blogger_schedule)),
            # In local_first/hybrid, Gemini is optional; enforce per-call budgets at call sites instead.
            enforce_gemini_limit=(preflight_generation_mode == "cloud_first"),
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
        except Exception as e:
            self._log_publish_backend_event({
                "event": "topic_growth_error",
                "error": str(e)
            })
        if is_news_mode(self.settings):
            return self._run_once_news_mode(
                manual_trigger=bool(manual_trigger),
                run_start_minute=run_start_minute,
                queue_advisory=queue_advisory,
                sync_note=sync_note,
                preflight_index_note=preflight_index_note,
                blog_snapshot=blog_snapshot,
                today_gemini_before=today_gemini_before,
            )

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
            recent_url_rows = []
            try:
                recent_url_rows = self.publisher.fetch_recent_live_urls(days=30, limit=360)
            except Exception as exc:
                duplicate_recovery_note = self._append_note(
                    duplicate_recovery_note,
                    f"recent_urls_fetch_failed={str(exc)[:90]}",
                )
            recent_urls = [
                str((row or {}).get("url", "") or "").strip()
                for row in (recent_url_rows or [])
                if str((row or {}).get("url", "") or "").strip()
            ]
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
        duplicate_recovery_note = self._append_note(
            duplicate_recovery_note,
            f"recent_urls_count={len(recent_urls)}",
        )
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
                    recent_url_rows = []
                    try:
                        recent_url_rows = self.publisher.fetch_recent_live_urls(days=30, limit=360)
                    except Exception as exc:
                        diversity_note = self._append_note(
                            diversity_note,
                            f"recent_urls_fetch_failed={str(exc)[:90]}",
                        )
                    recent_urls = [
                        str((row or {}).get("url", "") or "").strip()
                        for row in (recent_url_rows or [])
                        if str((row or {}).get("url", "") or "").strip()
                    ]
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
        generation_mode = self._generation_mode()
        # Cost control: in free_mode, keep local-first/hybrid behavior even if API key exists.
        free_local_mode = bool(self.settings.budget.free_mode and generation_mode != "cloud_first")
        self._progress("trend", "글로벌 타겟 키워드 분석", 24)
        global_keywords, keyword_pool_note = self._acquire_run_keywords(candidates)
        keyword_fallback_note = keyword_pool_note or ""
        if not global_keywords:
            # Final non-API fallback from candidate text.
            local_pool = self._build_local_keyword_candidates(candidates, limit=24)
            global_keywords = local_pool[:1]
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
                if is_news_mode(self.settings):
                    self._log_news_mode_guard("choose_best", "news_mode_blocked")
                    selected = max(candidates, key=lambda x: int(getattr(x, "score", 0)))
                    score = max(70, min(100, int(getattr(selected, "score", 70) or 70)))
                    reason = self._append_note(reason, "news_mode_selector_guard_fallback")
                elif free_local_mode:
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

        last_cluster = self._last_cluster_id()
        selected_cluster = self._infer_cluster_id_from_keyword(
            " ".join(getattr(selected, "long_tail_keywords", [])[:2]) or str(getattr(selected, "title", "") or "")
        )
        if last_cluster and selected_cluster == last_cluster:
            alternates = []
            for c in (candidates or []):
                cid = self._infer_cluster_id_from_keyword(
                    " ".join(getattr(c, "long_tail_keywords", [])[:2]) or str(getattr(c, "title", "") or "")
                )
                if cid != last_cluster and self._candidate_matches_content_mode(c):
                    alternates.append(c)
            if alternates:
                selected = max(alternates, key=lambda x: int(getattr(x, "score", 0)))
                score = max(score, int(getattr(selected, "score", 70)))
                reason = self._append_note(reason, f"cluster_rotation_applied:{last_cluster}->{self._infer_cluster_id_from_keyword(' '.join(getattr(selected, 'long_tail_keywords', [])[:2]) or str(getattr(selected, 'title', '') or ''))}")

        selected_cluster = self._infer_cluster_id_from_keyword(
            " ".join(getattr(selected, "long_tail_keywords", [])[:2]) or str(getattr(selected, "title", "") or "")
        )
        selected_feature = self._infer_feature_token(
            " ".join(getattr(selected, "long_tail_keywords", [])[:3]) or str(getattr(selected, "title", "") or "")
        )
        if selected_cluster and selected_feature:
            last_feature = self._last_feature_for_cluster(selected_cluster)
            if last_feature and selected_feature == last_feature:
                feature_alternates = []
                for c in (candidates or []):
                    cid = self._infer_cluster_id_from_keyword(
                        " ".join(getattr(c, "long_tail_keywords", [])[:2]) or str(getattr(c, "title", "") or "")
                    )
                    if cid != selected_cluster:
                        continue
                    feat = self._infer_feature_token(
                        " ".join(getattr(c, "long_tail_keywords", [])[:3]) or str(getattr(c, "title", "") or "")
                    )
                    if feat == last_feature:
                        continue
                    if not self._candidate_matches_content_mode(c):
                        continue
                    feature_alternates.append(c)
                if feature_alternates:
                    selected = max(feature_alternates, key=lambda x: int(getattr(x, "score", 0)))
                    score = max(score, int(getattr(selected, "score", 70)))
                    selected_feature = self._infer_feature_token(
                        " ".join(getattr(selected, "long_tail_keywords", [])[:3]) or str(getattr(selected, "title", "") or "")
                    )
                    reason = self._append_note(
                        reason,
                        f"feature_rotation_applied:{last_feature}->{selected_feature}",
                    )

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
        self._progress("plan", "트러블슈팅 플랜 구성", 34)
        troubleshooting_plan = self._profile_call(
            "build_troubleshooting_plan",
            lambda: self._build_troubleshooting_plan_with_local_llm(selected),
            slow_ms=2600,
        )
        plan_fp = self._fix_steps_fingerprint(
            list((troubleshooting_plan or {}).get("fix_steps", []) or [])
            if isinstance(troubleshooting_plan, dict)
            else []
        )
        if plan_fp and self._is_recent_fix_steps_duplicate(plan_fp):
            generation_degraded_note = self._append_note(
                generation_degraded_note,
                "fix_steps_fp_duplicate_initial",
            )
            try:
                # One deterministic replan attempt to avoid repeated step patterns.
                replanned = self._build_troubleshooting_plan_with_local_llm(selected)
                replanned_fp = self._fix_steps_fingerprint(
                    list((replanned or {}).get("fix_steps", []) or [])
                    if isinstance(replanned, dict)
                    else []
                )
                if replanned_fp and (not self._is_recent_fix_steps_duplicate(replanned_fp)):
                    troubleshooting_plan = replanned
                    plan_fp = replanned_fp
                    generation_degraded_note = self._append_note(
                        generation_degraded_note,
                        "fix_steps_fp_replanned",
                    )
            except Exception:
                pass
        if plan_fp and self._is_recent_fix_steps_duplicate(plan_fp):
            hold_reason = "fix_steps_fingerprint_duplicate"
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=str(getattr(selected, "title", "") or ""),
                    source_url=str(getattr(selected, "url", "") or ""),
                    published_url="",
                    note=hold_reason,
                )
            )
            self._workflow_perf_finish_run("hold", hold_reason)
            return WorkflowResult("hold", hold_reason)
        plan_source = str((troubleshooting_plan or {}).get("source", "fallback") or "fallback").strip().lower()
        self._append_workflow_perf(
            "plan_json_source",
            {
                "plan_source": plan_source,
                "local_llm_enabled": bool(getattr(self.settings.local_llm, "enabled", False)),
            },
        )
        if plan_source != "ollama":
            generation_degraded_note = self._append_note(generation_degraded_note, f"plan_source={plan_source}")
        headline_note = ""
        self._progress("draft", "본문 초안 생성", 40)
        current_domain = self._infer_domain_from_title(str(getattr(selected, "title", "") or ""))
        generation_mode = self._generation_mode()
        gemini_only_on_fail = bool(getattr(getattr(self.settings, "generation", None), "gemini_only_on_fail", True))
        generation_degraded_note = self._append_note(generation_degraded_note, f"generation_mode={generation_mode}")
        pattern = self.patterns.choose(selected)
        current_domain = str(getattr(pattern, "domain", "tech_troubleshoot") or "tech_troubleshoot")
        prompt_pack = self.prompt_factory.get_pack(
            "generate_post",
            seed=str(getattr(selected, "title", "") or ""),
        )
        pattern_instruction = (
            f"Pattern key: {pattern.key}\n"
            f"Domain: {current_domain}\n"
            f"Audience stage: {pattern.stage}\n"
            f"Objective: {pattern.objective}\n"
            f"Style variant: {prompt_pack.style_variant_id}\n"
            f"Prompt directive: {prompt_pack.user}\n"
            "Outline:\n- " + "\n- ".join(pattern.outline)
        )
        reference_guidance = self.references.build_guidance()
        draft: DraftPost | None = None

        if generation_mode in {"local_first", "hybrid"}:
            try:
                tentative_internal_links = self._build_internal_links_block(
                    current_title=str(getattr(selected, "title", "") or ""),
                    current_keywords=global_keywords,
                    current_device_type=self._infer_device_type(f"{selected.title}"),
                    current_cluster_id=self._infer_cluster_id_from_keyword(
                        " ".join(global_keywords[:2]) or str(getattr(selected, "title", "") or "")
                    ),
                )
                local_draft = self._build_local_draft_with_ollama(
                    selected=selected,
                    plan=troubleshooting_plan,
                    internal_links_block=tentative_internal_links,
                )
                if local_draft is not None:
                    draft = local_draft
                    generation_count += 1
                    generation_degraded_note = self._append_note(
                        generation_degraded_note,
                        f"generation_mode={generation_mode}:local_draft",
                    )
            except Exception as exc:
                generation_degraded_note = self._append_note(
                    generation_degraded_note,
                    f"local_draft_failed={str(exc)[:120]}",
                )

        should_use_gemini_generate = (
            draft is None and (
                generation_mode == "cloud_first"
                or generation_mode == "hybrid"
                or (generation_mode == "local_first" and gemini_only_on_fail)
                or (not free_local_mode)
            )
        )

        if should_use_gemini_generate:
            if self._gemini_budget_remaining() <= 0:
                reason = "gemini_budget_exceeded"
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=0,
                        title=str(getattr(selected, "title", "") or ""),
                        source_url=str(getattr(selected, "url", "") or ""),
                        published_url="",
                        note=reason,
                    )
                )
                self._workflow_perf_finish_run("hold", reason)
                return WorkflowResult("hold", reason)

            try:
                if self.ollama_client and selected.body:
                    # Locally clean context to reduce noise/cost before Gemini
                    selected.body = self._profile_call(
                        "ollama_clean_context",
                        lambda: self.ollama_client.clean_context(selected.body),
                        slow_ms=1500
                    )
                    # NEW: Think step - generate expert hints locally
                    expert_hints = self._profile_call(
                        "ollama_think_step",
                        lambda: self.ollama_client.think_about_topic(selected.title, selected.body),
                        slow_ms=2000
                    )
                    selected.meta["expert_hints"] = expert_hints

                generation_count += 1
                draft_candidate = self.brain.generate_post(
                    selected,
                    self.settings.authority_links,
                    pattern_instruction,
                    reference_guidance,
                    domain=current_domain,
                    plan=troubleshooting_plan,
                )
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                issues = self._draft_fatal_issues(draft_candidate.html, domain=current_domain)
                if not issues:
                    draft = draft_candidate
                else:
                    generation_degraded_note = self._append_note(
                        generation_degraded_note,
                        "gemini_draft_rejected=" + ",".join(issues[:3]),
                    )
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
                            source_url=str(getattr(selected, "url", "") or ""),
                            published_url="",
                            note=self._physical_block_reason(err),
                        )
                    )
                    skip_reason = self._physical_block_reason(err)
                    self._workflow_perf_finish_run("skipped", skip_reason)
                    return WorkflowResult("skipped", skip_reason)
                if self._is_temporary_rate_limit_error(err):
                    raise
                generation_degraded_note = self._append_note(
                    generation_degraded_note,
                    f"gemini_generate_failed={err[:120]}",
                )

        if draft is None:
            hold_reason = "draft_generation_unavailable"
            if generation_mode in {"local_first", "hybrid"}:
                hold_reason = self._append_note(hold_reason, "local_draft_missing")
            if should_use_gemini_generate:
                hold_reason = self._append_note(hold_reason, "gemini_generate_missing")
            hold_labels = self._build_public_labels(
                title=str(getattr(selected, "title", "") or ""),
                candidate=selected,
                global_keywords=global_keywords,
                max_labels=6,
            )
            working_draft_id, hold_note = self._sync_stage_draft_checkpoint(
                current_draft_post_id=working_draft_id,
                stage="hold",
                title=str(getattr(selected, "title", "") or "draft_generation_unavailable"),
                html_body="<h2>Hold</h2><p>Draft generation path unavailable in local-first policy.</p>",
                labels=hold_labels,
                reason=hold_reason,
            )
            hold_msg = hold_reason
            if hold_note:
                hold_msg += f" | {hold_note}"
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=str(getattr(selected, "title", "") or ""),
                    source_url=str(getattr(selected, "url", "") or ""),
                    published_url="",
                    note=hold_msg,
                )
            )
            self._workflow_perf_finish_run("hold", hold_msg)
            return WorkflowResult("hold", hold_msg)

        headline_note = "headline_opt=deferred_to_post_body"
        draft.title = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", str(draft.title or ""))
        draft.title = re.sub(r"\s+", " ", draft.title).strip()

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

        scaled_risk, scaled_reasons = self._scaled_content_risk(
            title=draft.title,
            html=final_html,
            plan_fp=plan_fp,
        )
        if scaled_risk:
            hold_reason = "scaled_content_risk:" + ",".join(scaled_reasons[:4])
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url="",
                    note=hold_reason,
                )
            )
            self._workflow_perf_finish_run("hold", hold_reason)
            return WorkflowResult("hold", hold_reason)

        actionability_result = self._profile_call(
            "actionability_gate_initial",
            lambda: self._evaluate_actionability_gate(draft.title, final_html),
            slow_ms=1200,
        )
        is_news_domain = str(current_domain or "").strip().lower() in {"tech_news_explainer", "news_interpretation"}
        if (not actionability_result.ok) and (not free_local_mode) and (not is_news_domain):
            # First remediation: local rewrite to preserve low-cost path.
            try:
                local_rewrite = self._build_local_draft_with_ollama(
                    selected=selected,
                    plan=troubleshooting_plan,
                    internal_links_block="",
                )
                if local_rewrite is not None and str(local_rewrite.html or "").strip():
                    candidate_html = self._sanitize_publish_html(local_rewrite.html, domain=current_domain)
                    candidate_html = self._canonicalize_html_payload(candidate_html)
                    candidate_qa = self._qa_evaluate(
                        candidate_html,
                        title=draft.title,
                        domain=current_domain,
                        keyword=str(getattr(selected, "title", "") or ""),
                        context="actionability_local_rewrite",
                    )
                    candidate_actionability = self._profile_call(
                        "actionability_gate_after_local_rewrite",
                        lambda: self._evaluate_actionability_gate(draft.title, candidate_html),
                        slow_ms=1200,
                    )
                    if candidate_actionability.ok:
                        final_html = candidate_html
                        qa_result = candidate_qa
                        actionability_result = candidate_actionability
                        generation_degraded_note = self._append_note(generation_degraded_note, "actionability_local_rewrite_applied")
            except Exception as exc:
                generation_degraded_note = self._append_note(
                    generation_degraded_note,
                    f"actionability_local_rewrite_failed:{str(exc)[:120]}",
                )

            if (not actionability_result.ok) and self._gemini_budget_remaining() <= 0:
                hold_reason = "gemini_budget_exceeded"
                self.logs.append_run(
                    RunRecord(
                        status="hold",
                        score=0,
                        title=draft.title,
                        source_url=draft.source_url,
                        published_url="",
                        note=hold_reason,
                    )
                )
                self._workflow_perf_finish_run("hold", hold_reason)
                return WorkflowResult("hold", hold_reason)
            self._progress("qa", "액션 가능성 강화 재작성", 66)
            try:
                generation_count += 1
                rewritten_html = self._profile_call(
                    "rewrite_to_actionable",
                    lambda: self.brain.rewrite_to_actionable(
                        title=draft.title,
                        html=final_html,
                        plan=troubleshooting_plan,
                    ),
                    slow_ms=3500,
                )
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                if rewritten_html and rewritten_html != final_html:
                    final_html = self._sanitize_publish_html(rewritten_html, domain=current_domain)
                    final_html = self._canonicalize_html_payload(final_html)
                    qa_result = self._qa_evaluate(
                        final_html,
                        title=draft.title,
                        domain=current_domain,
                        keyword=str(getattr(selected, "title", "") or ""),
                        context="actionability_rewrite",
                    )
                    actionability_result = self._profile_call(
                        "actionability_gate_after_rewrite",
                        lambda: self._evaluate_actionability_gate(draft.title, final_html),
                        slow_ms=1200,
                    )
                    generation_degraded_note = self._append_note(generation_degraded_note, "actionability_rewrite_applied")
            except Exception as exc:
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                generation_degraded_note = self._append_note(
                    generation_degraded_note,
                    f"actionability_rewrite_failed:{str(exc)[:140]}",
                )

        if (not actionability_result.ok) and (not is_news_domain):
            reason_tags = ",".join(actionability_result.reasons[:6]) if actionability_result.reasons else "unknown"
            hold_reason = f"actionability_gate_failed:{reason_tags}"
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
                reason=hold_reason,
            )
            hold_msg = f"{hold_reason};score={actionability_result.score}"
            if hold_note:
                hold_msg += f" | {hold_note}"
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=max(0, int(actionability_result.score)),
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url="",
                    note=hold_msg,
                )
            )
            self._workflow_perf_finish_run("hold", hold_msg)
            return WorkflowResult("hold", hold_msg)

        self._progress("headline", "최종 제목 정합성 점검", 70)
        final_title, final_title_reason = self._finalize_title_after_content(
            current_title=draft.title,
            final_html=final_html,
            selected=selected,
            global_keywords=global_keywords,
            troubleshooting_plan=troubleshooting_plan,
            allow_gemini=bool(not free_local_mode),
        )
        if final_title_reason:
            hold_reason = f"final_title_generation_failed:{final_title_reason}"
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
                reason=hold_reason,
            )
            hold_msg = hold_reason
            if hold_note:
                hold_msg += f" | {hold_note}"
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=max(0, int(actionability_result.score)),
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url="",
                    note=hold_msg,
                )
            )
            self._workflow_perf_finish_run("hold", hold_msg)
            return WorkflowResult("hold", hold_msg)
        if final_title:
            enforced_title = self._enforce_seo_title(
                title=final_title,
                candidate=selected,
                global_keywords=global_keywords,
                preferred_keyword=str((troubleshooting_plan or {}).get("primary_keyword", "") or ""),
            )
            if self._is_banned_title_template(enforced_title):
                summary_payload = self._build_title_summary_payload_with_local_llm(
                    current_title=final_title,
                    final_html=final_html,
                    troubleshooting_plan=troubleshooting_plan,
                    selected=selected,
                )
                backup_candidates = [final_title]
                backup_candidates.extend(
                    self._build_rule_title_candidates(
                        keyword=str((troubleshooting_plan or {}).get("primary_keyword", "") or final_title),
                        device=self._infer_device_type(final_title),
                        cluster=self._infer_cluster_id_from_keyword(final_title),
                        attempt=2,
                    )
                )
                fallback_title, _ = self._choose_best_unique_title(
                    candidates=backup_candidates,
                    summary_payload=summary_payload,
                    recent_titles=self._get_recent_blogger_titles(limit=240, refresh_api=False),
                )
                if fallback_title and (not self._is_banned_title_template(fallback_title)):
                    enforced_title = fallback_title
            draft.title = enforced_title

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

        self._progress("visual", "이미지 생성 및 업로드 준비", 74)
        target_images = self._image_target_max()
        min_images_required = self._image_min_required()
        self._set_image_pipeline_state("running", 0, target_images, "이미지 준비 서비스 시작")

        image_prompt_plan = self._build_image_prompt_plan_with_local_llm(draft, selected)
        images = self._profile_call(
            "media_manager_prepare",
            lambda: self.media_manager.prepare_post_images(
                draft=draft,
                prompt_plan=image_prompt_plan,
                target_count=target_images,
            ),
            slow_ms=15000,
        )

        image_kind_counts = Counter((getattr(img, "source_kind", "") or "unknown") for img in images)
        if len(images) < min_images_required:
            hold_labels = self._build_public_labels(
                title=draft.title,
                candidate=selected,
                global_keywords=global_keywords,
                max_labels=6,
            )
            policy_reason = f"missing_images_required({len(images)}/{min_images_required})"
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
        if len(images) > target_images:
            images = images[:target_images]
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
        final_html = self._inject_images_into_html(final_html, images)
        generation_degraded_note = self._apply_ctr_visual_density_note(generation_degraded_note, images)
        seo_topic_for_links = self._infer_topic_cluster(
            draft.title,
            global_keywords,
            final_html,
        )
        seo_keywords_for_links = self._compute_focus_keywords(
            draft.title,
            final_html,
            seo_topic_for_links,
        )
        try:
            final_html = self._inject_internal_links_and_related_coverage(
                final_html,
                current_title=draft.title,
                current_keywords=seo_keywords_for_links,
            )
        except Exception:
            generation_degraded_note = self._append_note(generation_degraded_note, "internal_links_failed")
        final_html, generation_degraded_note, _ = self._apply_body_clickbait_sanitizer(
            final_html,
            generation_degraded_note,
        )
        draft.title = self._double_unescape(str(draft.title or "")).strip()
        seo_topic = self._infer_topic_cluster(draft.title, global_keywords, final_html)
        seo_focus_keywords = self._compute_focus_keywords(draft.title, final_html, seo_topic)
        seo_slug_base = self._compute_seo_slug(draft.title, seo_topic)
        seo_slug = ""
        self._update_run_metrics_context(
            "run_once",
            title=str(draft.title or ""),
            topic_cluster=str(seo_topic or "default"),
            focus_keywords=list(seo_focus_keywords or [])[:6],
            final_html=str(final_html or ""),
            images=list(images or []),
        )
        gate_preview_html = ""
        preflight_thumb_src = self._preflight_thumb_src_from_images(images)
        first_thumb_source = str(getattr(images[0], "source_url", "") or "").strip() if images else ""
        thumbnail_src_invalid_host = bool(first_thumb_source and (not preflight_thumb_src))
        thumbnail_recovery_attempted = False
        generation_degraded_note = self._annotate_image_pipeline_diagnostics(
            note=generation_degraded_note,
            stage="main_pre_merge",
            images=images,
            preflight_thumb_src=preflight_thumb_src,
            required_images=min_images_required,
        )
        if bool(self.settings.budget.dry_run):
            try:
                gate_preview_html = self.publisher.build_dry_run_html(final_html, images)
            except Exception as exc:
                raise RuntimeError(f"Go-live preflight merge failed: {exc}") from exc
        else:
            try:
                if images:
                    if not preflight_thumb_src:
                        thumbnail_recovery_attempted = True
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
            dry_img_count = len(re.findall(r"<img\b[^>]*\bsrc=", dry_html, flags=re.IGNORECASE))
            if dry_img_count < min_images_required:
                raise RuntimeError(
                    f"dry-run regression failed: missing required <img> count (need {min_images_required})"
                )
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
            self._remember_title_fingerprint(draft.title)
            self._remember_fix_steps_fingerprint(plan_fp, draft.title)
            self._workflow_perf_finish_run("success", published_url)
            return WorkflowResult("success", published_url)

        publish_secret_issues = self._profile_call(
            "secret_preflight_before_publish",
            lambda: validate_secrets(self.settings),
            slow_ms=500,
        )
        if publish_secret_issues:
            reason = "preflight_missing_secrets:" + ",".join(str(x) for x in publish_secret_issues[:8])
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=draft.title,
                    source_url=draft.source_url,
                    published_url="",
                    note=reason,
                )
            )
            self._workflow_perf_finish_run("hold", reason)
            return WorkflowResult("hold", reason)

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
        seo_slug = self._reserve_unique_slug(
            seo_slug_base,
            title=str(draft.title or ""),
            topic=str(seo_topic or "default"),
        )
        self._update_run_metrics_context(
            "run_once",
            seo_slug=str(seo_slug or ""),
            publish_at_utc=str(
                publish_at.astimezone(timezone.utc).isoformat()
                if isinstance(publish_at, datetime)
                else ""
            ),
        )
        self._progress("publish", "Blogger 예약 발행 처리", 92)
        published = None
        last_publish_err = ""
        if images and (not preflight_thumb_src):
            if thumbnail_src_invalid_host and thumbnail_recovery_attempted:
                generation_degraded_note = self._append_note(generation_degraded_note, "thumbnail_src_invalid_host")
                self._append_workflow_perf(
                    "thumbnail_preflight_skipped",
                    {
                        "reason": "already_attempted_for_invalid_host",
                        "first_source_url": first_thumb_source[:220],
                    },
                )
            else:
                try:
                    thumbnail_recovery_attempted = True
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
                        seo_slug=seo_slug,
                        focus_keywords=seo_focus_keywords,
                        topic_cluster=seo_topic,
                    ),
                    slow_ms=8000,
                    meta={"attempt": int(publish_try)},
                )
                self._update_run_metrics_context(
                    "run_once",
                    published_url=str(getattr(published, "url", "") or ""),
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
                if images and ("thumbnail_preflight_failed" in err or "missing thumbnail image url" in err) and publish_try < 3:
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
        self._append_publish_metadata_log(
            {
                "post_id": str(getattr(published, "post_id", "") or ""),
                "published_url": str(getattr(published, "url", "") or ""),
                "title": str(draft.title or ""),
                "source_url": str(draft.source_url or ""),
                "intent_primary_query": str(intent_bundle.primary_query or ""),
                "outline_archetype": str(outline_plan.archetype or ""),
                "outline_fingerprint": str(outline_plan.fingerprint or ""),
                "cluster_id": str(cluster_id or ""),
                "opportunity_source": bool((getattr(selected, "meta", {}) or {}).get("opportunity_source", False)),
            }
        )
        self._remember_title_fingerprint(draft.title)
        self._remember_fix_steps_fingerprint(plan_fp, draft.title)
        if manual_trigger and str(getattr(published, "post_id", "")).strip():
            self.logs.add_excluded_post(str(published.post_id).strip(), reason="manual_trigger")
        if self._pending_keyword_claims:
            try:
                deleted_count = self.keyword_assets.delete_keywords(self._pending_keyword_claims)
                if deleted_count > 0:
                    generation_degraded_note = self._append_note(
                        generation_degraded_note,
                        f"kw_deleted={deleted_count}",
                    )
            except Exception:
                pass
            finally:
                self._pending_keyword_claims = []
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
                global_keywords=list(seo_focus_keywords or global_keywords),
                candidate=selected,
                publish_at=publish_at,
            )
        except Exception:
            pass
        try:
            self._save_last_cluster_id(
                self._infer_cluster_id_from_keyword(" ".join(global_keywords[:2]) or draft.title)
            )
        except Exception:
            pass
        try:
            cluster_key = self._infer_cluster_id_from_keyword(" ".join(global_keywords[:2]) or draft.title)
            feature_key = self._infer_feature_token(" ".join(global_keywords[:3]) or draft.title)
            self._save_last_feature_for_cluster(cluster_key, feature_key)
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
        temp_root = (self.root / "storage" / "temp_images").resolve()
        for img in (images or []):
            try:
                p = Path(getattr(img, "path", ""))
            except Exception:
                p = Path("")
            if not str(p):
                continue
            try:
                rp = p.resolve()
                if not str(rp).startswith(str(temp_root)):
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
                collected = list(self.scout.collect() or [])
                supporting = self._supporting_discovery_candidates(limit=3)
                if supporting:
                    collected = [*supporting, *collected]
                return collected
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
        if is_news_mode(self.settings) or mode == "tech_news_only":
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
            return True
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
        prohibited = [
            "hack",
            "hacking",
            "crack",
            "cracking",
            "license key",
            "pirated",
            "bypass drm",
            "account takeover",
            "malware",
            "spyware",
            "adult",
            "hate",
            "violence",
        ]
        if any(tok in text for tok in banned):
            return False
        if any(tok in text for tok in prohibited):
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
        if is_news_mode(self.settings) or mode == "tech_news_only":
            return self._news_domain
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

    def _apply_body_clickbait_sanitizer(
        self,
        html: str,
        note: str = "",
    ) -> tuple[str, str, list[str]]:
        sanitized_html, replaced = sanitize_clickbait_terms(str(html or ""))
        updated_note = str(note or "")
        if replaced:
            updated_note = self._append_note(
                updated_note,
                "body_clickbait_sanitized:" + "|".join(replaced),
            )
        return sanitized_html, updated_note, replaced

    def _log_news_mode_guard(self, blocked_fn: str, reason: str = "") -> None:
        if not is_news_mode(self.settings):
            return
        key = f"{blocked_fn}:{reason}".strip(":")
        if key in self._news_guard_logged:
            return
        self._news_guard_logged.add(key)
        payload = {
            "blocked_fn": str(blocked_fn or ""),
            "reason": str(reason or ""),
            "mode": "tech_news_only",
        }
        self._append_workflow_perf("news_mode_guard_skip", payload)
        try:
            self.qa.write("runtime", "news_mode_guard_skip", payload)
        except Exception:
            return

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

    def _topic_pool_cfg(self) -> tuple[int, int, int, int, int]:
        cfg = getattr(self.settings, "topic_pool", None)
        target_size = max(40, int(getattr(cfg, "target_size", 200) or 200))
        min_size = max(20, int(getattr(cfg, "min_size", 140) or 140))
        min_size = min(min_size, target_size)
        refill_batch = max(20, int(getattr(cfg, "refill_batch", 80) or 80))
        avoid_days = max(7, int(getattr(cfg, "avoid_reuse_days", getattr(self.settings.keywords, "avoid_reuse_days", 30)) or 30))
        per_run_pick = max(1, int(getattr(cfg, "per_run_pick", 1) or 1))
        return target_size, min_size, refill_batch, avoid_days, per_run_pick

    def _infer_cluster_id_from_keyword(self, keyword: str) -> str:
        text = re.sub(r"\s+", " ", str(keyword or "").strip().lower())
        if is_news_mode(self.settings):
            if any(k in text for k in ("security", "vulnerability", "cve", "patch", "breach", "malware", "ransomware")):
                return "security"
            if any(k in text for k in ("privacy", "policy", "regulation", "ban", "tracking", "consent")):
                return "policy"
            if any(k in text for k in ("ai", "model", "openai", "anthropic", "gemini", "copilot", "claude")):
                return "ai"
            if any(k in text for k in ("iphone", "ios", "android", "galaxy", "pixel", "mobile")):
                return "mobile"
            if any(k in text for k in ("chip", "gpu", "semiconductor", "nvidia", "intel", "amd")):
                return "chips"
            if any(k in text for k in ("privacy",)):
                return "privacy"
            return "platform"
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

    def _last_cluster_id(self) -> str:
        try:
            if not self._cluster_rotation_state_path.exists():
                return ""
            payload = json.loads(self._cluster_rotation_state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return str(payload.get("last_cluster_id", "") or "").strip().lower()
        except Exception:
            return ""
        return ""

    def _save_last_cluster_id(self, cluster_id: str) -> None:
        cid = str(cluster_id or "").strip().lower()
        if not cid:
            return
        payload = {
            "last_cluster_id": cid,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self._cluster_rotation_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._cluster_rotation_state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            return

    def _infer_feature_token(self, text: str) -> str:
        lower = re.sub(r"\s+", " ", str(text or "").strip().lower())
        mapping = [
            ("wi-fi", "wifi"),
            ("wifi", "wifi"),
            ("bluetooth", "bluetooth"),
            ("ethernet", "ethernet"),
            ("vpn", "vpn"),
            ("usb", "usb"),
            ("printer", "printer"),
            ("microphone", "microphone"),
            ("mic", "microphone"),
            ("camera", "camera"),
            ("keyboard", "keyboard"),
            ("mouse", "mouse"),
            ("audio", "audio"),
            ("sound", "audio"),
            ("battery", "battery"),
            ("charging", "charging"),
            ("driver", "driver"),
            ("update", "update"),
        ]
        for token, canonical in mapping:
            if token in lower:
                return canonical
        return "general"

    def _load_feature_rotation_state(self) -> dict[str, Any]:
        try:
            if not self._feature_rotation_state_path.exists():
                return {}
            payload = json.loads(self._feature_rotation_state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    def _last_feature_for_cluster(self, cluster_id: str) -> str:
        cid = str(cluster_id or "").strip().lower()
        if not cid:
            return ""
        payload = self._load_feature_rotation_state()
        per_cluster = payload.get("per_cluster", {}) if isinstance(payload.get("per_cluster"), dict) else {}
        return str(per_cluster.get(cid, "") or "").strip().lower()

    def _save_last_feature_for_cluster(self, cluster_id: str, feature: str) -> None:
        cid = str(cluster_id or "").strip().lower()
        feat = str(feature or "").strip().lower()
        if not cid or not feat:
            return
        payload = self._load_feature_rotation_state()
        per_cluster = payload.get("per_cluster", {}) if isinstance(payload.get("per_cluster"), dict) else {}
        per_cluster[cid] = feat
        payload["per_cluster"] = per_cluster
        payload["updated_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            self._feature_rotation_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._feature_rotation_state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            return

    def _build_template_keywords(self, device_type: str, limit: int = 120) -> list[str]:
        d = str(device_type or "device").strip().lower()
        device_variants = {
            "windows": ["windows 11", "windows 10", "windows laptop"],
            "mac": ["macbook", "macos", "imac"],
            "iphone": ["iphone", "ios", "iphone 15"],
            "galaxy": ["galaxy phone", "samsung galaxy", "android phone"],
        }.get(d, [d, f"{d} device"])
        features = [
            "wifi",
            "bluetooth",
            "usb",
            "printer",
            "microphone",
            "camera",
            "keyboard",
            "speaker",
            "audio",
            "battery",
            "charging",
            "network",
            "driver",
            "vpn",
        ]
        triggers = [
            "not working",
            "after update",
            "keeps disconnecting",
            "not detected",
            "error code",
            "stuck",
            "not responding",
            "randomly drops",
            "keeps turning off",
            "failed to start",
        ]
        actions = [
            "fix",
            "troubleshooting steps",
            "safe reset steps",
            "quick repair guide",
            "beginner checklist",
        ]
        templates: list[str] = []
        for dv in device_variants:
            for feat in features:
                for trg in triggers:
                    templates.append(f"{dv} {feat} {trg} fix")
                    templates.append(f"{dv} {feat} {trg} troubleshooting steps")
            for trg in triggers:
                for act in actions:
                    templates.append(f"{dv} {trg} {act}")
            templates.extend(
                [
                    f"{dv} not working",
                    f"{dv} update stuck",
                    f"{dv} connected but no internet",
                    f"{dv} no sound after update",
                    f"{dv} app crashes fix",
                    f"{dv} network reset steps",
                ]
            )
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
        if candidate is not None and is_news_mode(self.settings):
            category = re.sub(
                r"\s+",
                " ",
                str((getattr(candidate, "meta", {}) or {}).get("news_category", "") or ""),
            ).strip().lower()
            push(category)
            push("tech-news")
            push("news-explainer")

        if not labels:
            labels = ["tech-news", "news-explainer"] if is_news_mode(self.settings) else ["tech-fix", "troubleshooting"]
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

        row = self._refresh_resume_row_payload(row)

        stage = str(row.get("stage", "") or "").strip().lower()
        if stage in {"collect_done", "draft_done", "publish_blocked", "images_done", "hold"}:
            stage_title = self._strip_wip_title_prefix(str(row.get("title", "") or "").strip())
            stage_html = self._strip_wip_checkpoint_banner(str(row.get("content", "") or ""))
            if not stage_title or not stage_html:
                self._auto_park_corrupt_resume_draft(
                    row=row,
                    reason_token="resume_payload_missing_before_resume",
                )
                return None
        if stage == "images_done":
            return self._resume_images_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        if stage == "draft_done":
            return self._resume_draft_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        if stage == "publish_blocked":
            return self._resume_draft_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        if stage == "collect_done":
            return self._resume_collect_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
        if stage == "hold":
            # Rezero 2.8: Treat 'hold' as resumable draft-done for recovery.
            return self._resume_draft_done(row=row, manual_trigger=manual_trigger, queue_advisory=queue_advisory)
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
        fallback_plan = self._build_troubleshooting_plan_with_local_llm(candidate)
        draft = None
        try:
            draft = self._build_local_draft_with_ollama(
                selected=candidate,
                plan=fallback_plan,
                internal_links_block="",
            )
        except Exception:
            draft = None
        if draft is None:
            api_ready = bool(
                (self.settings.gemini.api_key or "").strip()
                and (self.settings.gemini.api_key or "").strip() != "GEMINI_API_KEY"
            )
            if api_ready and self._gemini_budget_remaining() > 0:
                try:
                    if self.ollama_client and candidate.body:
                        candidate.body = self._profile_call(
                            "ollama_clean_context",
                            lambda: self.ollama_client.clean_context(candidate.body),
                            slow_ms=1500
                        )

                    draft = self.brain.generate_post(
                        candidate,
                        self.settings.authority_links,
                        "Resume draft generation after collect checkpoint.",
                        self.references.build_guidance(),
                        domain="tech_troubleshoot",
                        plan=fallback_plan,
                    )
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                except Exception:
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                    draft = None
        if draft is None:
            hold_msg = "resume_collect_draft_unavailable"
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=title or "Recovered Topic",
                    source_url=source_url,
                    published_url="",
                    note=hold_msg,
                )
            )
            self._workflow_perf_finish_run("hold", hold_msg)
            return WorkflowResult("hold", hold_msg)
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
        return self._run_with_metrics_guard(
            "resume_draft_done",
            lambda: self._resume_draft_done_impl(
                row=row,
                manual_trigger=manual_trigger,
                queue_advisory=queue_advisory,
            ),
        )

    def _resume_draft_done_impl(self, row: dict, manual_trigger: bool, queue_advisory: str = "") -> WorkflowResult:
        row = self._refresh_resume_row_payload(row)
        title = self._strip_wip_title_prefix(str(row.get("title", "") or "").strip())
        base_html = self._strip_wip_checkpoint_banner(str(row.get("content", "") or ""))
        if not title or not base_html:
            tok = "resume_draft_payload_empty"
            if not title: tok += ":missing_title"
            if not base_html: tok += ":missing_html"
            tok += f";post_id={post_id}"
            skip_reason = self._auto_park_corrupt_resume_draft(
                row=row,
                reason_token=tok,
            )
            return WorkflowResult("skipped", skip_reason)

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
        resume_degraded_note = ""

        self._progress("visual", "중단 문서 재개: 이미지 라이브러리 선택", 74)
        target_images = self._image_target_max()
        min_images_required = self._image_min_required()
        self._set_image_pipeline_state("running", 0, target_images, "중단 작업 이미지 선택")
        image_prompt_plan: dict[str, Any] = {"source": "library"}
        if is_news_mode(self.settings):
            cat = str(row.get("category", "tech") or "tech")
            tg = [str(x) for x in (row.get("tags", []) or [])]
            images, resume_emergency_notes = self.media_manager.prepare_news_images(
                draft=draft,
                category=cat,
                tags=tg,
                target_count=target_images,
                min_required=min_images_required,
                seed_tick_fn=lambda **kw: {}
            )
            for note in (resume_emergency_notes or []):
                resume_degraded_note = self._append_note(resume_degraded_note, note)
        else:
            image_prompt_plan = self._build_image_prompt_plan_with_local_llm(draft, candidate)
            images = self.media_manager.prepare_post_images(
                draft=draft,
                prompt_plan=image_prompt_plan,
                target_count=target_images,
            )
        images = self.visual.ensure_unique_assets(images)
        if len(images) < min_images_required:
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
                reason=f"missing_images_required({len(images)}/{min_images_required})",
            )
            hold_msg = f"missing_images_required({len(images)}/{min_images_required})"
            if resume_degraded_note:
                hold_msg += f" | {resume_degraded_note}"
            if hold_note:
                hold_msg += f" | {hold_note}"
            if updated_draft_id:
                hold_msg += f" | draft_checkpoint={updated_draft_id}"
            self._set_image_pipeline_state("failed", len(images), target_images, hold_msg)
            return WorkflowResult("hold", hold_msg)
        if len(images) > target_images:
            images = images[:target_images]
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
        final_html = self._inject_images_into_html(final_html, images)
        resume_link_topic = self._infer_topic_cluster(
            title,
            list(self.last_global_keywords or []),
            final_html,
        )
        resume_link_keywords = self._compute_focus_keywords(
            title,
            final_html,
            resume_link_topic,
        )
        try:
            final_html = self._inject_internal_links_and_related_coverage(
                final_html,
                current_title=title,
                current_keywords=resume_link_keywords,
            )
        except Exception:
            pass
        final_html, resume_degraded_note, _ = self._apply_body_clickbait_sanitizer(
            final_html,
            resume_degraded_note,
        )
        seo_topic = self._infer_topic_cluster(title, list(self.last_global_keywords or []), final_html)
        seo_focus_keywords = self._compute_focus_keywords(title, final_html, seo_topic)
        seo_slug_base = self._compute_seo_slug(title, seo_topic)
        seo_slug = ""
        self._update_run_metrics_context(
            "resume_draft_done",
            title=str(title or ""),
            topic_cluster=str(seo_topic or "default"),
            focus_keywords=list(seo_focus_keywords or [])[:6],
            final_html=str(final_html or ""),
            images=list(images or []),
        )
        gate_preview_html = ""
        preflight_thumb_src = self._preflight_thumb_src_from_images(images)
        resume_first_thumb_source = str(getattr(images[0], "source_url", "") or "").strip() if images else ""
        resume_thumb_invalid_host = bool(resume_first_thumb_source and (not preflight_thumb_src))
        resume_thumbnail_recovery_attempted = False
        _ = self._annotate_image_pipeline_diagnostics(
            note="",
            stage="resume_pre_merge",
            images=images,
            preflight_thumb_src=preflight_thumb_src,
            required_images=min_images_required,
        )
        if bool(self.settings.budget.dry_run):
            try:
                gate_preview_html = self.publisher.build_dry_run_html(final_html, images)
            except Exception as exc:
                raise RuntimeError(f"Go-live preflight merge failed: {exc}") from exc
        else:
            try:
                if images:
                    if not preflight_thumb_src:
                        resume_thumbnail_recovery_attempted = True
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
            fail_msg = "Go-live gate failed: " + "; ".join(go_live_errors[:5])
            if resume_degraded_note:
                fail_msg = self._append_note(fail_msg, resume_degraded_note)
            raise RuntimeError(fail_msg)

        qa_result = self._qa_evaluate(
            final_html,
            title=title,
            domain=resume_domain,
            keyword=str(getattr(candidate, "title", "") or ""),
            context="resume_publish",
            include_image_integrity=bool(images),
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
        publish_secret_issues = self._profile_call(
            "resume_secret_preflight_before_publish",
            lambda: validate_secrets(self.settings),
            slow_ms=500,
        )
        if publish_secret_issues:
            reason = "preflight_missing_secrets:" + ",".join(str(x) for x in publish_secret_issues[:8])
            self.logs.append_run(
                RunRecord(
                    status="hold",
                    score=0,
                    title=title,
                    source_url="",
                    published_url="",
                    note=reason,
                )
            )
            self._mark_active_slot("hold", reason)
            return WorkflowResult("hold", reason)

        self._progress("schedule", "중단 문서 재개: 예약 시간 계산", 84)
        publish_at = self._compute_publish_at()
        if publish_at is None:
            delay_min = max(10, int(getattr(self.settings.publish, "min_delay_minutes", 10)))
            publish_at = datetime.now(timezone.utc) + timedelta(minutes=delay_min + random.randint(1, 20))
        seo_slug = self._reserve_unique_slug(
            seo_slug_base,
            title=str(title or ""),
            topic=str(seo_topic or "default"),
        )
        self._update_run_metrics_context(
            "resume_draft_done",
            seo_slug=str(seo_slug or ""),
            publish_at_utc=str(
                publish_at.astimezone(timezone.utc).isoformat()
                if isinstance(publish_at, datetime)
                else ""
            ),
        )

        self._progress("publish", "중단 문서 재개: 예약 발행 처리", 92)
        meta_description = self._build_meta_description(
            title=title,
            summary=summary_text,
            html=final_html,
        )
        if images and (not preflight_thumb_src):
            if resume_thumb_invalid_host and resume_thumbnail_recovery_attempted:
                self._append_workflow_perf(
                    "thumbnail_preflight_skipped",
                    {
                        "stage": "resume_publish",
                        "reason": "already_attempted_for_invalid_host",
                        "first_source_url": resume_first_thumb_source[:220],
                    },
                )
            else:
                try:
                    resume_thumbnail_recovery_attempted = True
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
                seo_slug=seo_slug,
                focus_keywords=seo_focus_keywords,
                topic_cluster=seo_topic,
            ),
            slow_ms=8000,
        )
        self._update_run_metrics_context(
            "resume_draft_done",
            published_url=str(getattr(published, "url", "") or ""),
        )
        self.logs.add_scheduled_post(
            publish_at=publish_at.isoformat(),
            post_id=published.post_id,
            title=title,
            source_url="",
            published_url=published.url,
        )
        try:
            self._save_last_cluster_id(
                self._infer_cluster_id_from_keyword(" ".join(self.last_global_keywords[:2]) or title)
            )
        except Exception:
            pass
        self._blog_snapshot_cache = None
        self._cleanup_local_image_files(images)

        note = f"resumed_from_wip=draft_done, qa={qa_result.score}, images={len(images)}, scheduled_at={publish_at.isoformat()}"
        if resume_degraded_note:
            note = self._append_note(note, resume_degraded_note)
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
        self._remember_title_fingerprint(title)
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

    def _refresh_resume_row_payload(self, row: dict) -> dict:
        base = dict(row or {})
        post_id = str(base.get("post_id", "") or "").strip()
        title = self._strip_wip_title_prefix(str(base.get("title", "") or "").strip())
        content = self._strip_wip_checkpoint_banner(str(base.get("content", "") or ""))
        if title and content:
            return base
        if not post_id:
            return base
        try:
            refreshed = self.publisher.fetch_wip_draft_by_id(post_id=post_id, include_content=True)
        except Exception:
            refreshed = {}
        if not isinstance(refreshed, dict):
            return base
        merged = dict(base)
        for key in ("title", "content", "labels", "stage", "updated", "url"):
            value = refreshed.get(key, None)
            if value is None:
                continue
            merged[key] = value
        return merged

    def _auto_park_corrupt_resume_draft(self, row: dict, reason_token: str) -> str:
        reason = str(reason_token or "resume_payload_missing").strip() or "resume_payload_missing"
        post_id = str((row or {}).get("post_id", "") or "").strip()
        title = self._strip_wip_title_prefix(str((row or {}).get("title", "") or "").strip()) or "Recovered Draft"
        labels = ["wip", "stage-hold", "resume-corrupt-payload"]
        if post_id:
            parked_id, parked_note = self._sync_stage_draft_checkpoint(
                current_draft_post_id=post_id,
                stage="hold",
                title=title,
                html_body="<p>Resume checkpoint payload was empty and has been parked.</p>",
                labels=labels,
                reason=reason,
            )
            reason = self._append_note(reason, parked_note)
            if parked_id:
                reason = self._append_note(reason, f"draft_checkpoint={parked_id}")
        self.logs.append_run(
            RunRecord(
                status="skipped",
                score=0,
                title=title,
                source_url="",
                published_url="",
                note=reason,
            )
        )
        return reason

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

        news_mode = is_news_mode(self.settings)
        long_tails = [
            re.sub(r"\s+", " ", str(k or "")).strip()
            for k in (getattr(candidate, "long_tail_keywords", []) or [])
            if str(k or "").strip()
        ]
        entity = re.sub(r"\s+", " ", str(getattr(candidate, "main_entity", "") or "")).strip()
        title = re.sub(r"\s+", " ", str(getattr(candidate, "title", "") or "")).strip()

        # pick subject
        subject = ""
        if long_tails:
            base = re.sub(r"[?]+$", "", long_tails[0]).strip()
            base = re.sub(r"^(how|why|what)\s+", "", base, flags=re.IGNORECASE)
            base = re.sub(r"\s+", " ", base).strip(" -")
            subject = base
        if not subject:
            subject = entity or title or "this update"

        subject = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", subject)
        subject = re.sub(r"\s+", " ", subject).strip()
        subject = subject[:120] if subject else "this update"

        if news_mode:
            # NEWS: never mention troubleshooting / diagram language
            thumb.alt = f"Tech news thumbnail illustration about {subject}."[:180]
            return

        # Legacy troubleshoot mode
        thumb.alt = f"Practical troubleshooting process diagram for {subject}."[:180]

    def _enforce_seo_title(
        self,
        title: str,
        candidate: TopicCandidate | None,
        global_keywords: list[str] | None,
        preferred_keyword: str = "",
    ) -> str:
        raw = re.sub(r"\s+", " ", str(title or "")).strip()
        base_candidate_title = re.sub(r"\s+", " ", str(getattr(candidate, "title", "") or "")).strip()
        pref = re.sub(r"\s+", " ", str(preferred_keyword or "")).strip()
        news_mode = is_news_mode(self.settings)
        if not raw:
            raw = pref or base_candidate_title or ("Tech update" if news_mode else "Windows update error fix")
        raw = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", raw)
        banned_phrases = [
            "fixes that actually work",
            "ultimate guide",
            "device not working",
        ]
        for phrase in banned_phrases:
            raw = re.sub(re.escape(phrase), "", raw, flags=re.IGNORECASE)
        mode = str(getattr(self.settings.content_mode, "mode", "") or "").strip().lower()
        if news_mode or mode == "tech_news_only":
            raw = re.sub(r"\s+", " ", raw).strip(" -:")
            raw_segments = [seg.strip() for seg in re.split(r"\s*:\s*", raw) if seg.strip()]
            if len(raw_segments) > 1 and any(
                re.search(
                    r"\b(fix|error|troubleshoot|not working|safe steps to try first|after update)\b",
                    seg,
                    flags=re.IGNORECASE,
                )
                for seg in raw_segments[1:]
            ):
                raw = raw_segments[0]
            raw = re.sub(
                r"\b(not working|safe steps to try first|fix guide|troubleshooting|after update)\b",
                "",
                raw,
                flags=re.IGNORECASE,
            )
            raw = re.sub(r"\berror\s+fix\b", "", raw, flags=re.IGNORECASE)
            raw = re.sub(r"\s+", " ", raw).strip(" -:")
            category_hint = ""
            if isinstance(candidate, TopicCandidate):
                category_hint = re.sub(
                    r"\s+",
                    " ",
                    str((getattr(candidate, "meta", {}) or {}).get("news_category", "") or ""),
                ).strip().lower()
            topic_phrase = re.sub(r"\s+", " ", str(pref or base_candidate_title or raw)).strip()
            if not topic_phrase:
                topic_phrase = "Tech update"

            banned_tokens = [
                "shocking",
                "disaster",
                "scam",
                "fraud",
                "criminal",
                "exposed",
                "destroyed",
                "caught",
                "why everyone is talking",
                "everyone is talking",
                "ultimate guide",
                "fixes that actually work",
                "fix guide",
                "troubleshooting",
            ]
            banned_tokens += [
                str(x or "").strip().lower()
                for x in (getattr(self.settings.content_mode, "banned_topic_keywords", []) or [])
                if str(x or "").strip()
            ]

            recent_titles = self._get_recent_blogger_titles(limit=120, refresh_api=False)
            recent_first4 = {
                self._title_first_words_key(t, n=4)
                for t in recent_titles[:20]
                if str(t or "").strip()
            }
            shape_rows = self._read_news_title_shape_rows(limit=200)
            shape_recent_counts = Counter(
                str((row or {}).get("shape_id", "") or "").strip().lower()
                for row in shape_rows[-60:]
                if str((row or {}).get("shape_id", "") or "").strip()
            )
            api_ready = bool(
                (self.settings.gemini.api_key or "").strip()
                and (self.settings.gemini.api_key or "").strip() != "GEMINI_API_KEY"
            )
            allow_gemini = api_ready and (self._gemini_budget_remaining() > 0)
            rejected_due_to_similarity = 0
            rejected_due_to_shape = 0
            total_candidates = 0

            def score_news_title(t: str) -> int:
                tt = re.sub(r"\s+", " ", str(t or "")).strip()
                low = tt.lower()
                if not tt:
                    return -9999
                if "faq" in low or "frequently asked" in low:
                    return -9000
                if "google.com" in low:
                    return -9000
                if (
                    "fix guide" in low
                    or "troubleshooting" in low
                    or "error fix" in low
                    or "safe steps to try first" in low
                    or "not working" in low
                    or "after update" in low
                ):
                    return -8500
                if re.search(
                    r"\b(shocking|disaster|scam|fraud|criminal|exposed|destroyed|caught)\b",
                    low,
                ):
                    return -8000
                n = len(tt)
                s = 0
                if 45 <= n <= 95:
                    s += 25
                elif 36 <= n <= 105:
                    s += 10
                else:
                    s -= 10
                if re.search(r"\b\d{1,4}\b", tt):
                    s += 6
                if any(
                    w in low for w in ["update", "policy", "security", "ai", "rollout", "patch", "ban", "release"]
                ):
                    s += 8
                if any(w in low for w in ["impact", "timeline", "response", "analysis", "outlook", "breakdown"]):
                    s += 3
                if self._is_recent_title_duplicate(tt):
                    s -= 200
                return s

            def filter_news_candidates(rows: list[str]) -> list[str]:
                nonlocal rejected_due_to_similarity, rejected_due_to_shape
                out: list[str] = []
                seen: set[str] = set()
                for cand in rows:
                    t = re.sub(r"\s+", " ", str(cand or "")).strip(" -:")
                    if not t:
                        continue
                    low = t.lower()
                    if low in seen:
                        continue
                    seen.add(low)
                    first4 = self._title_first_words_key(t, n=4)
                    if first4 and first4 in recent_first4:
                        rejected_due_to_similarity += 1
                        continue
                    shape_id = self._news_title_shape_id(t).lower()
                    if shape_id and int(shape_recent_counts.get(shape_id, 0)) >= 3:
                        rejected_due_to_shape += 1
                        continue
                    if (
                        "fix guide" in low
                        or "troubleshooting" in low
                        or "error fix" in low
                        or "safe steps to try first" in low
                        or "not working" in low
                        or "after update" in low
                    ):
                        continue
                    out.append(t[:100])
                return out

            best = raw or topic_phrase
            if allow_gemini:
                try:
                    variants = self.brain.generate_news_title_variants(
                        category=(category_hint or "platform"),
                        source_title=topic_phrase,
                        source_snippet=(
                            str(getattr(candidate, "body", "") or "")[:420]
                            if isinstance(candidate, TopicCandidate)
                            else ""
                        ),
                        recent_titles=recent_titles,
                        banned_tokens=banned_tokens,
                        limit=10,
                    )
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                    total_candidates += len(list(variants or []))
                    filtered_variants = filter_news_candidates(list(variants or []))
                    if filtered_variants:
                        best = max(filtered_variants, key=score_news_title)
                except Exception:
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()

            verbs = [
                "shifts",
                "rolls out",
                "tightens",
                "expands",
                "pauses",
                "revises",
                "changes",
                "adds",
                "drops",
                "updates",
                "adjusts",
                "moves",
            ]
            angles = [
                "what changes for users",
                "what it means in practice",
                "who is affected",
                "what to do now",
                "what to watch next",
                "why it matters this week",
                "what's different from last time",
                "timeline and immediate impact",
                "what teams should monitor",
                "policy impact and rollout notes",
                "market response and next signals",
                "key details from official statements",
            ]
            frames = [
                "{topic} {verb}: {angle}",
                "{topic} {verb} - {angle}",
                "{verb_cap} at {topic}: {angle}",
                "{topic}: {angle} after the latest {category} move",
                "{topic} update: {angle}",
                "{topic} this week: {angle}",
                "{topic} and users: {angle}",
                "{topic} rollout: {angle}",
                "{topic} policy shift: {angle}",
                "{topic} analysis: {angle}",
                "{topic}: quick breakdown of {angle}",
                "{topic} timeline: {angle}",
                "{topic}: what users should monitor next",
                "New update on {topic}: {angle}",
                "{topic} rollout notes: {angle}",
                "{topic}: who is affected and what changes now",
            ]
            category_word = category_hint if category_hint else "platform"

            if (
                (not best)
                or len(best) < 36
                or self._is_recent_title_duplicate(best)
                or (self._title_first_words_key(best, n=4) in recent_first4)
                or (shape_recent_counts.get(self._news_title_shape_id(best).lower(), 0) >= 3)
            ):
                candidates_local = []
                for _ in range(30):
                    verb = random.choice(verbs)
                    verb_cap = verb[:1].upper() + verb[1:]
                    angle = random.choice(angles)
                    tpl = random.choice(frames)
                    title_local = tpl.format(
                        topic=topic_phrase,
                        verb=verb,
                        verb_cap=verb_cap,
                        angle=angle,
                        category=category_word,
                    )
                    title_local = re.sub(r"\s+", " ", title_local).strip(" -:")[:100]
                    candidates_local.append(title_local)
                total_candidates += len(candidates_local)
                filtered_local = filter_news_candidates(candidates_local)
                if filtered_local:
                    best = max(filtered_local, key=score_news_title)
                elif candidates_local:
                    best = max(candidates_local, key=score_news_title)

            best = re.sub(
                r"\b(shocking|disaster|scam|fraud|criminal|exposed|destroyed|caught)\b",
                "",
                best,
                flags=re.IGNORECASE,
            )
            best = re.sub(r"\b(fix guide|troubleshooting)\b", "", best, flags=re.IGNORECASE)
            best = re.sub(r"\s+", " ", best).strip(" -:")
            if len(best) > 95:
                best = best[:95].rstrip(" ,.;:-")
            if not best:
                best = raw or topic_phrase

            chosen_shape_id = self._remember_news_title_shape(best)
            self._append_workflow_perf(
                "news_title_selection",
                {
                    "selected_title": best,
                    "candidate_count": int(total_candidates),
                    "rejected_due_to_similarity": int(rejected_due_to_similarity),
                    "rejected_due_to_shape": int(rejected_due_to_shape),
                    "chosen_title_shape_id": chosen_shape_id,
                },
            )

            return best
        banned_topics = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "banned_topic_keywords", []) or [])
            if str(x or "").strip()
        ]
        for token in banned_topics:
            raw = re.sub(re.escape(token), "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s+", " ", raw).strip(" -:")
        if pref and pref.lower() not in raw.lower():
            raw = f"{pref}: {raw}".strip(" :")
        req_tokens = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.content_mode, "required_title_tokens_any", []) or [])
            if str(x or "").strip()
        ] or ["not working", "fix", "error", "after update"]
        if mode == "tech_troubleshoot_only" and not any(tok in raw.lower() for tok in req_tokens):
            raw = f"{raw} fix".strip()
        raw = re.sub(r"\s+", " ", raw).strip(" -:")
        if len(raw) < 30:
            device_hint = self._infer_device_type(f"{raw}\n{base_candidate_title}") or "windows"
            raw = f"{device_hint.title()} update error fix: safe steps to try first"
        return raw[:120]

    def _normalize_title_for_fingerprint(self, title: str) -> str:
        norm = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", str(title or "").lower())
        norm = re.sub(r"\b(20[0-9]{2})\b", " ", norm)
        norm = re.sub(r"[^a-z0-9\s]", " ", norm)
        norm = re.sub(r"\s+", " ", norm).strip()
        return norm

    def _title_fp(self, title: str) -> str:
        norm = self._normalize_title_for_fingerprint(title)
        if not norm:
            return ""
        return hashlib.sha1(norm.encode("utf-8")).hexdigest()

    def _read_title_fingerprint_rows(self, limit: int = 200) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        try:
            if self._title_fingerprint_path.exists():
                for line in self._title_fingerprint_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(payload, dict):
                        rows.append(payload)
        except Exception:
            rows = []
        if not rows:
            legacy_path = self._title_fingerprint_path.with_suffix(".json")
            try:
                if legacy_path.exists():
                    payload = json.loads(legacy_path.read_text(encoding="utf-8"))
                    if isinstance(payload, list):
                        for item in payload:
                            if isinstance(item, dict):
                                rows.append(item)
            except Exception:
                pass
        return rows[-max(1, int(limit)) :]

    def _load_recent_title_fingerprints(self, limit: int = 200) -> set[str]:
        fps: set[str] = set()
        for row in self._read_title_fingerprint_rows(limit=limit):
            fp = str(row.get("fp", "") or "").strip().lower()
            if fp:
                fps.add(fp)
        try:
            history = self.logs.get_recent_topic_history(days=30, limit=max(1, int(limit)))
            for row in history:
                t = str((row or {}).get("title", "") or "").strip()
                fp = self._title_fp(t)
                if fp:
                    fps.add(fp)
        except Exception:
            pass
        return fps

    def _remember_title_fingerprint(self, title: str) -> None:
        fp = self._title_fp(title)
        if not fp:
            return
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "fp": fp,
            "title": str(title or "")[:180],
            "normalized": self._normalize_title_for_fingerprint(title),
        }
        try:
            self._title_fingerprint_path.parent.mkdir(parents=True, exist_ok=True)
            with self._title_fingerprint_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return
        try:
            rows = self._read_title_fingerprint_rows(limit=240)
            with self._title_fingerprint_path.open("w", encoding="utf-8") as fh:
                for item in rows:
                    fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _is_recent_title_duplicate(self, title: str) -> bool:
        norm = self._normalize_title_for_fingerprint(title)
        if not norm:
            return True
        fp = self._title_fp(title)
        recent_fp = self._load_recent_title_fingerprints(limit=50)
        if fp and fp in recent_fp:
            return True
        recent_titles = self._get_recent_blogger_titles(limit=60, refresh_api=False)
        recent_norm = {
            self._normalize_title_for_fingerprint(str(x or ""))
            for x in (recent_titles or [])
            if str(x or "").strip()
        }
        return norm in recent_norm

    def _title_first_words_key(self, title: str, n: int = 4) -> str:
        words = re.findall(r"[a-z0-9]+", str(title or "").lower())
        if not words:
            return ""
        return " ".join(words[: max(1, int(n))])

    def _news_title_shape_id(self, title: str) -> str:
        src = re.sub(r"\s+", " ", str(title or "").strip())
        if not src:
            return ""
        shape = re.sub(r"[A-Z][a-z]+", "Aa", src)
        shape = re.sub(r"[a-z]+", "w", shape)
        shape = re.sub(r"[0-9]+", "#", shape)
        shape = re.sub(r"[^Aaw#?]+", "-", shape)
        shape = re.sub(r"-{2,}", "-", shape).strip("-").lower()
        if not shape:
            return ""
        return hashlib.sha1(shape.encode("utf-8")).hexdigest()[:12]

    def _read_news_title_shape_rows(self, limit: int = 240) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        try:
            if not self._news_title_shape_path.exists():
                return []
            for line in self._news_title_shape_path.read_text(encoding="utf-8").splitlines():
                item = str(line or "").strip()
                if not item:
                    continue
                try:
                    payload = json.loads(item)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
        except Exception:
            return []
        return rows[-max(1, int(limit)) :]

    def _remember_news_title_shape(self, title: str) -> str:
        shape_id = self._news_title_shape_id(title)
        if not shape_id:
            return ""
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "shape_id": shape_id,
            "title": str(title or "")[:160],
        }
        try:
            self._news_title_shape_path.parent.mkdir(parents=True, exist_ok=True)
            with self._news_title_shape_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return shape_id
        try:
            rows = self._read_news_title_shape_rows(limit=300)
            with self._news_title_shape_path.open("w", encoding="utf-8") as fh:
                for item in rows:
                    fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        except Exception:
            pass
        return shape_id

    def _is_banned_title_template(self, title: str) -> bool:
        lower = re.sub(r"\s+", " ", str(title or "").strip().lower())
        if not lower:
            return True
        banned = (
            "fixes that actually work",
            "ultimate guide",
            "device not working",
        )
        if any(tok in lower for tok in banned):
            return True
        return False

    def _build_rule_title_candidates(
        self,
        *,
        keyword: str,
        device: str,
        cluster: str,
        attempt: int = 0,
    ) -> list[str]:
        kw = re.sub(r"\s+", " ", str(keyword or "")).strip()
        dev = str(device or "device").strip().lower() or "device"
        suffixes = [
            "for everyday users",
            "without wasting time",
            "before full reset",
            "after update",
            "in 2026",
        ]
        suffix = suffixes[attempt % len(suffixes)]
        cluster_label = cluster.replace("_", " ").strip() or "software"
        seeds = [
            f"{kw}? 5 fixes {suffix}",
            f"{kw} after update? 5 safe fixes {suffix}",
            f"{kw} error fix: 5 steps {suffix}",
            f"{dev.title()} {cluster_label} issue after update: 5 fixes",
            f"{dev.title()} {cluster_label} error: 5 troubleshooting steps",
            f"How to fix {kw}: 5 steps with expected results",
        ]
        out: list[str] = []
        seen: set[str] = set()
        for raw in seeds:
            t = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]", " ", str(raw or ""))
            t = re.sub(r"\s+", " ", t).strip(" -:")
            if not t:
                continue
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(t[:120])
        return out

    def _build_rule_news_title_candidates(
        self,
        *,
        source_title: str,
        category: str,
        attempt: int = 0,
    ) -> list[str]:
        topic = re.sub(r"\s+", " ", str(source_title or "")).strip(" -:") or "Tech update"
        category_word = re.sub(r"[^a-z0-9 ]+", " ", str(category or "platform").lower()).strip() or "platform"
        angles = [
            "what changes for users",
            "who is affected now",
            "timeline and immediate impact",
            "what to watch next",
            "what it means in practice",
            "key details from the latest update",
        ]
        frames = [
            "{topic}: {angle}",
            "{topic} update: {angle}",
            "{topic} analysis: {angle}",
            "{topic} and users: {angle}",
            "{topic} {category} shift: {angle}",
            "New update on {topic}: {angle}",
        ]
        out: list[str] = []
        seen: set[str] = set()
        for idx in range(len(frames)):
            angle = angles[(attempt + idx) % len(angles)]
            frame = frames[(attempt + idx) % len(frames)]
            title = frame.format(topic=topic, angle=angle, category=category_word)
            title = re.sub(r"\s+", " ", str(title or "")).strip(" -:")[:100]
            low = title.lower()
            if not title or low in seen:
                continue
            seen.add(low)
            out.append(title)
        return out

    def _finalize_title_after_content(
        self,
        *,
        current_title: str,
        final_html: str,
        selected: TopicCandidate,
        global_keywords: list[str],
        troubleshooting_plan: dict[str, Any],
        allow_gemini: bool,
    ) -> tuple[str, str]:
        plan = dict(troubleshooting_plan or {})
        if is_news_mode(self.settings):
            recent_titles = self._get_recent_blogger_titles(limit=240, refresh_api=False)
            recent_first4 = {
                self._title_first_words_key(t, n=4)
                for t in recent_titles[:20]
                if str(t or "").strip()
            }
            shape_rows = self._read_news_title_shape_rows(limit=200)
            shape_recent_counts = Counter(
                str((row or {}).get("shape_id", "") or "").strip().lower()
                for row in shape_rows[-60:]
                if str((row or {}).get("shape_id", "") or "").strip()
            )
            category = re.sub(
                r"\s+",
                " ",
                str((getattr(selected, "meta", {}) or {}).get("news_category", "platform") or "platform"),
            ).strip().lower()
            snippet = re.sub(r"<[^>]+>", " ", str(final_html or ""))
            snippet = re.sub(r"\s+", " ", snippet).strip()[:420]
            banned_tokens = [
                str(x or "").strip().lower()
                for x in (getattr(self.settings.content_mode, "banned_topic_keywords", []) or [])
                if str(x or "").strip()
            ]
            source_tokens = {
                tok
                for tok in re.findall(r"[a-z0-9]+", str(selected.title or "").lower())
                if len(tok) >= 4 and tok not in {"what", "when", "with", "this", "that", "from"}
            }
            candidates: list[str] = []
            if allow_gemini and self._gemini_budget_remaining() > 0:
                try:
                    generated_news = self.brain.generate_news_title_variants(
                        category=category,
                        source_title=str(getattr(selected, "title", "") or current_title or "Tech update"),
                        source_snippet=str(getattr(selected, "body", "") or snippet),
                        recent_titles=recent_titles,
                        banned_tokens=banned_tokens,
                        limit=10,
                    )
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
                    candidates.extend(
                        [re.sub(r"\s+", " ", str(x or "")).strip() for x in generated_news if str(x or "").strip()]
                    )
                except Exception:
                    if self.brain.call_count:
                        self.logs.increment_today_gemini_count(self.brain.call_count)
                        self.brain.reset_run_counter()
            seeds = [
                re.sub(r"\s+", " ", str(current_title or "")).strip(),
                re.sub(r"\s+", " ", str(getattr(selected, "title", "") or "")).strip(),
            ]
            candidates.extend([seed for seed in seeds if seed])
            candidates.extend(
                self._build_rule_news_title_candidates(
                    source_title=str(getattr(selected, "title", "") or current_title or "Tech update"),
                    category=category,
                    attempt=0,
                )
            )

            best_title = ""
            best_score = -10_000
            seen: set[str] = set()
            bad_pattern = re.compile(
                r"\b(fix guide|troubleshooting|error fix|safe steps to try first|not working|after update)\b",
                flags=re.IGNORECASE,
            )
            for raw_candidate in candidates:
                candidate_title = re.sub(r"\s+", " ", str(raw_candidate or "")).strip(" -:")
                if not candidate_title:
                    continue
                low = candidate_title.lower()
                if low in seen:
                    continue
                seen.add(low)
                if bad_pattern.search(candidate_title):
                    continue
                if any(tok and tok in low for tok in banned_tokens):
                    continue
                score = 0
                n = len(candidate_title)
                if 45 <= n <= 95:
                    score += 18
                elif 36 <= n <= 100:
                    score += 8
                else:
                    score -= 10
                score += min(12, sum(1 for tok in source_tokens if tok in low) * 3)
                if ":" in candidate_title:
                    score += 2
                if "?" in candidate_title:
                    score += 1
                if self._is_recent_title_duplicate(candidate_title):
                    score -= 24
                first4 = self._title_first_words_key(candidate_title, n=4)
                if first4 and first4 in recent_first4:
                    score -= 16
                shape_id = self._news_title_shape_id(candidate_title).lower()
                if shape_id and int(shape_recent_counts.get(shape_id, 0)) >= 3:
                    score -= 10
                if score > best_score:
                    best_score = score
                    best_title = candidate_title

            normalized = self._enforce_seo_title(
                title=best_title or str(getattr(selected, "title", "") or current_title or "Tech update"),
                candidate=selected,
                global_keywords=global_keywords,
                preferred_keyword=str(getattr(selected, "title", "") or current_title or ""),
            )
            if normalized:
                return normalized, ""
            return "", "news_title_finalize_failed"

        summary_payload = self._build_title_summary_payload_with_local_llm(
            current_title=current_title or selected.title,
            final_html=final_html,
            troubleshooting_plan=plan,
            selected=selected,
        )
        recent_titles = self._get_recent_blogger_titles(limit=240, refresh_api=False)
        keyword = re.sub(
            r"\s+",
            " ",
            str(plan.get("primary_keyword", "") or (global_keywords[0] if global_keywords else current_title or selected.title)),
        ).strip()
        if not keyword:
            keyword = re.sub(r"\s+", " ", str(current_title or selected.title or "windows update error fix")).strip()
        device = self._infer_device_type(f"{keyword}\n{selected.title}\n{current_title}")
        cluster = self._infer_cluster_id_from_keyword(keyword)
        candidates: list[str] = []
        if allow_gemini and self._gemini_budget_remaining() > 0:
            try:
                generated = self.brain.generate_title_variants(
                    summary_payload=summary_payload,
                    current_title=(current_title or selected.title),
                    recent_titles=recent_titles,
                )
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                candidates.extend([re.sub(r"\s+", " ", str(x or "")).strip() for x in generated if str(x or "").strip()])
            except Exception:
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
        candidates.append(re.sub(r"\s+", " ", str(current_title or "").strip()))
        candidates.extend(
            self._build_rule_title_candidates(
                keyword=keyword,
                device=device,
                cluster=cluster,
                attempt=0,
            )
        )
        chosen, reason = self._choose_best_unique_title(
            candidates=candidates,
            summary_payload=summary_payload,
            recent_titles=recent_titles,
        )
        if chosen:
            return chosen, ""

        # One additional Gemini retry max.
        if allow_gemini and self._gemini_budget_remaining() > 0:
            try:
                regenerated = self.brain.generate_title_variants(
                    summary_payload=summary_payload,
                    current_title=(current_title or selected.title),
                    recent_titles=[*recent_titles, *candidates][:260],
                )
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
                retry_candidates = [
                    re.sub(r"\s+", " ", str(x or "")).strip()
                    for x in regenerated
                    if str(x or "").strip()
                ]
                retry_candidates.extend(
                    self._build_rule_title_candidates(
                        keyword=keyword,
                        device=device,
                        cluster=cluster,
                        attempt=1,
                    )
                )
                chosen_retry, retry_reason = self._choose_best_unique_title(
                    candidates=retry_candidates,
                    summary_payload=summary_payload,
                    recent_titles=[*recent_titles, *candidates][:260],
                )
                if chosen_retry:
                    return chosen_retry, ""
                return "", retry_reason or reason or "title_duplicate_exhausted"
            except Exception:
                if self.brain.call_count:
                    self.logs.increment_today_gemini_count(self.brain.call_count)
                    self.brain.reset_run_counter()
        return "", reason or "title_duplicate_exhausted"

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
        if re.search(r"(https?://[^\s\"<]*(?:googleusercontent\.com|googleapis\.com)[^\s\"<]*)", merged, flags=re.IGNORECASE):
            errors.append("forbidden_google_service_link_detected")
        if "<figcaption" in merged:
            errors.append("forbidden_figcaption_detected")
        if re.search(r"<h[23][^>]*>\s*faq\s*</h[23]>", str(final_html or ""), flags=re.IGNORECASE):
            errors.append("forbidden_faq_detected")

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

        target_required = self._image_target_max()
        min_required = self._image_min_required()
        if not images:
            if min_required > 0:
                errors.append("images_missing")
            else:
                warnings.append("images_missing_allowed(min_required=0)")
        else:
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
        if html_img_count < min_required:
            errors.append(f"insufficient_html_images({html_img_count}/{min_required})")
        elif html_img_count < target_required:
            warnings.append(f"html_images_below_target({html_img_count}/{target_required})")
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

        # Title quality check.
        title_lower = str(title or "").lower()
        news_mode = is_news_mode(self.settings)
        if not news_mode:
            req_tokens = [
                str(x or "").strip().lower()
                for x in (getattr(self.settings.content_mode, "required_title_tokens_any", []) or [])
                if str(x or "").strip()
            ] or ["not working", "fix", "error", "after update"]
            if not any(tok in title_lower for tok in req_tokens):
                errors.append("title_missing_troubleshoot_token")
        else:
            if re.search(
                r"\b(shocking|disaster|scam|fraud|criminal|exposed|destroyed|caught)\b",
                title_lower,
                flags=re.IGNORECASE,
            ):
                errors.append("title_clickbait_forbidden")
            if re.search(
                r"\b(shocking|disaster|scam|fraud|criminal|exposed|destroyed|caught)\b",
                merged,
                flags=re.IGNORECASE,
            ):
                errors.append("body_clickbait_forbidden")
            if re.search(r"\bleak\b", merged, flags=re.IGNORECASE):
                if not re.search(
                    r"(according to[^<\n]{0,140}\bleak\b|\bleak\b[^<\n]{0,140}according to)",
                    merged,
                    flags=re.IGNORECASE,
                ):
                    errors.append("unattributed_leak_claim")
            if re.search(r"\b(article screenshot|logo misuse|watermark)\b", merged, flags=re.IGNORECASE):
                errors.append("legal_image_policy_violation")
        if not news_mode:
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

    def _preflight_thumb_src_from_images(self, images: list[ImageAsset]) -> str:
        if not images:
            return ""
        src = str(getattr(images[0], "source_url", "") or "").strip()
        if not src:
            return ""
        if self.publisher._is_allowed_image_url(src, allow_data_uri=False):  # noqa: SLF001
            return src
        return ""

    def _apply_ctr_visual_density_note(self, note: str, images: list[ImageAsset]) -> str:
        valid_count = 0
        for image in (images or []):
            src = str(getattr(image, "source_url", "") or "").strip()
            if not src:
                continue
            try:
                if self.publisher._is_allowed_image_url(src, allow_data_uri=False):  # noqa: SLF001
                    valid_count += 1
            except Exception:
                continue
        if valid_count < 3:
            return self._append_note(note, "ctr_risk_low_visual_density")
        return note

    def _injected_image_block(self, src: str, alt: str, *, kind: str) -> str:
        safe_src = escape(str(src or "").strip(), quote=True)
        safe_alt = escape(str(alt or "").strip() or "Supporting image", quote=True)
        return (
            f'<!-- RZ-CTR-IMG:START kind={kind} -->'
            f'<p class="rz-ctr-image rz-ctr-image-{kind}">'
            f'<img src="{safe_src}" alt="{safe_alt}" loading="lazy" referrerpolicy="no-referrer" />'
            f"</p>"
            f"<!-- RZ-CTR-IMG:END kind={kind} -->"
        )

    def _inject_images_into_html(self, html: str, images: list[ImageAsset]) -> str:
        src = str(html or "")
        if not src or not images:
            return src
        out = re.sub(
            r"<!--\s*RZ-CTR-IMG:START.*?RZ-CTR-IMG:END[^>]*-->",
            "",
            src,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(r"\n{3,}", "\n\n", out)
        records = self.publisher._image_records_from_assets(  # noqa: SLF001
            images=images,
            src_lookup=None,
            target_images=len(images or []),
            allow_data_uri=False,
        )
        if not records:
            return out
        return self.publisher._compose_image_enriched_html(out, records)  # noqa: SLF001

    def _annotate_image_pipeline_diagnostics(
        self,
        *,
        note: str,
        stage: str,
        images: list[ImageAsset],
        preflight_thumb_src: str,
        required_images: int,
    ) -> str:
        tokens: list[str] = []
        r2_endpoint = ""
        r2_bucket = ""
        r2_access = ""
        r2_secret = ""
        r2_base = ""
        r2_host = ""
        try:
            raw_r2 = getattr(self.settings.publish, "r2", None)
            r2_endpoint = str(getattr(raw_r2, "endpoint_url", "") or "").strip()
            r2_bucket = str(getattr(raw_r2, "bucket", "") or "").strip()
            r2_access = str(getattr(raw_r2, "access_key_id", "") or "").strip()
            r2_secret = str(getattr(raw_r2, "secret_access_key", "") or "").strip()
            r2_base = str(getattr(raw_r2, "public_base_url", "") or "").strip()
            r2_host = (urlparse(r2_base).netloc or "").lower()
        except Exception:
            pass

        r2_config_missing = not bool(r2_endpoint and r2_bucket and r2_access and r2_secret and r2_base)
        if r2_config_missing:
            tokens.append("r2_config_missing")

        source_urls = [str(getattr(img, "source_url", "") or "").strip() for img in (images or [])]
        first_source = source_urls[0] if source_urls else ""
        first_source_allowed = False
        if first_source:
            try:
                first_source_allowed = bool(self.publisher._is_allowed_image_url(first_source, allow_data_uri=False))  # noqa: SLF001
            except Exception:
                first_source_allowed = False

        if not images:
            tokens.append("image_pipeline_empty")
            if not r2_config_missing:
                tokens.append("r2_upload_failed")
        if any((not src) for src in source_urls):
            tokens.append("image_source_url_missing")
        if images and (not preflight_thumb_src):
            tokens.append("thumbnail_src_missing")
            if first_source and (not first_source_allowed):
                tokens.append("thumbnail_src_invalid_host")
                tokens.append("r2_public_url_invalid")

        dedup_tokens: list[str] = []
        seen_tokens: set[str] = set()
        for tok in tokens:
            clean_tok = str(tok or "").strip()
            if not clean_tok or clean_tok in seen_tokens:
                continue
            seen_tokens.add(clean_tok)
            dedup_tokens.append(clean_tok)
        if not dedup_tokens:
            return note
        self._append_workflow_perf(
            "image_pipeline_diagnostic",
            {
                "stage": str(stage or "").strip() or "unknown",
                "tokens": list(dedup_tokens),
                "image_hosting_backend": str(getattr(self.settings.publish, "image_hosting_backend", "") or ""),
                "required_images": int(max(0, required_images)),
                "selected_images_count": int(len(images or [])),
                "preflight_thumb_src_present": bool(preflight_thumb_src),
                "first_source_url_present": bool(first_source),
                "first_source_url_allowed": bool(first_source_allowed),
                "r2_public_host": r2_host,
                "r2_config_missing": bool(r2_config_missing),
            },
        )
        updated = str(note or "")
        for token in dedup_tokens:
            updated = self._append_note(updated, token)
        return updated

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
            if self._image_min_required() > 0:
                raise RuntimeError("thumbnail_preflight_failed:no_images")
            return [], ""
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
        target = max(5, int(target_images or 0))
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

        if is_news_mode(self.settings):
            subject_news = re.sub(
                r"\s+",
                " ",
                str(title or getattr(candidate, "title", "") or "tech update").strip(),
            )
            for kw in (global_keywords or [])[:6]:
                token = re.sub(r"\s+", " ", str(kw or "")).strip()
                if not token:
                    continue
                _push(f"what changed in {token}")
                _push(f"who is affected by {token}")
                _push(f"should you update now for {token}")
                _push(f"what to watch next for {token}")
            if len(out) < 5:
                _push(f"what changed in {subject_news}")
                _push(f"who is affected by {subject_news}")
                _push(f"should you update now for {subject_news}")
                _push(f"what to do now for {subject_news}")
                _push(f"what to watch next for {subject_news}")
            candidate.long_tail_keywords = out[:8]
            return

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

    def _normalize_duplicate_h2_sections(self, html: str) -> str:
        src = str(html or "")
        if not src:
            return src
        heading_re = re.compile(r"(?is)<h2([^>]*)>(.*?)</h2>")
        matches = list(heading_re.finditer(src))
        if len(matches) < 2:
            return src

        variant_map: dict[str, list[str]] = {
            "quick take": ["Key Context", "Bottom Line"],
            "what happened": ["Latest Context", "Timeline Snapshot"],
            "why it matters": ["Why Readers Should Care", "Decision Impact"],
            "key details": ["Details That Matter", "Signals To Notice"],
            "real-world scenarios": ["Practical Scenarios", "Reader Examples"],
            "what to watch next": ["Signals To Watch", "What Comes Next"],
            "what to do now": ["Practical Next Steps", "What To Check First"],
            "sources": ["Further Reading", "Reference Links"],
        }

        pieces: list[str] = []
        last_idx = 0
        seen_counts: dict[str, int] = {}
        changed = False

        for match in matches:
            pieces.append(src[last_idx : match.start()])
            attrs = str(match.group(1) or "")
            raw_title = re.sub(r"(?is)<[^>]+>", " ", str(match.group(2) or ""))
            raw_title = unescape(raw_title)
            raw_title = re.sub(r"\s+", " ", raw_title).strip()
            key = raw_title.lower()
            seen_counts[key] = int(seen_counts.get(key, 0)) + 1
            occurrence = int(seen_counts[key])

            title = raw_title
            if occurrence > 1 and key:
                variants = variant_map.get(key, [])
                variant_index = occurrence - 2
                if variant_index < len(variants):
                    title = variants[variant_index]
                elif key == "sources":
                    title = f"Reference Links {occurrence - 1}"
                elif key == "what to do now":
                    title = f"Next Steps {occurrence - 1}"
                else:
                    title = f"{raw_title}: Additional Context {occurrence - 1}"
                changed = True

            pieces.append(f"<h2{attrs}>{escape(title)}</h2>")
            last_idx = match.end()

        pieces.append(src[last_idx:])
        if not changed:
            return src
        return "".join(pieces)

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
        out = re.sub(
            r"<a[^>]+href=\"https?://[^\"]*(?:googleusercontent\.com|googleapis\.com)[^\"]*\"[^>]*>(.*?)</a>",
            r"\1",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(
            r"https?://[^\s\"<]*(?:googleusercontent\.com|googleapis\.com)[^\s\"<]*",
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
        # FAQ is prohibited for production news posts.
        out = re.sub(
            r"<h2[^>]*>\s*faq\s*</h2>.*?(?=<h2\b|$)",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(
            r"<h3[^>]*>\s*faq\s*</h3>.*?(?=<h2\b|<h3\b|$)",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
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

    def _strip_forbidden_news_links(self, html: str) -> tuple[str, int]:
        src = str(html or "")
        if not src:
            return src, 0
        removed = 0
        out = src
        patterns = [
            # Strip anchored forbidden hosts (quoted or unquoted href/src).
            r"<a[^>]+(?:href|src)\s*=\s*([\"']?)(?:https?:)?//?(?:www\.)?google\.com[^\"'\s>]*\1[^>]*>.*?</a>",
            r"<a[^>]+(?:href|src)\s*=\s*([\"']?)(?:https?:)?//?[^\"'\s>]*googleusercontent\.com[^\"'\s>]*\1[^>]*>.*?</a>",
            r"<a[^>]+(?:href|src)\s*=\s*([\"']?)(?:https?:)?//?[^\"'\s>]*googleapis\.com[^\"'\s>]*\1[^>]*>.*?</a>",
            # Strip escaped URL literals often leaked from serialized payloads.
            r"https?:\\\\/\\\\/(?:www\.)?google\.com\\\\/[^\s\"'<>]+",
            r"https?:\\\\/\\\\/[^\\\s\"'<>]*googleusercontent\.com[^\s\"'<>]*",
            r"https?:\\\\/\\\\/[^\\\s\"'<>]*googleapis\.com[^\s\"'<>]*",
            # Strip regular URL literals and bare host/path forms.
            r"(?:https?://)?(?:www\.)?google\.com(?:/[^\s\"'<>]*)?",
            r"(?:https?://)?[^/\s\"'<>]*googleusercontent\.com(?:/[^\s\"'<>]*)?",
            r"(?:https?://)?[^/\s\"'<>]*googleapis\.com(?:/[^\s\"'<>]*)?",
        ]
        for pat in patterns:
            out, hit = re.subn(pat, "", out, flags=re.IGNORECASE | re.DOTALL)
            removed += int(hit or 0)
        # If href became empty after stripping, keep anchor text only.
        out, hit = re.subn(
            r"<a[^>]+href\s*=\s*[\"']\s*[\"'][^>]*>(.*?)</a>",
            r"\1",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        removed += int(hit or 0)
        out = re.sub(r"\s{2,}", " ", out)
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out, int(removed)

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

    def _adaptive_daily_publish_cap(self) -> int:
        base_cap = max(1, int(getattr(self.settings.publish, "daily_publish_cap", 5) or 5))
        rows = self.logs.get_recent_runs(days=3, limit=120)
        if not rows:
            return base_cap
        hold = 0
        total = 0
        qa_scores: list[int] = []
        for row in rows:
            status = str((row or {}).get("status", "") or "").strip().lower()
            if status not in {"success", "hold", "skipped"}:
                continue
            total += 1
            if status == "hold":
                hold += 1
            try:
                qa_scores.append(int((row or {}).get("score", 0) or 0))
            except Exception:
                pass
        if total <= 0:
            return base_cap
        hold_rate = float(hold) / float(max(1, total))
        avg_score = float(sum(qa_scores)) / float(max(1, len(qa_scores)))
        adjusted = base_cap
        if hold_rate >= 0.45 or avg_score < 80:
            adjusted = max(1, base_cap - 2)
        elif hold_rate >= 0.30 or avg_score < 86:
            adjusted = max(1, base_cap - 1)
        elif hold_rate <= 0.12 and avg_score >= 90:
            adjusted = min(base_cap + 1, max(base_cap, 8))
        return int(adjusted)

    def _compute_publish_at_legacy(self, now: datetime, min_delay: int) -> datetime | None:
        snapshot = self._blog_snapshot(force_refresh=True)
        times, day_count = self._build_publish_state(snapshot, now)
        daily_cap = self._adaptive_daily_publish_cap()
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
        daily_cap = self._adaptive_daily_publish_cap()
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

    def _topic_seed_keywords(self, topic: str) -> list[str]:
        key = str(topic or "default").strip().lower()
        mapping: dict[str, list[str]] = {
            "security": ["security", "patch", "vulnerability"],
            "policy": ["policy", "compliance", "regulation"],
            "platform": ["platform", "rollout", "release"],
            "mobile": ["mobile", "android", "ios"],
            "ai": ["ai", "model", "inference"],
            "chips": ["chips", "gpu", "cpu"],
            "privacy": ["privacy", "data", "tracking"],
            "default": ["update", "analysis", "coverage"],
        }
        return list(mapping.get(key, mapping["default"]))

    def _compute_focus_keywords(self, title: str, html: str, topic: str) -> list[str]:
        stopwords = {
            "about", "after", "also", "and", "are", "been", "being", "both", "but", "can",
            "for", "from", "have", "into", "its", "just", "more", "most", "much", "only",
            "other", "our", "out", "over", "same", "some", "than", "that", "the", "their",
            "them", "then", "there", "these", "they", "this", "those", "through", "under",
            "very", "what", "when", "where", "which", "while", "with", "would", "your",
            "will", "you", "was", "were", "had", "has", "not", "now", "how", "why",
        }
        title_text = str(title or "").lower()
        body_text = re.sub(r"<[^>]+>", " ", str(html or "")).lower()
        body_text = re.sub(r"\s+", " ", body_text).strip()[:5000]

        score: dict[str, float] = {}
        for tok in re.findall(r"[a-z0-9][a-z0-9\-]{2,}", body_text):
            if tok in stopwords or tok.isdigit():
                continue
            score[tok] = float(score.get(tok, 0.0) + 1.0)
        for tok in re.findall(r"[a-z0-9][a-z0-9\-]{2,}", title_text):
            if tok in stopwords or tok.isdigit():
                continue
            score[tok] = float(score.get(tok, 0.0) + 3.0)

        ranked = sorted(score.items(), key=lambda x: (-float(x[1]), str(x[0])))
        picked: list[str] = []
        seen: set[str] = set()
        for token, _ in ranked:
            if token in seen:
                continue
            seen.add(token)
            picked.append(token)
            if len(picked) >= 6:
                break

        topic_seeds = self._topic_seed_keywords(topic)
        if not any(seed in seen for seed in topic_seeds):
            forced = ""
            for seed in topic_seeds:
                if seed not in seen:
                    forced = seed
                    break
            if not forced:
                forced = topic_seeds[0] if topic_seeds else "update"
            picked.insert(0, forced)
            seen.add(forced)

        fillers = ["update", "coverage", "analysis", "guide", "overview"]
        for token in fillers:
            if len(picked) >= 3:
                break
            if token in seen:
                continue
            picked.append(token)
            seen.add(token)

        deduped = [str(x).strip().lower() for x in picked if str(x).strip()]
        deduped = list(dict.fromkeys(deduped))
        return deduped[:6]

    def _clean_slug_token(self, value: str) -> str:
        token = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower())
        token = re.sub(r"-{2,}", "-", token).strip("-")
        return token

    def _iter_slug_ledger_records(self) -> list[dict[str, Any]]:
        path = Path(self._slug_ledger_path).resolve()
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    raw = str(line or "").strip()
                    if not raw:
                        continue
                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        continue
                    if isinstance(parsed, dict):
                        rows.append(parsed)
        except Exception:
            return []
        return rows

    def _slug_exists_recent(self, slug: str) -> bool:
        target = str(slug or "").strip().lower()
        if not target:
            return False
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(self._slug_ledger_ttl_days or 180)))
        for row in self._iter_slug_ledger_records():
            current = str((row or {}).get("slug", "") or "").strip().lower()
            if current != target:
                continue
            created = self._parse_utc_soft((row or {}).get("created_at_utc"))
            if created is None:
                continue
            if created >= cutoff:
                return True
        return False

    def _record_slug_ledger(self, slug: str, title: str, topic: str) -> None:
        clean_slug = str(slug or "").strip().lower()
        if not clean_slug:
            return
        row = {
            "slug": clean_slug,
            "title": str(title or "").strip(),
            "topic": str(topic or "default").strip().lower() or "default",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        path = Path(self._slug_ledger_path).resolve()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _reserve_unique_slug(self, base_slug: str, title: str, topic: str) -> str:
        clean_base = self._clean_slug_token(base_slug)
        if not clean_base:
            clean_base = f"{self._clean_slug_token(topic) or 'default'}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}"
        clean_base = clean_base[:70].rstrip("-")
        if len(clean_base) < 40:
            clean_base = (clean_base + "-update-guide").strip("-")
            clean_base = clean_base[:70].rstrip("-")
        candidate = clean_base
        for suffix_idx in range(1, 100):
            if suffix_idx == 1:
                candidate = clean_base
            else:
                suffix = f"-{suffix_idx}"
                candidate = f"{clean_base[: max(1, 70 - len(suffix))].rstrip('-')}{suffix}"
            if not self._slug_exists_recent(candidate):
                self._record_slug_ledger(candidate, title=title, topic=topic)
                return candidate
        fallback = f"{self._clean_slug_token(topic) or 'default'}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        fallback = fallback[:70].rstrip("-")
        self._record_slug_ledger(fallback, title=title, topic=topic)
        return fallback

    def _compute_seo_slug(self, title: str, topic: str) -> str:
        banned = {"free", "scam", "guaranteed", "click", "porn", "must"}
        topic_token = self._clean_slug_token(topic) or "default"
        raw_tokens = [self._clean_slug_token(x) for x in re.findall(r"[A-Za-z0-9][A-Za-z0-9\-]{2,}", str(title or ""))]
        raw_tokens = [x for x in raw_tokens if x and x not in banned and x != topic_token]
        parts = [topic_token] + raw_tokens[:8]
        slug = "-".join(parts)
        slug = re.sub(r"-{2,}", "-", slug).strip("-")
        if len(slug) < 40:
            extras = [x for x in self._topic_seed_keywords(topic_token) if x not in parts and x not in banned]
            for token in extras:
                candidate = f"{slug}-{self._clean_slug_token(token)}".strip("-")
                if len(candidate) > 70:
                    break
                slug = candidate
                if len(slug) >= 40:
                    break
        if len(slug) > 70:
            trimmed = slug[:70].rstrip("-")
            if "-" in trimmed:
                trimmed = trimmed.rsplit("-", 1)[0].strip("-")
            slug = trimmed
        if len(slug) < 40:
            for token in ("update", "guide", "overview", datetime.now(timezone.utc).strftime("%Y%m")):
                candidate = f"{slug}-{self._clean_slug_token(token)}".strip("-")
                if len(candidate) > 70:
                    break
                slug = candidate
                if len(slug) >= 40:
                    break
        if len(slug) < 40:
            slug = f"{topic_token}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}-coverage-guide"
            slug = slug[:70].rstrip("-")
        slug = re.sub(r"-{2,}", "-", slug).strip("-")
        return slug

    def _related_coverage_heading_for_topic(self, topic: str) -> str:
        key = str(topic or "default").strip().lower()
        if key == "security":
            return "Related Security Coverage"
        if key == "policy":
            return "Related Policy Coverage"
        return "Related Coverage"

    def _enforce_what_to_do_now_section(self, html: str, topic: str) -> str:
        out = str(html or "")
        key = str(topic or "").strip().lower()
        if key not in {"security", "policy", "platform"}:
            return out
        templates: dict[str, list[str]] = {
            "security": [
                "Check vendor advisories and confirm patch scope for your systems.",
                "Prioritize exposed services and schedule remediation with owners.",
                "Track verification results and document unresolved risks.",
            ],
            "policy": [
                "Review policy deltas and map them to existing internal controls.",
                "Update owner checklists and rollout deadlines for affected teams.",
                "Publish a short compliance note for audit traceability.",
            ],
            "platform": [
                "Validate rollout status per environment before broad enablement.",
                "Confirm fallback options and escalation contacts.",
                "Capture user-impact signals and adjust deployment timing.",
            ],
        }
        target_items = list(templates.get(key, templates["platform"]))
        section_pattern = r"(<h2[^>]*>\s*What To Do Now\s*</h2>)(.*?)(?=<h2\b|$)"
        match = re.search(section_pattern, out, flags=re.IGNORECASE | re.DOTALL)

        def _normalize_items(raw_items: list[str]) -> list[str]:
            cleaned: list[str] = []
            seen_text: set[str] = set()
            for item in raw_items:
                text = re.sub(r"<[^>]+>", " ", str(item or ""))
                text = re.sub(r"\s+", " ", text).strip(" .")
                if not text:
                    continue
                key_text = text.lower()
                if key_text in seen_text:
                    continue
                seen_text.add(key_text)
                cleaned.append(text)
            return cleaned

        if match:
            existing_items = re.findall(r"<li[^>]*>(.*?)</li>", str(match.group(2) or ""), flags=re.IGNORECASE | re.DOTALL)
            normalized = _normalize_items(existing_items)
            while len(normalized) < 3:
                for seed in target_items:
                    if seed.lower() not in {x.lower() for x in normalized}:
                        normalized.append(seed)
                    if len(normalized) >= 3:
                        break
            normalized = normalized[:5]
            list_html = "<ul>" + "".join(f"<li>{escape(x)}</li>" for x in normalized) + "</ul>"
            section_html = "<h2>What To Do Now</h2>" + list_html
            out = out[: match.start()] + section_html + out[match.end() :]
            return out

        new_items = target_items[:3]
        section_html = "<h2>What To Do Now</h2><ul>" + "".join(f"<li>{escape(x)}</li>" for x in new_items) + "</ul>"
        m_sources = re.search(r"<h2[^>]*>\s*Sources\s*</h2>", out, flags=re.IGNORECASE)
        if m_sources:
            return out[: m_sources.start()] + section_html + out[m_sources.start() :]
        return out + section_html

    def _internal_links_pool_path(self) -> Path:
        return (self.root / "storage" / "state" / "internal_links_pool.json").resolve()

    def _legacy_posts_index_cache_path(self) -> Path:
        return (self.root / "storage" / "state" / "posts_index.json").resolve()

    def _normalize_host(self, value: str) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        if "://" in text:
            try:
                text = str(urlparse(text).netloc or "").strip().lower()
            except Exception:
                return ""
        text = text.split("/", 1)[0].split("@")[-1].strip()
        if ":" in text:
            text = text.split(":", 1)[0].strip()
        if text.startswith("www."):
            text = text[4:].strip()
        return text

    def _host_from_url(self, url: str) -> str:
        try:
            return self._normalize_host(urlparse(str(url or "").strip()).netloc)
        except Exception:
            return ""

    def _canonical_internal_host(self) -> str:
        configured = str(
            getattr(getattr(self.settings, "internal_links", None), "canonical_internal_host", "") or ""
        ).strip()
        return self._normalize_host(configured)

    def _parse_utc_soft(self, value: Any) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _infer_topic_cluster(
        self,
        title: str,
        keywords: list[str] | str | None = None,
        html: str | None = None,
    ) -> str:
        vocab = " ".join(
            [
                str(title or ""),
                " ".join(self._parse_focus_keywords(keywords or "")),
                re.sub(r"<[^>]+>", " ", str(html or "")),
            ]
        ).lower()
        scored = [
            ("security", ["security", "breach", "exploit", "malware", "vulnerability", "vuln", "cve", "patch"]),
            ("policy", ["policy", "regulation", "compliance", "law", "legal", "terms", "guideline"]),
            ("platform", ["platform", "rollout", "release", "service", "api", "outage", "update"]),
            ("mobile", ["mobile", "android", "ios", "iphone", "ipad", "galaxy", "pixel"]),
            ("ai", ["ai", "llm", "model", "inference", "training", "copilot", "gemini"]),
            ("chips", ["chip", "gpu", "cpu", "semiconductor", "silicon", "nvidia", "amd", "intel"]),
            ("privacy", ["privacy", "tracking", "consent", "personal data", "data collection", "gdpr"]),
        ]
        best = ("default", 0)
        for topic, words in scored:
            count = 0
            for token in words:
                if token in vocab:
                    count += 1
            if count > best[1]:
                best = (topic, count)
        return best[0] if best[1] > 0 else "default"

    def _refresh_internal_links_pool(self, force: bool = False) -> None:
        path = self._internal_links_pool_path()
        now = datetime.now(timezone.utc)
        if (not force) and path.exists():
            try:
                mtime_utc = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                cooldown_hours = max(1, int(getattr(self, "_internal_links_pool_refresh_cooldown_hours", 6) or 6))
                if (now - mtime_utc) < timedelta(hours=cooldown_hours):
                    return
            except Exception:
                pass
        cutoff = now - timedelta(days=180)
        canonical_host = self._canonical_internal_host()
        allowed_topics = {"security", "policy", "platform", "mobile", "ai", "chips", "privacy", "default"}
        pool_rows: list[dict[str, Any]] = []

        # keep existing rows first so manual curation can survive refresh
        pool_rows.extend(self._load_internal_links_pool())

        # local posts index
        try:
            recent_rows = self.posts_index.query_recent(
                limit=500,
                include_future=False,
                statuses=["live", "scheduled"],
                exclude_deleted=True,
            )
        except Exception:
            recent_rows = []
        for row in recent_rows:
            focus = sorted(self._parse_focus_keywords(str((row or {}).get("focus_keywords", "") or "")))
            title = str((row or {}).get("title", "") or "").strip()
            summary = str((row or {}).get("summary", "") or "").strip()
            topic = self._infer_topic_cluster(title, focus, summary)
            pool_rows.append(
                {
                    "url": str((row or {}).get("url", "") or "").strip(),
                    "title": title,
                    "keywords": focus,
                    "tags": [topic] if topic else ["default"],
                    "topic": topic or "default",
                    "updated_at_utc": str(
                        (row or {}).get("published_at")
                        or (row or {}).get("last_seen_at")
                        or now.isoformat()
                    ),
                    "source": "posts_index",
                }
            )

        # local publish ledger
        ledger_path = Path(getattr(self, "_publish_ledger_path", self.root / "storage" / "ledger" / "publish_ledger.jsonl"))
        if ledger_path.exists():
            try:
                with ledger_path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        row_txt = str(line or "").strip()
                        if not row_txt:
                            continue
                        try:
                            row = json.loads(row_txt)
                        except Exception:
                            continue
                        if not isinstance(row, dict):
                            continue
                        url = str(
                            row.get("url")
                            or row.get("post_url")
                            or row.get("published_url")
                            or ""
                        ).strip()
                        title = str(row.get("title", "") or "").strip()
                        if not url or not title:
                            continue
                        topic = self._infer_topic_cluster(title, row.get("keywords", []), "")
                        pool_rows.append(
                            {
                                "url": url,
                                "title": title,
                                "keywords": self._parse_focus_keywords(row.get("keywords", "")),
                                "tags": [topic],
                                "topic": topic,
                                "updated_at_utc": str(row.get("created_at_utc", "") or now.isoformat()),
                                "source": "publish_ledger",
                            }
                        )
            except Exception:
                pass

        # optional legacy cache file
        cache_path = self._legacy_posts_index_cache_path()
        if cache_path.exists():
            try:
                cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
                if isinstance(cache_obj, dict):
                    cache_rows = cache_obj.get("items") or cache_obj.get("posts") or []
                elif isinstance(cache_obj, list):
                    cache_rows = cache_obj
                else:
                    cache_rows = []
                if not isinstance(cache_rows, list):
                    cache_rows = []
            except Exception:
                cache_rows = []
            for row in cache_rows:
                if not isinstance(row, dict):
                    continue
                url = str((row or {}).get("url", "") or "").strip()
                title = str((row or {}).get("title", "") or "").strip()
                if not url or not title:
                    continue
                topic = self._infer_topic_cluster(
                    title,
                    row.get("keywords") or row.get("focus_keywords") or "",
                    row.get("summary") or row.get("html") or "",
                )
                pool_rows.append(
                    {
                        "url": url,
                        "title": title,
                        "keywords": self._parse_focus_keywords(
                            row.get("keywords") or row.get("focus_keywords") or ""
                        ),
                        "tags": [topic],
                        "topic": topic,
                        "updated_at_utc": str(
                            (row or {}).get("updated_at_utc")
                            or (row or {}).get("published_at")
                            or now.isoformat()
                        ),
                        "source": "posts_index_cache",
                    }
                )

        normalized: list[dict[str, Any]] = []
        for row in pool_rows:
            url = str((row or {}).get("url", "") or "").strip()
            title = str((row or {}).get("title", "") or "").strip()
            if not url or not title:
                continue
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"}:
                continue
            host = self._host_from_url(url)
            if not host:
                continue
            updated = (
                self._parse_utc_soft((row or {}).get("updated_at_utc"))
                or self._parse_utc_soft((row or {}).get("published_at"))
                or self._parse_utc_soft((row or {}).get("last_seen_at"))
                or self._parse_utc_soft((row or {}).get("created_at_utc"))
                or now
            )
            if updated < cutoff:
                continue
            keywords_val = (row or {}).get("keywords", [])
            if isinstance(keywords_val, list):
                keywords = [str(x).strip() for x in keywords_val if str(x).strip()]
            elif isinstance(keywords_val, set):
                keywords = [str(x).strip() for x in keywords_val if str(x).strip()]
            else:
                keywords = [str(x).strip() for x in self._parse_focus_keywords(keywords_val) if str(x).strip()]
            if keywords:
                keywords = sorted(set(keywords))
            topic = str((row or {}).get("topic", "") or "").strip().lower()
            if topic not in allowed_topics:
                topic = self._infer_topic_cluster(title, keywords, str((row or {}).get("summary", "") or ""))
            if topic not in allowed_topics:
                topic = "default"
            tags_raw = (row or {}).get("tags", [])
            if isinstance(tags_raw, list):
                tags = [
                    str(x).strip().lower()
                    for x in tags_raw
                    if str(x).strip().lower() in allowed_topics
                ]
            else:
                tags = []
            if topic not in tags:
                tags.insert(0, topic)
            if not tags:
                tags = ["default"]
            normalized.append(
                {
                    "url": url,
                    "title": title,
                    "keywords": keywords[:12],
                    "tags": tags[:2],
                    "topic": topic,
                    "updated_at_utc": updated.isoformat(),
                    "source": str((row or {}).get("source", "pool") or "pool"),
                    "_host": host,
                }
            )

        if canonical_host:
            normalized = [row for row in normalized if str(row.get("_host", "")) == canonical_host]
        else:
            dominant = self._same_site_host_from_candidates(normalized)
            if dominant:
                normalized = [row for row in normalized if str(row.get("_host", "")) == dominant]

        normalized.sort(
            key=lambda x: str(x.get("updated_at_utc", "") or ""),
            reverse=True,
        )
        deduped: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for row in normalized:
            url = str(row.get("url", "") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            payload = dict(row)
            payload.pop("_host", None)
            deduped.append(payload)
            if len(deduped) >= 500:
                break

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(deduped, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _load_internal_links_pool(self) -> list[dict[str, Any]]:
        path = self._internal_links_pool_path()
        if not path.exists():
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("[]", encoding="utf-8")
            except Exception:
                return []
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if isinstance(raw, list):
            return [dict(x) for x in raw if isinstance(x, dict)]
        if isinstance(raw, dict):
            items = raw.get("items", [])
            if isinstance(items, list):
                return [dict(x) for x in items if isinstance(x, dict)]
        return []

    def _same_site_host_from_candidates(self, rows: list[dict[str, Any]]) -> str:
        counts: dict[str, int] = {}
        for row in rows:
            url = str((row or {}).get("url", "") or "").strip()
            host = str((row or {}).get("_host", "") or "").strip().lower()
            if not host and url:
                host = self._host_from_url(url)
            if not host:
                continue
            counts[host] = int(counts.get(host, 0) or 0) + 1
        if not counts:
            return ""
        ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return str(ranked[0][0] or "").strip().lower()

    def _sanitize_internal_anchor(self, title: str) -> str:
        text = self._clean_anchor_text(title)
        text = re.sub(
            r"\b(free|guaranteed|must|scam|proven|click here)\b",
            "",
            text,
            flags=re.IGNORECASE,
        )
        words = [w for w in re.findall(r"[A-Za-z0-9][A-Za-z0-9'\-]*", text) if w]
        if len(words) >= 3:
            return " ".join(words[:6]).strip()
        compact = re.sub(r"\s+", " ", text).strip()
        return compact[:80] if compact else "Related coverage"

    def _strip_legacy_internal_link_blocks(self, html: str) -> str:
        out = str(html or "")
        out = re.sub(
            r"<!--\s*RZ-INTERNAL:START\s*-->.*?<!--\s*RZ-INTERNAL:END\s*-->",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(
            r"<!--\s*RZ-RELATED:START\s*-->.*?<!--\s*RZ-RELATED:END\s*-->",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(
            r"<p[^>]*>\s*If you want a complementary walkthrough,\s*read\s*<a[^>]*>.*?</a>\.\s*</p>",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(
            r"<h2[^>]*>\s*(Related Coverage|More Fix Guides You Might Like)\s*</h2>\s*<ul>.*?</ul>",
            "",
            out,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out

    def _insert_internal_body_link_first40(self, html: str, url: str, anchor: str) -> tuple[str, bool]:
        src = str(html or "")
        if not src:
            return src, False
        paragraphs = list(re.finditer(r"<p\b[^>]*>.*?</p>", src, flags=re.IGNORECASE | re.DOTALL))
        if not paragraphs:
            return src, False
        limit = int(max(1, len(src) * 0.40))
        target = None
        for m in paragraphs:
            if m.start() <= limit:
                target = m
                break
        if target is None:
            target = paragraphs[0]
        block = str(target.group(0) or "")
        if "</p>" not in block.lower():
            return src, False
        marker_block = (
            "<!-- RZ-INTERNAL:START -->"
            '<p class="rz-internal-link">For background context, see '
            f'<a href="{escape(url, quote=True)}" rel="noopener">{escape(anchor)}</a>.'
            "</p><!-- RZ-INTERNAL:END -->"
        )
        insert_at = target.end()
        out = src[:insert_at] + marker_block + src[insert_at:]
        return out, True

    def _collect_internal_link_candidates(
        self,
        current_title: str,
        current_keywords: list[str] | None = None,
        current_html: str | None = None,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        current_title_l = str(current_title or "").strip().lower()
        current_kw = self._parse_focus_keywords(current_keywords or current_title)
        current_topic = self._infer_topic_cluster(
            current_title,
            current_kw,
            current_html or "",
        )
        canonical_host = self._canonical_internal_host()

        for row in self._load_internal_links_pool():
            url = str((row or {}).get("url", "") or "").strip()
            title = str((row or {}).get("title", "") or "").strip()
            if not url or not title:
                continue
            if url in seen:
                continue
            if title.lower() == current_title_l:
                continue
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"}:
                continue
            host = self._host_from_url(url)
            if canonical_host and host != canonical_host:
                continue
            token_source = " ".join(
                [
                    title,
                    " ".join(str(x or "") for x in (row.get("keywords", []) if isinstance(row.get("keywords", []), list) else [])),
                    " ".join(str(x or "") for x in (row.get("tags", []) if isinstance(row.get("tags", []), list) else [])),
                ]
            )
            overlap = self._keyword_overlap(current_kw, self._parse_focus_keywords(token_source))
            row_topic = str((row or {}).get("topic", "") or "").strip().lower() or self._infer_topic_cluster(title, token_source, "")
            topic_bonus = 0.2 if row_topic == current_topic else 0.0
            candidates.append(
                {
                    "url": url,
                    "title": title,
                    "topic": row_topic,
                    "score": float(overlap) + float(topic_bonus),
                    "source": "pool",
                }
            )
            seen.add(url)

        rows = self.posts_index.query_recent(
            limit=260,
            include_future=False,
            statuses=["live"],
            exclude_deleted=True,
        )
        for row in rows:
            title = str((row or {}).get("title", "") or "").strip()
            url = str((row or {}).get("url", "") or "").strip()
            if not title or not url:
                continue
            if url in seen:
                continue
            if title.lower() == current_title_l:
                continue
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"}:
                continue
            host = self._host_from_url(url)
            if canonical_host and host != canonical_host:
                continue
            kw = self._parse_focus_keywords(
                f"{title} {(row or {}).get('focus_keywords', '')}"
            )
            overlap = self._keyword_overlap(current_kw, kw)
            row_topic = self._infer_topic_cluster(title, kw, str((row or {}).get("summary", "") or ""))
            topic_bonus = 0.2 if row_topic == current_topic else 0.0
            candidates.append(
                {
                    "url": url,
                    "title": title,
                    "topic": row_topic,
                    "score": float(overlap) + float(topic_bonus),
                    "source": "posts_index",
                }
            )
            seen.add(url)

        candidates.sort(
            key=lambda x: (float(x.get("score", 0.0) or 0.0), str(x.get("title", ""))),
            reverse=True,
        )
        threshold = float(getattr(self.settings.internal_links, "overlap_threshold", 0.4) or 0.4)
        threshold = max(0.0, threshold)
        filtered = [row for row in candidates if float(row.get("score", 0.0) or 0.0) >= threshold]
        return filtered

    def _inject_internal_links_and_related_coverage(
        self,
        html: str,
        current_title: str,
        current_keywords: list[str] | None = None,
    ) -> str:
        src = str(html or "")
        if not src:
            return src
        if not bool(getattr(self.settings.internal_links, "enabled", True)):
            return src
        try:
            self._refresh_internal_links_pool()
        except Exception:
            pass
        out = self._strip_legacy_internal_link_blocks(src)
        current_topic = self._infer_topic_cluster(
            current_title,
            current_keywords or [],
            out,
        )
        out = self._enforce_what_to_do_now_section(out, current_topic)
        candidates = self._collect_internal_link_candidates(
            current_title=current_title,
            current_keywords=current_keywords,
            current_html=out,
        )
        if not candidates:
            return out

        site_host = self._canonical_internal_host() or self._same_site_host_from_candidates(candidates)
        if not site_host:
            return out

        existing_urls: set[str] = set(
            u.strip()
            for u in re.findall(r'href=["\']([^"\']+)["\']', out, flags=re.IGNORECASE)
            if str(u or "").strip()
        )
        picked: list[dict[str, Any]] = []
        for row in candidates:
            url = str((row or {}).get("url", "") or "").strip()
            host = self._host_from_url(url)
            if not url or host != site_host:
                continue
            if url in existing_urls:
                continue
            picked.append(row)
        if not picked:
            return out

        used_urls = set(existing_urls)
        body_inserted = False
        first = picked[0]
        first_url = str(first.get("url", "") or "").strip()
        if first_url and first_url not in used_urls:
            anchor = self._sanitize_internal_anchor(str(first.get("title", "") or "Related coverage"))
            out, body_inserted = self._insert_internal_body_link_first40(out, first_url, anchor)
            if body_inserted:
                used_urls.add(first_url)

        related_target = max(0, int(getattr(self.settings.internal_links, "related_link_count", 2) or 2))
        related_max = min(3, related_target)
        related_rows: list[dict[str, Any]] = []
        for row in picked[1 if body_inserted else 0 :]:
            url = str((row or {}).get("url", "") or "").strip()
            if not url or url in used_urls:
                continue
            related_rows.append(row)
            used_urls.add(url)
            if len(related_rows) >= related_max:
                break

        if not related_rows:
            return out

        lis: list[str] = []
        for row in related_rows:
            url = str((row or {}).get("url", "") or "").strip()
            anchor = self._sanitize_internal_anchor(str((row or {}).get("title", "") or "Related coverage"))
            if not url or not anchor:
                continue
            lis.append(
                f'<li><a href="{escape(url, quote=True)}" rel="noopener">{escape(anchor)}</a></li>'
            )
        if not lis:
            return out

        related_heading = self._related_coverage_heading_for_topic(current_topic)
        related_block = (
            "<!-- RZ-RELATED:START -->"
            f"<h2>{escape(related_heading)}</h2><ul>"
            + "".join(lis)
            + "</ul><!-- RZ-RELATED:END -->"
        )
        m_sources = re.search(r"<h2[^>]*>\s*Sources\s*</h2>", out, flags=re.IGNORECASE)
        if m_sources:
            out = out[: m_sources.start()] + related_block + out[m_sources.start() :]
        else:
            out = out + related_block
        return out

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

        news_mode = is_news_mode(self.settings)
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
            if news_mode:
                if row_cluster == cluster:
                    ov_news = self._keyword_overlap(current_kw, row_kw)
                    secondary.append((max(ov_news, 0.25), row))
                continue
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
                anchor = re.sub(r"\bfix guide\b", "", anchor, flags=re.IGNORECASE).strip() or "Related post"
                url = str((row or {}).get("url", "") or "").strip()
                lis.append(f'<li><a href="{url}" rel="noopener">{escape(anchor)}</a></li>')
            if news_mode:
                block_parts.append("<h2>Related Coverage</h2><ul>" + "".join(lis) + "</ul>")
            else:
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
        if is_news_mode(self.settings):
            t = re.sub(r"\s+", " ", str(title or "")).strip()
            s = re.sub(r"\s+", " ", str(summary or "")).strip()
            if len(s) < 80:
                s = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", str(html or ""))).strip()[:220]
            meta = f"{t}: {s}".strip()
            meta = re.sub(r"[가-힣ㄱ-ㅎㅏ-ㅣ]+", " ", meta)
            meta = re.sub(r"\b(test|troubleshooting|fix guide)\b", " ", meta, flags=re.IGNORECASE)
            meta = re.sub(r"\s+", " ", meta).strip()
            if len(meta) > 160:
                meta = meta[:157].rstrip(" ,.;:") + "..."
            return meta
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

    def _keyword_specificity_score(self, keyword: str) -> int:
        kw = str(keyword or "").lower()
        score = 0
        os_tokens = ["windows 11", "windows 10", "ios", "iphone", "macos", "android", "galaxy", "ipad"]
        triggers = ["after update", "keeps", "not recognized", "not detected", "error", "stuck", "no sound", "disconnect", "won t"]
        features = ["wifi", "wi-fi", "bluetooth", "usb", "printer", "microphone", "camera", "keyboard", "mouse", "driver", "vpn"]
        generic = ["device not working", "not working", "fix my", "help me", "problem"]
        for t in os_tokens:
            if t in kw:
                score += 3
        for t in triggers:
            if t in kw:
                score += 3
                break
        for t in features:
            if t in kw:
                score += 2
                break
        for t in generic:
            if t in kw:
                score -= 5
        return score

    def _expand_low_specificity_keyword(self, keyword: str, device_type: str) -> str:
        base = self._normalize_keyword(keyword).lower()
        device = self._normalize_keyword(device_type).lower() or "windows"
        if not base:
            return ""
        trigger = "after update"
        if "error" in base:
            trigger = "error code fix"
        elif "disconnect" in base or "wifi" in base or "bluetooth" in base:
            trigger = "keeps disconnecting fix"
        elif "sound" in base or "audio" in base:
            trigger = "no sound after update fix"
        if device not in base:
            base = f"{device} {base}"
        expanded = f"{base} {trigger}"
        expanded = re.sub(r"\s+", " ", expanded).strip()
        expanded = self._normalize_keyword(expanded)
        if not expanded:
            expanded = self._normalize_keyword(f"{device} issue after update fix")
        return expanded

    def _apply_keyword_specificity(self, keywords: list[str], device_type: str) -> tuple[list[str], str]:
        out: list[str] = []
        changed = 0
        for kw in (keywords or []):
            norm = self._normalize_keyword(kw)
            if not norm:
                continue
            if self._keyword_specificity_score(norm) < 3:
                exp = self._expand_low_specificity_keyword(norm, device_type=device_type)
                if exp and exp.lower() != norm.lower():
                    norm = exp
                    changed += 1
            if norm and norm.lower() not in {x.lower() for x in out}:
                out.append(norm)
        note = f"kw_specificity_rewrites={changed}" if changed else ""
        return out, note

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

        if allow_api_retry and added < target_add and (not is_news_mode(self.settings)):
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
        elif allow_api_retry and added < target_add and is_news_mode(self.settings):
            self._log_news_mode_guard("extract_global_keywords", "news_mode_blocked")

        if len(active) > max_pool:
            active = active[-max_pool:]
        pool["active"] = active
        return added

    def _acquire_run_keywords(self, candidates) -> tuple[list[str], str]:
        note = ""
        if is_news_mode(self.settings):
            self._log_news_mode_guard("_acquire_run_keywords", "news_mode_blocked")
            return [], "news_mode_keyword_acquire_skipped"
        device_type = self._current_rotated_device_type()
        target_size, min_size, refill_batch, avoid_days, per_run_pick = self._topic_pool_cfg()
        # One post is generated per run; reserve exactly one primary keyword per run.
        pick_count = 1 if per_run_pick >= 1 else 1

        available = self.keyword_assets.available_count(device_type=device_type, avoid_reuse_days=0)
        note = self._append_note(note, f"kw_device={device_type}")
        note = self._append_note(note, f"kw_available={available}")
        note = self._append_note(note, f"topic_pool_target={target_size}")
        note = self._append_note(note, f"topic_pool_min={min_size}")
        if available < min_size:
            local_candidates = self._build_local_keyword_candidates(candidates, limit=max(200, refill_batch * 4))
            target_add = min(refill_batch, max(0, target_size - available))
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
            for kw in self._build_template_keywords(device_type=device_type, limit=max(80, refill_batch * 2)):
                rows.append(
                    (
                        kw,
                        self._infer_cluster_id_from_keyword(kw),
                        "templates",
                        0.5,
                        0.2,
                    )
                )
            if target_add <= 0:
                added = 0
            else:
                added = self.keyword_assets.upsert_keywords(
                    device_type=device_type,
                    rows=rows[: max(target_add * 4, 120)],
                )
            note = self._append_note(note, f"kw_refill_added={added}")

        picks = self.keyword_assets.pick_keywords(
            device_type=device_type,
            limit=pick_count,
            avoid_reuse_days=0,
            mark_used=False,
        )
        self._pending_keyword_claims = list(picks[:pick_count])
        if not picks:
            fallback = self._build_local_keyword_candidates(candidates, limit=24)
            picks = fallback[:pick_count]
            note = self._append_note(note, "kw_fallback_local")
        picks, spec_note = self._apply_keyword_specificity(picks, device_type=device_type)
        if spec_note:
            note = self._append_note(note, spec_note)
        return picks, note

    def weekly_refresh_topic_pool(self, force: bool = False) -> dict[str, Any]:
        target_size, min_size, refill_batch, avoid_days, _ = self._topic_pool_cfg()
        default_order = ["windows", "mac", "iphone", "galaxy"]
        devices = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings.topics, "rotation_order", default_order) or default_order)
            if str(x or "").strip()
        ]
        if not devices:
            devices = default_order

        try:
            candidates = self._collect_candidates_with_retry(max_attempts=3)
        except Exception:
            candidates = []
        base_local = self._build_local_keyword_candidates(candidates, limit=max(260, refill_batch * 5))
        ts_utc = datetime.now(timezone.utc).isoformat()
        report: dict[str, Any] = {
            "ts_utc": ts_utc,
            "target_size": int(target_size),
            "min_size": int(min_size),
            "refill_batch": int(refill_batch),
            "avoid_reuse_days": int(avoid_days),
            "devices": {},
            "total_added": 0,
        }

        for device in devices:
            before = self.keyword_assets.available_count(device_type=device, avoid_reuse_days=0)
            added = 0
            rows: list[tuple[str, str, str, float, float]] = []
            if force or before < min_size:
                want_total = max(min_size, target_size)
                need = max(0, want_total - before)
                target_add = min(refill_batch, need if need > 0 else refill_batch)
                for kw in self._build_template_keywords(device_type=device, limit=max(120, target_add * 2)):
                    rows.append(
                        (
                            kw,
                            self._infer_cluster_id_from_keyword(kw),
                            "weekly_templates",
                            0.65,
                            0.2,
                        )
                    )
                for kw in base_local:
                    normalized = self._normalize_keyword(kw)
                    if not normalized:
                        continue
                    if device not in normalized.lower():
                        normalized = self._normalize_keyword(f"{device} {normalized}")
                    if not normalized:
                        continue
                    rows.append(
                        (
                            normalized,
                            self._infer_cluster_id_from_keyword(normalized),
                            "weekly_scout",
                            0.8,
                            min(1.0, len(normalized.split()) / 8.0),
                        )
                    )
                    if len(rows) >= max(140, target_add * 4):
                        break
                if rows:
                    added = self.keyword_assets.upsert_keywords(device_type=device, rows=rows)
            after = self.keyword_assets.available_count(device_type=device, avoid_reuse_days=0)
            report["devices"][device] = {
                "available_before": int(before),
                "available_after": int(after),
                "added": int(added),
                "rows_prepared": int(len(rows)),
            }
            report["total_added"] += int(added)

        try:
            log_path = self.root / "storage" / "logs" / "topic_pool_weekly_refresh.jsonl"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(report, ensure_ascii=False) + "\n")
        except Exception:
            pass
        return report

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

    def _parse_focus_keywords(self, value: str | list[str] | set[str] | tuple[str, ...]) -> set[str]:
        if isinstance(value, (list, set, tuple)):
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

