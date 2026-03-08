from __future__ import annotations

import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from rezero_v2.core.domain.workflow_final import WorkflowFinalSummary
from rezero_v2.core.guards.coherence_guard import CoherenceGuard
from rezero_v2.core.guards.grounding_guard import GroundingGuard
from rezero_v2.core.guards.image_relevance_guard import ImageRelevanceGuard
from rezero_v2.core.guards.source_relevance_guard import SourceRelevanceGuard
from rezero_v2.core.guards.story_guard import StoryGuard
from rezero_v2.core.guards.structure_diversity_guard import StructureDiversityGuard
from rezero_v2.core.guards.support_drift_guard import SupportDriftGuard
from rezero_v2.core.orchestrator.run_context import RunContext
from rezero_v2.core.orchestrator.run_result import RunExecutionResult
from rezero_v2.core.pipeline import (
    DraftStage,
    FeedbackStage,
    GateStage,
    ImageStage,
    IngestStage,
    IntentStage,
    OutlineStage,
    PublishStage,
)
from rezero_v2.core.services.allocation_engine import AllocationEngine
from rezero_v2.core.services.cluster_builder import ClusterBuilder
from rezero_v2.core.services.draft_engine import DraftEngine
from rezero_v2.core.services.intent_engine import IntentEngine
from rezero_v2.core.services.internal_link_engine import InternalLinkEngine
from rezero_v2.core.services.outline_engine import OutlineEngine
from rezero_v2.core.services.topic_scorer import TopicScorer
from rezero_v2.integrations.blogger_client import BloggerClient
from rezero_v2.integrations.gdelt_client import GDELTClient
from rezero_v2.integrations.gemini_client import GeminiClient
from rezero_v2.integrations.ollama_client import OllamaClient
from rezero_v2.integrations.pollinations_client import PollinationsClient
from rezero_v2.integrations.search_console_client import SearchConsoleClient
from rezero_v2.stores.app_settings_store import AppSettingsStore
from rezero_v2.stores.candidate_store import CandidateStore
from rezero_v2.stores.cluster_store import ClusterStore
from rezero_v2.stores.publish_store import PublishStore
from rezero_v2.stores.run_store import RunStore


class RunEngine:
    def __init__(
        self,
        root: Path,
        settings_path: Path,
        *,
        progress_hook: Callable[[dict[str, Any]], None] | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        self.settings_path = Path(settings_path).resolve()
        self.progress_hook = progress_hook
        self.overrides = overrides or {}
        self.storage_root = self.root / "storage" / "v2"
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_root / "rezero_v2.sqlite3"
        self.run_store = RunStore(self.db_path)
        self.candidate_store = CandidateStore(self.db_path)
        self.cluster_store = ClusterStore(self.db_path)
        self.publish_store = PublishStore(self.db_path)
        self.settings_store = AppSettingsStore(self.settings_path)

    def run_once(self, *, force_content_type: str | None = None, dry_run: bool | None = None) -> RunExecutionResult:
        settings = self.settings_store.load_app_settings()
        v2_config = self.settings_store.load_v2_config()
        if dry_run is None:
            dry_run = bool(getattr(settings.budget, "dry_run", False))
        allocation_engine = AllocationEngine(
            mix_hot=int(v2_config.mix_hot),
            mix_search_derived=int(v2_config.mix_search_derived),
            mix_evergreen=int(v2_config.mix_evergreen),
            content_lengths={
                "hot": (int(v2_config.hot_min), int(v2_config.hot_max)),
                "search_derived": (int(v2_config.search_derived_min), int(v2_config.search_derived_max)),
                "evergreen": (int(v2_config.evergreen_min), int(v2_config.evergreen_max)),
            },
        )
        day_key = datetime.now().astimezone().date().isoformat()
        counts = self.publish_store.get_daily_counts(day_key)
        allocation = allocation_engine.choose_next_slot(published_counts=counts)
        if force_content_type and force_content_type in {"hot", "search_derived", "evergreen"}:
            allocation = AllocationEngine(
                mix_hot=1 if force_content_type == "hot" else 0,
                mix_search_derived=1 if force_content_type == "search_derived" else 0,
                mix_evergreen=1 if force_content_type == "evergreen" else 0,
                content_lengths=allocation_engine.content_lengths,
            ).choose_next_slot(published_counts={"hot": 0, "search_derived": 0, "evergreen": 0})
        context = RunContext(
            root=self.root,
            settings_path=self.settings_path,
            settings=settings,
            v2_config=v2_config,
            allocation=allocation,
            run_id=uuid.uuid4().hex[:12],
            day_key=day_key,
            run_store=self.run_store,
            candidate_store=self.candidate_store,
            cluster_store=self.cluster_store,
            publish_store=self.publish_store,
            dry_run=bool(dry_run),
        )
        self._emit({"type": "run_start", "run_id": context.run_id, "slot_type": allocation.slot_type})
        runtime = self._build_runtime(settings=settings, v2_config=v2_config, dry_run=bool(dry_run))
        stage_results: list[dict[str, Any]] = []

        ingest_result = runtime["ingest_stage"].run(context)
        self._record_stage(context, ingest_result, stage_results)
        if ingest_result.status != "success" or not ingest_result.payload:
            return self._finalize(context, stage_results, ingest_result, selected_title="", source_domain="")

        gate_result = runtime["gate_stage"].run(context, ingest_result.payload)
        self._record_stage(context, gate_result, stage_results)
        if gate_result.status != "success" or not gate_result.payload:
            return self._finalize(context, stage_results, gate_result, selected_title="", source_domain="")

        selected = gate_result.payload[0]
        candidate = selected["candidate"]
        story_decision = selected["story"]

        intent_result = runtime["intent_stage"].run(context, candidate)
        self._record_stage(context, intent_result, stage_results)
        if intent_result.status != "success" or not intent_result.payload:
            failed_candidate = (intent_result.payload or {}).get("candidate", candidate) if isinstance(intent_result.payload, dict) else candidate
            return self._finalize(context, stage_results, intent_result, selected_title=failed_candidate.title, source_domain=failed_candidate.source_domain)
        candidate = intent_result.payload["candidate"]
        intent_bundle = intent_result.payload["intent_bundle"]

        cluster = runtime["cluster_builder"].assign_cluster(
            title=candidate.title,
            primary_query=intent_bundle.primary_query,
            content_type=candidate.content_type,
            entity_terms=list(candidate.entity_terms),
        )
        candidate.raw_meta["cluster_id"] = cluster.cluster_id

        outline_result = runtime["outline_stage"].run(
            context,
            candidate=candidate,
            intent_bundle=intent_bundle,
            story_decision=story_decision,
        )
        self._record_stage(context, outline_result, stage_results)
        if outline_result.status != "success" or not outline_result.payload:
            return self._finalize(context, stage_results, outline_result, selected_title=candidate.title, source_domain=candidate.source_domain)
        grounding_packet = outline_result.payload["grounding_packet"]
        outline_plan = outline_result.payload["outline_plan"]

        draft_result = runtime["draft_stage"].run(
            context,
            candidate=candidate,
            intent_bundle=intent_bundle,
            grounding_packet=grounding_packet,
            outline_plan=outline_plan,
        )
        self._record_stage(context, draft_result, stage_results)
        if draft_result.status != "success" or not draft_result.payload:
            return self._finalize(context, stage_results, draft_result, selected_title=candidate.title, source_domain=candidate.source_domain)
        draft = draft_result.payload["draft"]

        image_result = runtime["image_stage"].run(context, candidate=candidate, draft=draft)
        self._record_stage(context, image_result, stage_results)
        if image_result.status != "success":
            return self._finalize(context, stage_results, image_result, selected_title=candidate.title, source_domain=candidate.source_domain)
        images = image_result.payload or []

        publish_result = runtime["publish_stage"].run(
            context,
            candidate=candidate,
            intent_bundle=intent_bundle,
            draft=draft,
            images=images,
            cluster=cluster,
        )
        self._record_stage(context, publish_result, stage_results)
        if publish_result.status != "success" or not publish_result.payload:
            return self._finalize(context, stage_results, publish_result, selected_title=candidate.title, source_domain=candidate.source_domain)
        publish_artifact = publish_result.payload["publish_artifact"]

        feedback_result = runtime["feedback_stage"].run(
            context,
            candidate=candidate,
            intent_bundle=intent_bundle,
            cluster=cluster,
            publish_artifact=publish_artifact,
        )
        self._record_stage(context, feedback_result, stage_results)
        return self._finalize(
            context,
            stage_results,
            feedback_result,
            selected_title=candidate.title,
            source_domain=candidate.source_domain,
            content_type=candidate.content_type,
            published_url=publish_artifact.post_url,
            repair_attempted=bool(draft.repair_attempted),
            repair_succeeded=bool(draft.repair_succeeded),
        )

    def _build_runtime(self, *, settings, v2_config, dry_run: bool) -> dict[str, Any]:
        gdelt_client = self.overrides.get("gdelt_client") or GDELTClient()
        search_console_client = self.overrides.get("search_console_client")
        if search_console_client is None:
            try:
                search_console_client = SearchConsoleClient(
                    credentials_path=self.root / "config" / "blogger_token.json",
                    site_url=str(getattr(settings.integrations, "search_console_site_url", "") or ""),
                    enabled=bool(getattr(settings.integrations, "search_console_enabled", True)),
                )
            except Exception:
                search_console_client = None
        pollinations_client = self.overrides.get("pollinations_client") or PollinationsClient(model=str(v2_config.pollinations_model or "flux"))
        ollama_client = self.overrides.get("ollama_client")
        if ollama_client is None and bool(getattr(settings.local_llm, "enabled", False)):
            ollama_client = OllamaClient(
                base_url=str(getattr(settings.local_llm, "base_url", "http://127.0.0.1:11434") or "http://127.0.0.1:11434"),
                model=str(getattr(settings.local_llm, "model", "qwen2.5:3b") or "qwen2.5:3b"),
                timeout_sec=int(getattr(settings.search_intent, "timeout_sec", 15) or 15),
            )
        gemini_client = self.overrides.get("gemini_client")
        if gemini_client is None:
            gemini_client = GeminiClient(
                api_key=str(getattr(settings.gemini, "api_key", "") or ""),
                model=str(getattr(settings.gemini, "model", "gemini-2.0-flash") or "gemini-2.0-flash"),
            )
        blogger_client = self.overrides.get("blogger_client") or BloggerClient(
            credentials_path=self.root / "config" / "blogger_token.json",
            blog_id=str(getattr(settings.blogger, "blog_id", "") or ""),
            dry_run=bool(dry_run),
        )
        topic_scorer = self.overrides.get("topic_scorer") or TopicScorer()
        story_guard = self.overrides.get("story_guard") or StoryGuard()
        support_guard = self.overrides.get("support_guard") or SupportDriftGuard()
        source_guard = self.overrides.get("source_guard") or SourceRelevanceGuard()
        grounding_guard = self.overrides.get("grounding_guard") or GroundingGuard()
        coherence_guard = self.overrides.get("coherence_guard") or CoherenceGuard()
        structure_guard = self.overrides.get("structure_guard") or StructureDiversityGuard()
        image_guard = self.overrides.get("image_guard") or ImageRelevanceGuard()
        cluster_builder = self.overrides.get("cluster_builder") or ClusterBuilder()
        internal_link_engine = self.overrides.get("internal_link_engine") or InternalLinkEngine(
            publish_store=self.publish_store,
            cluster_store=self.cluster_store,
        )
        intent_engine = self.overrides.get("intent_engine") or IntentEngine(ollama_client=ollama_client)
        outline_engine = self.overrides.get("outline_engine") or OutlineEngine(
            ollama_client=ollama_client,
            grounding_guard=grounding_guard,
            diversity_guard=structure_guard,
            support_guard=support_guard,
        )
        draft_engine = self.overrides.get("draft_engine") or DraftEngine(gemini_client=gemini_client)
        return {
            "cluster_builder": cluster_builder,
            "ingest_stage": self.overrides.get("ingest_stage")
            or IngestStage(gdelt_client=gdelt_client, candidate_store=self.candidate_store, search_console_client=search_console_client, topic_scorer=topic_scorer),
            "gate_stage": self.overrides.get("gate_stage") or GateStage(story_guard=story_guard, support_guard=support_guard, candidate_store=self.candidate_store),
            "intent_stage": self.overrides.get("intent_stage") or IntentStage(intent_engine=intent_engine, candidate_store=self.candidate_store, topic_scorer=topic_scorer),
            "outline_stage": self.overrides.get("outline_stage") or OutlineStage(grounding_guard=grounding_guard, outline_engine=outline_engine),
            "draft_stage": self.overrides.get("draft_stage") or DraftStage(draft_engine=draft_engine, coherence_guard=coherence_guard),
            "image_stage": self.overrides.get("image_stage") or ImageStage(pollinations_client=pollinations_client, image_guard=image_guard, image_policy=v2_config),
            "publish_stage": self.overrides.get("publish_stage")
            or PublishStage(
                blogger_client=blogger_client,
                internal_link_engine=internal_link_engine,
                source_guard=source_guard,
                publish_store=self.publish_store,
                candidate_store=self.candidate_store,
                cluster_store=self.cluster_store,
            ),
            "feedback_stage": self.overrides.get("feedback_stage") or FeedbackStage(search_console_client=search_console_client, candidate_store=self.candidate_store, cluster_store=self.cluster_store),
        }

    def _record_stage(self, context: RunContext, result, stage_results: list[dict[str, Any]]) -> None:
        context.stage_timings_ms[result.stage_name] = int(result.timing_ms)
        self.run_store.append_stage_result(context.run_id, result)
        stage_results.append(
            {
                "stage_name": result.stage_name,
                "status": result.status,
                "reason_code": result.reason_code,
                "human_message": result.human_message,
                "timing_ms": int(result.timing_ms),
                "payload": self._json_ready(result.payload),
                "debug_meta": self._json_ready(result.debug_meta),
            }
        )
        self._emit(
            {
                "type": "stage",
                "run_id": context.run_id,
                "stage_name": result.stage_name,
                "status": result.status,
                "reason_code": result.reason_code,
                "human_message": result.human_message,
                "timing_ms": int(result.timing_ms),
            }
        )

    def _finalize(
        self,
        context: RunContext,
        stage_results: list[dict[str, Any]],
        last_result,
        *,
        selected_title: str,
        source_domain: str,
        content_type: str | None = None,
        published_url: str = "",
        repair_attempted: bool = False,
        repair_succeeded: bool = False,
    ) -> RunExecutionResult:
        summary = WorkflowFinalSummary(
            run_id=context.run_id,
            runtime_version="v2",
            result=last_result.status,
            reason_code=last_result.reason_code,
            human_message=last_result.human_message,
            content_type=content_type or context.allocation.slot_type,
            selected_title=selected_title,
            source_domain=source_domain,
            final_stage=last_result.stage_name,
            repair_attempted=repair_attempted or bool(last_result.debug_meta.get("repair_attempted", False)),
            repair_succeeded=repair_succeeded or bool(last_result.debug_meta.get("repair_succeeded", False)),
            published_url=published_url,
            stage_timings_ms=dict(context.stage_timings_ms),
            debug_meta={"allocation": asdict(context.allocation), "last_stage_meta": self._json_ready(last_result.debug_meta)},
        )
        self.run_store.record_final_summary(summary)
        self._emit({"type": "final", "summary": self._json_ready(summary)})
        return RunExecutionResult(summary=summary, stage_results=stage_results)

    def _emit(self, event: dict[str, Any]) -> None:
        if self.progress_hook is None:
            return
        try:
            self.progress_hook(event)
        except Exception:
            pass

    def _json_ready(self, value: Any) -> Any:
        if value is None:
            return None
        if is_dataclass(value):
            return {k: self._json_ready(v) for k, v in asdict(value).items()}
        if isinstance(value, dict):
            return {str(k): self._json_ready(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_ready(item) for item in value]
        return value
