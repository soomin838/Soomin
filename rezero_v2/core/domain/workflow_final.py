from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class WorkflowFinalSummary:
    run_id: str
    runtime_version: Literal["v2"]
    result: Literal["success", "skipped", "held", "failed"]
    reason_code: str
    human_message: str
    content_type: str
    selected_title: str
    source_domain: str
    final_stage: str
    repair_attempted: bool
    repair_succeeded: bool
    published_url: str
    stage_timings_ms: dict[str, int] = field(default_factory=dict)
    debug_meta: dict[str, Any] = field(default_factory=dict)
