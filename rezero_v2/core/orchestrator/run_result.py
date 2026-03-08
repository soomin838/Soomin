from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from rezero_v2.core.domain.workflow_final import WorkflowFinalSummary


@dataclass(frozen=True)
class RunExecutionResult:
    summary: WorkflowFinalSummary
    stage_results: list[dict[str, Any]] = field(default_factory=list)
