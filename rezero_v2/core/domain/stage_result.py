from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeVar

T = TypeVar("T")
StageStatus = Literal["success", "skipped", "held", "failed"]


@dataclass(frozen=True)
class StageResult(Generic[T]):
    stage_name: str
    status: StageStatus
    reason_code: str
    human_message: str
    timing_ms: int
    payload: T | None = None
    debug_meta: dict[str, Any] = field(default_factory=dict)
