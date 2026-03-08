from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from re_core.settings import AppSettings
from rezero_v2.core.domain.allocation import AllocationDecision
from rezero_v2.stores.app_settings_store import V2RuntimeConfig


@dataclass
class RunContext:
    root: Path
    settings_path: Path
    settings: AppSettings
    v2_config: V2RuntimeConfig
    allocation: AllocationDecision
    run_id: str
    day_key: str
    run_store: Any
    candidate_store: Any
    cluster_store: Any
    publish_store: Any
    dry_run: bool = False
    stage_timings_ms: dict[str, int] = field(default_factory=dict)
    debug_meta: dict[str, Any] = field(default_factory=dict)
