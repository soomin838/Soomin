from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class AllocationPlan:
    day: str
    slots: int
    mix_hot: int
    mix_search_derived: int
    mix_evergreen: int


class ContentAllocationEngine:
    def __init__(
        self,
        *,
        enabled: bool = False,
        mix_hot: int = 2,
        mix_search_derived: int = 2,
        mix_evergreen: int = 1,
        log_path: Path | None = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.mix_hot = max(0, int(mix_hot))
        self.mix_search_derived = max(0, int(mix_search_derived))
        self.mix_evergreen = max(0, int(mix_evergreen))
        self.log_path = Path(log_path).resolve() if log_path else None

    def plan(self, *, day: date | str, slots: int = 5) -> AllocationPlan:
        plan = AllocationPlan(
            day=str(day.isoformat() if isinstance(day, date) else day),
            slots=max(1, int(slots)),
            mix_hot=int(self.mix_hot),
            mix_search_derived=int(self.mix_search_derived),
            mix_evergreen=int(self.mix_evergreen),
        )
        self._log(plan)
        return plan

    def _log(self, plan: AllocationPlan) -> None:
        if self.log_path is None:
            return
        row = {"ts_utc": datetime.now(timezone.utc).isoformat(), **asdict(plan)}
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return
