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


@dataclass(frozen=True)
class ContentPolicy:
    content_type: str
    min_words: int
    max_words: int
    title_strategy: str
    source_strategy: str
    image_strategy: str
    min_images: int
    max_images: int


class ContentAllocationEngine:
    _DEFAULT_PRIORITY = ("hot", "search_derived", "evergreen")
    _POLICY_MAP = {
        "hot": ContentPolicy(
            content_type="hot",
            min_words=700,
            max_words=1000,
            title_strategy="timely_explainer",
            source_strategy="news_source_plus_corroboration",
            image_strategy="hero_plus_one_inline",
            min_images=1,
            max_images=2,
        ),
        "search_derived": ContentPolicy(
            content_type="search_derived",
            min_words=1100,
            max_words=1500,
            title_strategy="query_match",
            source_strategy="authority_first",
            image_strategy="hero_plus_one_inline",
            min_images=1,
            max_images=2,
        ),
        "evergreen": ContentPolicy(
            content_type="evergreen",
            min_words=1600,
            max_words=2200,
            title_strategy="evergreen_utility",
            source_strategy="authority_first",
            image_strategy="single_meaningful_visual",
            min_images=1,
            max_images=1,
        ),
    }

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

    def targets(self) -> dict[str, int]:
        return {
            "hot": int(self.mix_hot),
            "search_derived": int(self.mix_search_derived),
            "evergreen": int(self.mix_evergreen),
        }

    def total_slots(self) -> int:
        total = sum(self.targets().values())
        return max(1, int(total or 5))

    def daily_sequence(self) -> list[str]:
        remaining = self.targets()
        sequence: list[str] = []
        previous = ""
        while sum(remaining.values()) > 0:
            candidates = sorted(
                [key for key, count in remaining.items() if count > 0],
                key=lambda key: (
                    -remaining[key],
                    self._DEFAULT_PRIORITY.index(key) if key in self._DEFAULT_PRIORITY else 99,
                ),
            )
            pick = candidates[0]
            if len(candidates) > 1 and pick == previous:
                alternate = next((key for key in candidates if key != previous), "")
                if alternate:
                    pick = alternate
            sequence.append(pick)
            remaining[pick] = max(0, int(remaining.get(pick, 0)) - 1)
            previous = pick
        return sequence

    def policy_for(self, content_type: str) -> ContentPolicy:
        key = str(content_type or "hot").strip().lower()
        if key not in self._POLICY_MAP:
            key = "hot"
        return self._POLICY_MAP[key]

    def next_content_types(self, *, day: date | str, published_counts: dict[str, int] | None = None) -> list[str]:
        counts = {key: max(0, int((published_counts or {}).get(key, 0) or 0)) for key in self.targets()}
        sequence = self.daily_sequence()
        used_slots = 0
        targets = self.targets()
        for key, target in targets.items():
            used_slots += min(max(0, int(target)), counts.get(key, 0))
        order: list[str] = []
        if sequence and used_slots < len(sequence):
            order.append(sequence[max(0, used_slots)])
        remaining = {
            key: max(0, int(targets.get(key, 0)) - counts.get(key, 0))
            for key in targets
        }
        for key, _ in sorted(
            remaining.items(),
            key=lambda item: (
                -item[1],
                self._DEFAULT_PRIORITY.index(item[0]) if item[0] in self._DEFAULT_PRIORITY else 99,
            ),
        ):
            if key not in order and (remaining[key] > 0):
                order.append(key)
        for key in self._DEFAULT_PRIORITY:
            if key not in order:
                order.append(key)
        self._log(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "event": "content_allocation_decision",
                "day": str(day.isoformat() if isinstance(day, date) else day),
                "published_counts": counts,
                "targets": targets,
                "sequence": sequence,
                "selected_order": order,
            }
        )
        return order

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

    def _log(self, plan: AllocationPlan | dict[str, object]) -> None:
        if self.log_path is None:
            return
        row = (
            {"ts_utc": datetime.now(timezone.utc).isoformat(), **asdict(plan)}
            if isinstance(plan, AllocationPlan)
            else dict(plan or {})
        )
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return
