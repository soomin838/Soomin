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
    source_type: str
    generation_mode_hint: str
    min_words: int
    max_words: int
    title_strategy: str
    source_strategy: str
    image_strategy: str
    min_images: int
    max_images: int


@dataclass(frozen=True)
class AllocationSlot:
    content_type: str
    source_type: str
    generation_mode_hint: str
    target_word_range: tuple[int, int]


class ContentAllocationEngine:
    _DEFAULT_PRIORITY = ("hot", "search_derived", "evergreen")
    def __init__(
        self,
        *,
        enabled: bool = False,
        mix_hot: int = 2,
        mix_search_derived: int = 2,
        mix_evergreen: int = 1,
        content_lengths: object | None = None,
        log_path: Path | None = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.mix_hot = max(0, int(mix_hot))
        self.mix_search_derived = max(0, int(mix_search_derived))
        self.mix_evergreen = max(0, int(mix_evergreen))
        self.log_path = Path(log_path).resolve() if log_path else None
        self.content_lengths = content_lengths
        self._policy_map = self._build_policy_map(content_lengths)

    def _build_policy_map(self, content_lengths: object | None) -> dict[str, ContentPolicy]:
        hot_min = max(500, int(getattr(content_lengths, "hot_news_min", 700) or 700))
        hot_max = max(hot_min, int(getattr(content_lengths, "hot_news_max", 1000) or 1000))
        search_min = max(700, int(getattr(content_lengths, "search_derived_min", 1100) or 1100))
        search_max = max(search_min, int(getattr(content_lengths, "search_derived_max", 1500) or 1500))
        evergreen_min = max(900, int(getattr(content_lengths, "evergreen_min", 1600) or 1600))
        evergreen_max = max(evergreen_min, int(getattr(content_lengths, "evergreen_max", 2200) or 2200))
        return {
            "hot": ContentPolicy(
                content_type="hot",
                source_type="news_pool",
                generation_mode_hint="news_explainer_fast",
                min_words=hot_min,
                max_words=hot_max,
                title_strategy="timely_explainer",
                source_strategy="news_source_plus_corroboration",
                image_strategy="hero_plus_one_inline",
                min_images=1,
                max_images=2,
            ),
            "search_derived": ContentPolicy(
                content_type="search_derived",
                source_type="search_console_or_news_seed",
                generation_mode_hint="search_answer",
                min_words=search_min,
                max_words=search_max,
                title_strategy="query_match",
                source_strategy="authority_first",
                image_strategy="hero_plus_one_inline",
                min_images=1,
                max_images=2,
            ),
            "evergreen": ContentPolicy(
                content_type="evergreen",
                source_type="evergreen_seed_pool",
                generation_mode_hint="evergreen_hub",
                min_words=evergreen_min,
                max_words=evergreen_max,
                title_strategy="evergreen_utility",
                source_strategy="authority_first",
                image_strategy="single_meaningful_visual",
                min_images=1,
                max_images=1,
            ),
        }

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
        if key not in self._policy_map:
            key = "hot"
        return self._policy_map[key]

    def slot_for(self, content_type: str) -> AllocationSlot:
        policy = self.policy_for(content_type)
        return AllocationSlot(
            content_type=policy.content_type,
            source_type=policy.source_type,
            generation_mode_hint=policy.generation_mode_hint,
            target_word_range=(int(policy.min_words), int(policy.max_words)),
        )

    def next_content_types(self, *, day: date | str, published_counts: dict[str, int] | None = None) -> list[str]:
        return [slot.content_type for slot in self.next_slots(day=day, published_counts=published_counts)]

    def next_slots(self, *, day: date | str, published_counts: dict[str, int] | None = None) -> list[AllocationSlot]:
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
        slots = [self.slot_for(key) for key in order]
        self._log(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "event": "content_allocation_decision",
                "day": str(day.isoformat() if isinstance(day, date) else day),
                "published_counts": counts,
                "targets": targets,
                "sequence": sequence,
                "selected_order": order,
                "selected_slots": [asdict(slot) for slot in slots],
            }
        )
        return slots

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
