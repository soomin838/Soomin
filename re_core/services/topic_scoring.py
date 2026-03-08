from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class TopicScore:
    topic_id: str
    content_type: str
    trend: float
    search: float
    ctr: float
    position: float
    cluster: float
    total: float
    search_demand: float = 0.0
    usefulness: float = 0.0
    comparison_potential: float = 0.0
    tutorial_potential: float = 0.0
    evergreen_potential: float = 0.0
    competition_inverse: float = 0.0
    freshness: float = 0.0


class TopicScoring:
    def __init__(self, *, log_path: Path | None = None) -> None:
        self.log_path = Path(log_path).resolve() if log_path else None

    def score(
        self,
        topic_id: str,
        *,
        trend: float,
        search: float,
        ctr: float,
        position: float,
        cluster: float,
    ) -> TopicScore:
        total = (
            (0.30 * float(trend))
            + (0.30 * float(search))
            + (0.15 * float(ctr))
            + (0.15 * float(position))
            + (0.10 * float(cluster))
        )
        result = TopicScore(
            topic_id=str(topic_id or "").strip(),
            content_type="generic",
            trend=float(trend),
            search=float(search),
            ctr=float(ctr),
            position=float(position),
            cluster=float(cluster),
            total=float(round(total, 2)),
        )
        self._log(result)
        return result

    def score_for_type(
        self,
        topic_id: str,
        *,
        content_type: str,
        freshness: float = 0.0,
        trend_score: float = 0.0,
        search: float = 0.0,
        ctr: float = 0.0,
        position: float = 0.0,
        cluster: float = 0.0,
        relevance: float = 0.0,
        durability: float = 0.0,
        search_demand: float = 0.0,
        usefulness: float = 0.0,
        comparison_potential: float = 0.0,
        tutorial_potential: float = 0.0,
        evergreen_potential: float = 0.0,
        competition_inverse: float = 0.0,
    ) -> TopicScore:
        content_key = str(content_type or "hot").strip().lower() or "hot"
        fresh = self._cap(freshness)
        trend_val = self._cap(trend_score or freshness)
        search_val = self._cap(search)
        ctr_val = self._cap(ctr)
        position_val = self._cap(position)
        cluster_val = self._cap(cluster)
        relevance_val = self._cap(relevance)
        durability_val = self._cap(durability)
        search_demand_val = self._cap(search_demand or search)
        usefulness_val = self._cap(usefulness or relevance)
        comparison_val = self._cap(comparison_potential)
        tutorial_val = self._cap(tutorial_potential)
        evergreen_val = self._cap(evergreen_potential or durability)
        competition_val = self._cap(
            competition_inverse or ((0.55 * position_val) + (0.45 * max(0.0, 100.0 - ctr_val)))
        )

        if content_key == "search_derived":
            total = (
                (0.35 * search_demand_val)
                + (0.25 * usefulness_val)
                + (0.15 * max(comparison_val, tutorial_val))
                + (0.15 * competition_val)
                + (0.10 * fresh)
            )
        elif content_key == "evergreen":
            total = (
                (0.35 * evergreen_val)
                + (0.25 * search_demand_val)
                + (0.20 * usefulness_val)
                + (0.10 * competition_val)
                + (0.10 * fresh)
            )
        else:
            total = (
                (0.35 * trend_val)
                + (0.25 * fresh)
                + (0.20 * usefulness_val)
                + (0.10 * search_demand_val)
                + (0.10 * competition_val)
            )

        result = TopicScore(
            topic_id=str(topic_id or "").strip(),
            content_type=content_key,
            trend=float(round(trend_val, 2)),
            search=float(round(search_val, 2)),
            ctr=float(round(ctr_val, 2)),
            position=float(round(position_val, 2)),
            cluster=float(round(cluster_val, 2)),
            total=float(round(total, 2)),
            search_demand=float(round(search_demand_val, 2)),
            usefulness=float(round(usefulness_val, 2)),
            comparison_potential=float(round(comparison_val, 2)),
            tutorial_potential=float(round(tutorial_val, 2)),
            evergreen_potential=float(round(evergreen_val, 2)),
            competition_inverse=float(round(competition_val, 2)),
            freshness=float(round(fresh, 2)),
        )
        self._log(result)
        return result

    @staticmethod
    def _cap(value: float) -> float:
        try:
            numeric = float(value)
        except Exception:
            numeric = 0.0
        return max(0.0, min(100.0, numeric))

    def _log(self, result: TopicScore) -> None:
        if self.log_path is None:
            return
        row = {"ts_utc": datetime.now(timezone.utc).isoformat(), **asdict(result)}
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return
