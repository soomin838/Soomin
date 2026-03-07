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
        search: float = 0.0,
        ctr: float = 0.0,
        position: float = 0.0,
        cluster: float = 0.0,
        relevance: float = 0.0,
        durability: float = 0.0,
    ) -> TopicScore:
        content_key = str(content_type or "hot").strip().lower() or "hot"
        fresh = self._cap(freshness)
        search_val = self._cap(search)
        ctr_val = self._cap(ctr)
        position_val = self._cap(position)
        cluster_val = self._cap(cluster)
        relevance_val = self._cap(relevance)
        durability_val = self._cap(durability)

        if content_key == "search_derived":
            total = (
                (0.35 * search_val)
                + (0.25 * relevance_val)
                + (0.20 * ctr_val)
                + (0.10 * position_val)
                + (0.10 * cluster_val)
            )
            trend_val = relevance_val
        elif content_key == "evergreen":
            total = (
                (0.40 * durability_val)
                + (0.25 * search_val)
                + (0.15 * cluster_val)
                + (0.10 * ctr_val)
                + (0.10 * relevance_val)
            )
            trend_val = durability_val
        else:
            total = (
                (0.40 * fresh)
                + (0.20 * search_val)
                + (0.15 * relevance_val)
                + (0.15 * cluster_val)
                + (0.10 * ctr_val)
            )
            trend_val = fresh

        result = TopicScore(
            topic_id=str(topic_id or "").strip(),
            content_type=content_key,
            trend=float(round(trend_val, 2)),
            search=float(round(search_val, 2)),
            ctr=float(round(ctr_val, 2)),
            position=float(round(position_val, 2)),
            cluster=float(round(cluster_val, 2)),
            total=float(round(total, 2)),
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
