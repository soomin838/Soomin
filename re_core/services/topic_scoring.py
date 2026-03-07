from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class TopicScore:
    topic_id: str
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
            trend=float(trend),
            search=float(search),
            ctr=float(ctr),
            position=float(position),
            cluster=float(cluster),
            total=float(round(total, 2)),
        )
        self._log(result)
        return result

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
