from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class SearchConsoleRow:
    date: str
    query: str
    page: str
    clicks: float
    impressions: float
    ctr: float
    position: float


@dataclass(frozen=True)
class DiscoveryOpportunity:
    action_type: str
    query: str
    page: str
    clicks: float
    impressions: float
    ctr: float
    position: float
    priority_score: float


class KeywordDiscovery:
    def __init__(
        self,
        *,
        db_path: Path,
        fetch_rows_callback: Callable[[str, str, tuple[str, ...], int, int], list[dict[str, Any]]] | None = None,
        log_path: Path | None = None,
        safety_filter: Any | None = None,
        cluster_resolver: Callable[[str], str] | None = None,
    ) -> None:
        self.db_path = Path(db_path).resolve()
        self.fetch_rows_callback = fetch_rows_callback
        self.log_path = Path(log_path).resolve() if log_path else None
        self.safety_filter = safety_filter
        self.cluster_resolver = cluster_resolver
        self.last_run_summary: dict[str, Any] = {}
        self._ensure_db()

    def run(self, *, start_date: str, end_date: str) -> list[DiscoveryOpportunity]:
        run_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc)
        fetched = 0
        created = 0
        opportunities: list[DiscoveryOpportunity] = []
        note = ""
        try:
            rows = []
            if self.fetch_rows_callback is not None:
                rows = self.fetch_rows_callback(start_date, end_date, ("query", "page"), 25000, 50000) or []
            normalized = [self._normalize_row(row) for row in rows]
            normalized = [row for row in normalized if row is not None]
            fetched = len(normalized)
            if normalized:
                self._store_rows(normalized)
                opportunities = self._discover_opportunities(normalized)
                self._store_opportunities(opportunities)
                created = len(opportunities)
            else:
                note = "no_rows"
        except Exception as exc:
            note = str(exc)[:200]
        finally:
            self.last_run_summary = {
                "run_id": run_id,
                "start_date": start_date,
                "end_date": end_date,
                "rows_fetched": int(fetched),
                "opportunities_created": int(created),
                "note": str(note or ""),
            }
            self._store_run(
                run_id=run_id,
                started_at=started,
                ended_at=datetime.now(timezone.utc),
                start_date=start_date,
                end_date=end_date,
                rows_fetched=fetched,
                opportunities_created=created,
                note=note,
            )
            self._log(
                event="keyword_discovery_run",
                payload={
                    "run_id": run_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "rows_fetched": fetched,
                    "opportunities_created": created,
                    "note": note,
                },
            )
        return opportunities

    def queued_supporting_candidates(self, limit: int = 5) -> list[DiscoveryOpportunity]:
        query = (
            "SELECT action_type, query, page, clicks, impressions, ctr, position, priority_score "
            "FROM keyword_opportunities "
            "WHERE status='queued' AND action_type='supporting_post' "
            "ORDER BY priority_score DESC, impressions DESC LIMIT ?"
        )
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(query, (max(1, int(limit)),)).fetchall()
        return [
            DiscoveryOpportunity(
                action_type=str(row[0] or ""),
                query=str(row[1] or ""),
                page=str(row[2] or ""),
                clicks=float(row[3] or 0.0),
                impressions=float(row[4] or 0.0),
                ctr=float(row[5] or 0.0),
                position=float(row[6] or 0.0),
                priority_score=float(row[7] or 0.0),
            )
            for row in rows
        ]

    def _ensure_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS search_console_rows (
                    date TEXT NOT NULL,
                    query TEXT NOT NULL,
                    page TEXT NOT NULL,
                    clicks REAL NOT NULL,
                    impressions REAL NOT NULL,
                    ctr REAL NOT NULL,
                    position REAL NOT NULL,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY (date, query, page)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS keyword_opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    query TEXT NOT NULL,
                    page TEXT NOT NULL,
                    clicks REAL NOT NULL,
                    impressions REAL NOT NULL,
                    ctr REAL NOT NULL,
                    position REAL NOT NULL,
                    priority_score REAL NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS keyword_discovery_runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    rows_fetched INTEGER NOT NULL,
                    opportunities_created INTEGER NOT NULL,
                    note TEXT NOT NULL
                )
                """
            )

    def _normalize_row(self, row: dict[str, Any] | SearchConsoleRow | None) -> SearchConsoleRow | None:
        if row is None:
            return None
        if isinstance(row, SearchConsoleRow):
            return row
        if not isinstance(row, dict):
            return None
        date = str(row.get("date", "") or "").strip()
        query = str(row.get("query", "") or "").strip()
        page = str(row.get("page", "") or "").strip()
        if not date or not query or not page:
            return None
        if self.safety_filter is not None:
            decision = self.safety_filter.evaluate(
                title=query,
                snippet="",
                body_excerpt="",
                category="search_discovery",
                route="keyword_discovery",
                source_url=page,
            )
            if not decision.allow:
                return None
        return SearchConsoleRow(
            date=date,
            query=query,
            page=page,
            clicks=float(row.get("clicks", 0.0) or 0.0),
            impressions=float(row.get("impressions", 0.0) or 0.0),
            ctr=float(row.get("ctr", 0.0) or 0.0),
            position=float(row.get("position", 0.0) or 0.0),
        )

    def _store_rows(self, rows: list[SearchConsoleRow]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO search_console_rows (date, query, page, clicks, impressions, ctr, position, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, query, page) DO UPDATE SET
                    clicks=excluded.clicks,
                    impressions=excluded.impressions,
                    ctr=excluded.ctr,
                    position=excluded.position,
                    fetched_at=excluded.fetched_at
                """,
                [
                    (row.date, row.query, row.page, row.clicks, row.impressions, row.ctr, row.position, now)
                    for row in rows
                ],
            )

    def _discover_opportunities(self, rows: list[SearchConsoleRow]) -> list[DiscoveryOpportunity]:
        out: list[DiscoveryOpportunity] = []
        for row in rows:
            if row.impressions < 50:
                continue
            actions: list[str] = []
            if 8 <= row.position <= 20 and row.ctr >= 0.01:
                actions.append("supporting_post")
            if row.impressions >= 200 and row.ctr < 0.01:
                actions.append("title_rewrite")
            if row.impressions >= 200 and row.clicks == 0:
                actions.append("intent_fix")
            for action_type in actions:
                out.append(
                    DiscoveryOpportunity(
                        action_type=action_type,
                        query=row.query,
                        page=row.page,
                        clicks=row.clicks,
                        impressions=row.impressions,
                        ctr=row.ctr,
                        position=row.position,
                        priority_score=self._priority_score(row, action_type),
                    )
                )
        out.sort(key=lambda item: (item.priority_score, item.impressions, -item.position), reverse=True)
        return out

    def _priority_score(self, row: SearchConsoleRow, action_type: str) -> float:
        impression_score = min(100.0, float(row.impressions) / 20.0)
        top10_proximity = max(0.0, 100.0 - abs(float(row.position) - 10.0) * 8.0)
        ctr_headroom = max(0.0, min(100.0, (0.05 - float(row.ctr)) * 2000.0))
        cluster_fit = 0.0
        if self.cluster_resolver is not None:
            cluster = str(self.cluster_resolver(row.query) or "").strip().lower()
            cluster_fit = 30.0 if cluster and cluster != "default" else 0.0
        type_boost = {"supporting_post": 10.0, "title_rewrite": 6.0, "intent_fix": 8.0}.get(action_type, 0.0)
        return round((0.45 * impression_score) + (0.25 * top10_proximity) + (0.20 * ctr_headroom) + (0.10 * cluster_fit) + type_boost, 2)

    def _store_opportunities(self, opportunities: list[DiscoveryOpportunity]) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO keyword_opportunities (
                    created_at, action_type, query, page, clicks, impressions, ctr, position, priority_score, status, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        created_at,
                        item.action_type,
                        item.query,
                        item.page,
                        item.clicks,
                        item.impressions,
                        item.ctr,
                        item.position,
                        item.priority_score,
                        "queued",
                        json.dumps(asdict(item), ensure_ascii=False),
                    )
                    for item in opportunities
                ],
            )

    def _store_run(
        self,
        *,
        run_id: str,
        started_at: datetime,
        ended_at: datetime,
        start_date: str,
        end_date: str,
        rows_fetched: int,
        opportunities_created: int,
        note: str,
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO keyword_discovery_runs (
                    run_id, started_at, ended_at, start_date, end_date, rows_fetched, opportunities_created, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    started_at.isoformat(),
                    ended_at.isoformat(),
                    start_date,
                    end_date,
                    int(rows_fetched),
                    int(opportunities_created),
                    str(note or ""),
                ),
            )

    def _log(self, *, event: str, payload: dict[str, Any]) -> None:
        if self.log_path is None:
            return
        row = {"ts_utc": datetime.now(timezone.utc).isoformat(), "event": event, **dict(payload or {})}
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return
