from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.stores.db import connect_db


class CandidateStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).resolve()
        self._init_db()

    def _init_db(self) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS candidate_queue (candidate_id TEXT PRIMARY KEY, content_type TEXT NOT NULL, source_type TEXT NOT NULL, title TEXT NOT NULL, cluster_id TEXT NOT NULL, intent_family TEXT NOT NULL, priority REAL NOT NULL, status TEXT NOT NULL, created_at_utc TEXT NOT NULL, used_at_utc TEXT NOT NULL, payload_json TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS candidate_decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_id TEXT NOT NULL, decision TEXT NOT NULL, reason_code TEXT NOT NULL, created_at_utc TEXT NOT NULL, payload_json TEXT NOT NULL)")

    def enqueue_candidates(self, candidates: Iterable[Candidate], *, priority: float = 50.0, cluster_id: str = "", intent_family: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        with connect_db(self.db_path) as conn:
            for candidate in candidates:
                conn.execute("INSERT OR REPLACE INTO candidate_queue (candidate_id, content_type, source_type, title, cluster_id, intent_family, priority, status, created_at_utc, used_at_utc, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (candidate.candidate_id, candidate.content_type, candidate.source_type, candidate.title, str(cluster_id or candidate.raw_meta.get('cluster_id', '') or ''), str(intent_family or candidate.raw_meta.get('intent_family', '') or ''), float(priority), 'pending', now, '', json.dumps(asdict(candidate), ensure_ascii=False, default=str)))

    def get_pending(self, content_type: str, limit: int = 20) -> list[Candidate]:
        with connect_db(self.db_path) as conn:
            rows = conn.execute("SELECT payload_json FROM candidate_queue WHERE status='pending' AND content_type=? ORDER BY priority DESC, created_at_utc ASC LIMIT ?", (str(content_type or ''), max(1, int(limit)))).fetchall()
        out = []
        for row in rows:
            try:
                out.append(Candidate(**json.loads(str(row[0] or '{}'))))
            except Exception:
                continue
        return out

    def mark_used(self, candidate_id: str) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute("UPDATE candidate_queue SET status='used', used_at_utc=? WHERE candidate_id=?", (datetime.now(timezone.utc).isoformat(), str(candidate_id or '')))

    def record_decision(self, candidate_id: str, decision: str, reason_code: str, payload: dict | None = None) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute("INSERT INTO candidate_decisions (candidate_id, decision, reason_code, created_at_utc, payload_json) VALUES (?, ?, ?, ?, ?)", (str(candidate_id or ''), str(decision or ''), str(reason_code or ''), datetime.now(timezone.utc).isoformat(), json.dumps(payload or {}, ensure_ascii=False, default=str)))
