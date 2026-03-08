from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rezero_v2.core.domain.cluster import TopicCluster
from rezero_v2.stores.db import connect_db


class ClusterStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).resolve()
        self._init_db()

    def _init_db(self) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS clusters (cluster_id TEXT PRIMARY KEY, topic_label TEXT NOT NULL, payload_json TEXT NOT NULL, updated_at_utc TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS cluster_members (id INTEGER PRIMARY KEY AUTOINCREMENT, cluster_id TEXT NOT NULL, member_query TEXT NOT NULL, role TEXT NOT NULL, created_at_utc TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS internal_link_edges (id INTEGER PRIMARY KEY AUTOINCREMENT, from_cluster_id TEXT NOT NULL, to_post_url TEXT NOT NULL, score REAL NOT NULL, reason TEXT NOT NULL, created_at_utc TEXT NOT NULL)")

    def upsert_cluster(self, cluster: TopicCluster, role: str = 'supporting') -> None:
        now = datetime.now(timezone.utc).isoformat()
        with connect_db(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO clusters (cluster_id, topic_label, payload_json, updated_at_utc) VALUES (?, ?, ?, ?)", (cluster.cluster_id, cluster.topic_label, json.dumps(asdict(cluster), ensure_ascii=False, default=str), now))
            for query in cluster.member_queries:
                conn.execute("INSERT INTO cluster_members (cluster_id, member_query, role, created_at_utc) VALUES (?, ?, ?, ?)", (cluster.cluster_id, str(query or ''), str(role or 'supporting'), now))

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        with connect_db(self.db_path) as conn:
            row = conn.execute("SELECT payload_json FROM clusters WHERE cluster_id=?", (str(cluster_id or ''),)).fetchone()
        if row is None:
            return None
        try:
            return json.loads(str(row[0] or '{}'))
        except Exception:
            return None

    def remember_internal_link(self, from_cluster_id: str, to_post_url: str, score: float, reason: str) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute("INSERT INTO internal_link_edges (from_cluster_id, to_post_url, score, reason, created_at_utc) VALUES (?, ?, ?, ?, ?)", (str(from_cluster_id or ''), str(to_post_url or ''), float(score), str(reason or ''), datetime.now(timezone.utc).isoformat()))
