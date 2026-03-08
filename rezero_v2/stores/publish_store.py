from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rezero_v2.core.domain.publish_result import PublishArtifact
from rezero_v2.stores.db import connect_db


class PublishStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).resolve()
        self._init_db()

    def _init_db(self) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS published_posts (id INTEGER PRIMARY KEY AUTOINCREMENT, post_id TEXT NOT NULL, post_url TEXT NOT NULL, title TEXT NOT NULL, cluster_id TEXT NOT NULL, entity_blob TEXT NOT NULL, intent_family TEXT NOT NULL, content_type TEXT NOT NULL, status TEXT NOT NULL, published_at_utc TEXT NOT NULL, payload_json TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS daily_mix_counters (day TEXT PRIMARY KEY, hot_count INTEGER NOT NULL, search_derived_count INTEGER NOT NULL, evergreen_count INTEGER NOT NULL, updated_at_utc TEXT NOT NULL)")

    def get_daily_counts(self, day: str) -> dict[str, int]:
        with connect_db(self.db_path) as conn:
            row = conn.execute("SELECT hot_count, search_derived_count, evergreen_count FROM daily_mix_counters WHERE day=?", (str(day or ''),)).fetchone()
        if row is None:
            return {'hot': 0, 'search_derived': 0, 'evergreen': 0}
        return {'hot': int(row[0] or 0), 'search_derived': int(row[1] or 0), 'evergreen': int(row[2] or 0)}

    def record_publish(self, artifact: PublishArtifact, *, title: str, cluster_id: str, entity_terms: list[str], intent_family: str, content_type: str, day: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with connect_db(self.db_path) as conn:
            conn.execute("INSERT INTO published_posts (post_id, post_url, title, cluster_id, entity_blob, intent_family, content_type, status, published_at_utc, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (artifact.post_id, artifact.post_url, str(title or ''), str(cluster_id or ''), json.dumps(list(entity_terms or []), ensure_ascii=False), str(intent_family or ''), str(content_type or ''), artifact.status, artifact.published_at_utc or now, json.dumps({'status': artifact.status, 'post_id': artifact.post_id, 'post_url': artifact.post_url, 'internal_links_added': artifact.internal_links_added, 'source_links_kept': artifact.source_links_kept}, ensure_ascii=False, default=str)))
            counts = self.get_daily_counts(day)
            counts[str(content_type or 'hot')] = counts.get(str(content_type or 'hot'), 0) + 1
            conn.execute("INSERT OR REPLACE INTO daily_mix_counters (day, hot_count, search_derived_count, evergreen_count, updated_at_utc) VALUES (?, ?, ?, ?, ?)", (str(day or ''), counts.get('hot', 0), counts.get('search_derived', 0), counts.get('evergreen', 0), now))

    def list_recent_posts(self, limit: int = 20) -> list[dict[str, Any]]:
        with connect_db(self.db_path) as conn:
            rows = conn.execute("SELECT title, post_url, cluster_id, intent_family, content_type, status, published_at_utc, payload_json FROM published_posts ORDER BY id DESC LIMIT ?", (max(1, int(limit)),)).fetchall()
        out = []
        for row in rows:
            try:
                payload = json.loads(str(row[7] or '{}'))
            except Exception:
                payload = {}
            out.append({'title': str(row[0] or ''), 'post_url': str(row[1] or ''), 'cluster_id': str(row[2] or ''), 'intent_family': str(row[3] or ''), 'content_type': str(row[4] or ''), 'status': str(row[5] or ''), 'published_at_utc': str(row[6] or ''), 'payload': payload})
        return out
