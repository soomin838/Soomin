from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class RunRecord:
    status: str
    score: int
    title: str
    source_url: str
    published_url: str
    note: str


class LogStore:
    def __init__(self, db_path: Path, json_log_path: Path) -> None:
        self.db_path = db_path
        self.json_log_path = json_log_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.json_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    score INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    published_url TEXT NOT NULL,
                    note TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS indexing_usage (
                    day TEXT PRIMARY KEY,
                    count INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gemini_usage (
                    day TEXT PRIMARY KEY,
                    count INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS content_fingerprints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    excerpt TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduled_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    publish_at TEXT NOT NULL,
                    post_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    published_url TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS excluded_posts (
                    post_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS url_index_audit (
                    url TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    index_notified_at TEXT NOT NULL DEFAULT '',
                    inspection_checked_at TEXT NOT NULL DEFAULT '',
                    inspection_verdict TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT ''
                )
                """
            )

    def append_run(self, record: RunRecord) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (created_at, status, score, title, source_url, published_url, note)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    record.status,
                    record.score,
                    record.title,
                    record.source_url,
                    record.published_url,
                    record.note,
                ),
            )

        event = {
            "created_at": now,
            "status": record.status,
            "score": record.score,
            "title": record.title,
            "source_url": record.source_url,
            "published_url": record.published_url,
            "note": record.note,
        }
        with self.json_log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=True) + "\n")

    def get_today_indexing_count(self) -> int:
        day = datetime.now(timezone.utc).date().isoformat()
        return self._get_daily_counter("indexing_usage", day)

    def increment_today_indexing_count(self, amount: int = 1) -> int:
        day = datetime.now(timezone.utc).date().isoformat()
        return self._increment_daily_counter("indexing_usage", day, amount)

    def get_today_gemini_count(self) -> int:
        day = datetime.now(timezone.utc).date().isoformat()
        return self._get_daily_counter("gemini_usage", day)

    def get_today_index_notified_count(self) -> int:
        day = datetime.now(timezone.utc).date().isoformat()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM url_index_audit
                WHERE index_notified_at != ''
                  AND substr(index_notified_at, 1, 10) = ?
                """,
                (day,),
            ).fetchone()
        return int(row[0]) if row else 0

    def get_today_inspection_checked_count(self) -> int:
        day = datetime.now(timezone.utc).date().isoformat()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM url_index_audit
                WHERE inspection_checked_at != ''
                  AND substr(inspection_checked_at, 1, 10) = ?
                """,
                (day,),
            ).fetchone()
        return int(row[0]) if row else 0

    def increment_today_gemini_count(self, amount: int = 1) -> int:
        day = datetime.now(timezone.utc).date().isoformat()
        return self._increment_daily_counter("gemini_usage", day, amount)

    def get_today_success_posts(self) -> int:
        day_prefix = datetime.now(timezone.utc).date().isoformat()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM runs
                WHERE status = 'success'
                  AND substr(created_at, 1, 10) = ?
                  AND instr(lower(note), 'manual_excluded=true') = 0
                """,
                (day_prefix,),
            ).fetchone()
        return int(row[0]) if row else 0

    def get_recent_runs(
        self,
        days: int = 3,
        limit: int = 50,
        statuses: list[str] | None = None,
    ) -> list[dict]:
        safe_days = max(1, int(days))
        safe_limit = max(1, int(limit))
        status_rows = [str(s or "").strip() for s in (statuses or []) if str(s or "").strip()]
        where_sql = "created_at >= datetime('now', ?)"
        params: list[object] = [f"-{safe_days} days"]
        if status_rows:
            holders = ",".join("?" for _ in status_rows)
            where_sql += f" AND lower(status) IN ({holders})"
            params.extend([s.lower() for s in status_rows])
        params.append(safe_limit)
        query = (
            "SELECT id, created_at, status, score, title, source_url, published_url, note "
            "FROM runs "
            f"WHERE {where_sql} "
            "ORDER BY id DESC "
            "LIMIT ?"
        )
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "created_at": str(r[1] or ""),
                    "status": str(r[2] or ""),
                    "score": int(r[3] or 0),
                    "title": str(r[4] or ""),
                    "source_url": str(r[5] or ""),
                    "published_url": str(r[6] or ""),
                    "note": str(r[7] or ""),
                }
            )
        return out

    def get_recent_topic_history(self, days: int = 14, limit: int = 300) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT title, source_url, created_at
                FROM runs
                WHERE source_url != ''
                  AND created_at >= datetime('now', ?)
                ORDER BY id DESC
                LIMIT ?
                """,
                (f"-{int(days)} days", int(limit)),
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append({"title": str(r[0] or ""), "source_url": str(r[1] or ""), "created_at": str(r[2] or "")})
        return out

    def get_recent_published_posts(self, days: int = 90, limit: int = 200) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT title, published_url, created_at
                FROM runs
                WHERE status = 'success'
                  AND published_url != ''
                  AND published_url != 'dry-run://not-published'
                  AND created_at >= datetime('now', ?)
                ORDER BY id DESC
                LIMIT ?
                """,
                (f"-{int(days)} days", int(limit)),
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append({"title": str(r[0] or ""), "published_url": str(r[1] or ""), "created_at": str(r[2] or "")})
        return out

    def add_content_fingerprint(self, title: str, source_url: str, excerpt: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO content_fingerprints (created_at, title, source_url, excerpt)
                VALUES (?, ?, ?, ?)
                """,
                (now, title, source_url, excerpt),
            )

    def get_recent_content_fingerprints(self, days: int = 30, limit: int = 200) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT title, source_url, excerpt, created_at
                FROM content_fingerprints
                WHERE created_at >= datetime('now', ?)
                ORDER BY id DESC
                LIMIT ?
                """,
                (f"-{int(days)} days", int(limit)),
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "title": str(r[0] or ""),
                    "source_url": str(r[1] or ""),
                    "excerpt": str(r[2] or ""),
                    "created_at": str(r[3] or ""),
                }
            )
        return out

    def add_scheduled_post(
        self,
        publish_at: str,
        post_id: str,
        title: str,
        source_url: str,
        published_url: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO scheduled_queue (created_at, publish_at, post_id, title, source_url, published_url)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (now, publish_at, post_id, title, source_url, published_url),
            )

    def add_excluded_post(self, post_id: str, reason: str = "manual_trigger") -> None:
        key = str(post_id or "").strip()
        if not key:
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO excluded_posts (post_id, created_at, reason)
                VALUES (?, ?, ?)
                """,
                (key, now, str(reason or "manual_trigger")),
            )

    def get_excluded_post_ids(self, days: int = 14) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT post_id
                FROM excluded_posts
                WHERE created_at >= datetime('now', ?)
                """,
                (f"-{max(1, int(days))} days",),
            ).fetchall()
        out: set[str] = set()
        for r in rows:
            key = str((r[0] if r else "") or "").strip()
            if key:
                out.add(key)
        return out

    def purge_expired_scheduled(self, now_iso: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM scheduled_queue WHERE publish_at <= ?",
                (now_iso,),
            )
            return int(cur.rowcount or 0)

    def count_scheduled_in_window(self, start_iso: str, end_iso: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM scheduled_queue
                WHERE publish_at > ? AND publish_at <= ?
                """,
                (start_iso, end_iso),
            ).fetchone()
        return int(row[0]) if row else 0

    def list_scheduled_in_window(self, start_iso: str, end_iso: str, limit: int = 500) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT publish_at, post_id, title, source_url, published_url
                FROM scheduled_queue
                WHERE publish_at > ? AND publish_at <= ?
                ORDER BY publish_at ASC
                LIMIT ?
                """,
                (start_iso, end_iso, int(limit)),
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "publish_at": str(r[0] or ""),
                    "post_id": str(r[1] or ""),
                    "title": str(r[2] or ""),
                    "source_url": str(r[3] or ""),
                    "published_url": str(r[4] or ""),
                }
            )
        return out

    def touch_index_audit_url(self, url: str) -> None:
        key = str(url or "").strip()
        if not key:
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO url_index_audit (
                    url, first_seen_at, last_seen_at, index_notified_at,
                    inspection_checked_at, inspection_verdict, last_error
                ) VALUES (?, ?, ?, '', '', '', '')
                ON CONFLICT(url) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at
                """,
                (key, now, now),
            )

    def get_index_audit(self, url: str) -> dict:
        key = str(url or "").strip()
        if not key:
            return {}
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT first_seen_at, last_seen_at, index_notified_at,
                       inspection_checked_at, inspection_verdict, last_error
                FROM url_index_audit
                WHERE url = ?
                """,
                (key,),
            ).fetchone()
        if not row:
            return {}
        return {
            "url": key,
            "first_seen_at": str(row[0] or ""),
            "last_seen_at": str(row[1] or ""),
            "index_notified_at": str(row[2] or ""),
            "inspection_checked_at": str(row[3] or ""),
            "inspection_verdict": str(row[4] or ""),
            "last_error": str(row[5] or ""),
        }

    def mark_index_notified(self, url: str) -> None:
        key = str(url or "").strip()
        if not key:
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO url_index_audit (
                    url, first_seen_at, last_seen_at, index_notified_at,
                    inspection_checked_at, inspection_verdict, last_error
                ) VALUES (?, ?, ?, ?, '', '', '')
                ON CONFLICT(url) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at,
                    index_notified_at=excluded.index_notified_at,
                    last_error=''
                """,
                (key, now, now, now),
            )

    def mark_inspection_checked(self, url: str, verdict: str = "") -> None:
        key = str(url or "").strip()
        if not key:
            return
        now = datetime.now(timezone.utc).isoformat()
        safe_verdict = str(verdict or "").strip()[:240]
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO url_index_audit (
                    url, first_seen_at, last_seen_at, index_notified_at,
                    inspection_checked_at, inspection_verdict, last_error
                ) VALUES (?, ?, ?, '', ?, ?, '')
                ON CONFLICT(url) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at,
                    inspection_checked_at=excluded.inspection_checked_at,
                    inspection_verdict=excluded.inspection_verdict,
                    last_error=''
                """,
                (key, now, now, now, safe_verdict),
            )

    def mark_index_audit_error(self, url: str, error: str) -> None:
        key = str(url or "").strip()
        if not key:
            return
        now = datetime.now(timezone.utc).isoformat()
        safe_err = str(error or "").strip()[:300]
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO url_index_audit (
                    url, first_seen_at, last_seen_at, index_notified_at,
                    inspection_checked_at, inspection_verdict, last_error
                ) VALUES (?, ?, ?, '', '', '', ?)
                ON CONFLICT(url) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at,
                    last_error=excluded.last_error
                """,
                (key, now, now, safe_err),
            )

    def _get_daily_counter(self, table: str, day: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT count FROM {table} WHERE day = ?",
                (day,),
            ).fetchone()
        return int(row[0]) if row else 0

    def _increment_daily_counter(self, table: str, day: str, amount: int) -> int:
        with self._connect() as conn:
            current = conn.execute(
                f"SELECT count FROM {table} WHERE day = ?",
                (day,),
            ).fetchone()
            new_count = (int(current[0]) if current else 0) + amount
            conn.execute(
                f"INSERT OR REPLACE INTO {table}(day, count) VALUES(?, ?)",
                (day, new_count),
            )
        return new_count
