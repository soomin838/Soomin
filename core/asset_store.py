from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass
class KeywordRow:
    keyword: str
    device_type: str
    cluster_id: str
    source: str
    priority_score: float
    difficulty_score: float
    used_at: str | None
    created_at: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_keyword(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9\-\s]", " ", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _token_set(value: str) -> set[str]:
    toks = re.findall(r"[a-z0-9][a-z0-9\-]{1,}", _norm_keyword(value))
    return {t for t in toks if len(t) >= 2}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    uni = len(a | b)
    if uni == 0:
        return 0.0
    return inter / uni


class KeywordAssetStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS keywords (
                    keyword TEXT PRIMARY KEY,
                    device_type TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    priority_score REAL DEFAULT 0.0,
                    difficulty_score REAL DEFAULT 0.0,
                    used_at TEXT DEFAULT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_keywords_device_used
                ON keywords(device_type, used_at)
                """
            )

    def available_count(self, device_type: str, avoid_reuse_days: int) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, int(avoid_reuse_days)))).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM keywords
                WHERE lower(device_type)=lower(?)
                  AND (used_at IS NULL OR used_at <= ?)
                """,
                (str(device_type or ""), cutoff),
            ).fetchone()
        return int(row[0]) if row else 0

    def upsert_keywords(
        self,
        device_type: str,
        rows: Iterable[tuple[str, str, str, float, float]],
        dedupe_jaccard_threshold: float = 0.85,
    ) -> int:
        """
        rows: (keyword, cluster_id, source, priority_score, difficulty_score)
        """
        candidates: list[tuple[str, str, str, float, float]] = []
        for keyword, cluster_id, source, priority_score, difficulty_score in rows:
            kw = _norm_keyword(keyword)
            if not kw:
                continue
            if len(kw.split()) < 2:
                continue
            candidates.append(
                (
                    kw,
                    str(cluster_id or "general"),
                    str(source or "templates"),
                    float(priority_score or 0.0),
                    float(difficulty_score or 0.0),
                )
            )
        if not candidates:
            return 0

        inserted = 0
        with self._connect() as conn:
            existing_rows = conn.execute(
                "SELECT keyword FROM keywords WHERE lower(device_type)=lower(?) ORDER BY created_at DESC LIMIT 2000",
                (str(device_type or ""),),
            ).fetchall()
            existing_kw = [str((r[0] if r else "") or "") for r in existing_rows]
            existing_sets = [_token_set(k) for k in existing_kw]
            now = _utc_now_iso()

            for kw, cluster_id, source, priority, difficulty in candidates:
                kw_set = _token_set(kw)
                if not kw_set:
                    continue
                near = False
                for old_set in existing_sets:
                    if _jaccard(kw_set, old_set) >= float(dedupe_jaccard_threshold):
                        near = True
                        break
                if near:
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO keywords(
                        keyword, device_type, cluster_id, source, priority_score, difficulty_score, used_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
                    """,
                    (kw, str(device_type or ""), cluster_id, source, float(priority), float(difficulty), now),
                )
                existing_sets.append(kw_set)
                inserted += 1
        return inserted

    def pick_keywords(
        self,
        device_type: str,
        limit: int,
        avoid_reuse_days: int,
    ) -> list[str]:
        cap = max(1, int(limit))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, int(avoid_reuse_days)))).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT keyword
                FROM keywords
                WHERE lower(device_type)=lower(?)
                  AND (used_at IS NULL OR used_at <= ?)
                ORDER BY priority_score DESC, difficulty_score ASC, created_at DESC
                LIMIT ?
                """,
                (str(device_type or ""), cutoff, cap),
            ).fetchall()
            out = [str((r[0] if r else "") or "").strip() for r in rows if str((r[0] if r else "") or "").strip()]
            if out:
                now = _utc_now_iso()
                conn.executemany(
                    "UPDATE keywords SET used_at=? WHERE keyword=?",
                    [(now, kw) for kw in out],
                )
        return out


class PostsIndexStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posts (
                    post_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    focus_keywords TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    device_type TEXT NOT NULL,
                    word_count INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'live',
                    deleted_at TEXT DEFAULT NULL,
                    last_seen_at TEXT DEFAULT NULL,
                    source TEXT NOT NULL DEFAULT 'blogger'
                )
                """
            )
            self._migrate_posts_schema(conn)
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_posts_device_cluster
                ON posts(device_type, cluster_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_posts_published_at
                ON posts(published_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_posts_status_deleted
                ON posts(status, deleted_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_posts_last_seen
                ON posts(last_seen_at)
                """
            )

    def _migrate_posts_schema(self, conn: sqlite3.Connection) -> None:
        cols = {str(r[1]).strip().lower() for r in conn.execute("PRAGMA table_info(posts)").fetchall()}
        if "status" not in cols:
            conn.execute("ALTER TABLE posts ADD COLUMN status TEXT NOT NULL DEFAULT 'live'")
        if "deleted_at" not in cols:
            conn.execute("ALTER TABLE posts ADD COLUMN deleted_at TEXT DEFAULT NULL")
        if "last_seen_at" not in cols:
            conn.execute("ALTER TABLE posts ADD COLUMN last_seen_at TEXT DEFAULT NULL")
        if "source" not in cols:
            conn.execute("ALTER TABLE posts ADD COLUMN source TEXT NOT NULL DEFAULT 'blogger'")
        conn.execute("UPDATE posts SET status='live' WHERE status IS NULL OR trim(status)=''")
        conn.execute("UPDATE posts SET source='blogger' WHERE source IS NULL OR trim(source)=''")
        conn.execute("UPDATE posts SET last_seen_at=COALESCE(last_seen_at, published_at)")

    def upsert_post(
        self,
        post_id: str,
        url: str,
        title: str,
        published_at: str,
        summary: str = "",
        focus_keywords: list[str] | str = "",
        cluster_id: str = "general",
        device_type: str = "windows",
        word_count: int = 0,
        status: str = "live",
        deleted_at: str | None = None,
        last_seen_at: str | None = None,
        source: str = "blogger",
    ) -> None:
        key = str(post_id or "").strip()
        if not key:
            key = str(url or "").strip()
        if not key:
            return
        now_iso = _utc_now_iso()
        kw_text = (
            ",".join([str(k).strip() for k in (focus_keywords or []) if str(k).strip()])
            if isinstance(focus_keywords, list)
            else str(focus_keywords or "")
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO posts(
                    post_id, url, title, published_at, summary, focus_keywords, cluster_id, device_type, word_count,
                    status, deleted_at, last_seen_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id) DO UPDATE SET
                    url=excluded.url,
                    title=excluded.title,
                    published_at=excluded.published_at,
                    summary=excluded.summary,
                    focus_keywords=excluded.focus_keywords,
                    cluster_id=excluded.cluster_id,
                    device_type=excluded.device_type,
                    word_count=excluded.word_count,
                    status=excluded.status,
                    deleted_at=excluded.deleted_at,
                    last_seen_at=excluded.last_seen_at,
                    source=excluded.source
                """,
                (
                    key,
                    str(url or ""),
                    str(title or ""),
                    str(published_at or now_iso),
                    str(summary or ""),
                    str(kw_text or ""),
                    str(cluster_id or "general"),
                    str(device_type or "windows"),
                    int(max(0, int(word_count or 0))),
                    str(status or "live").strip().lower() or "live",
                    (str(deleted_at).strip() if deleted_at else None),
                    str(last_seen_at or now_iso),
                    str(source or "blogger").strip().lower() or "blogger",
                ),
            )

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM posts").fetchone()
        return int(row[0]) if row else 0

    def query_recent(
        self,
        limit: int = 120,
        include_future: bool = False,
        statuses: list[str] | tuple[str, ...] | None = None,
        exclude_deleted: bool = True,
    ) -> list[dict]:
        cap = max(1, int(limit))
        now_iso = _utc_now_iso()
        requested = [str(s or "").strip().lower() for s in (statuses or ["live"]) if str(s or "").strip()]
        requested = [s for s in requested if s in {"live", "scheduled", "draft", "deleted"}]
        if not requested:
            requested = ["live"]
        placeholders = ",".join("?" for _ in requested)
        where = [f"status IN ({placeholders})", "url IS NOT NULL", "trim(url) <> ''"]
        params: list[Any] = list(requested)
        if exclude_deleted:
            where.append("deleted_at IS NULL")
        if not include_future:
            where.append("published_at <= ?")
            params.append(now_iso)
        where_sql = " AND ".join(where)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT post_id, url, title, published_at, summary, focus_keywords, cluster_id, device_type, word_count, status, deleted_at, last_seen_at, source
                FROM posts
                WHERE {where_sql}
                ORDER BY published_at DESC
                LIMIT ?
                """,
                tuple(params + [cap]),
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "post_id": str(r[0] or ""),
                    "url": str(r[1] or ""),
                    "title": str(r[2] or ""),
                    "published_at": str(r[3] or ""),
                    "summary": str(r[4] or ""),
                    "focus_keywords": str(r[5] or ""),
                    "cluster_id": str(r[6] or ""),
                    "device_type": str(r[7] or ""),
                    "word_count": int(r[8] or 0),
                    "status": str(r[9] or "live").strip().lower() or "live",
                    "deleted_at": str(r[10] or "").strip(),
                    "last_seen_at": str(r[11] or "").strip(),
                    "source": str(r[12] or "blogger").strip().lower() or "blogger",
                }
            )
        return out

    def fetch_all(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT post_id, url, title, published_at, summary, focus_keywords, cluster_id, device_type, word_count, status, deleted_at, last_seen_at, source
                FROM posts
                """
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "post_id": str(r[0] or ""),
                    "url": str(r[1] or ""),
                    "title": str(r[2] or ""),
                    "published_at": str(r[3] or ""),
                    "summary": str(r[4] or ""),
                    "focus_keywords": str(r[5] or ""),
                    "cluster_id": str(r[6] or ""),
                    "device_type": str(r[7] or ""),
                    "word_count": int(r[8] or 0),
                    "status": str(r[9] or "live").strip().lower() or "live",
                    "deleted_at": str(r[10] or "").strip(),
                    "last_seen_at": str(r[11] or "").strip(),
                    "source": str(r[12] or "blogger").strip().lower() or "blogger",
                }
            )
        return out

    def soft_delete_posts(self, post_ids: Iterable[str], deleted_at: str | None = None) -> int:
        ids = [str(x or "").strip() for x in (post_ids or []) if str(x or "").strip()]
        if not ids:
            return 0
        when = str(deleted_at or _utc_now_iso())
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE posts
                SET status='deleted', deleted_at=?, source=COALESCE(NULLIF(source, ''), 'blogger')
                WHERE post_id IN ({placeholders})
                  AND (deleted_at IS NULL OR deleted_at='')
                """,
                tuple([when] + ids),
            )
            return int(cur.rowcount or 0)

    def purge_deleted(self, purge_after_days: int = 7) -> int:
        days = max(1, int(purge_after_days or 1))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM posts
                WHERE status='deleted'
                  AND deleted_at IS NOT NULL
                  AND trim(deleted_at) <> ''
                  AND deleted_at < ?
                """,
                (cutoff,),
            )
            return int(cur.rowcount or 0)
