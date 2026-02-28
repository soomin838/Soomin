from __future__ import annotations

import hashlib
import random
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    return (dt or _utc_now()).astimezone(timezone.utc).isoformat()


def _parse_iso(value: str) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


@dataclass
class NewsPoolItem:
    id: int
    url: str
    title: str
    source: str
    published_at: str
    snippet: str
    category: str
    status: str
    score: int
    claimed_at: str
    used_at: str
    published_url: str
    created_at: str
    topic_fp: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": int(self.id),
            "url": str(self.url or ""),
            "title": str(self.title or ""),
            "source": str(self.source or ""),
            "published_at": str(self.published_at or ""),
            "snippet": str(self.snippet or ""),
            "category": str(self.category or ""),
            "status": str(self.status or ""),
            "score": int(self.score or 0),
            "claimed_at": str(self.claimed_at or ""),
            "used_at": str(self.used_at or ""),
            "published_url": str(self.published_url or ""),
            "created_at": str(self.created_at or ""),
            "topic_fp": str(self.topic_fp or ""),
        }


class NewsPoolStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS news_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    published_at TEXT NOT NULL DEFAULT '',
                    snippet TEXT NOT NULL DEFAULT '',
                    category TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'queued',
                    score INTEGER NOT NULL DEFAULT 0,
                    claimed_at TEXT NOT NULL DEFAULT '',
                    used_at TEXT NOT NULL DEFAULT '',
                    published_url TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT '',
                    topic_fp TEXT NOT NULL DEFAULT ''
                )
                """
            )
            cols = {
                str(r[1] or "").strip().lower()
                for r in (conn.execute("PRAGMA table_info(news_items)").fetchall() or [])
            }
            if "topic_fp" not in cols:
                conn.execute("ALTER TABLE news_items ADD COLUMN topic_fp TEXT NOT NULL DEFAULT ''")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_status_score ON news_items(status, score DESC, published_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_published_at ON news_items(published_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_used_at ON news_items(used_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_topic_fp_created ON news_items(topic_fp, created_at)")

    @staticmethod
    def _compute_topic_fp(title: str, snippet: str) -> str:
        raw = f"{str(title or '')} {str(snippet or '')}".lower()
        tokens = re.findall(r"[a-z0-9]{3,}", raw)
        if not tokens:
            return ""
        counts = Counter(tokens)
        top = [tok for tok, _ in sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:8]]
        payload = " ".join(top).strip()
        if not payload:
            return ""
        return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()

    def upsert_items(self, rows: list[dict[str, Any]]) -> int:
        added_or_updated = 0
        now = _iso()
        fp_cutoff = _iso(_utc_now() - timedelta(hours=72))
        with self._connect() as conn:
            for row in rows or []:
                url = str((row or {}).get("url", "") or "").strip()
                title = str((row or {}).get("title", "") or "").strip()
                if not url or not title:
                    continue
                source = str((row or {}).get("source", "") or (urlparse(url).netloc or "")).strip().lower()
                published_at = str((row or {}).get("published_at", "") or "").strip()
                snippet = str((row or {}).get("snippet", "") or "").strip()
                category = str((row or {}).get("category", "") or "").strip().lower()
                score = int((row or {}).get("score", 0) or 0)
                topic_fp = str((row or {}).get("topic_fp", "") or "").strip().lower()
                if not topic_fp:
                    topic_fp = self._compute_topic_fp(title, snippet)
                if topic_fp:
                    dup = conn.execute(
                        """
                        SELECT id
                        FROM news_items
                        WHERE topic_fp=?
                          AND url!=?
                          AND created_at!=''
                          AND created_at>=?
                        LIMIT 1
                        """,
                        (topic_fp, url, fp_cutoff),
                    ).fetchone()
                    if dup is not None:
                        continue
                conn.execute(
                    """
                    INSERT INTO news_items (
                        url, title, source, published_at, snippet, category, status, score, created_at, topic_fp
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title,
                        source=excluded.source,
                        published_at=excluded.published_at,
                        snippet=excluded.snippet,
                        category=excluded.category,
                        score=excluded.score,
                        topic_fp=excluded.topic_fp
                    WHERE news_items.status IN ('queued', 'claimed')
                    """,
                    (url, title, source, published_at, snippet, category, score, now, topic_fp),
                )
                added_or_updated += 1
        return int(added_or_updated)

    def queued_count(self, days: int = 7) -> int:
        cutoff = _iso(_utc_now() - timedelta(days=max(1, int(days))))
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM news_items
                WHERE status='queued'
                  AND (published_at='' OR published_at >= ?)
                """,
                (cutoff,),
            ).fetchone()
        return int(row[0] if row else 0)

    def _release_stale_claims(self, stale_minutes: int = 90) -> None:
        cutoff = _iso(_utc_now() - timedelta(minutes=max(10, int(stale_minutes))))
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE news_items
                SET status='queued', claimed_at=''
                WHERE status='claimed'
                  AND claimed_at != ''
                  AND claimed_at < ?
                """,
                (cutoff,),
            )

    def claim_one(
        self,
        *,
        news_pool_days: int = 7,
        top_k: int = 60,
        source_weights: dict[str, float] | None = None,
        avoid_category: str = "",
        recent_domains: list[str] | None = None,
    ) -> dict[str, Any] | None:
        self._release_stale_claims()
        safe_days = max(1, int(news_pool_days))
        safe_top_k = max(1, int(top_k))
        cutoff = _iso(_utc_now() - timedelta(days=safe_days))
        source_weights = {
            str(k or "").strip().lower(): float(v)
            for k, v in (source_weights or {}).items()
            if str(k or "").strip()
        }
        avoid_category_norm = str(avoid_category or "").strip().lower()
        recent_domain_list = [
            str(x or "").strip().lower()
            for x in (recent_domains or [])
            if str(x or "").strip()
        ][:6]

        for _ in range(2):
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                rows = conn.execute(
                    """
                    SELECT id, url, title, source, published_at, snippet, category, status, score, claimed_at, used_at, published_url, created_at, topic_fp
                    FROM news_items
                    WHERE status='queued'
                      AND (published_at='' OR published_at >= ?)
                    ORDER BY score DESC, published_at DESC
                    LIMIT ?
                    """,
                    (cutoff, safe_top_k),
                ).fetchall()
                if not rows:
                    conn.execute("COMMIT")
                    return None

                now = _utc_now()
                weighted: list[tuple[float, sqlite3.Row]] = []
                for row in rows:
                    score = max(1.0, float(row["score"] or 0.0))
                    published = _parse_iso(str(row["published_at"] or ""))
                    if published is None:
                        recency_factor = 0.55
                    else:
                        age_h = max(0.0, (now - published).total_seconds() / 3600.0)
                        recency_factor = max(0.10, 1.25 - min(1.15, age_h / 72.0))
                    src = str(row["source"] or "").strip().lower()
                    src_weight = float(source_weights.get(src, 1.0))
                    category_penalty = 1.0
                    row_category = str(row["category"] or "").strip().lower()
                    if avoid_category_norm and row_category and row_category == avoid_category_norm:
                        category_penalty = 0.35
                    domain_penalty = 1.0
                    if src and src in recent_domain_list:
                        idx = recent_domain_list.index(src)
                        domain_penalty = max(0.35, 1.0 - ((len(recent_domain_list) - idx) * 0.08))
                    weighted_score = max(
                        1.0,
                        score * recency_factor * max(0.2, src_weight) * category_penalty * domain_penalty,
                    )
                    weighted.append((weighted_score, row))

                choice_rows = [r for _, r in weighted]
                choice_weights = [w for w, _ in weighted]
                selected = random.choices(choice_rows, weights=choice_weights, k=1)[0]
                selected_id = int(selected["id"])
                changed = conn.execute(
                    """
                    UPDATE news_items
                    SET status='claimed', claimed_at=?
                    WHERE id=? AND status='queued'
                    """,
                    (_iso(), selected_id),
                ).rowcount
                conn.execute("COMMIT")
                if changed == 1:
                    return self.get_by_id(selected_id)
        return None

    def rollback_claim(self, item_id: int) -> bool:
        with self._connect() as conn:
            changed = conn.execute(
                """
                UPDATE news_items
                SET status='queued', claimed_at=''
                WHERE id=? AND status='claimed'
                """,
                (int(item_id),),
            ).rowcount
        return bool(changed)

    def mark_used(self, item_id: int, published_url: str) -> bool:
        with self._connect() as conn:
            changed = conn.execute(
                """
                UPDATE news_items
                SET status='used', used_at=?, published_url=?
                WHERE id=?
                """,
                (_iso(), str(published_url or "").strip(), int(item_id)),
            ).rowcount
        return bool(changed)

    def purge(
        self,
        *,
        news_pool_days: int,
        keep_used_days: int,
        max_items: int,
    ) -> dict[str, int]:
        cutoff_pool = _iso(_utc_now() - timedelta(days=max(1, int(news_pool_days) + 1)))
        cutoff_used = _iso(_utc_now() - timedelta(days=max(1, int(keep_used_days))))
        removed_old = 0
        removed_used = 0
        removed_cap = 0
        with self._connect() as conn:
            removed_old = conn.execute(
                """
                DELETE FROM news_items
                WHERE status IN ('queued', 'claimed')
                  AND published_at != ''
                  AND published_at < ?
                """,
                (cutoff_pool,),
            ).rowcount
            removed_used = conn.execute(
                """
                DELETE FROM news_items
                WHERE status='used'
                  AND used_at != ''
                  AND used_at < ?
                """,
                (cutoff_used,),
            ).rowcount

            row = conn.execute("SELECT COUNT(*) FROM news_items").fetchone()
            total = int(row[0] if row else 0)
            cap = max(100, int(max_items))
            overflow = max(0, total - cap)
            if overflow > 0:
                remove_ids = conn.execute(
                    """
                    SELECT id
                    FROM news_items
                    WHERE status='queued'
                    ORDER BY published_at ASC, id ASC
                    LIMIT ?
                    """,
                    (overflow,),
                ).fetchall()
                ids = [int(r[0]) for r in remove_ids]
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    removed_cap = conn.execute(
                        f"DELETE FROM news_items WHERE id IN ({placeholders})",
                        tuple(ids),
                    ).rowcount
        return {
            "removed_old": int(removed_old),
            "removed_used": int(removed_used),
            "removed_cap": int(removed_cap),
        }

    def get_by_id(self, item_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, url, title, source, published_at, snippet, category, status, score, claimed_at, used_at, published_url, created_at, topic_fp
                FROM news_items
                WHERE id=?
                LIMIT 1
                """,
                (int(item_id),),
            ).fetchone()
        if row is None:
            return None
        return NewsPoolItem(
            id=int(row["id"]),
            url=str(row["url"] or ""),
            title=str(row["title"] or ""),
            source=str(row["source"] or ""),
            published_at=str(row["published_at"] or ""),
            snippet=str(row["snippet"] or ""),
            category=str(row["category"] or ""),
            status=str(row["status"] or ""),
            score=int(row["score"] or 0),
            claimed_at=str(row["claimed_at"] or ""),
            used_at=str(row["used_at"] or ""),
            published_url=str(row["published_url"] or ""),
            created_at=str(row["created_at"] or ""),
            topic_fp=str(row["topic_fp"] or ""),
        ).as_dict()
