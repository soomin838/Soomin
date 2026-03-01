from __future__ import annotations

import hashlib
import random
import re
import sqlite3
import uuid
from collections import Counter
from contextlib import contextmanager
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


def _source_from_url(url: str) -> str:
    return str(urlparse(str(url or "").strip()).netloc or "").strip().lower()


@dataclass
class NewsPoolItem:
    id: str
    event_id: str
    event_fp: str
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
            "id": str(self.id or ""),
            "event_id": str(self.event_id or ""),
            "event_fp": str(self.event_fp or ""),
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

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS news_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL DEFAULT '',
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS news_events (
                    event_id TEXT PRIMARY KEY,
                    event_fp TEXT,
                    canonical_title TEXT,
                    canonical_snippet TEXT,
                    representative_url TEXT,
                    category TEXT,
                    first_seen_utc TEXT,
                    last_seen_utc TEXT,
                    status TEXT DEFAULT 'queued'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS publish_ledger (
                    event_id TEXT PRIMARY KEY,
                    post_url TEXT,
                    published_at_utc TEXT
                )
                """
            )

            news_cols = {
                str(r[1] or "").strip().lower()
                for r in (conn.execute("PRAGMA table_info(news_items)").fetchall() or [])
            }
            if "topic_fp" not in news_cols:
                conn.execute("ALTER TABLE news_items ADD COLUMN topic_fp TEXT NOT NULL DEFAULT ''")
            if "event_id" not in news_cols:
                conn.execute("ALTER TABLE news_items ADD COLUMN event_id TEXT NOT NULL DEFAULT ''")

            event_cols = {
                str(r[1] or "").strip().lower()
                for r in (conn.execute("PRAGMA table_info(news_events)").fetchall() or [])
            }
            if "claimed_at" not in event_cols:
                conn.execute("ALTER TABLE news_events ADD COLUMN claimed_at TEXT NOT NULL DEFAULT ''")

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_status_score ON news_items(status, score DESC, published_at DESC)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_published_at ON news_items(published_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_used_at ON news_items(used_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_topic_fp_created ON news_items(topic_fp, created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_items_event_id ON news_items(event_id)")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_events_fp_last_seen ON news_events(event_fp, last_seen_utc DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_events_status_last_seen ON news_events(status, last_seen_utc DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_publish_ledger_published_at ON publish_ledger(published_at_utc DESC)")

            self._backfill_events(conn)
            self._migrate_publish_ledger(conn)
            self._discard_stale_and_published(conn, now=_utc_now())

    @staticmethod
    def _top_tokens(text: str, limit: int) -> list[str]:
        raw = re.findall(r"[a-z0-9]{2,}", str(text or "").lower())
        if not raw:
            return []
        counts = Counter(raw)
        return [tok for tok, _ in sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[: max(1, int(limit))]]

    @classmethod
    def _compute_event_fp(cls, title: str, snippet: str) -> str:
        title_norm = " ".join(cls._top_tokens(title, 8)).strip()
        snippet_norm = " ".join(cls._top_tokens(snippet, 20)).strip()
        payload = f"{title_norm}|{snippet_norm}".strip("|")
        if not payload:
            return ""
        return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()

    @classmethod
    def _compute_topic_fp(cls, title: str, snippet: str) -> str:
        # Legacy alias for downstream code already reading topic_fp.
        return cls._compute_event_fp(title, snippet)

    @staticmethod
    def _make_event_id() -> str:
        return f"evt_{uuid.uuid4().hex[:20]}"

    def _resolve_or_create_event_record(
        self,
        conn: sqlite3.Connection,
        *,
        event_fp: str,
        title: str,
        snippet: str,
        url: str,
        category: str,
        now_iso: str,
        recent_cutoff_iso: str = "",
        allow_recent_window: bool = True,
        status_hint: str = "queued",
    ) -> str:
        if not event_fp:
            return ""
        params: tuple[Any, ...]
        sql = (
            """
            SELECT event_id
            FROM news_events
            WHERE event_fp=?
            ORDER BY last_seen_utc DESC
            LIMIT 1
            """
        )
        params = (event_fp,)
        if allow_recent_window and recent_cutoff_iso:
            sql = (
                """
                SELECT event_id
                FROM news_events
                WHERE event_fp=?
                  AND last_seen_utc >= ?
                ORDER BY last_seen_utc DESC
                LIMIT 1
                """
            )
            params = (event_fp, recent_cutoff_iso)
        row = conn.execute(sql, params).fetchone()
        if row is not None:
            event_id = str(row["event_id"] or "").strip()
            conn.execute(
                """
                UPDATE news_events
                SET last_seen_utc=?
                WHERE event_id=?
                """,
                (
                    now_iso,
                    event_id,
                ),
            )
            return event_id

        # Secondary clustering guard:
        # Same headline across different outlets often has slightly different snippets.
        if allow_recent_window and recent_cutoff_iso:
            title_key = " ".join(self._top_tokens(title, 8)).strip()
            if title_key:
                cands = conn.execute(
                    """
                    SELECT event_id, canonical_title
                    FROM news_events
                    WHERE last_seen_utc >= ?
                    ORDER BY last_seen_utc DESC
                    LIMIT 240
                    """,
                    (recent_cutoff_iso,),
                ).fetchall()
                for cand in cands:
                    cand_key = " ".join(self._top_tokens(str(cand["canonical_title"] or ""), 8)).strip()
                    if cand_key != title_key:
                        continue
                    event_id = str(cand["event_id"] or "").strip()
                    if not event_id:
                        continue
                    conn.execute(
                        """
                        UPDATE news_events
                        SET last_seen_utc=?
                        WHERE event_id=?
                        """,
                        (
                            now_iso,
                            event_id,
                        ),
                    )
                    return event_id

        event_id = self._make_event_id()
        status = str(status_hint or "queued").strip().lower()
        if status not in {"queued", "claimed", "used", "discarded"}:
            status = "queued"
        conn.execute(
            """
            INSERT INTO news_events (
                event_id, event_fp, canonical_title, canonical_snippet, representative_url,
                category, first_seen_utc, last_seen_utc, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                event_fp,
                str(title or "").strip(),
                str(snippet or "").strip(),
                str(url or "").strip(),
                str(category or "").strip().lower(),
                now_iso,
                now_iso,
                status,
            ),
        )
        return event_id

    def _backfill_events(self, conn: sqlite3.Connection) -> None:
        now_iso = _iso()
        rows = conn.execute(
            """
            SELECT id, event_id, url, title, snippet, category, topic_fp, status
            FROM news_items
            WHERE event_id='' OR event_id IS NULL
            ORDER BY id ASC
            """
        ).fetchall()
        for row in rows:
            url = str(row["url"] or "").strip()
            title = str(row["title"] or "").strip()
            snippet = str(row["snippet"] or "").strip()
            category = str(row["category"] or "").strip().lower()
            topic_fp = str(row["topic_fp"] or "").strip().lower() or self._compute_event_fp(title, snippet)
            if not topic_fp:
                continue
            event_id = self._resolve_or_create_event_record(
                conn,
                event_fp=topic_fp,
                title=title,
                snippet=snippet,
                url=url,
                category=category,
                now_iso=now_iso,
                allow_recent_window=False,
                status_hint=str(row["status"] or "queued"),
            )
            if not event_id:
                continue
            conn.execute(
                "UPDATE news_items SET event_id=?, topic_fp=? WHERE id=?",
                (event_id, topic_fp, int(row["id"])),
            )

    def _migrate_publish_ledger(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """
            SELECT DISTINCT event_id, published_url, COALESCE(NULLIF(used_at, ''), created_at, ?) AS published_at_utc
            FROM news_items
            WHERE status='used'
              AND event_id!=''
            """,
            (_iso(),),
        ).fetchall()
        for row in rows:
            event_id = str(row["event_id"] or "").strip()
            if not event_id:
                continue
            conn.execute(
                """
                INSERT INTO publish_ledger(event_id, post_url, published_at_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    post_url=COALESCE(NULLIF(excluded.post_url, ''), publish_ledger.post_url),
                    published_at_utc=COALESCE(NULLIF(excluded.published_at_utc, ''), publish_ledger.published_at_utc)
                """,
                (
                    event_id,
                    str(row["published_url"] or "").strip(),
                    str(row["published_at_utc"] or "").strip(),
                ),
            )
        conn.execute(
            """
            UPDATE news_events
            SET status='used'
            WHERE event_id IN (SELECT event_id FROM publish_ledger)
            """
        )

    def _release_stale_claims(self, conn: sqlite3.Connection, *, stale_minutes: int = 90) -> None:
        cutoff = _iso(_utc_now() - timedelta(minutes=max(10, int(stale_minutes))))
        conn.execute(
            """
            UPDATE news_events
            SET status='queued', claimed_at=''
            WHERE status='claimed'
              AND claimed_at!=''
              AND claimed_at < ?
            """,
            (cutoff,),
        )
        conn.execute(
            """
            UPDATE news_items
            SET status='queued', claimed_at=''
            WHERE status='claimed'
              AND claimed_at!=''
              AND claimed_at < ?
            """,
            (cutoff,),
        )

    def _discard_stale_and_published(self, conn: sqlite3.Connection, *, now: datetime | None = None) -> None:
        cutoff = _iso((now or _utc_now()) - timedelta(hours=72))
        conn.execute(
            """
            UPDATE news_events
            SET status='discarded', claimed_at=''
            WHERE status IN ('queued', 'claimed')
              AND (
                    event_id IN (SELECT event_id FROM publish_ledger)
                    OR (last_seen_utc!='' AND last_seen_utc < ?)
                  )
            """,
            (cutoff,),
        )

    def upsert_items(self, rows: list[dict[str, Any]]) -> int:
        added_or_updated = 0
        now_dt = _utc_now()
        now_iso = _iso(now_dt)
        fp_cutoff = _iso(now_dt - timedelta(hours=72))
        with self._connect() as conn:
            for row in rows or []:
                url = str((row or {}).get("url", "") or "").strip()
                title = str((row or {}).get("title", "") or "").strip()
                if not url or not title:
                    continue
                source = str((row or {}).get("source", "") or _source_from_url(url)).strip().lower()
                published_at = str((row or {}).get("published_at", "") or "").strip()
                snippet = str((row or {}).get("snippet", "") or "").strip()
                category = str((row or {}).get("category", "") or "").strip().lower()
                score = int((row or {}).get("score", 0) or 0)

                event_fp = str((row or {}).get("topic_fp", "") or "").strip().lower()
                if not event_fp:
                    event_fp = self._compute_event_fp(title, snippet)
                if not event_fp:
                    continue

                event_id = self._resolve_or_create_event_record(
                    conn,
                    event_fp=event_fp,
                    title=title,
                    snippet=snippet,
                    url=url,
                    category=category,
                    now_iso=now_iso,
                    recent_cutoff_iso=fp_cutoff,
                    allow_recent_window=True,
                    status_hint="queued",
                )
                if not event_id:
                    continue

                conn.execute(
                    """
                    INSERT INTO news_items (
                        event_id, url, title, source, published_at, snippet, category,
                        status, score, created_at, topic_fp
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        event_id=excluded.event_id,
                        title=excluded.title,
                        source=excluded.source,
                        published_at=excluded.published_at,
                        snippet=excluded.snippet,
                        category=excluded.category,
                        score=excluded.score,
                        topic_fp=excluded.topic_fp
                    WHERE news_items.status IN ('queued', 'claimed')
                    """,
                    (
                        event_id,
                        url,
                        title,
                        source,
                        published_at,
                        snippet,
                        category,
                        score,
                        now_iso,
                        event_fp,
                    ),
                )
                added_or_updated += 1
        return int(added_or_updated)

    def queued_count(self, days: int = 7) -> int:
        safe_days = max(1, int(days))
        # Hard rule in news mode claim path: events older than 72h are stale.
        cutoff = _iso(_utc_now() - timedelta(hours=min(72, safe_days * 24)))
        with self._connect() as conn:
            self._discard_stale_and_published(conn, now=_utc_now())
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM news_events
                WHERE status='queued'
                  AND event_id NOT IN (SELECT event_id FROM publish_ledger)
                  AND (last_seen_utc='' OR last_seen_utc >= ?)
                """,
                (cutoff,),
            ).fetchone()
        return int(row[0] if row else 0)

    def _resolve_event_id(self, conn: sqlite3.Connection, claim_id: Any) -> str:
        txt = str(claim_id or "").strip()
        if not txt:
            return ""
        direct = conn.execute(
            "SELECT event_id FROM news_events WHERE event_id=? LIMIT 1",
            (txt,),
        ).fetchone()
        if direct is not None:
            return txt
        if txt.isdigit():
            by_item = conn.execute(
                "SELECT event_id FROM news_items WHERE id=? LIMIT 1",
                (int(txt),),
            ).fetchone()
            if by_item is not None:
                return str(by_item["event_id"] or "").strip()
        return ""

    def claim_one(
        self,
        *,
        news_pool_days: int = 7,
        top_k: int = 60,
        source_weights: dict[str, float] | None = None,
        avoid_category: str = "",
        recent_domains: list[str] | None = None,
    ) -> dict[str, Any] | None:
        _ = max(1, int(news_pool_days))
        safe_top_k = max(1, int(top_k))
        cutoff_72h = _iso(_utc_now() - timedelta(hours=72))
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
                self._release_stale_claims(conn)
                self._discard_stale_and_published(conn, now=_utc_now())

                rows = conn.execute(
                    """
                    SELECT e.event_id, e.event_fp, e.canonical_title, e.canonical_snippet,
                           e.representative_url, e.category, e.first_seen_utc, e.last_seen_utc,
                           e.status, e.claimed_at, COALESCE(s.max_score, 70) AS score
                    FROM news_events e
                    LEFT JOIN (
                        SELECT event_id, MAX(score) AS max_score
                        FROM news_items
                        GROUP BY event_id
                    ) s ON s.event_id=e.event_id
                    WHERE e.status='queued'
                      AND e.event_id NOT IN (SELECT event_id FROM publish_ledger)
                      AND (e.last_seen_utc='' OR e.last_seen_utc >= ?)
                    ORDER BY e.last_seen_utc DESC
                    LIMIT ?
                    """,
                    (cutoff_72h, safe_top_k),
                ).fetchall()
                if not rows:
                    conn.execute("COMMIT")
                    return None

                now = _utc_now()
                weighted: list[tuple[float, sqlite3.Row]] = []
                for row in rows:
                    score = max(1.0, float(row["score"] or 0.0))
                    last_seen = _parse_iso(str(row["last_seen_utc"] or ""))
                    if last_seen is None:
                        recency_factor = 0.55
                    else:
                        age_h = max(0.0, (now - last_seen).total_seconds() / 3600.0)
                        recency_factor = max(0.10, 1.25 - min(1.15, age_h / 72.0))
                    src = _source_from_url(str(row["representative_url"] or ""))
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
                selected_event_id = str(selected["event_id"] or "").strip()
                if not selected_event_id:
                    conn.execute("COMMIT")
                    continue
                changed = conn.execute(
                    """
                    UPDATE news_events
                    SET status='claimed', claimed_at=?
                    WHERE event_id=? AND status='queued'
                    """,
                    (_iso(), selected_event_id),
                ).rowcount
                conn.execute(
                    """
                    UPDATE news_items
                    SET status='claimed', claimed_at=?
                    WHERE event_id=? AND status='queued'
                    """,
                    (_iso(), selected_event_id),
                )
                conn.execute("COMMIT")
                if changed == 1:
                    return self.get_by_event_id(selected_event_id)
        return None

    def rollback_claim(self, claim_id: Any) -> bool:
        with self._connect() as conn:
            event_id = self._resolve_event_id(conn, claim_id)
            if not event_id:
                return False
            changed = conn.execute(
                """
                UPDATE news_events
                SET status='queued', claimed_at=''
                WHERE event_id=? AND status='claimed'
                """,
                (event_id,),
            ).rowcount
            conn.execute(
                """
                UPDATE news_items
                SET status='queued', claimed_at=''
                WHERE event_id=? AND status='claimed'
                """,
                (event_id,),
            )
        return bool(changed)

    def mark_used(self, claim_id: Any, published_url: str) -> bool:
        with self._connect() as conn:
            event_id = self._resolve_event_id(conn, claim_id)
            if not event_id:
                return False
            now_iso = _iso()
            conn.execute(
                """
                INSERT INTO publish_ledger(event_id, post_url, published_at_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    post_url=excluded.post_url,
                    published_at_utc=excluded.published_at_utc
                """,
                (event_id, str(published_url or "").strip(), now_iso),
            )
            changed = conn.execute(
                """
                UPDATE news_events
                SET status='used'
                WHERE event_id=?
                """,
                (event_id,),
            ).rowcount
            conn.execute(
                """
                UPDATE news_items
                SET status='used', used_at=?, published_url=?
                WHERE event_id=? AND status IN ('queued', 'claimed', 'used')
                """,
                (now_iso, str(published_url or "").strip(), event_id),
            )
        return bool(changed)

    def mark_discarded(self, claim_id: Any, reason: str = "") -> bool:
        _ = str(reason or "").strip()
        with self._connect() as conn:
            event_id = self._resolve_event_id(conn, claim_id)
            if not event_id:
                return False
            changed = conn.execute(
                """
                UPDATE news_events
                SET status='discarded', claimed_at=''
                WHERE event_id=?
                """,
                (event_id,),
            ).rowcount
            conn.execute(
                """
                UPDATE news_items
                SET status='discarded', claimed_at=''
                WHERE event_id=? AND status IN ('queued', 'claimed')
                """,
                (event_id,),
            )
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
            self._discard_stale_and_published(conn, now=_utc_now())

            removed_old = conn.execute(
                """
                DELETE FROM news_events
                WHERE status='discarded'
                  AND last_seen_utc != ''
                  AND last_seen_utc < ?
                """,
                (cutoff_pool,),
            ).rowcount

            stale_used_rows = conn.execute(
                """
                SELECT event_id
                FROM publish_ledger
                WHERE published_at_utc != ''
                  AND published_at_utc < ?
                """,
                (cutoff_used,),
            ).fetchall()
            stale_used_ids = [str(r["event_id"] or "").strip() for r in stale_used_rows if str(r["event_id"] or "").strip()]
            if stale_used_ids:
                placeholders = ",".join("?" for _ in stale_used_ids)
                conn.execute(f"DELETE FROM publish_ledger WHERE event_id IN ({placeholders})", tuple(stale_used_ids))
                removed_used = conn.execute(
                    f"DELETE FROM news_events WHERE event_id IN ({placeholders}) AND status='used'",
                    tuple(stale_used_ids),
                ).rowcount
                conn.execute(f"DELETE FROM news_items WHERE event_id IN ({placeholders})", tuple(stale_used_ids))

            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM news_events
                WHERE status='queued'
                """
            ).fetchone()
            total = int(row[0] if row else 0)
            cap = max(100, int(max_items))
            overflow = max(0, total - cap)
            if overflow > 0:
                overflow_rows = conn.execute(
                    """
                    SELECT event_id
                    FROM news_events
                    WHERE status='queued'
                    ORDER BY last_seen_utc ASC, event_id ASC
                    LIMIT ?
                    """,
                    (overflow,),
                ).fetchall()
                overflow_ids = [str(r["event_id"] or "").strip() for r in overflow_rows if str(r["event_id"] or "").strip()]
                if overflow_ids:
                    placeholders = ",".join("?" for _ in overflow_ids)
                    removed_cap = conn.execute(
                        f"UPDATE news_events SET status='discarded', claimed_at='' WHERE event_id IN ({placeholders})",
                        tuple(overflow_ids),
                    ).rowcount

        return {
            "removed_old": int(removed_old),
            "removed_used": int(removed_used),
            "removed_cap": int(removed_cap),
        }

    def get_by_event_id(self, event_id: str) -> dict[str, Any] | None:
        event_key = str(event_id or "").strip()
        if not event_key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT e.event_id, e.event_fp, e.canonical_title, e.canonical_snippet,
                       e.representative_url, e.category, e.first_seen_utc, e.last_seen_utc,
                       e.status, e.claimed_at,
                       COALESCE(s.max_score, 70) AS score,
                       COALESCE(l.post_url, '') AS published_url,
                       COALESCE(l.published_at_utc, '') AS used_at
                FROM news_events e
                LEFT JOIN (
                    SELECT event_id, MAX(score) AS max_score
                    FROM news_items
                    GROUP BY event_id
                ) s ON s.event_id=e.event_id
                LEFT JOIN publish_ledger l ON l.event_id=e.event_id
                WHERE e.event_id=?
                LIMIT 1
                """,
                (event_key,),
            ).fetchone()
        if row is None:
            return None
        url = str(row["representative_url"] or "").strip()
        return NewsPoolItem(
            id=str(row["event_id"] or ""),
            event_id=str(row["event_id"] or ""),
            event_fp=str(row["event_fp"] or ""),
            url=url,
            title=str(row["canonical_title"] or ""),
            source=_source_from_url(url),
            published_at=str(row["last_seen_utc"] or ""),
            snippet=str(row["canonical_snippet"] or ""),
            category=str(row["category"] or ""),
            status=str(row["status"] or ""),
            score=int(row["score"] or 0),
            claimed_at=str(row["claimed_at"] or ""),
            used_at=str(row["used_at"] or ""),
            published_url=str(row["published_url"] or ""),
            created_at=str(row["first_seen_utc"] or ""),
            topic_fp=str(row["event_fp"] or ""),
        ).as_dict()

    def get_by_id(self, item_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, event_id, url, title, source, published_at, snippet, category,
                       status, score, claimed_at, used_at, published_url, created_at, topic_fp
                FROM news_items
                WHERE id=?
                LIMIT 1
                """,
                (int(item_id),),
            ).fetchone()
            if row is None:
                return None
            event_id = str(row["event_id"] or "").strip()
        if event_id:
            return self.get_by_event_id(event_id)
        return NewsPoolItem(
            id=str(row["id"] or ""),
            event_id="",
            event_fp=str(row["topic_fp"] or ""),
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
