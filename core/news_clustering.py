from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


SIMILARITY_THRESHOLD = 0.82
CLUSTER_TTL_DAYS = 14


def normalize_text(text: str) -> str:
    src = str(text or "").lower()
    src = re.sub(r"https?://\S+|www\.\S+", " ", src, flags=re.IGNORECASE)
    src = re.sub(r"[\(\)\[\]\{\}<>\"'`~!@#$%^&*_=+|\\/:;,.?]", " ", src)
    src = re.sub(r"\s+", " ", src).strip()
    return src


def token_set(text: str) -> set[str]:
    return {tok for tok in normalize_text(text).split(" ") if len(tok) >= 2}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a.intersection(b))
    union = len(a.union(b))
    if union <= 0:
        return 0.0
    return max(0.0, min(1.0, float(inter) / float(union)))


def similarity(
    title: str,
    body: str,
    other_title: str = "",
    other_body: str = "",
) -> float:
    # text signature uses title + truncated body as requested for local heuristic comparison.
    left_text = normalize_text(f"{str(title or '')} {str(body or '')[:500]}")
    right_text = normalize_text(f"{str(other_title or '')} {str(other_body or '')[:500]}")
    left_title_tokens = token_set(title)
    right_title_tokens = token_set(other_title)
    left_body_tokens = token_set(left_text)
    right_body_tokens = token_set(right_text)
    title_sim = jaccard(left_title_tokens, right_title_tokens)
    body_sim = jaccard(left_body_tokens, right_body_tokens)
    score = (0.65 * title_sim) + (0.35 * body_sim)
    return max(0.0, min(1.0, float(score)))


def make_cluster_id(rep_text: str, stable_hash_fn: Callable[[str], int]) -> str:
    value = int(stable_hash_fn(str(rep_text or "").strip() or "cluster-empty"))
    hex_text = f"{max(0, value):x}".rjust(24, "0")
    return hex_text[:20]


def should_skip_same_run(cluster_id: str, seen_cluster_ids: set[str]) -> bool:
    key = str(cluster_id or "").strip()
    if not key:
        return False
    if key in seen_cluster_ids:
        return True
    seen_cluster_ids.add(key)
    return False


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: str) -> datetime | None:
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


@dataclass(frozen=True)
class ClusterDecision:
    cluster_id: str
    matched_existing: bool
    best_similarity: float
    compared_count: int
    state_available: bool


class NewsClusterEngine:
    def __init__(
        self,
        *,
        state_path: Path,
        stable_hash_fn: Callable[[str], int],
        threshold: float = SIMILARITY_THRESHOLD,
        ttl_days: int = CLUSTER_TTL_DAYS,
    ) -> None:
        self.state_path = Path(state_path).resolve()
        self.stable_hash_fn = stable_hash_fn
        self.threshold = float(max(0.0, min(1.0, threshold)))
        self.ttl_days = max(1, int(ttl_days))
        self._memory_clusters: list[dict[str, object]] = []

    def _default_state(self) -> dict[str, object]:
        return {"version": 1, "updated_at_utc": _utc_now().isoformat(), "clusters": []}

    def _prune_clusters(self, rows: list[dict[str, object]]) -> list[dict[str, object]]:
        cutoff = _utc_now() - timedelta(days=self.ttl_days)
        out: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            last_seen = _parse_utc(str(row.get("last_seen_utc", "") or ""))
            if last_seen is None:
                continue
            if last_seen < cutoff:
                continue
            cluster_id = str(row.get("cluster_id", "") or "").strip()
            rep_title = str(row.get("rep_title", "") or "").strip()
            rep_text = normalize_text(str(row.get("rep_text", "") or ""))[:400]
            try:
                seen_count = max(1, int(row.get("seen_count", 1) or 1))
            except Exception:
                seen_count = 1
            if not cluster_id or not rep_text:
                continue
            out.append(
                {
                    "cluster_id": cluster_id,
                    "rep_title": rep_title[:220],
                    "rep_text": rep_text,
                    "last_seen_utc": last_seen.isoformat(),
                    "seen_count": seen_count,
                }
            )
        return out

    def _load_state(self) -> tuple[dict[str, object], bool]:
        try:
            if not self.state_path.exists():
                return self._default_state(), True
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return self._default_state(), False
            clusters = payload.get("clusters", [])
            if not isinstance(clusters, list):
                clusters = []
            payload["clusters"] = self._prune_clusters([dict(x or {}) for x in clusters if isinstance(x, dict)])
            return payload, True
        except Exception:
            return self._default_state(), False

    def _save_state(self, state: dict[str, object]) -> bool:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            state = dict(state or {})
            state["version"] = 1
            state["updated_at_utc"] = _utc_now().isoformat()
            clusters = state.get("clusters", [])
            if not isinstance(clusters, list):
                clusters = []
            state["clusters"] = self._prune_clusters([dict(x or {}) for x in clusters if isinstance(x, dict)])
            self.state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        except Exception:
            return False

    def assign_cluster(
        self,
        *,
        event_id: str,
        title: str,
        body: str,
        run_start_minute: str,
    ) -> ClusterDecision:
        _ = event_id
        _ = run_start_minute
        rep_title = re.sub(r"\s+", " ", str(title or "").strip())[:220]
        rep_text = normalize_text(f"{str(title or '')} {str(body or '')[:250]}")[:400]
        state, state_available = self._load_state()
        clusters_raw = state.get("clusters", []) if state_available else self._memory_clusters
        clusters = self._prune_clusters([dict(x or {}) for x in (clusters_raw or []) if isinstance(x, dict)])

        best_idx = -1
        best_score = 0.0
        for idx, row in enumerate(clusters):
            row_title = str(row.get("rep_title", "") or "")
            row_text = str(row.get("rep_text", "") or "")
            score = similarity(rep_title, rep_text, row_title, row_text)
            if score > best_score:
                best_score = float(score)
                best_idx = int(idx)

        matched_existing = bool(best_idx >= 0 and best_score >= self.threshold)
        if matched_existing:
            row = dict(clusters[best_idx] or {})
            row["rep_title"] = rep_title or str(row.get("rep_title", "") or "")
            row["rep_text"] = rep_text or str(row.get("rep_text", "") or "")
            row["last_seen_utc"] = _utc_now().isoformat()
            row["seen_count"] = max(1, int(row.get("seen_count", 1) or 1) + 1)
            clusters[best_idx] = row
            cluster_id = str(row.get("cluster_id", "") or "")
        else:
            cluster_id = make_cluster_id(rep_text, self.stable_hash_fn)
            existing_ids = {str(x.get("cluster_id", "") or "") for x in clusters}
            if cluster_id in existing_ids:
                cluster_id = make_cluster_id(
                    f"{rep_text}|{str(event_id or '').strip()}|{str(run_start_minute or '').strip()}",
                    self.stable_hash_fn,
                )
            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "rep_title": rep_title,
                    "rep_text": rep_text,
                    "last_seen_utc": _utc_now().isoformat(),
                    "seen_count": 1,
                }
            )

        if state_available:
            state["clusters"] = clusters
            self._save_state(state)
        else:
            # FAIL OPEN: keep in-memory fallback so current process can still avoid duplicate clusters.
            self._memory_clusters = clusters

        return ClusterDecision(
            cluster_id=str(cluster_id or ""),
            matched_existing=bool(matched_existing),
            best_similarity=max(0.0, min(1.0, float(best_score))),
            compared_count=int(len(clusters)),
            state_available=bool(state_available),
        )

