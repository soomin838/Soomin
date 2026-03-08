from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class TopicClusterAssignment:
    cluster_id: str
    cluster_label: str
    cluster_role: str
    pillar_title: str
    pillar_query: str
    intent_family: str
    content_type: str


@dataclass(frozen=True)
class TopicClusterRecord:
    cluster_id: str
    cluster_label: str
    pillar_title: str = ""
    pillar_query: str = ""
    updated_at_utc: str = ""
    members: list[dict[str, str]] = field(default_factory=list)


class TopicClusterBuilder:
    def __init__(self, *, state_path: Path, log_path: Path | None = None, ttl_days: int = 180) -> None:
        self.state_path = Path(state_path).resolve()
        self.log_path = Path(log_path).resolve() if log_path else None
        self.ttl_days = max(30, int(ttl_days))

    def preview_assignment(
        self,
        *,
        title: str,
        primary_query: str = "",
        content_type: str = "hot",
        cluster_id: str = "",
        intent_family: str = "",
    ) -> TopicClusterAssignment:
        clean_title = re.sub(r"\s+", " ", str(title or "").strip())
        clean_query = re.sub(r"\s+", " ", str(primary_query or "").strip())
        cid = self._clean_slug(cluster_id or clean_query or clean_title or "general")
        label = self._cluster_label(clean_query or clean_title)
        role = self._infer_role(title=clean_title, primary_query=clean_query, content_type=content_type)
        state = self._load_state()
        cluster = dict((state.get("clusters", {}) or {}).get(cid, {}) or {})
        pillar_title = str(cluster.get("pillar_title", "") or "").strip()
        pillar_query = str(cluster.get("pillar_query", "") or "").strip()
        if role == "pillar":
            pillar_title = clean_title or pillar_title
            pillar_query = clean_query or clean_title or pillar_query
        if not pillar_title:
            pillar_title = clean_title
        if not pillar_query:
            pillar_query = clean_query or clean_title
        assignment = TopicClusterAssignment(
            cluster_id=cid or "general",
            cluster_label=label,
            cluster_role=role,
            pillar_title=pillar_title[:140],
            pillar_query=pillar_query[:180],
            intent_family=self._clean_slug(intent_family or "news-explainer").replace("-", "_"),
            content_type=str(content_type or "hot").strip().lower() or "hot",
        )
        self._log("cluster_preview", asdict(assignment))
        return assignment

    def remember_published(
        self,
        *,
        assignment: TopicClusterAssignment,
        title: str,
        url: str = "",
    ) -> None:
        state = self._load_state()
        clusters = state.setdefault("clusters", {})
        cluster = dict(clusters.get(assignment.cluster_id, {}) or {})
        members = list(cluster.get("members", []) or [])
        member_row = {
            "title": re.sub(r"\s+", " ", str(title or "").strip())[:140],
            "url": str(url or "").strip()[:320],
            "cluster_role": str(assignment.cluster_role or "").strip(),
            "intent_family": str(assignment.intent_family or "").strip(),
            "content_type": str(assignment.content_type or "").strip(),
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        deduped = [row for row in members if str((row or {}).get("title", "") or "").strip().lower() != member_row["title"].lower()]
        deduped.insert(0, member_row)
        cluster["cluster_id"] = assignment.cluster_id
        cluster["cluster_label"] = assignment.cluster_label
        if assignment.cluster_role == "pillar" or not str(cluster.get("pillar_title", "") or "").strip():
            cluster["pillar_title"] = assignment.pillar_title
            cluster["pillar_query"] = assignment.pillar_query
        cluster["members"] = deduped[:40]
        cluster["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        clusters[assignment.cluster_id] = cluster
        state["clusters"] = clusters
        state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        self._save_state(state)
        self._log("cluster_published", {"assignment": asdict(assignment), "url": str(url or "").strip()[:320]})

    def _infer_role(self, *, title: str, primary_query: str, content_type: str) -> str:
        merged = f"{title} {primary_query}".lower()
        if str(content_type or "").strip().lower() == "evergreen":
            return "pillar"
        if re.search(r"\b(complete guide|guide|explained|what is|best .* guide|overview)\b", merged):
            return "pillar"
        return "supporting"

    def _cluster_label(self, text: str) -> str:
        clean = re.sub(r"[^A-Za-z0-9\s-]", " ", str(text or "").strip())
        clean = re.sub(r"\s+", " ", clean).strip()
        words = [word for word in clean.split(" ") if len(word) >= 3]
        return " ".join(words[:4]) or "general"

    def _clean_slug(self, value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
        slug = re.sub(r"-{2,}", "-", slug).strip("-")
        return slug[:80] or "general"

    def _load_state(self) -> dict:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.ttl_days)
        try:
            if not self.state_path.exists():
                return {"clusters": {}}
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return {"clusters": {}}
        clusters = payload.get("clusters", {}) if isinstance(payload, dict) else {}
        out: dict[str, dict] = {}
        for key, row in (clusters.items() if isinstance(clusters, dict) else []):
            if not isinstance(row, dict):
                continue
            updated = self._parse_iso(str(row.get("updated_at_utc", "") or ""))
            if updated is None or updated < cutoff:
                continue
            out[str(key or "").strip().lower()] = row
        return {"clusters": out}

    def _save_state(self, payload: dict) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            return

    def _parse_iso(self, value: str) -> datetime | None:
        txt = str(value or "").strip()
        if not txt:
            return None
        try:
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _log(self, event: str, payload: dict) -> None:
        if self.log_path is None:
            return
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip(),
            **dict(payload or {}),
        }
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return
