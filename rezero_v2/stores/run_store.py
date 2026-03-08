from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rezero_v2.core.domain.stage_result import StageResult
from rezero_v2.core.domain.workflow_final import WorkflowFinalSummary
from rezero_v2.stores.db import connect_db


class RunStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).resolve()
        self._init_db()

    def _init_db(self) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, created_at_utc TEXT NOT NULL, status TEXT NOT NULL, reason_code TEXT NOT NULL, final_stage TEXT NOT NULL, content_type TEXT NOT NULL, selected_title TEXT NOT NULL, source_domain TEXT NOT NULL, repair_attempted INTEGER NOT NULL, repair_succeeded INTEGER NOT NULL, published_url TEXT NOT NULL, stage_timings_json TEXT NOT NULL, debug_meta_json TEXT NOT NULL, final_summary_json TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS stage_results (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, stage_name TEXT NOT NULL, status TEXT NOT NULL, reason_code TEXT NOT NULL, human_message TEXT NOT NULL, timing_ms INTEGER NOT NULL, payload_json TEXT NOT NULL, debug_meta_json TEXT NOT NULL, created_at_utc TEXT NOT NULL)"
            )

    def append_stage_result(self, run_id: str, result: StageResult[Any]) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute(
                "INSERT INTO stage_results (run_id, stage_name, status, reason_code, human_message, timing_ms, payload_json, debug_meta_json, created_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(run_id or ""),
                    result.stage_name,
                    result.status,
                    result.reason_code,
                    result.human_message,
                    int(result.timing_ms),
                    json.dumps(self._json_ready(result.payload), ensure_ascii=False, default=str),
                    json.dumps(self._json_ready(result.debug_meta), ensure_ascii=False, default=str),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def record_final_summary(self, summary: WorkflowFinalSummary) -> None:
        with connect_db(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO runs (run_id, created_at_utc, status, reason_code, final_stage, content_type, selected_title, source_domain, repair_attempted, repair_succeeded, published_url, stage_timings_json, debug_meta_json, final_summary_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    summary.run_id,
                    datetime.now(timezone.utc).isoformat(),
                    summary.result,
                    summary.reason_code,
                    summary.final_stage,
                    summary.content_type,
                    summary.selected_title,
                    summary.source_domain,
                    1 if summary.repair_attempted else 0,
                    1 if summary.repair_succeeded else 0,
                    summary.published_url,
                    json.dumps(self._json_ready(summary.stage_timings_ms), ensure_ascii=False, default=str),
                    json.dumps(self._json_ready(summary.debug_meta), ensure_ascii=False, default=str),
                    json.dumps(self._json_ready(summary), ensure_ascii=False, default=str),
                ),
            )

    def list_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with connect_db(self.db_path) as conn:
            rows = conn.execute(
                "SELECT final_summary_json FROM runs ORDER BY created_at_utc DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        out = []
        for row in rows:
            try:
                out.append(json.loads(str(row[0] or "{}")))
            except Exception:
                continue
        return out

    def latest_final_summary(self) -> dict[str, Any] | None:
        rows = self.list_recent_runs(limit=1)
        return rows[0] if rows else None

    def list_recent_heading_signatures(self, limit: int = 30) -> list[str]:
        with connect_db(self.db_path) as conn:
            rows = conn.execute(
                "SELECT payload_json FROM stage_results WHERE stage_name='outline_stage' ORDER BY id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        out: list[str] = []
        for row in rows:
            try:
                payload = json.loads(str(row[0] or "{}"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            outline = payload.get("outline_plan") if isinstance(payload.get("outline_plan"), dict) else payload
            signature = str((outline or {}).get("heading_signature", "") or "").strip()
            if signature:
                out.append(signature)
        return out

    def get_stage_results(self, run_id: str) -> list[dict[str, Any]]:
        with connect_db(self.db_path) as conn:
            rows = conn.execute(
                "SELECT stage_name, status, reason_code, human_message, timing_ms, payload_json, debug_meta_json, created_at_utc FROM stage_results WHERE run_id=? ORDER BY id ASC",
                (str(run_id or ""),),
            ).fetchall()
        out = []
        for row in rows:
            try:
                payload = json.loads(str(row[5] or "{}"))
            except Exception:
                payload = {}
            try:
                debug_meta = json.loads(str(row[6] or "{}"))
            except Exception:
                debug_meta = {}
            out.append(
                {
                    "stage_name": str(row[0] or ""),
                    "status": str(row[1] or ""),
                    "reason_code": str(row[2] or ""),
                    "human_message": str(row[3] or ""),
                    "timing_ms": int(row[4] or 0),
                    "payload": payload,
                    "debug_meta": debug_meta,
                    "created_at_utc": str(row[7] or ""),
                }
            )
        return out

    def _json_ready(self, value: Any) -> Any:
        if value is None:
            return None
        if is_dataclass(value):
            return {k: self._json_ready(v) for k, v in asdict(value).items()}
        if isinstance(value, dict):
            return {str(k): self._json_ready(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_ready(item) for item in value]
        return value