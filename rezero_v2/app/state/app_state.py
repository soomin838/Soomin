from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from rezero_v2.app.state.ui_models import DashboardCard, PublishRow, RunRow


@dataclass
class V2AppState:
    runtime_version: str = "v2"
    current_status: str = "idle"
    current_stage: str = "idle"
    current_message: str = "대기 중"
    next_run_at: str = ""
    latest_final_summary: dict[str, Any] | None = None
    recent_runs: list[RunRow] = field(default_factory=list)
    recent_posts: list[PublishRow] = field(default_factory=list)
    recent_skip_reasons: list[str] = field(default_factory=list)
    today_mix: dict[str, int] = field(default_factory=lambda: {"hot": 0, "search_derived": 0, "evergreen": 0})
    dashboard_cards: list[DashboardCard] = field(default_factory=list)
    inspector_stage_rows: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def build(
        cls,
        *,
        latest_final_summary: dict[str, Any] | None,
        recent_runs: list[dict[str, Any]],
        recent_posts: list[dict[str, Any]],
        today_mix: dict[str, int],
        current_status: str,
        current_stage: str,
        current_message: str,
        schedule_hours: float,
        inspector_stage_rows: list[dict[str, Any]] | None = None,
    ) -> "V2AppState":
        next_run = (datetime.now().astimezone() + timedelta(hours=max(0.25, float(schedule_hours or 2.4)))).strftime("%Y-%m-%d %H:%M")
        run_rows = [
            RunRow(
                run_id=str(row.get("run_id", "") or ""),
                result=str(row.get("result", "") or ""),
                reason_code=str(row.get("reason_code", "") or ""),
                selected_title=str(row.get("selected_title", "") or ""),
                final_stage=str(row.get("final_stage", "") or ""),
                content_type=str(row.get("content_type", "") or ""),
            )
            for row in recent_runs
        ]
        post_rows = [
            PublishRow(
                title=str(row.get("title", "") or ""),
                post_url=str(row.get("post_url", "") or ""),
                content_type=str(row.get("content_type", "") or ""),
                status=str(row.get("status", "") or ""),
                published_at_utc=str(row.get("published_at_utc", "") or ""),
            )
            for row in recent_posts
        ]
        skip_reasons = []
        for row in recent_runs:
            if str(row.get("result", "") or "") in {"skipped", "held"}:
                reason = str(row.get("reason_code", "") or "").strip()
                if reason and reason not in skip_reasons:
                    skip_reasons.append(reason)
            if len(skip_reasons) >= 5:
                break
        cards = [
            DashboardCard("오늘 hot", str(int(today_mix.get("hot", 0) or 0)), "hot"),
            DashboardCard("오늘 search", str(int(today_mix.get("search_derived", 0) or 0)), "search"),
            DashboardCard("오늘 evergreen", str(int(today_mix.get("evergreen", 0) or 0)), "evergreen"),
            DashboardCard("최근 결과", str((latest_final_summary or {}).get("result", "none") or "none"), "neutral"),
        ]
        return cls(
            latest_final_summary=latest_final_summary,
            recent_runs=run_rows,
            recent_posts=post_rows,
            recent_skip_reasons=skip_reasons,
            today_mix={k: int(v or 0) for k, v in (today_mix or {}).items()},
            current_status=current_status,
            current_stage=current_stage,
            current_message=current_message,
            next_run_at=next_run,
            dashboard_cards=cards,
            inspector_stage_rows=list(inspector_stage_rows or []),
        )
