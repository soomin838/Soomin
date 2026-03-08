from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DashboardCard:
    label: str
    value: str
    tone: str = "neutral"


@dataclass(frozen=True)
class RunRow:
    run_id: str
    result: str
    reason_code: str
    selected_title: str
    final_stage: str
    content_type: str


@dataclass(frozen=True)
class PublishRow:
    title: str
    post_url: str
    content_type: str
    status: str
    published_at_utc: str
