from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TopicCluster:
    cluster_id: str
    topic_label: str
    pillar_title: str
    pillar_query: str
    member_queries: list[str] = field(default_factory=list)
    entity_family: list[str] = field(default_factory=list)
