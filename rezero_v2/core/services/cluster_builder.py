from __future__ import annotations

import re

from rezero_v2.core.domain.cluster import TopicCluster


class ClusterBuilder:
    def assign_cluster(self, *, title: str, primary_query: str, content_type: str, entity_terms: list[str]) -> TopicCluster:
        base = primary_query or title or 'general'
        cluster_id = re.sub(r'[^a-z0-9]+', '-', base.lower()).strip('-')[:80] or 'general'
        topic_label = re.sub(r'\s+', ' ', base).strip()[:80] or 'general'
        is_pillar = str(content_type or '').lower() == 'evergreen'
        pillar_title = title if is_pillar else (primary_query or title)
        pillar_query = primary_query or title
        return TopicCluster(cluster_id=cluster_id, topic_label=topic_label, pillar_title=pillar_title, pillar_query=pillar_query, member_queries=[primary_query or title], entity_family=list(entity_terms or [])[:6])
