from __future__ import annotations

from typing import Any

from rezero_v2.stores.cluster_store import ClusterStore
from rezero_v2.stores.publish_store import PublishStore


class InternalLinkEngine:
    def __init__(self, *, publish_store: PublishStore, cluster_store: ClusterStore) -> None:
        self.publish_store = publish_store
        self.cluster_store = cluster_store

    def pick_links(self, *, cluster_id: str, entity_terms: list[str], intent_family: str, limit: int = 3) -> list[dict[str, Any]]:
        posts = self.publish_store.list_recent_posts(limit=60)
        scored: list[dict[str, Any]] = []
        for post in posts:
            score = 0.0
            reasons: list[str] = []
            if str(post.get('cluster_id', '') or '') == str(cluster_id or ''):
                score += 1.0
                reasons.append('same_cluster')
            if str(post.get('intent_family', '') or '') == str(intent_family or ''):
                score += 0.35
                reasons.append('same_intent_family')
            title_blob = str(post.get('title', '') or '').lower()
            entity_hits = sum(1 for term in entity_terms if str(term or '').lower() in title_blob)
            if entity_hits:
                score += min(0.6, 0.2 * entity_hits)
                reasons.append('same_entity')
            if score <= 0.0:
                continue
            scored.append({'title': post.get('title', ''), 'url': post.get('post_url', ''), 'score': round(score, 2), 'reasons': reasons})
        scored.sort(key=lambda item: item['score'], reverse=True)
        for item in scored[:limit]:
            self.cluster_store.remember_internal_link(cluster_id, str(item.get('url', '')), float(item.get('score', 0.0)), ','.join(item.get('reasons', [])))
        return scored[:limit]
