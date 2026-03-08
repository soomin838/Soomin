from __future__ import annotations

import time
from html import escape

from rezero_v2.core.domain.stage_result import StageResult


class PublishStage:
    def __init__(self, *, blogger_client, internal_link_engine, source_guard, publish_store, candidate_store, cluster_store) -> None:
        self.blogger_client = blogger_client
        self.internal_link_engine = internal_link_engine
        self.source_guard = source_guard
        self.publish_store = publish_store
        self.candidate_store = candidate_store
        self.cluster_store = cluster_store

    def run(self, context, *, candidate, intent_bundle, draft, images, cluster) -> StageResult[dict]:
        started = time.perf_counter()
        intent_family = str(candidate.raw_meta.get('intent_family', '') or (intent_bundle.expansions[0].intent_family if intent_bundle.expansions else ''))
        internal_links = self.internal_link_engine.pick_links(
            cluster_id=cluster.cluster_id,
            entity_terms=list(candidate.entity_terms),
            intent_family=intent_family,
            limit=3,
        )
        raw_source_links = [candidate.source_url] + [link.get('url', '') for link in internal_links if link.get('url')]
        source_links = self.source_guard.filter_links(
            title=candidate.title,
            category=candidate.category,
            source_domain=candidate.source_domain,
            links=raw_source_links,
        )
        html = self._compose_html(draft.html, images, internal_links, source_links)
        artifact = self.blogger_client.publish_post(title=draft.title or candidate.title, html=html, labels=[candidate.content_type, candidate.category])
        self.publish_store.record_publish(
            artifact,
            title=draft.title or candidate.title,
            cluster_id=cluster.cluster_id,
            entity_terms=list(candidate.entity_terms),
            intent_family=intent_family,
            content_type=candidate.content_type,
            day=context.day_key,
        )
        self.candidate_store.mark_used(candidate.candidate_id)
        return StageResult(
            'publish_stage',
            'success',
            artifact.status,
            '게시 단계를 완료했습니다.',
            int((time.perf_counter() - started) * 1000),
            {'publish_artifact': artifact, 'internal_links': internal_links, 'source_links': source_links},
            {'content_type': candidate.content_type, 'cluster_id': cluster.cluster_id},
        )

    def _compose_html(self, body_html: str, images, internal_links, source_links) -> str:
        parts = [str(body_html or '')]
        if images:
            parts.append('<section><h2>Visuals</h2>')
            for image in images:
                parts.append(
                    f'<figure><img src="{escape(image.url)}" alt="{escape(image.alt_text)}" loading="lazy"/><figcaption>{escape(image.alt_text)}</figcaption></figure>'
                )
            parts.append('</section>')
        if internal_links:
            parts.append('<section><h2>Related reading</h2><ul>')
            for item in internal_links:
                parts.append(f'<li><a href="{escape(str(item.get("url", "")))}">{escape(str(item.get("title", "")))}</a></li>')
            parts.append('</ul></section>')
        if source_links:
            parts.append('<section><h2>Sources</h2><ul>')
            for link in source_links:
                parts.append(f'<li><a href="{escape(str(link or ""))}">{escape(str(link or ""))}</a></li>')
            parts.append('</ul></section>')
        return ''.join(parts)
