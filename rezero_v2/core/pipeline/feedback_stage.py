from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.stage_result import StageResult


class FeedbackStage:
    def __init__(self, *, search_console_client, candidate_store, cluster_store) -> None:
        self.search_console_client = search_console_client
        self.candidate_store = candidate_store
        self.cluster_store = cluster_store

    def run(self, context, *, candidate, intent_bundle, cluster, publish_artifact) -> StageResult[dict]:
        started = time.perf_counter()
        if self.search_console_client is None:
            return StageResult(
                'feedback_stage',
                'success',
                'feedback_skipped_no_search_console',
                'Search Console이 비활성화되어 피드백 단계를 건너뜁니다.',
                int((time.perf_counter() - started) * 1000),
                {'opportunities': []},
            )
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=14)
        rows = self.search_console_client.fetch_rows(str(start_date), str(end_date))
        opportunities = self.search_console_client.discover_opportunities(rows)
        derived: list[Candidate] = []
        for item in opportunities[:10]:
            query = str(item.get('query', '') or '').strip()
            if not query:
                continue
            derived.append(
                Candidate(
                    candidate_id=uuid.uuid5(uuid.NAMESPACE_URL, f'v2:{query}').hex,
                    content_type='search_derived',
                    source_type='search_console',
                    title=query,
                    source_title=candidate.source_title,
                    source_url=candidate.source_url,
                    source_domain=candidate.source_domain,
                    source_snippet=candidate.source_snippet,
                    category=candidate.category,
                    published_at_utc=candidate.published_at_utc,
                    provider='search_console',
                    language='en',
                    entity_terms=list(candidate.entity_terms),
                    topic_terms=list(candidate.topic_terms),
                    tags=list(candidate.tags),
                    raw_meta={'intent_family': 'comparison', 'primary_query': query, 'cluster_id': cluster.cluster_id, 'opportunity_action': item.get('action', '')},
                )
            )
        if derived:
            self.candidate_store.enqueue_candidates(derived, priority=68.0, cluster_id=cluster.cluster_id, intent_family='comparison')
        self.cluster_store.upsert_cluster(cluster, role='pillar' if candidate.content_type == 'evergreen' else 'supporting')
        return StageResult(
            'feedback_stage',
            'success',
            'feedback_applied',
            'Search Console 피드백과 클러스터 갱신을 반영했습니다.',
            int((time.perf_counter() - started) * 1000),
            {'opportunities': opportunities[:10], 'enqueued_candidates': derived},
            {'published_url': publish_artifact.post_url},
        )
