from __future__ import annotations

import re
import time
import uuid
from urllib.parse import urlparse

from rezero_v2.core.domain.candidate import Candidate
from rezero_v2.core.domain.stage_result import StageResult
from rezero_v2.core.services.intent_engine import looks_non_english, normalize_candidate_identity, sanitize_source_headline, sanitize_source_snippet


class IngestStage:
    def __init__(self, *, gdelt_client, candidate_store, search_console_client=None, topic_scorer=None) -> None:
        self.gdelt_client = gdelt_client
        self.candidate_store = candidate_store
        self.search_console_client = search_console_client
        self.topic_scorer = topic_scorer

    def run(self, context) -> StageResult[list[Candidate]]:
        started = time.perf_counter()
        slot = context.allocation.slot_type
        candidates: list[Candidate] = []
        if slot == 'search_derived':
            candidates.extend(normalize_candidate_identity(candidate) for candidate in self.candidate_store.get_pending('search_derived', limit=20))
        elif slot == 'evergreen':
            candidates.extend(normalize_candidate_identity(candidate) for candidate in self.candidate_store.get_pending('evergreen', limit=20))
        if not candidates:
            groups = list(self.gdelt_client.fetch_trending_topics() or [])
            for group in groups:
                topic = str((group or {}).get('topic', '') or '').strip()
                for row in list((group or {}).get('articles', []) or []):
                    title = str((row or {}).get('title', '') or '').strip()
                    url = str((row or {}).get('url', '') or '').strip()
                    if not title or not url:
                        continue
                    clean_headline = sanitize_source_headline(title)
                    source_domain = (urlparse(url).netloc or str((row or {}).get('source', '') or '')).lower()
                    source_snippet = sanitize_source_snippet(str((row or {}).get('summary', '') or '').strip())
                    entity_terms = re.findall(r"\b[A-Z][a-zA-Z0-9&.-]{2,}\b", title)
                    topic_terms = [
                        word
                        for word in re.findall(r"\b[a-zA-Z][a-zA-Z0-9-]{3,}\b", f"{title} {source_snippet}".lower())
                        if word not in {'with', 'from', 'this', 'that', 'what', 'when'}
                    ][:8]
                    content_type = slot if slot in {'search_derived', 'evergreen'} else 'hot'
                    source_type = 'cluster_seed' if content_type == 'evergreen' else 'gdelt'
                    score = 60.0
                    if self.topic_scorer is not None:
                        if content_type == 'hot':
                            score = self.topic_scorer.score_hot(trend_score=72, freshness=85, explicit_tech_signal=65, grounding_strength=60)
                        elif content_type == 'search_derived':
                            score = self.topic_scorer.score_search_derived(search_demand=68, usefulness_score=70, competition_inverse=58, freshness=60, cluster_fit=55)
                        else:
                            score = self.topic_scorer.score_evergreen(durability=72, usefulness_score=70, cluster_gap=64, search_demand=55, authority_source_availability=60)
                    candidates.append(
                        normalize_candidate_identity(Candidate(
                            candidate_id=uuid.uuid5(uuid.NAMESPACE_URL, url).hex,
                            content_type=content_type,
                            source_type=source_type,
                            title=clean_headline,
                            source_title=clean_headline,
                            source_url=url,
                            source_domain=source_domain,
                            source_snippet=source_snippet,
                            category=topic or 'technology',
                            published_at_utc=str((row or {}).get('published_date', '') or ''),
                            provider='gdelt',
                            language='non_english' if looks_non_english(clean_headline) else 'en',
                            source_headline=clean_headline,
                            normalized_source_headline=clean_headline,
                            derived_primary_query='',
                            entity_terms=entity_terms[:6],
                            topic_terms=topic_terms,
                            tags=[topic] if topic else [],
                            raw_meta={'score': score, 'topic': topic},
                        ))
                    )
                    if len(candidates) >= 20:
                        break
                if len(candidates) >= 20:
                    break
        if not candidates:
            return StageResult(
                'ingest_stage',
                'skipped',
                'no_candidates_found',
                '이번 슬롯에서 사용할 후보를 찾지 못했습니다.',
                int((time.perf_counter() - started) * 1000),
                [],
            )
        return StageResult(
            'ingest_stage',
            'success',
            'candidates_loaded',
            f'{len(candidates)}개의 후보를 불러왔습니다.',
            int((time.perf_counter() - started) * 1000),
            candidates,
            {'slot': slot},
        )
