from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from re_core.services.news_collector import fetch_news, fetch_trending_topics


@dataclass(frozen=True)
class GDELTArticle:
    title: str
    url: str
    source: str
    published_date: str
    summary: str
    topic: str
    provider: str = 'gdelt'


class GDELTClient:
    def fetch_trending_topics(self) -> list[dict[str, Any]]:
        return list(fetch_trending_topics() or [])

    def fetch_news(self, query: str, max_records: int = 20) -> list[GDELTArticle]:
        rows = fetch_news(query, max_records=max_records)
        out: list[GDELTArticle] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            out.append(GDELTArticle(title=str(row.get('title', '') or '').strip(), url=str(row.get('url', '') or '').strip(), source=str(row.get('source', '') or '').strip(), published_date=str(row.get('published_date', '') or '').strip(), summary=str(row.get('summary', '') or '').strip(), topic=str(row.get('topic', '') or '').strip()))
        return out
