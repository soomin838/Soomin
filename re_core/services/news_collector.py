"""GDELT-based news ingestion module for RezeroAgent.

This module replaces WorldMonitor-style external API-key assumptions with a
public, unauthenticated GDELT collector. It is designed for stable automated
runs and returns normalized structured article data for downstream topic
classification and blog generation.

Primary public functions:
    - fetch_news(query: str, max_records: int = 50)
    - fetch_trending_topics()
    - clean_news_data(news_list)
    - expand_keywords(topic: str)

The collector favors safety and stability over strict completeness:
    - request timeout
    - retry with backoff
    - defensive JSON parsing
    - duplicate removal
    - normalized output schema
"""
from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Iterable
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

GDELT_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_TIMEOUT = 15
DEFAULT_PER_REQUEST = 50
MAX_PER_REQUEST = 250
DEFAULT_TREND_QUERIES = [
    "AI",
    "technology",
    "startup",
    "cybersecurity",
    "software",
    "open source",
]
KEYWORD_EXPANSIONS: dict[str, list[str]] = {
    "ai": ["artificial intelligence", "machine learning", "LLM"],
    "technology": ["tech", "innovation", "digital transformation"],
    "startup": ["venture", "seed round", "funding"],
    "cybersecurity": ["infosec", "data breach", "zero trust"],
    "software": ["developer tools", "SaaS", "enterprise software"],
    "open source": ["open-source", "GitHub", "OSS"],
}


class _GDELTCollector:
    """Internal session-backed collector with retry logic."""

    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.timeout = max(5, int(timeout))
        self.session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "RezeroAgent-GDELTNewsCollector/1.0",
                "Accept": "application/json",
            }
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _request(self, params: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self.session.get(
                GDELT_BASE_URL,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                logger.warning("GDELT response was not a dict. params=%s", params)
                return {}
            return data
        except requests.Timeout:
            logger.warning("GDELT request timed out. params=%s", params)
        except requests.RequestException as exc:
            logger.warning("GDELT request failed: %s. params=%s", exc, params)
        except ValueError as exc:
            logger.warning("GDELT JSON parsing failed: %s. params=%s", exc, params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Unexpected GDELT error: %s. params=%s", exc, params)
        return {}

    def fetch_page(self, query: str, max_records: int) -> list[dict[str, Any]]:
        """Fetch a single page of GDELT results.

        GDELT does not expose conventional offset pagination for this endpoint in a
        stable way. This method fetches one result page using maxrecords. The
        public ``fetch_news`` function batches large requests across expanded
        queries and then de-duplicates results.
        """
        params = {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "sort": "DateDesc",
            "maxrecords": max(1, min(int(max_records), MAX_PER_REQUEST)),
        }
        data = self._request(params=params)
        articles = data.get("articles", []) if isinstance(data, dict) else []
        if not isinstance(articles, list):
            logger.warning("GDELT articles payload was not a list. query=%s", query)
            return []
        return [item for item in articles if isinstance(item, dict)]


def _normalize_date(value: Any) -> str:
    """Normalize GDELT-style date values into ISO 8601 strings.

    Handles common values such as ``YYYYMMDDHHMMSS`` or already-ISO-like strings.
    Falls back to an empty string if the value is unusable.
    """
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    # Common GDELT format: 20260307123000
    if len(text) == 14 and text.isdigit():
        try:
            dt = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass

    # Already ISO-like
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


def _guess_source(article: dict[str, Any]) -> str:
    domain = str(article.get("domain", "") or "").strip()
    if domain:
        return domain
    url = str(article.get("url", "") or "").strip()
    if url:
        parsed = urlparse(url)
        if parsed.netloc:
            return parsed.netloc
    source_country = str(article.get("sourcecountry", "") or "").strip()
    return source_country


def _normalize_article(article: dict[str, Any], *, default_topic: str = "") -> dict[str, str]:
    title = str(article.get("title", "") or "").strip()
    url = str(article.get("url", "") or "").strip()
    summary = str(article.get("seendate", "") or "").strip()
    # Prefer socialimage/metadata description is not consistently present; many
    # GDELT results do not expose article summary text. We keep this field stable.
    if isinstance(article.get("excerpt"), str) and article.get("excerpt", "").strip():
        summary = str(article.get("excerpt", "")).strip()
    elif isinstance(article.get("snippet"), str) and article.get("snippet", "").strip():
        summary = str(article.get("snippet", "")).strip()
    elif summary:
        summary = f"Seen by GDELT at {summary}"
    else:
        summary = ""

    published = _normalize_date(
        article.get("seendate")
        or article.get("published")
        or article.get("date")
    )

    return {
        "title": title,
        "url": url,
        "source": _guess_source(article),
        "published_date": published,
        "summary": summary,
        "topic": str(article.get("topic", "") or default_topic).strip(),
        "provider": "gdelt",
    }


def clean_news_data(news_list: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    """Normalize and deduplicate a collection of news items.

    Processing rules:
        - remove duplicates using URL
        - remove empty titles
        - normalize date formats into ISO 8601 when possible
        - guarantee the standard output schema

    Args:
        news_list: Iterable of raw or partially normalized article dictionaries.

    Returns:
        A cleaned list of article dictionaries using the schema:
        ``title, url, source, published_date, summary, topic, provider``.
    """
    seen_urls: set[str] = set()
    cleaned: list[dict[str, str]] = []

    for item in news_list:
        if not isinstance(item, dict):
            continue

        article = (
            _normalize_article(item, default_topic=str(item.get("topic", "") or "").strip())
            if any(k not in item for k in ("title", "url", "source", "published_date", "summary", "topic", "provider"))
            else {
                "title": str(item.get("title", "") or "").strip(),
                "url": str(item.get("url", "") or "").strip(),
                "source": str(item.get("source", "") or "").strip(),
                "published_date": _normalize_date(item.get("published_date")),
                "summary": str(item.get("summary", "") or "").strip(),
                "topic": str(item.get("topic", "") or "").strip(),
                "provider": str(item.get("provider", "") or "gdelt").strip() or "gdelt",
            }
        )

        if not article["title"]:
            continue
        if not article["url"]:
            continue
        if article["url"] in seen_urls:
            continue
        seen_urls.add(article["url"])
        if not article["provider"]:
            article["provider"] = "gdelt"
        cleaned.append(article)

    return cleaned


def expand_keywords(topic: str) -> list[str]:
    """Expand a seed topic into related discovery queries.

    Args:
        topic: Seed topic such as ``AI`` or ``cybersecurity``.

    Returns:
        A list containing the original topic plus curated related terms.

    Example:
        ``expand_keywords("AI")`` ->
        ``["AI", "artificial intelligence", "machine learning", "LLM"]``
    """
    raw = str(topic or "").strip()
    if not raw:
        return []
    lower = raw.lower()
    related = KEYWORD_EXPANSIONS.get(lower, [])
    out = [raw]
    for item in related:
        if item not in out:
            out.append(item)
    return out


def fetch_news(query: str, max_records: int = 50) -> list[dict[str, str]]:
    """Fetch the latest news articles for a given keyword.

    The function queries the public GDELT Doc API and returns normalized article
    objects for downstream content generation.

    Args:
        query: Keyword string to search.
        max_records: Approximate number of articles to retrieve.

    Returns:
        A cleaned list of dictionaries using this schema::

            {
              "title": "...",
              "url": "...",
              "source": "...",
              "published_date": "...",
              "summary": "...",
              "topic": "...",
              "provider": "gdelt"
            }

    Notes:
        - GDELT does not require authentication.
        - For larger requests, the function batches across keyword expansions and
          de-duplicates the final result set.
        - On failure, the function logs the error and returns a safe empty list.
    """
    if not str(query or "").strip():
        logger.warning("fetch_news called with empty query")
        return []

    collector = _GDELTCollector()
    target = max(1, int(max_records))
    all_rows: list[dict[str, Any]] = []

    queries = expand_keywords(query)
    if not queries:
        return []

    # Split requested volume across the expanded queries, capped per request.
    per_query = max(5, min(MAX_PER_REQUEST, (target // max(1, len(queries))) + 5))

    for q in queries:
        rows = collector.fetch_page(query=q, max_records=min(per_query, target))
        all_rows.extend(rows)
        if len(all_rows) >= target * 2:
            break

    tagged_rows = []
    for row in all_rows:
        if not isinstance(row, dict):
            continue
        tagged = dict(row)
        tagged["topic"] = str(query or "").strip()
        tagged["provider"] = "gdelt"
        tagged_rows.append(tagged)

    cleaned = clean_news_data(tagged_rows)
    return cleaned[:target]


def fetch_trending_topics() -> list[dict[str, Any]]:
    """Fetch trending global technology-related news grouped by topic.

    This function runs a curated set of high-value technology and innovation
    queries, merges results, and returns cleaned per-topic payloads.

    Returns:
        A list like::

            [
                {
                    "topic": "AI",
                    "articles": [{...}, {...}],
                },
                {
                    "topic": "technology",
                    "articles": [{...}, {...}],
                },
            ]

    Each article in ``articles`` follows the standard normalized schema.
    """
    results: list[dict[str, Any]] = []
    global_seen_urls: set[str] = set()

    for topic in DEFAULT_TREND_QUERIES:
        articles = fetch_news(topic, max_records=20)
        filtered: list[dict[str, str]] = []
        for article in articles:
            url = article.get("url", "")
            if not url or url in global_seen_urls:
                continue
            global_seen_urls.add(url)
            tagged = dict(article)
            tagged["topic"] = str(topic or "").strip()
            tagged["provider"] = "gdelt"
            filtered.append(tagged)
        if filtered:
            results.append({"topic": topic, "articles": filtered})

    return results


__all__ = [
    "fetch_news",
    "fetch_trending_topics",
    "clean_news_data",
    "expand_keywords",
]
