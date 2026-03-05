"""World Monitor API Client.

Fetches real-time news from the World Monitor public API
(api.worldmonitor.app) to replace the legacy RSS-based news_pool system.
"""
from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.worldmonitor.app"

# Category lists from World Monitor docs
TECH_CATEGORIES = [
    "technology", "cybersecurity", "ai", "software",
]
GEOPOLITICS_CATEGORIES = [
    "geopolitics", "defense", "military", "diplomacy",
]
FINANCE_CATEGORIES = [
    "finance", "markets", "economy", "energy",
]
ALL_CATEGORIES = TECH_CATEGORIES + GEOPOLITICS_CATEGORIES + FINANCE_CATEGORIES


class WorldMonitorClient:
    """Lightweight REST client for api.worldmonitor.app."""

    def __init__(
        self,
        *,
        timeout: int = 15,
        cache_ttl_sec: int = 900,  # 15 min, matches server cache
    ) -> None:
        self._timeout = max(5, int(timeout))
        self._cache_ttl = max(60, int(cache_ttl_sec))
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "RezeroAgent/2.0",
            "Accept": "application/json",
        })
        # Simple in-memory cache: key -> (timestamp, data)
        self._cache: dict[str, tuple[float, Any]] = {}

    def _get_cached(self, key: str) -> Any | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts, data = entry
        if time.time() - ts > self._cache_ttl:
            del self._cache[key]
            return None
        return data

    def _set_cache(self, key: str, data: Any) -> None:
        self._cache[key] = (time.time(), data)

    def _api_get(self, path: str, params: dict | None = None) -> Any:
        """Make a GET request to the World Monitor API."""
        url = urljoin(BASE_URL + "/", path.lstrip("/"))
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.Timeout:
            logger.warning("WorldMonitor API timeout: %s", path)
            return None
        except requests.RequestException as exc:
            logger.warning("WorldMonitor API error: %s — %s", path, exc)
            return None
        except Exception as exc:
            logger.warning("WorldMonitor API unexpected error: %s — %s", path, exc)
            return None

    def fetch_feed_digest(self, *, categories: list[str] | None = None) -> list[dict[str, Any]]:
        """Fetch the aggregated RSS feed digest from World Monitor.

        Returns a list of news items with keys like:
            title, link, snippet, source, category, publishedAt
        """
        cache_key = "feed_digest"
        cached = self._get_cached(cache_key)
        if cached is not None:
            items = cached
        else:
            data = self._api_get("/api/rss/v1/listFeedDigest")
            if data is None:
                return []
            # Normalize response — could be a dict with categories or a flat list
            items = []
            if isinstance(data, dict):
                for cat_key, cat_items in data.items():
                    if isinstance(cat_items, list):
                        for item in cat_items:
                            if isinstance(item, dict):
                                item.setdefault("category", str(cat_key))
                                items.append(item)
            elif isinstance(data, list):
                items = [x for x in data if isinstance(x, dict)]
            self._set_cache(cache_key, items)

        # Filter by categories if requested
        if categories:
            cat_set = {c.lower().strip() for c in categories if c}
            items = [
                item for item in items
                if str(item.get("category", "")).lower().strip() in cat_set
            ]

        return items

    def fetch_trending_keywords(self) -> list[dict[str, Any]]:
        """Fetch trending keyword spikes."""
        cache_key = "trending"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        data = self._api_get("/api/rss/v1/listTrendingKeywords")
        if not isinstance(data, list):
            data = []
        self._set_cache(cache_key, data)
        return data

    def search_relevant_news(
        self,
        *,
        topic: str = "",
        category: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search for news relevant to a given topic/category.

        This fetches the full feed digest and filters client-side
        based on topic keywords and category.
        """
        categories = None
        if category:
            categories = [category]

        items = self.fetch_feed_digest(categories=categories)

        # Simple keyword matching if topic is provided
        if topic:
            topic_lower = topic.lower()
            keywords = [w.strip() for w in topic_lower.split() if len(w.strip()) > 2]
            if keywords:
                scored = []
                for item in items:
                    text = f"{item.get('title', '')} {item.get('snippet', '')}".lower()
                    score = sum(1 for kw in keywords if kw in text)
                    if score > 0:
                        scored.append((score, item))
                scored.sort(key=lambda x: x[0], reverse=True)
                items = [item for _, item in scored]

        return items[:max(1, limit)]

    def get_news_summary_for_prompt(
        self,
        *,
        category: str = "technology",
        limit: int = 15,
    ) -> str:
        """Get a formatted summary of recent news for use in LLM prompts.

        Returns a plain text block with numbered recent headlines+snippets.
        """
        items = self.fetch_feed_digest(categories=[category] if category else None)
        items = items[:max(1, limit)]

        if not items:
            return "(No real-time news available from World Monitor)"

        lines = ["[Real-Time News Feed from World Monitor]"]
        for i, item in enumerate(items, 1):
            title = str(item.get("title", "")).strip()
            snippet = str(item.get("snippet", "")).strip()[:200]
            source = str(item.get("source", "")).strip()
            link = str(item.get("link", "") or item.get("url", "")).strip()
            line = f"{i}. [{source}] {title}"
            if snippet:
                line += f"\n   {snippet}"
            if link:
                line += f"\n   → {link}"
            lines.append(line)

        return "\n".join(lines)

    def health_check(self) -> bool:
        """Quick check if the World Monitor API is reachable."""
        try:
            resp = self._session.get(
                f"{BASE_URL}/api/rss/v1/listFeedDigest",
                timeout=5,
            )
            return resp.status_code == 200
        except Exception:
            return False
