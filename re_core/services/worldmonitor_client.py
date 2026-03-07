"""World Monitor API client with authenticated API-first, legacy fallback behavior."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests

from re_core.settings import WorldMonitorSettings

logger = logging.getLogger(__name__)

BASE_URL = "https://api.worldmonitor.app"


@dataclass(frozen=True)
class WorldMonitorStatus:
    ok: bool
    status_code: int
    source: str
    auth_mode: str
    note: str = ""


class WorldMonitorClient:
    """REST client for WorldMonitor with API key auth and legacy fallback."""

    def __init__(
        self,
        settings: WorldMonitorSettings | None = None,
        *,
        timeout: int | None = None,
        cache_ttl_sec: int = 900,
    ) -> None:
        self.settings = settings or WorldMonitorSettings()
        self._timeout = max(5, int(timeout or getattr(self.settings, "timeout_sec", 15) or 15))
        self._cache_ttl = max(60, int(cache_ttl_sec))
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "RezeroAgent/2.0",
                "Accept": "application/json",
            }
        )
        self._cache: dict[str, tuple[float, Any]] = {}
        self.last_status: dict[str, WorldMonitorStatus] = {}

    @property
    def api_key(self) -> str:
        return str(getattr(self.settings, "api_key", "") or "").strip()

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

    def _status(self, name: str, status_code: int, *, ok: bool, source: str, auth_mode: str, note: str = "") -> None:
        self.last_status[name] = WorldMonitorStatus(
            ok=bool(ok),
            status_code=int(status_code),
            source=str(source or ""),
            auth_mode=str(auth_mode or ""),
            note=str(note or "")[:180],
        )

    def _request(self, path: str, *, auth_mode: str = "none", name: str = "") -> Any | None:
        url = urljoin(BASE_URL + "/", path.lstrip("/"))
        headers = dict(self._session.headers)
        params: dict[str, str] = {}
        if auth_mode == "header" and self.api_key:
            headers["x-api-key"] = self.api_key
        elif auth_mode == "query" and self.api_key:
            params["api-key"] = self.api_key
        try:
            resp = self._session.get(url, params=params or None, headers=headers, timeout=self._timeout)
        except requests.Timeout:
            self._status(name or path, 0, ok=False, source=path, auth_mode=auth_mode, note="timeout")
            logger.warning("WorldMonitor timeout path=%s auth=%s", path, auth_mode)
            return None
        except requests.RequestException as exc:
            code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
            self._status(name or path, code, ok=False, source=path, auth_mode=auth_mode, note=str(exc))
            logger.warning("WorldMonitor request error path=%s auth=%s status=%s err=%s", path, auth_mode, code, exc)
            return None
        status_code = int(resp.status_code or 0)
        if status_code != 200:
            note = resp.text[:160] if resp.text else ""
            self._status(name or path, status_code, ok=False, source=path, auth_mode=auth_mode, note=note)
            logger.info("WorldMonitor non-200 path=%s auth=%s status=%s", path, auth_mode, status_code)
            return None
        try:
            payload = resp.json()
        except Exception as exc:
            self._status(name or path, status_code, ok=False, source=path, auth_mode=auth_mode, note=f"json_error:{exc}")
            logger.warning("WorldMonitor json parse error path=%s auth=%s err=%s", path, auth_mode, exc)
            return None
        self._status(name or path, status_code, ok=True, source=path, auth_mode=auth_mode)
        return payload

    def _normalize_items(self, data: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        if isinstance(data, dict):
            if isinstance(data.get("items"), list):
                items = [x for x in data.get("items", []) if isinstance(x, dict)]
            else:
                for cat_key, cat_items in data.items():
                    if isinstance(cat_items, list):
                        for item in cat_items:
                            if isinstance(item, dict):
                                item.setdefault("category", str(cat_key))
                                items.append(item)
        elif isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
        normalized: list[dict[str, Any]] = []
        for item in items:
            clean = dict(item)
            if "url" not in clean and clean.get("link"):
                clean["url"] = clean.get("link")
            if "link" not in clean and clean.get("url"):
                clean["link"] = clean.get("url")
            normalized.append(clean)
        return normalized

    def _fetch_with_fallback(self, *, name: str, new_path: str, legacy_path: str) -> list[dict[str, Any]]:
        cache_key = f"{name}:all"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return list(cached)

        attempts: list[tuple[str, str]] = []
        prefer_api = bool(getattr(self.settings, "prefer_api", True))
        if prefer_api and self.api_key:
            attempts.extend([("header", new_path), ("query", new_path)])
        elif prefer_api:
            self._status(name, 401, ok=False, source=new_path, auth_mode="missing_key", note="api_key_missing")
        attempts.append(("legacy", legacy_path))

        for auth_mode, path in attempts:
            data = self._request(path, auth_mode=("none" if auth_mode == "legacy" else auth_mode), name=name)
            items = self._normalize_items(data)
            if items:
                self._set_cache(cache_key, items)
                return items
        return []

    def fetch_feed_digest(self, *, categories: list[str] | None = None) -> list[dict[str, Any]]:
        items = self._fetch_with_fallback(
            name="feed_digest",
            new_path="/api/news/v1/list-feed-digest",
            legacy_path="/api/rss/v1/listFeedDigest",
        )
        if categories:
            cat_set = {c.lower().strip() for c in categories if c}
            items = [
                item
                for item in items
                if str(item.get("category", "")).lower().strip() in cat_set
            ]
        return items

    def fetch_trending_keywords(self) -> list[dict[str, Any]]:
        return self._fetch_with_fallback(
            name="trending_keywords",
            new_path="/api/news/v1/list-trending-keywords",
            legacy_path="/api/rss/v1/listTrendingKeywords",
        )

    def search_relevant_news(self, *, topic: str = "", category: str = "", limit: int = 20) -> list[dict[str, Any]]:
        categories = [category] if category else None
        items = self.fetch_feed_digest(categories=categories)
        if topic:
            keywords = [w.strip().lower() for w in str(topic or "").split() if len(w.strip()) > 2]
            scored: list[tuple[int, dict[str, Any]]] = []
            for item in items:
                text = f"{item.get('title', '')} {item.get('snippet', '')}".lower()
                score = sum(1 for kw in keywords if kw in text)
                if score > 0:
                    scored.append((score, item))
            scored.sort(key=lambda row: row[0], reverse=True)
            items = [item for _, item in scored]
        return items[: max(1, int(limit))]

    def get_news_summary_for_prompt(self, *, category: str = "technology", limit: int = 15) -> str:
        items = self.fetch_feed_digest(categories=[category] if category else None)[: max(1, int(limit))]
        if not items:
            return "(No real-time news available from World Monitor)"
        lines = ["[Real-Time News Feed from World Monitor]"]
        for idx, item in enumerate(items, start=1):
            title = str(item.get("title", "")).strip()
            snippet = str(item.get("snippet", "")).strip()[:200]
            source = str(item.get("source", "")).strip()
            link = str(item.get("link", "") or item.get("url", "")).strip()
            line = f"{idx}. [{source}] {title}"
            if snippet:
                line += f"\n   {snippet}"
            if link:
                line += f"\n   {link}"
            lines.append(line)
        return "\n".join(lines)

    def health_check(self) -> bool:
        _ = self.fetch_feed_digest(categories=None)
        status = self.last_status.get("feed_digest")
        return bool(status and status.ok)
