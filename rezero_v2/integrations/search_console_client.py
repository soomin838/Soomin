from __future__ import annotations

from pathlib import Path
from typing import Any

from re_core.insights import GrowthInsights, InsightsSettingsView


class SearchConsoleClient:
    def __init__(self, *, credentials_path: Path, site_url: str, enabled: bool = True) -> None:
        self.credentials_path = Path(credentials_path).resolve()
        self.site_url = str(site_url or '').strip()
        self.enabled = bool(enabled)
        self._client = GrowthInsights(credentials_path=self.credentials_path, settings=InsightsSettingsView(enabled=True, adsense_enabled=False, analytics_enabled=False, search_console_enabled=self.enabled, ga4_property_id='', search_console_site_url=self.site_url))

    def fetch_rows(self, start_date: str, end_date: str, dimensions: tuple[str, ...] = ('query', 'page'), page_size: int = 250, max_rows: int = 50000) -> list[dict[str, Any]]:
        if not self.enabled or not self.site_url:
            return []
        return list(self._client.fetch_search_console_rows(start_date, end_date, dimensions=dimensions, page_size=page_size, max_rows=max_rows) or [])

    def discover_opportunities(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            impressions = float((row or {}).get('impressions', 0.0) or 0.0)
            clicks = float((row or {}).get('clicks', 0.0) or 0.0)
            ctr = float((row or {}).get('ctr', 0.0) or 0.0)
            position = float((row or {}).get('position', 0.0) or 0.0)
            query = str((row or {}).get('query', '') or '').strip()
            if not query or impressions < 50:
                continue
            action = ''
            if 5 <= position <= 15 and ctr < 0.03:
                action = 'title_rewrite'
            if 5 <= position <= 15 and ctr >= 0.01:
                action = 'supporting_post'
            if impressions >= 200 and clicks == 0:
                action = 'intent_fix'
            if not action:
                continue
            out.append({'query': query, 'page': str((row or {}).get('page', '') or '').strip(), 'impressions': impressions, 'clicks': clicks, 'ctr': ctr, 'position': position, 'action': action})
        return out
