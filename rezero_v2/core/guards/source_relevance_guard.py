from __future__ import annotations

from urllib.parse import urlparse

DEV_DOC_HOSTS = {'docs.python.org', 'github.com', 'stackoverflow.com', 'stackexchange.com'}


class SourceRelevanceGuard:
    def filter_links(self, *, title: str, category: str, source_domain: str, links: list[str]) -> list[str]:
        blob = f"{title} {category}".lower()
        out: list[str] = []
        for link in links:
            host = (urlparse(str(link or '')).netloc or '').lower()
            if not host:
                continue
            if host == str(source_domain or '').lower():
                out.append(link)
                continue
            if host in DEV_DOC_HOSTS and not any(term in blob for term in {'software', 'python', 'developer', 'api', 'app', 'release'}):
                continue
            out.append(link)
        seen = set()
        unique = []
        for link in out:
            if link in seen:
                continue
            seen.add(link)
            unique.append(link)
        return unique
