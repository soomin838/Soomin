from __future__ import annotations

import re
from html import escape
from typing import Any
from urllib.parse import urlparse

from .story_profile import is_relevant_source_domain_for_story


_URL_TEXT_RE = re.compile(r"https?://[^\s<>'\"`]+", flags=re.IGNORECASE)
_SOURCES_H2_RE = re.compile(r"(?is)<h2[^>]*>\s*Sources\s*</h2>")
_SOURCES_BLOCK_RE = re.compile(
    r"(?is)(<h2[^>]*>\s*Sources\s*</h2>)(.*?)(?=(<h2\b[^>]*>)|$)"
)
_ATTR_RE = re.compile(r"(?i)\baccording to(?:\s+reports?)?(?:\s+from)?")
_ATTR_VARIANTS = (
    "Based on",
    "In a statement from",
    "As reported by",
    "From release notes and updates from",
)
_FORBIDDEN_SOURCE_DOMAINS = ("google.com", "googleusercontent.com", "googleapis.com")


def _setting(settings: dict[str, Any] | Any, key: str, default: Any) -> Any:
    if isinstance(settings, dict):
        return settings.get(key, default)
    return getattr(settings, key, default)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _split_body_and_sources(html: str) -> tuple[str, str]:
    src = str(html or "")
    marker = _SOURCES_H2_RE.search(src)
    if not marker:
        return src, ""
    return src[: marker.start()], src[marker.start() :]


def _transform_text_nodes(html: str, transform) -> str:
    parts = re.split(r"(<[^>]+>)", str(html or ""))
    out: list[str] = []
    for part in parts:
        if not part:
            continue
        if part.startswith("<") and part.endswith(">"):
            out.append(part)
            continue
        out.append(transform(part))
    return "".join(out)


def _cleanup_spacing(text: str) -> str:
    out = str(text or "")
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    out = re.sub(r"\(\s+", "(", out)
    out = re.sub(r"\s+\)", ")", out)
    out = re.sub(r"[ \t]{2,}", " ", out)
    return out


def _canonical_http_url(url: str) -> str:
    raw = re.sub(r"\s+", " ", str(url or "")).strip()
    if not raw:
        return ""
    if re.match(r"^https?://", raw, flags=re.IGNORECASE):
        return raw
    if re.match(r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,}([/:?#].*)?$", raw):
        return f"https://{raw}"
    return ""


def extract_domain(url: str) -> str:
    raw = re.sub(r"\s+", " ", str(url or "")).strip()
    if not raw:
        return ""
    parse_target = raw
    if not re.match(r"^[a-z][a-z0-9+.-]*://", parse_target, flags=re.IGNORECASE):
        parse_target = f"https://{parse_target}"
    parsed = urlparse(parse_target)
    host = str(parsed.netloc or "").strip()
    if not host:
        head = str(parsed.path or "").split("/", 1)[0].strip()
        if re.match(r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", head):
            host = head
    host = host.split("@")[-1].split(":")[0].strip().lower()
    host = re.sub(r"^www\d*\.", "", host)
    return host


def compact_source_label(url: str) -> str:
    domain = extract_domain(url)
    return domain or "official source"


def _is_forbidden_source_domain(domain: str) -> bool:
    low = str(domain or "").strip().lower()
    if not low:
        return True
    if low in _FORBIDDEN_SOURCE_DOMAINS:
        return True
    return any(low.endswith("." + bad) for bad in _FORBIDDEN_SOURCE_DOMAINS)


def normalize_inline_attribution(html: str, max_inline: int) -> str:
    limit = max(0, _safe_int(max_inline, 3))
    body, suffix = _split_body_and_sources(html)
    seen = {"count": 0}

    def _replace(text: str) -> str:
        def _sub(match: re.Match[str]) -> str:
            idx = int(seen["count"])
            seen["count"] = idx + 1
            if idx < limit:
                return _ATTR_VARIANTS[idx % len(_ATTR_VARIANTS)]
            return ""

        out = _ATTR_RE.sub(_sub, str(text or ""))
        return _cleanup_spacing(out)

    normalized = _transform_text_nodes(body, _replace)
    normalized = re.sub(r"\s{2,}", " ", normalized)
    return normalized + suffix


def remove_raw_urls_in_body(html: str) -> str:
    body, suffix = _split_body_and_sources(html)

    def _replace_text_node(text: str) -> str:
        raw = str(text or "")

        def _url_sub(match: re.Match[str]) -> str:
            token = str(match.group(0) or "")
            stripped = token.rstrip(").,;:!?")
            trailing = token[len(stripped) :]
            domain = extract_domain(stripped)
            if not domain:
                return "official source" + trailing
            return domain + trailing

        out = _URL_TEXT_RE.sub(_url_sub, raw)
        return _cleanup_spacing(out)

    cleaned = _transform_text_nodes(body, _replace_text_node)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned + suffix


def normalize_sources_section(
    html: str,
    source_url: str,
    authority_links: list[str],
    max_items: int,
    require: bool,
    title: str,
    snippet: str,
    category: str,
    topic: str,
) -> str:
    src = str(html or "")
    max_items = max(1, _safe_int(max_items, 6))
    block = _SOURCES_BLOCK_RE.search(src)
    existing_urls: list[str] = []
    if block:
        section = str(block.group(2) or "")
        for href in re.findall(r'href=["\']([^"\']+)["\']', section, flags=re.IGNORECASE):
            if str(href or "").strip():
                existing_urls.append(str(href).strip())
        for raw in _URL_TEXT_RE.findall(section):
            if str(raw or "").strip():
                existing_urls.append(str(raw).strip())

    queue: list[str] = []
    if str(source_url or "").strip():
        queue.append(str(source_url).strip())
    queue.extend([str(x).strip() for x in (authority_links or []) if str(x).strip()])
    queue.extend(existing_urls)

    items: list[tuple[str, str]] = []
    seen_domains: set[str] = set()
    for row in queue:
        url = _canonical_http_url(row)
        if not url:
            continue
        domain = extract_domain(url)
        if not domain:
            continue
        if _is_forbidden_source_domain(domain):
            continue
        if source_url and url == _canonical_http_url(source_url):
            pass
        elif not is_relevant_source_domain_for_story(
            url,
            title=title,
            snippet=snippet,
            category=category,
            topic=topic,
        ):
            continue
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        items.append((compact_source_label(url), url))
        if len(items) >= max_items:
            break

    if not items and (not require):
        return src
    if not items and require:
        placeholder_url = _canonical_http_url(source_url)
        if placeholder_url:
            items.append((compact_source_label(placeholder_url), placeholder_url))
        else:
            fallback = _canonical_http_url(authority_links[0]) if authority_links else ""
            if fallback:
                items.append((compact_source_label(fallback), fallback))

    list_items: list[str] = []
    for label, url in items[:max_items]:
        list_items.append(
            f'<li><a href="{escape(url)}" rel="nofollow noopener" target="_blank">{escape(label)}</a></li>'
        )
    if (not list_items) and require:
        list_items.append("<li>official source</li>")

    sources_html = "<h2>Sources</h2><ul>" + "".join(list_items) + "</ul>"
    if block:
        return src[: block.start()] + sources_html + src[block.end() :]
    if not require:
        return src
    return src.rstrip() + "\n" + sources_html


def apply_source_naturalization(
    *,
    html: str,
    source_url: str,
    authority_links: list[str],
    settings: dict[str, Any] | Any,
    title: str = "",
    snippet: str = "",
    category: str = "",
    topic: str = "",
) -> str:
    src = str(html or "")
    try:
        allow_raw_urls = bool(_setting(settings, "allow_raw_urls_in_body", False))
        max_inline = max(0, _safe_int(_setting(settings, "max_inline_attributions_per_article", 3), 3))
        max_items = max(1, _safe_int(_setting(settings, "max_sources_list_items", 6), 6))
        require_sources = bool(_setting(settings, "require_sources_section", True))

        out = src
        if not allow_raw_urls:
            out = remove_raw_urls_in_body(out)
        out = normalize_inline_attribution(out, max_inline=max_inline)
        out = normalize_sources_section(
            out,
            source_url=source_url,
            authority_links=list(authority_links or []),
            max_items=max_items,
            require=require_sources,
            title=str(title or ""),
            snippet=str(snippet or ""),
            category=str(category or ""),
            topic=str(topic or ""),
        )
        return out
    except Exception:
        return src
