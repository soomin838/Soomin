from __future__ import annotations

import re
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.source_naturalization import apply_source_naturalization, extract_domain, normalize_sources_section  # noqa: E402


DEFAULT_SETTINGS = {
    "enabled": True,
    "max_inline_attributions_per_article": 3,
    "allow_raw_urls_in_body": False,
    "max_sources_list_items": 6,
    "require_sources_section": True,
}


def _body_without_sources(html: str) -> str:
    src = str(html or "")
    marker = re.search(r"(?is)<h2[^>]*>\s*Sources\s*</h2>", src)
    if not marker:
        return src
    return src[: marker.start()]


def _sources_block(html: str) -> str:
    src = str(html or "")
    match = re.search(
        r"(?is)<h2[^>]*>\s*Sources\s*</h2>(.*?)(?=(<h2\b[^>]*>)|$)",
        src,
    )
    return str(match.group(1) or "") if match else ""


def validate_raw_url_replacement() -> str:
    html = (
        "<h2>Quick Take</h2>"
        "<p>Check https://example.com/path?x=1 and https://support.google.com/a/docs for updates.</p>"
    )
    cfg = dict(DEFAULT_SETTINGS)
    cfg["require_sources_section"] = False
    out = apply_source_naturalization(
        html=html,
        source_url="",
        authority_links=[],
        settings=cfg,
    )
    body = _body_without_sources(out)
    if re.search(r"https?://", body, flags=re.IGNORECASE):
        raise AssertionError("Case1 failed: raw URL text remained in body.")
    if ("example.com" not in body) and ("support.google.com" not in body):
        raise AssertionError("Case1 failed: URL should be compacted to domain label.")
    return body


def validate_inline_attribution_cap() -> int:
    html = (
        "<h2>What Happened</h2>"
        "<p>According to Microsoft, patching started early. "
        "According to reports from CISA, scope widened. "
        "According to Apple, fixes are rolling out. "
        "According to Google, new checks are live. "
        "According to AWS, monitoring remains active.</p>"
    )
    cfg = dict(DEFAULT_SETTINGS)
    cfg["allow_raw_urls_in_body"] = True
    cfg["require_sources_section"] = False
    cfg["max_inline_attributions_per_article"] = 3
    out = apply_source_naturalization(
        html=html,
        source_url="",
        authority_links=[],
        settings=cfg,
    )
    count = len(re.findall(r"(?i)\baccording to\b", out))
    if count > 3:
        raise AssertionError(f"Case2 failed: 'According to' count should be <=3, got {count}.")
    return count


def validate_sources_section_required() -> bool:
    html = "<h2>Quick Take</h2><p>Short summary.</p>"
    out = normalize_sources_section(
        html,
        source_url="https://openai.com/blog",
        authority_links=["https://support.google.com"],
        max_items=6,
        require=True,
    )
    return bool(re.search(r"(?is)<h2[^>]*>\s*Sources\s*</h2>", out))


def validate_sources_max_items() -> int:
    html = "<h2>Quick Take</h2><p>Summary text.</p>"
    links = [
        "https://a.example.com",
        "https://b.example.com",
        "https://c.example.com",
        "https://d.example.com",
        "https://e.example.com",
        "https://f.example.com",
        "https://g.example.com",
        "https://h.example.com",
    ]
    out = normalize_sources_section(
        html,
        source_url="https://origin.example.com/news",
        authority_links=links,
        max_items=6,
        require=True,
    )
    section = _sources_block(out)
    count = len(re.findall(r"(?is)<li\b[^>]*>", section))
    if count > 6:
        raise AssertionError(f"Case4 failed: sources items exceed max 6 (got {count}).")
    return count


def validate_domain_dedup() -> tuple[int, int]:
    html = "<h2>Quick Take</h2><p>Summary text.</p><h2>Sources</h2><ul><li>old</li></ul>"
    out = normalize_sources_section(
        html,
        source_url="https://www.openai.com/blog",
        authority_links=[
            "https://openai.com/research",
            "https://support.google.com/a",
            "https://www.google.com/security",
            "https://support.google.com/docs",
        ],
        max_items=6,
        require=True,
    )
    section = _sources_block(out)
    hrefs = re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', section)
    domains = [extract_domain(x) for x in hrefs if extract_domain(x)]
    if len(domains) != len(set(domains)):
        raise AssertionError(f"Case5 failed: duplicate domains remained in sources ({domains}).")
    return len(hrefs), len(set(domains))


def main() -> int:
    body = validate_raw_url_replacement()
    according_to_count = validate_inline_attribution_cap()
    has_sources = validate_sources_section_required()
    sources_count = validate_sources_max_items()
    href_count, domain_count = validate_domain_dedup()
    if not has_sources:
        raise AssertionError("Case3 failed: Sources section was not created when required.")
    print("Case 1 OK: raw URL text in body is replaced with domain labels")
    print(f"  body_preview={body[:120]}")
    print("Case 2 OK: repeated 'According to' is capped by max_inline")
    print(f"  according_to_count={according_to_count}")
    print("Case 3 OK: Sources section is created when required")
    print(f"  has_sources={has_sources}")
    print("Case 4 OK: Sources list items are capped at max_sources_list_items")
    print(f"  sources_count={sources_count}")
    print("Case 5 OK: domain duplicates are removed in Sources section")
    print(f"  href_count={href_count}, unique_domains={domain_count}")
    print("Stage-8 source naturalization validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

