from __future__ import annotations

import email.utils
import re
from datetime import datetime, timezone
from html import unescape
from typing import Any
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import requests


def _strip_html(text: str) -> str:
    plain = re.sub(r"<[^>]+>", " ", str(text or ""))
    plain = unescape(plain)
    plain = re.sub(r"\s+", " ", plain).strip()
    return plain


def _first_text(node: ET.Element | None, tags: list[str]) -> str:
    if node is None:
        return ""
    for tag in tags:
        child = node.find(tag)
        if child is not None:
            txt = "".join(child.itertext()).strip()
            if txt:
                return txt
    return ""


def _parse_dt(raw: str) -> datetime | None:
    value = re.sub(r"\s+", " ", str(raw or "").strip())
    if not value:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(value)
        if dt is not None:
            return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_feed_xml(xml_text: str, feed_url: str = "") -> list[dict[str, Any]]:
    text = str(xml_text or "").strip()
    if not text:
        return []
    try:
        root = ET.fromstring(text)
    except Exception:
        return []

    parsed: list[dict[str, Any]] = []
    source_host = (urlparse(str(feed_url or "")).netloc or "").lower()

    # RSS2
    rss_items = root.findall(".//item")
    for item in rss_items:
        title = _strip_html(_first_text(item, ["title"]))[:220]
        link = _first_text(item, ["link"]).strip()
        snippet = _strip_html(_first_text(item, ["description", "summary", "content:encoded"]))[:380]
        published_raw = _first_text(item, ["pubDate", "published", "updated", "dc:date"])
        published_at = _parse_dt(published_raw)
        if not title or not link:
            continue
        parsed.append(
            {
                "url": link,
                "title": title,
                "snippet": snippet,
                "published_at": published_at,
                "source": source_host or (urlparse(link).netloc or "").lower(),
            }
        )

    # Atom
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
    }
    atom_entries = root.findall(".//atom:entry", ns) or root.findall(".//entry")
    for entry in atom_entries:
        title = _strip_html(_first_text(entry, ["atom:title", "title"]))[:220]
        link = ""
        for tag in ["atom:link", "link"]:
            for node in entry.findall(tag, ns):
                href = str(node.attrib.get("href", "") or "").strip()
                rel = str(node.attrib.get("rel", "alternate") or "alternate").strip().lower()
                if href and rel in {"alternate", ""}:
                    link = href
                    break
            if link:
                break
        snippet = _strip_html(_first_text(entry, ["atom:summary", "summary", "atom:content", "content"]))[:380]
        published_raw = _first_text(entry, ["atom:published", "published", "atom:updated", "updated"])
        published_at = _parse_dt(published_raw)
        if not title or not link:
            continue
        parsed.append(
            {
                "url": link,
                "title": title,
                "snippet": snippet,
                "published_at": published_at,
                "source": source_host or (urlparse(link).netloc or "").lower(),
            }
        )

    # Deduplicate by URL
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in parsed:
        url = str(row.get("url", "") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(row)
    return out


def fetch_feed(feed_url: str, timeout: int = 20) -> list[dict[str, Any]]:
    url = str(feed_url or "").strip()
    if not url:
        return []
    try:
        res = requests.get(url, timeout=max(5, int(timeout)), headers={"User-Agent": "RezeroAgent-NewsPool/1.0"})
        res.raise_for_status()
        return parse_feed_xml(res.text, feed_url=url)
    except Exception:
        return []


def fetch_feed_detailed(feed_url: str, timeout: int = 20) -> dict[str, Any]:
    url = str(feed_url or "").strip()
    if not url:
        return {
            "ok": False,
            "status_code": 0,
            "error": "empty_feed_url",
            "items": [],
            "feed_url": "",
        }
    try:
        res = requests.get(
            url,
            timeout=max(5, int(timeout)),
            headers={"User-Agent": "RezeroAgent-NewsPool/1.0"},
        )
        status = int(getattr(res, "status_code", 0) or 0)
        if status != 200:
            return {
                "ok": False,
                "status_code": status,
                "error": f"http_{status}",
                "items": [],
                "feed_url": url,
                "response_preview": str(getattr(res, "text", "") or "")[:220],
            }
        items = parse_feed_xml(str(getattr(res, "text", "") or ""), feed_url=url)
        return {
            "ok": True,
            "status_code": status,
            "error": "",
            "items": items,
            "feed_url": url,
        }
    except requests.Timeout:
        return {
            "ok": False,
            "status_code": 0,
            "error": "timeout",
            "items": [],
            "feed_url": url,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": 0,
            "error": str(exc)[:180] or "request_failed",
            "items": [],
            "feed_url": url,
        }
