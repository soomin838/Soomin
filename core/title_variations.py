from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class TitleContext:
    keyword: str
    device: str
    cluster: str
    year: str = "2026"


_TEMPLATES = [
    "{kw}: 7 fixes in {device} ({year})",
    "{kw}: what to try first in {device}",
    "{kw}: the checklist that actually works",
    "{kw}: fix it in under 10 minutes",
    "{kw}: 5 fast fixes before reinstalling",
    "{kw}: beginner-safe repair steps",
    "{kw}: fix order from fastest to deepest",
    "{kw}: practical fixes for everyday users",
    "{kw}: stop the loop with these 6 checks",
    "{kw}: after-update recovery guide ({year})",
    "{kw}: error-focused fix flow",
    "{kw}: quick wins, then advanced checks",
    "{kw}: what works on {device} now",
    "{kw}: fix guide with expected results",
    "{kw}: safe troubleshooting playbook",
    "{kw}: 8 checks that prevent repeat failures",
    "{kw}: no-fluff fix steps ({year})",
    "{kw}: fix sequence for stable results",
    "{kw}: fastest path to recovery",
    "{kw}: when to reset, when to reinstall",
    "{kw}: fix checklist for {cluster}",
    "{kw}: avoid common mistakes while fixing",
    "{kw}: practical diagnosis and repair",
    "{kw}: field-tested recovery order",
    "{kw}: complete troubleshooting checklist",
    "{kw}: what to skip and what to do first",
]


def title_fingerprint(title: str) -> str:
    t = str(title or "").lower()
    t = re.sub(r"\b(2024|2025|2026)\b", "", t)
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return hashlib.sha1(t.encode("utf-8")).hexdigest()


def render_title(ctx: TitleContext, attempt: int = 0) -> str:
    kw = re.sub(r"\s+", " ", str(ctx.keyword or "").strip())
    kw = re.sub(r"[?]+$", "", kw).strip()
    if not kw:
        kw = f"{ctx.device} not working"
    seed = f"{kw}|{ctx.device}|{ctx.cluster}|{ctx.year}|{attempt}".encode("utf-8")
    idx = int(hashlib.sha1(seed).hexdigest(), 16) % len(_TEMPLATES)
    title = _TEMPLATES[idx].format(
        kw=kw,
        device=str(ctx.device or "Windows").title(),
        cluster=str(ctx.cluster or "general").replace("_", " "),
        year=str(ctx.year or "2026"),
    )
    title = re.sub(r"\s+", " ", title).strip()
    return title[:120]

