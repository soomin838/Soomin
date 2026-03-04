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
    "{kw}: The Strategic Impact and Future Outlook ({year})",
    "{kw}: Inside the Major Industry Shift This Week",
    "{kw}: Why This Recent Announcement Matters for Everyone",
    "{kw}: 5 Key Takeaways from the Latest Intelligence Report",
    "{kw}: Decoding the Strategic Roadmap for {year}",
    "{kw}: The Unfiltered Truth Behind the Latest Move",
    "{kw}: Analysis: Who Wins and Who Loses in This Shift",
    "{kw}: Breaking Down the Technical Milestones in {device}",
    "{kw}: Why the Experts are Divided on This Development",
    "{kw}: Strategic Insights: Predicting the Next {year} Trends",
    "{kw}: The Intersection of Innovation and Market Stability",
    "{kw}: Behind the Scenes of the {kw} Breakthrough",
    "{kw}: What this Latest {device} Feature Means for Users",
    "{kw}: A Deep Dive into the {cluster} Ecosystem Changes",
    "{kw}: Leading the Charge: How {kw} is Redefining Tech",
    "{kw}: The Roadmap to 2027: Leaks, Rumors, and Facts",
    "{kw}: Essential Analysis for Tech Decision Makers",
    "{kw}: Why the Current Momentum for {kw} is Unstoppable",
    "{kw}: Navigating the Complexities of the Latest Update",
    "{kw}: Technical Editorial: The Future of {kw} on {device}",
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

