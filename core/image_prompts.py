from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class PromptPack:
    category: str
    prompt: str
    negative: str


NEGATIVE_DEFAULT = (
    "no text, no letters, no numbers, no logos, no watermark, "
    "no brand names, no UI text, "
    "no fire, no smoke, no explosion, no hazard, no injury, "
    "no physical damage, no broken hardware, no cracked screen"
)

STYLE_DEFAULT = (
    "flat vector illustration, soft pastel palette, rounded shapes, minimal shading, "
    "clean modern UI-inspired design, subtle gradient background, centered composition"
)


def month_primary_category(rotation_order: list[str] | None = None, month: int | None = None) -> str:
    order = [x.strip().lower() for x in (rotation_order or ["windows", "mac", "iphone", "galaxy"]) if x.strip()]
    if not order:
        order = ["windows", "mac", "iphone", "galaxy"]
    m = month if month is not None else datetime.now(ET).month
    return order[(m - 1) % len(order)]


def vector_prompt_for_category(category: str) -> PromptPack:
    c = (category or "generic").strip().lower()

    if c == "windows":
        prompt = (
            f"{STYLE_DEFAULT}. "
            "Scene: a desktop monitor with floating settings panels, toggle switches, and a neutral error indicator icon. "
            "Software troubleshooting context only."
        )
        return PromptPack("windows", prompt, NEGATIVE_DEFAULT)

    if c == "mac":
        prompt = (
            f"{STYLE_DEFAULT}. "
            "Scene: a laptop with floating system settings panels, WiFi/audio icons, and a small neutral warning badge. "
            "Software troubleshooting context only."
        )
        return PromptPack("mac", prompt, NEGATIVE_DEFAULT)

    if c == "iphone":
        prompt = (
            f"{STYLE_DEFAULT}. "
            "Scene: a smartphone with abstract settings panels, toggles, and connectivity icons. "
            "Software troubleshooting context only."
        )
        return PromptPack("iphone", prompt, NEGATIVE_DEFAULT)

    if c == "galaxy":
        prompt = (
            f"{STYLE_DEFAULT}. "
            "Scene: an android phone with abstract settings panels, bluetooth/wifi icons, and a neutral error badge. "
            "Software troubleshooting context only."
        )
        return PromptPack("galaxy", prompt, NEGATIVE_DEFAULT)

    prompt = (
        f"{STYLE_DEFAULT}. "
        "Scene: a generic device with floating settings panels and a neutral error indicator. "
        "Software troubleshooting context only."
    )
    return PromptPack("generic", prompt, NEGATIVE_DEFAULT)
