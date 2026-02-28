from __future__ import annotations

import random
import re
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


class ThumbnailOverlayRenderer:
    def __init__(
        self,
        *,
        style: str = "yt_clean",
        font_paths: list[str] | None = None,
        max_words: int = 3,
    ) -> None:
        self.style = str(style or "yt_clean").strip().lower()
        self.font_paths = [str(x or "").strip() for x in (font_paths or []) if str(x or "").strip()]
        self.max_words = max(1, int(max_words or 3))

    def render(
        self,
        *,
        source_path: Path,
        hook_text: str,
        tag_label: str,
        output_path: Path,
    ) -> Path:
        src = Path(source_path).resolve()
        dst = Path(output_path).resolve()
        dst.parent.mkdir(parents=True, exist_ok=True)
        text = self._normalize_hook(hook_text)
        label = self._normalize_label(tag_label)

        image = Image.open(src).convert("RGBA")
        w, h = image.size
        panel_ratio = random.uniform(0.28, 0.38)
        panel_h = int(h * panel_ratio)
        panel_top = h - panel_h

        overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        for idx in range(panel_h):
            alpha = int(24 + (206 * (idx / max(1, panel_h - 1))))
            draw.rectangle([(0, panel_top + idx), (w, panel_top + idx + 1)], fill=(10, 14, 26, alpha))
        image = Image.alpha_composite(image, overlay)
        draw = ImageDraw.Draw(image)

        title_font = self._pick_font(max(36, int(w * random.uniform(0.066, 0.082))))
        label_font = self._pick_font(max(18, int(w * 0.023)))
        x = int(w * 0.05)
        y = int(h * random.uniform(0.73, 0.78))
        for ox, oy in ((-2, 0), (2, 0), (0, -2), (0, 2)):
            draw.text((x + ox, y + oy), text, font=title_font, fill=(0, 0, 0, 230))
        draw.text((x, y), text, font=title_font, fill=(255, 255, 255, 255))

        label_w = int(w * 0.22)
        label_h = int(h * 0.08)
        lx = int(w * 0.05)
        ly = int(h * 0.64)
        draw.rounded_rectangle(
            [(lx, ly), (lx + label_w, ly + label_h)],
            radius=max(8, int(label_h * 0.25)),
            fill=(28, 90, 184, 210),
            outline=(170, 215, 255, 220),
            width=2,
        )
        draw.text((lx + int(label_w * 0.1), ly + int(label_h * 0.22)), label, font=label_font, fill=(242, 248, 255, 255))
        image.convert("RGB").save(dst, format="PNG", optimize=True)
        return dst

    def _pick_font(self, size: int):
        candidates = list(self.font_paths) + [
            "C:/Windows/Fonts/segoeuib.ttf",
            "C:/Windows/Fonts/arialbd.ttf",
            "C:/Windows/Fonts/verdanab.ttf",
        ]
        for path in candidates:
            try:
                return ImageFont.truetype(path, max(12, int(size)))
            except Exception:
                continue
        return ImageFont.load_default()

    def _normalize_hook(self, text: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9\s]", " ", str(text or "").upper())
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        words = cleaned.split()[: self.max_words]
        out = " ".join(words).strip()
        return out or "TECH UPDATE"

    def _normalize_label(self, text: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9\s]", " ", str(text or "").upper())
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if not cleaned:
            return "TECH NEWS"
        return cleaned[:18]

