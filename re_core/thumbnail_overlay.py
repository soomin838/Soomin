from __future__ import annotations

import re
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


_LABEL_STYLES = {
    "AI": {
        "fill": (194, 98, 34, 232),
        "outline": (244, 188, 140, 255),
        "text": (255, 246, 238, 255),
    },
    "POLICY": {
        "fill": (126, 84, 214, 232),
        "outline": (208, 186, 255, 255),
        "text": (249, 243, 255, 255),
    },
    "PRIVACY": {
        "fill": (51, 118, 188, 232),
        "outline": (159, 212, 255, 255),
        "text": (244, 250, 255, 255),
    },
    "SECURITY": {
        "fill": (171, 52, 48, 232),
        "outline": (248, 181, 172, 255),
        "text": (255, 243, 241, 255),
    },
    "PLATFORM": {
        "fill": (107, 73, 203, 232),
        "outline": (196, 173, 255, 255),
        "text": (248, 244, 255, 255),
    },
    "CHIPS": {
        "fill": (27, 122, 92, 232),
        "outline": (167, 234, 210, 255),
        "text": (241, 255, 250, 255),
    },
    "NEWS": {
        "fill": (56, 78, 113, 232),
        "outline": (167, 191, 227, 255),
        "text": (245, 248, 255, 255),
    },
}

_LABEL_MAP = {
    "ai": "AI",
    "policy": "POLICY",
    "privacy": "PRIVACY",
    "security": "SECURITY",
    "platform": "PLATFORM",
    "mobile": "PLATFORM",
    "chips": "CHIPS",
    "chip": "CHIPS",
    "gpu": "CHIPS",
    "semiconductor": "CHIPS",
    "tech": "NEWS",
    "news": "NEWS",
}

_HOOK_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "for",
    "from",
    "how",
    "in",
    "into",
    "its",
    "need",
    "needs",
    "of",
    "on",
    "practical",
    "risks",
    "takeaways",
    "the",
    "their",
    "these",
    "this",
    "to",
    "update",
    "users",
    "what",
    "why",
    "with",
}


class ThumbnailOverlayRenderer:
    def __init__(
        self,
        *,
        style: str = "yt_clean",
        font_paths: list[str] | None = None,
        max_words: int = 4,
    ) -> None:
        self.style = str(style or "yt_clean").strip().lower()
        self.font_paths = [str(x or "").strip() for x in (font_paths or []) if str(x or "").strip()]
        self.max_words = max(4, int(max_words or 4))

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

        hook = self._normalize_hook(hook_text)
        label = self._normalize_label(tag_label)

        image = Image.open(src).convert("RGBA")
        w, h = image.size
        image = self._apply_bottom_gradient(image)
        draw = ImageDraw.Draw(image)

        max_title_width = int(w * 0.84)
        title_font, title_lines = self._fit_title_layout(
            draw=draw,
            text=hook,
            max_width=max_title_width,
            max_lines=2,
            max_size=max(56, int(w * 0.082)),
            min_size=max(34, int(w * 0.046)),
        )
        label_font = self._pick_font(max(20, int(w * 0.024)))

        label_style = _LABEL_STYLES.get(label, _LABEL_STYLES["NEWS"])
        label_padding_x = int(w * 0.024)
        label_padding_y = int(h * 0.014)
        label_bbox = draw.textbbox((0, 0), label, font=label_font)
        label_w = (label_bbox[2] - label_bbox[0]) + (label_padding_x * 2)
        label_h = (label_bbox[3] - label_bbox[1]) + (label_padding_y * 2)
        label_x = int(w * 0.048)
        label_y = int(h * 0.645)

        draw.rounded_rectangle(
            [(label_x, label_y), (label_x + label_w, label_y + label_h)],
            radius=max(14, int(label_h * 0.42)),
            fill=label_style["fill"],
            outline=label_style["outline"],
            width=2,
        )
        label_text_y = label_y + int((label_h - (label_bbox[3] - label_bbox[1])) / 2) - label_bbox[1]
        draw.text(
            (label_x + label_padding_x, label_text_y),
            label,
            font=label_font,
            fill=label_style["text"],
        )

        line_height = self._line_height(draw, title_font)
        total_title_h = line_height * len(title_lines)
        title_x = int(w * 0.05)
        title_y = max(label_y + label_h + int(h * 0.05), h - total_title_h - int(h * 0.07))
        shadow_offsets = [(0, 4), (2, 2), (-2, 2)]
        for idx, line in enumerate(title_lines):
            y = title_y + (idx * line_height)
            for ox, oy in shadow_offsets:
                draw.text((title_x + ox, y + oy), line, font=title_font, fill=(0, 0, 0, 170))
            draw.text((title_x, y), line, font=title_font, fill=(255, 255, 255, 255))

        image.convert("RGB").save(dst, format="PNG", optimize=True)
        return dst

    def _apply_bottom_gradient(self, image: Image.Image) -> Image.Image:
        w, h = image.size
        panel_top = int(h * 0.54)
        overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        panel_h = max(1, h - panel_top)
        for idx in range(panel_h):
            ratio = idx / float(max(1, panel_h - 1))
            alpha = int(18 + (215 * ratio))
            draw.rectangle([(0, panel_top + idx), (w, panel_top + idx + 1)], fill=(7, 11, 20, alpha))
        return Image.alpha_composite(image, overlay)

    def _fit_title_layout(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        text: str,
        max_width: int,
        max_lines: int,
        max_size: int,
        min_size: int,
    ) -> tuple[ImageFont.FreeTypeFont | ImageFont.ImageFont, list[str]]:
        for size in range(max_size, min_size - 1, -2):
            font = self._pick_font(size)
            lines = self._wrap_text(draw=draw, text=text, font=font, max_width=max_width, max_lines=max_lines)
            if lines:
                return font, lines
        fallback = self._pick_font(min_size)
        return fallback, self._wrap_text(draw=draw, text=text, font=fallback, max_width=max_width, max_lines=max_lines)

    def _wrap_text(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        text: str,
        font,
        max_width: int,
        max_lines: int,
    ) -> list[str]:
        words = [w for w in str(text or "").split() if w]
        if not words:
            return ["Tech News Analysis"]

        lines: list[str] = []
        current: list[str] = []
        idx = 0
        while idx < len(words):
            word = words[idx]
            trial_words = current + [word]
            trial = " ".join(trial_words)
            width = self._text_width(draw, trial, font)
            if (not current) or width <= max_width:
                current = trial_words
                idx += 1
                continue
            lines.append(" ".join(current))
            current = []
            if len(lines) >= max_lines - 1:
                remainder = " ".join(words[idx:])
                lines.append(self._trim_to_width(draw=draw, text=remainder, font=font, max_width=max_width))
                return lines[:max_lines]
        if current:
            lines.append(" ".join(current))

        if len(lines) > max_lines:
            head = lines[: max_lines - 1]
            tail = " ".join(lines[max_lines - 1 :])
            head.append(self._trim_to_width(draw=draw, text=tail, font=font, max_width=max_width))
            return head
        return lines[:max_lines]

    def _trim_to_width(self, *, draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> str:
        clean = re.sub(r"\s+", " ", str(text or "").strip())
        if not clean:
            return ""
        if self._text_width(draw, clean, font) <= max_width:
            return clean
        words = clean.split()
        while words:
            candidate = " ".join(words).rstrip()
            if self._text_width(draw, candidate + "...", font) <= max_width:
                return candidate + "..."
            words.pop()
        return clean[:18] + "..."

    def _text_width(self, draw: ImageDraw.ImageDraw, text: str, font) -> int:
        bbox = draw.textbbox((0, 0), text, font=font)
        return int(bbox[2] - bbox[0])

    def _line_height(self, draw: ImageDraw.ImageDraw, font) -> int:
        bbox = draw.textbbox((0, 0), "Ag", font=font)
        return int((bbox[3] - bbox[1]) * 1.1)

    def _pick_font(self, size: int):
        candidates = list(self.font_paths) + [
            "C:/Windows/Fonts/bahnschrift.ttf",
            "C:/Windows/Fonts/seguisb.ttf",
            "C:/Windows/Fonts/segoeuib.ttf",
            "C:/Windows/Fonts/arialbd.ttf",
        ]
        for path in candidates:
            try:
                return ImageFont.truetype(path, max(12, int(size)))
            except Exception:
                continue
        return ImageFont.load_default()

    def _normalize_hook(self, text: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9\s]", " ", str(text or ""))
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        words = [w for w in cleaned.split() if w]
        filtered = [w for w in words if w.lower() not in _HOOK_STOPWORDS]
        if len(filtered) < 2:
            filtered = words
        filtered = filtered[: self.max_words]
        out = " ".join(self._display_case(word) for word in filtered).strip()
        return out or "Tech News Analysis"

    def _normalize_label(self, text: str) -> str:
        lower = re.sub(r"[^a-z0-9\s]", " ", str(text or "").lower())
        lower = re.sub(r"\s+", " ", lower).strip()
        for token in lower.split():
            if token in _LABEL_MAP:
                return _LABEL_MAP[token]
        for token, label in _LABEL_MAP.items():
            if token in lower:
                return label
        return "NEWS"

    def _display_case(self, token: str) -> str:
        raw = str(token or "").strip()
        if not raw:
            return ""
        if any(ch.isdigit() for ch in raw):
            return raw.upper()
        if len(raw) <= 3:
            return raw.upper()
        if raw.isupper():
            return raw
        return raw.title()
