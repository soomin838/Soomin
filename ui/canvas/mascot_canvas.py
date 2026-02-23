from __future__ import annotations

import hashlib
import io
import json
import math
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from PIL import Image, ImageDraw
from PySide6.QtCore import QRectF, Qt
from PySide6.QtGui import QBrush, QColor, QLinearGradient, QPainter, QPen, QPixmap
from PySide6.QtWidgets import QSizePolicy, QWidget

_SUFFIX = ", no text, no letters, no numbers, no logos, no watermark, no UI text"


class PollinationsCache:
    def __init__(self, runtime_root: Path, api_key: str, base_url: str, allow_network: bool = False) -> None:
        self.runtime_root = runtime_root
        self.api_key = str(api_key or "").strip()
        self.base_url = str(base_url or "https://gen.pollinations.ai").strip().rstrip("/")
        self.allow_network = bool(allow_network)
        self.cache_dir = runtime_root / "assets" / "cache" / "pollinations"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_call_at = 0.0
        self._lock = threading.Lock()

    def get_or_generate(self, prompt: str, *, width: int, height: int, model: str = "gptimage") -> Path | None:
        normalized = f"{model}|{width}x{height}|{prompt.strip()}"
        digest = hashlib.sha1(normalized.encode("utf-8", errors="ignore")).hexdigest()
        out = self.cache_dir / f"{digest}.png"
        if out.exists():
            return out
        if not self.allow_network:
            return None
        if not self.api_key:
            return None

        endpoint = f"{self.base_url}/image/{quote(prompt, safe='')}"
        params = {
            "model": model,
            "width": str(width),
            "height": str(height),
            "safe": "true",
            "enhance": "true",
            "nologo": "true",
            "key": self.api_key,
            "seed": str(random.randint(1, 2_000_000_000)),
        }
        with self._lock:
            gap = time.time() - self._last_call_at
            if gap < 8.0:
                time.sleep(8.0 - gap)
            try:
                resp = requests.get(endpoint, params=params, headers={"Accept": "image/*"}, timeout=90)
            finally:
                self._last_call_at = time.time()
        ctype = str(resp.headers.get("content-type", "")).lower()
        if resp.status_code == 200 and ctype.startswith("image/") and resp.content:
            try:
                with Image.open(io.BytesIO(resp.content)).convert("RGBA") as im:
                    im.save(out, format="PNG", optimize=True)
                return out
            except Exception:
                return None
        return None


class MascotAssetManager:
    def __init__(
        self,
        runtime_root: Path,
        pollinations_api_key: str,
        pollinations_base_url: str = "https://gen.pollinations.ai",
        allow_ui_api_calls: bool = False,
    ) -> None:
        self.runtime_root = runtime_root
        self.api_key = str(pollinations_api_key or "").strip()
        self.base_url = str(pollinations_base_url or "https://gen.pollinations.ai").strip().rstrip("/")
        self.allow_ui_api_calls = bool(allow_ui_api_calls)
        self.asset_dir = runtime_root / "storage" / "ui" / "mascot"
        self.asset_dir.mkdir(parents=True, exist_ok=True)
        self.bg_dir = runtime_root / "storage" / "ui" / "background"
        self.bg_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = runtime_root / "storage" / "logs" / "ui_asset_events.jsonl"
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache = PollinationsCache(
            runtime_root=runtime_root,
            api_key=self.api_key,
            base_url=self.base_url,
            allow_network=self.allow_ui_api_calls,
        )
        self._lock = threading.Lock()
        self._inflight: set[str] = set()

    def asset_path(self, state: str) -> Path:
        key = self._normalize_state(state)
        return self.asset_dir / f"{key}.png"

    def background_path(self, mode: str) -> Path:
        clean = "dark" if str(mode or "light").strip().lower() == "dark" else "light"
        out = self.bg_dir / f"{clean}.png"
        if out.exists():
            return out
        prompt = self._background_prompt(clean)
        api_path = self._cache.get_or_generate(prompt, width=1536, height=864, model="gptimage")
        if api_path and api_path.exists():
            try:
                Image.open(api_path).convert("RGB").save(out, format="PNG", optimize=True)
                return out
            except Exception:
                pass
        self._render_fallback_background(clean, out)
        return out

    def request_state(self, state: str, force: bool = False) -> None:
        key = self._normalize_state(state)
        out = self.asset_path(key)
        if out.exists() and not force:
            return
        with self._lock:
            if key in self._inflight:
                return
            self._inflight.add(key)
        threading.Thread(target=self._generate_worker, args=(key,), daemon=True).start()

    def prewarm(self, full: bool = False) -> None:
        keys = ("idle", "running", "warning", "error", "success") if full else ("idle",)
        for key in keys:
            self.request_state(key, force=False)

    def _normalize_state(self, state: str) -> str:
        lower = (state or "idle").lower()
        if "error" in lower:
            return "error"
        if "success" in lower:
            return "success"
        if "hold" in lower or "warning" in lower:
            return "warning"
        if "running" in lower or "실행" in lower:
            return "running"
        return "idle"

    def _generate_worker(self, state: str) -> None:
        try:
            self._generate_state_asset(state)
        finally:
            with self._lock:
                self._inflight.discard(state)

    def _background_prompt(self, mode: str) -> str:
        if mode == "dark":
            return (
                "dark abstract gradient background, deep navy and violet, macOS glassmorphism wallpaper, "
                "subtle noise texture, soft glowing blobs, minimal, high resolution"
                + _SUFFIX
            )
        return (
            "soft abstract gradient background, macOS glassmorphism wallpaper, pastel lavender and mint, "
            "subtle noise texture, gentle bokeh blobs, minimal, high resolution"
            + _SUFFIX
        )

    def _state_prompt(self, state: str) -> str:
        if state == "running":
            return (
                "cute round mascot character, excited expression, sparkles around, slight motion pose, "
                "minimal vector, pastel, centered, transparent background, high resolution"
                + _SUFFIX
            )
        if state == "warning":
            return (
                "cute round mascot character, worried expression, small sweat drop, minimal vector, pastel, "
                "centered, transparent background, high resolution"
                + _SUFFIX
            )
        if state == "error":
            return (
                "cute round mascot character, sad expression, small tear, minimal vector, pastel, centered, "
                "transparent background, high resolution"
                + _SUFFIX
            )
        if state == "success":
            return (
                "cute round mascot character, proud expression, tiny confetti, minimal vector, pastel, centered, "
                "transparent background, high resolution"
                + _SUFFIX
            )
        return (
            "cute round mascot character, relaxed smile, tiny floating motion pose, minimal vector, pastel, centered, "
            "transparent background, high resolution"
            + _SUFFIX
        )

    def _generate_state_asset(self, state: str) -> None:
        output_path = self.asset_path(state)
        prompt = self._state_prompt(state)
        generated = self._cache.get_or_generate(prompt, width=1024, height=1024, model="gptimage")
        if generated and generated.exists():
            try:
                with Image.open(generated).convert("RGBA") as im:
                    im.thumbnail((700, 700), Image.Resampling.LANCZOS)
                    im.save(output_path, format="PNG", optimize=True)
                self._log({"event": "ui_asset_generated", "state": state, "path": str(output_path)})
                return
            except Exception:
                pass

        self._render_fallback_mascot(state, output_path)
        self._log({"event": "ui_asset_fallback", "state": state, "path": str(output_path)})

    def _render_fallback_background(self, mode: str, out: Path) -> None:
        w, h = 1536, 864
        img = Image.new("RGB", (w, h), (246, 247, 251))
        draw = ImageDraw.Draw(img)
        if mode == "dark":
            c1 = (11, 15, 26)
            c2 = (40, 30, 78)
            c3 = (35, 60, 100)
        else:
            c1 = (246, 247, 251)
            c2 = (198, 221, 255)
            c3 = (231, 205, 255)
        for y in range(h):
            t = y / max(1, h - 1)
            r = int((1 - t) * c1[0] + t * c2[0])
            g = int((1 - t) * c1[1] + t * c2[1])
            b = int((1 - t) * c1[2] + t * c2[2])
            draw.line([(0, y), (w, y)], fill=(r, g, b))
        draw.ellipse([w * 0.58, h * 0.04, w * 0.95, h * 0.48], fill=(*c3, 80))
        draw.ellipse([w * 0.06, h * 0.52, w * 0.48, h * 0.96], fill=(*c3, 60))
        img.save(out, format="PNG", optimize=True)

    def _render_fallback_mascot(self, state: str, out: Path) -> None:
        size = 768
        img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        base = (245, 224, 255, 255)
        outline = (99, 83, 148, 255)
        face = [140, 120, 620, 600]
        draw.ellipse(face, fill=base, outline=outline, width=10)
        draw.ellipse([285, 285, 325, 335], fill=outline)
        draw.ellipse([435, 285, 475, 335], fill=outline)
        if state == "error":
            draw.arc([290, 400, 470, 500], start=20, end=160, fill=outline, width=8)
            draw.ellipse([505, 355, 540, 420], fill=(120, 180, 255, 190))
        elif state == "warning":
            draw.arc([295, 395, 465, 485], start=5, end=175, fill=outline, width=8)
            draw.ellipse([505, 245, 560, 330], fill=(130, 200, 255, 180))
        elif state == "success":
            draw.arc([285, 365, 475, 505], start=200, end=340, fill=outline, width=10)
            draw.ellipse([130, 130, 165, 165], fill=(255, 210, 90, 230))
            draw.ellipse([620, 180, 655, 215], fill=(255, 210, 90, 230))
        elif state == "running":
            draw.arc([285, 370, 475, 500], start=200, end=340, fill=outline, width=9)
            draw.ellipse([120, 220, 160, 260], fill=(255, 228, 120, 220))
            draw.ellipse([620, 320, 660, 360], fill=(255, 228, 120, 220))
        else:
            draw.arc([295, 380, 465, 500], start=200, end=340, fill=outline, width=8)
        img.save(out, format="PNG", optimize=True)

    def _log(self, payload: dict) -> None:
        try:
            row = dict(payload or {})
            row["ts"] = datetime.now(timezone.utc).isoformat()
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            pass


class MascotCanvas(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._status = "idle"
        self._frame = 0
        self._asset_manager: MascotAssetManager | None = None
        self._pix_cache: dict[str, tuple[float, QPixmap]] = {}
        self.setMinimumSize(220, 148)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def set_asset_manager(self, manager: MascotAssetManager) -> None:
        self._asset_manager = manager
        try:
            self._asset_manager.request_state("idle")
        except Exception:
            pass

    def set_state(self, status: str, frame: int) -> None:
        self._status = (status or "idle").lower()
        self._frame = int(frame or 0)
        if self._asset_manager is not None:
            try:
                self._asset_manager.request_state(self._status_key())
            except Exception:
                pass
        self.update()

    def _status_key(self) -> str:
        s = (self._status or "").lower()
        if "error" in s:
            return "error"
        if "success" in s or "성공" in s:
            return "success"
        if "hold" in s or "warning" in s:
            return "warning"
        if "running" in s or "실행" in s:
            return "running"
        return "idle"

    def _draw_asset_sprite(self, p: QPainter, w: float, h: float, motion: str) -> bool:
        if self._asset_manager is None:
            return False
        state = self._status_key()
        path = self._asset_manager.asset_path(state)
        if not path.exists():
            self._asset_manager.request_state(state)
            return False
        key = str(path)
        try:
            mtime = float(path.stat().st_mtime)
        except Exception:
            return False
        cached = self._pix_cache.get(key)
        if cached is None or cached[0] != mtime:
            pm = QPixmap(key)
            if pm.isNull():
                return False
            self._pix_cache[key] = (mtime, pm)
        pm = self._pix_cache[key][1]

        base_h = h * 0.70
        ratio = pm.width() / max(1.0, float(pm.height()))
        target_h = base_h
        target_w = base_h * ratio
        cx = w * 0.5
        cy = h * 0.54

        bob = 0.0
        tilt = 0.0
        if motion == "running":
            bob = math.sin(self._frame / 1.8) * 7.0
            tilt = math.sin(self._frame / 2.4) * 5.0
        elif motion == "idle":
            bob = math.sin(self._frame / 7.0) * 3.0
            tilt = math.sin(self._frame / 9.0) * 1.2
        elif motion == "warning":
            bob = math.sin(self._frame / 3.0) * 2.0
            tilt = math.sin(self._frame / 2.0) * 2.0
        elif motion == "error":
            bob = math.sin(self._frame / 2.5) * 1.6
            tilt = (self._frame % 8 - 4) * 1.0
        elif motion == "success":
            bob = abs(math.sin(self._frame / 2.1)) * 8.0
            tilt = math.sin(self._frame / 2.2) * 5.5

        p.save()
        p.translate(cx, cy + bob)
        p.rotate(tilt)
        target = QRectF(-target_w / 2.0, -target_h / 2.0, target_w, target_h)
        source = QRectF(0, 0, float(pm.width()), float(pm.height()))
        p.drawPixmap(target, pm, source)
        p.restore()
        return True

    def paintEvent(self, event) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        w = float(self.width())
        h = float(self.height())

        grad = QLinearGradient(0, 0, w, h)
        grad.setColorAt(0.0, QColor(215, 227, 255, 110))
        grad.setColorAt(0.5, QColor(241, 228, 255, 125))
        grad.setColorAt(1.0, QColor(224, 246, 255, 110))
        p.setBrush(QBrush(grad))
        p.setPen(QPen(QColor(205, 188, 242, 120), 1))
        p.drawRoundedRect(QRectF(2, 2, w - 4, h - 4), 24, 24)

        state = self._status_key()
        self._draw_asset_sprite(p, w, h, state)

        if state == "error":
            pulse = 1 + (self._frame % 4)
            p.setPen(QPen(QColor(239, 68, 68, 155), pulse))
            p.drawRoundedRect(QRectF(10, 10, w - 20, h - 20), 18, 18)
        p.end()
