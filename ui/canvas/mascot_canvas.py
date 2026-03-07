from __future__ import annotations

import math
import os
import sys
from pathlib import Path
from PySide6.QtCore import QTimer, Qt, QRectF, QPointF
from PySide6.QtGui import QPainter, QColor, QPixmap, QRadialGradient
from PySide6.QtWidgets import QWidget, QSizePolicy


def _resolve_asset(name: str) -> Path:
    """Resolve asset path in both dev and PyInstaller frozen modes."""
    base = Path(getattr(sys, "_MEIPASS", ""))
    if base.exists():
        p = base / "ui" / "assets" / name
        if p.exists():
            return p
    here = Path(__file__).resolve().parent.parent / "assets" / name
    if here.exists():
        return here
    return Path("ui") / "assets" / name


class MascotCanvas(QWidget):
    """
    Rezy — Gemini-style 귀여운 마스코트.
    이미지 기반 캐릭터가 상태에 따라 바운스/이펙트 애니메이션.
    """

    _GLOW_COLORS = {
        "idle":    QColor(180, 160, 255, 35),
        "running": QColor(120, 160, 255, 50),
        "success": QColor(120, 230, 180, 50),
        "error":   QColor(255, 120, 120, 45),
        "paused":  QColor(160, 150, 190, 25),
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(160, 160)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._status = "idle"
        self._frame = 0
        self._pixmap: QPixmap | None = None

        # Load mascot image
        img_path = _resolve_asset("mascot.png")
        if img_path.exists():
            self._pixmap = QPixmap(str(img_path))

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._animate)
        self._timer.start(1000 // 30)  # 30 FPS

    def set_state(self, status: str, percent: int = 0):
        self._status = str(status).lower()
        self.update()

    def _animate(self):
        self._frame += 1
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        p.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)

        w = self.width()
        h = self.height()
        cx = w / 2.0
        cy = h / 2.0
        base = min(w, h) * 0.75

        # ── Bounce Animation ──
        if "error" in self._status:
            by = math.sin(self._frame * 0.45) * 4
            bx = math.cos(self._frame * 0.55) * 3
            scale = 1.0
        elif "running" in self._status:
            phase = self._frame * 0.14
            by = abs(math.sin(phase)) * base * 0.08
            bx = 0
            scale = 1.0 - abs(math.sin(phase)) * 0.04
        elif "success" in self._status:
            by = abs(math.sin(self._frame * 0.09)) * base * 0.12
            bx = 0
            scale = 1.0 + abs(math.sin(self._frame * 0.09)) * 0.03
        else:
            by = math.sin(self._frame * 0.04) * base * 0.03
            bx = 0
            scale = 1.0

        bcx = cx + bx
        bcy = cy - by

        # ── Ambient Glow ──
        glow_color = self._GLOW_COLORS.get(self._status, self._GLOW_COLORS["idle"])
        g_size = base * 0.8
        glow = QRadialGradient(bcx, bcy, g_size)
        glow.setColorAt(0, glow_color)
        glow.setColorAt(1, QColor(0, 0, 0, 0))
        p.setBrush(glow)
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(QRectF(bcx - g_size, bcy - g_size, g_size * 2, g_size * 2))

        # ── Shadow ──
        shadow_w = base * 0.35
        shadow_h = base * 0.06
        shadow_y = cy + base * 0.38
        shadow_alpha = max(20, int(40 - by * 2))
        p.setBrush(QColor(20, 15, 30, shadow_alpha))
        p.drawEllipse(QRectF(cx - shadow_w, shadow_y, shadow_w * 2, shadow_h * 2))

        # ── Draw Mascot Image ──
        if self._pixmap and not self._pixmap.isNull():
            img_size = base * scale
            draw_x = bcx - img_size / 2
            draw_y = bcy - img_size / 2
            target_rect = QRectF(draw_x, draw_y, img_size, img_size)
            p.drawPixmap(target_rect.toRect(), self._pixmap)

            # Paused → dim overlay
            if "paused" in self._status:
                p.setBrush(QColor(20, 18, 24, 100))
                p.drawEllipse(target_rect)

        # ── Sparkles (running/success) ──
        if self._status in ("running", "success"):
            p.setPen(Qt.PenStyle.NoPen)
            for i in range(8):
                angle = self._frame * 0.025 + i * math.pi / 4
                dist = base * (0.45 + math.sin(self._frame * 0.08 + i * 1.5) * 0.1)
                sx = bcx + math.cos(angle) * dist
                sy = bcy + math.sin(angle) * dist * 0.6
                alpha = int(120 + math.sin(self._frame * 0.15 + i * 2) * 80)
                s_size = base * (0.02 + math.sin(self._frame * 0.1 + i) * 0.01)
                p.setBrush(QColor(255, 255, 255, max(0, min(255, alpha))))
                p.drawEllipse(QRectF(sx - s_size, sy - s_size, s_size * 2, s_size * 2))

        p.end()
