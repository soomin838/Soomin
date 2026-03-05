from __future__ import annotations

import math

from PySide6.QtCore import QRectF, Qt
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import QWidget


class PipelineCanvas(QWidget):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._status = "idle"
        self._phase = "idle"
        self._percent = 0
        self._frame = 0
        self.setMinimumHeight(120)

    def set_state(self, status: str, phase: str, percent: int, frame: int) -> None:
        self._status = (status or "idle").lower()
        self._phase = (phase or "idle").lower()
        self._percent = max(0, min(100, int(percent)))
        self._frame = frame
        self.update()

    def paintEvent(self, event) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        w = self.width()
        h = self.height()
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(Qt.BrushStyle.NoBrush)

        stages = ["Source", "Draft", "Story", "SEO", "Image", "HTML", "Publish"]
        xs = [18 + int(i * (w - 36) / (len(stages) - 1)) for i in range(len(stages))]
        y = int(h * 0.48)
        p.setPen(QPen(QColor("#3D3552"), 3))
        p.drawLine(xs[0], y, xs[-1], y)

        phase_index = {
            "preflight": 0,
            "collect": 0,
            "select": 1,
            "draft": 1,
            "headline": 2,
            "qa": 3,
            "visual": 4,
            "schedule": 5,
            "publish": 6,
            "indexing": 6,
            "done": 6,
            "idle": 0,
        }
        if "running" in self._status or "실행" in self._status:
            active_idx = phase_index.get(self._phase, min(len(stages) - 1, max(0, int(self._percent / 15))))
        elif "success" in self._status or "성공" in self._status:
            active_idx = len(stages) - 1
        elif "error" in self._status:
            active_idx = phase_index.get(self._phase, 2)
        else:
            active_idx = phase_index.get(self._phase, 0)

        for i, x in enumerate(xs):
            color = QColor("#3D3552")
            if i < active_idx:
                color = QColor(49, 130, 246, 120)
            if i == active_idx:
                if "error" in self._status:
                    color = QColor(240, 68, 82)
                elif "running" in self._status or "실행" in self._status:
                    color = QColor("#C4A1FF")
                else:
                    color = QColor("#7FDBCA")
            radius = 8 if i != active_idx else 10 + int(abs(math.sin(self._frame / 2.4)) * 3)
            p.setBrush(color)
            p.setPen(Qt.PenStyle.NoPen)
            p.drawEllipse(QRectF(x - radius, y - radius, radius * 2, radius * 2))
            p.setPen(QPen(QColor("#C9BDE0"), 1))
            p.setFont(QFont("Pretendard", 9))
            cell_w = max(58, int((w - 36) / max(1, len(stages))))
            p.drawText(
                QRectF(x - (cell_w / 2), y + 16, cell_w, 20),
                Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop,
                stages[i],
            )
        p.end()
