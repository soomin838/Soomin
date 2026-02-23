from __future__ import annotations

from PySide6.QtGui import QColor
from PySide6.QtWidgets import QFrame, QGraphicsDropShadowEffect


class GlassCard(QFrame):
    def __init__(self, parent=None, state: str = "") -> None:
        super().__init__(parent)
        self.setObjectName("GlassCard")
        self.set_state(state)
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(24)
        shadow.setOffset(0, 8)
        shadow.setColor(QColor(0, 0, 0, 68))
        self.setGraphicsEffect(shadow)

    def set_state(self, state: str) -> None:
        clean = str(state or "").strip().lower()
        if str(self.property("state") or "") == clean:
            return
        self.setProperty("state", clean)
        self.style().unpolish(self)
        self.style().polish(self)
