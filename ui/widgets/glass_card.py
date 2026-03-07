from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QFrame, QGraphicsDropShadowEffect


class GlassCard(QFrame):
    """Reusable surface card that defers its visual treatment to the global theme."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("GlassCard")
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self._state = ""

        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(34)
        shadow.setOffset(0, 12)
        shadow.setColor(QColor(10, 14, 20, 46))
        self.setGraphicsEffect(shadow)

    def set_state(self, state: str = "") -> None:
        clean = str(state or "").strip().lower()
        if clean == self._state:
            return
        self._state = clean
        self.setProperty("state", clean)
        self.style().unpolish(self)
        self.style().polish(self)
        self.update()
