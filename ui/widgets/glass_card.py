from __future__ import annotations

from PySide6.QtWidgets import QFrame, QGraphicsDropShadowEffect
from PySide6.QtGui import QColor
from PySide6.QtCore import Qt


class GlassCard(QFrame):
    """A semi-transparent dark 'glass' card panel matching the neon theme."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("GlassCard")
        self.setStyleSheet(
            """
            QFrame#GlassCard {
                background-color: rgba(28, 24, 37, 0.85);
                border: 1px solid rgba(196, 161, 255, 0.12);
                border-radius: 14px;
            }
            """
        )
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(20)
        shadow.setOffset(0, 3)
        shadow.setColor(QColor(20, 18, 24, 140))
        self.setGraphicsEffect(shadow)
