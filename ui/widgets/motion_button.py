from __future__ import annotations

from PySide6.QtCore import QEasingCurve, QPoint, QPropertyAnimation, QSequentialAnimationGroup
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QGraphicsDropShadowEffect, QPushButton


class MotionButton(QPushButton):
    def __init__(self, text: str = "", parent=None) -> None:
        super().__init__(text, parent)
        self._shadow = QGraphicsDropShadowEffect(self)
        self._shadow.setBlurRadius(18)
        self._shadow.setOffset(0, 5)
        self._shadow.setColor(QColor(0, 0, 0, 62))
        self.setGraphicsEffect(self._shadow)
        self._anim_refs: list[QSequentialAnimationGroup] = []

    def enterEvent(self, event) -> None:
        self._shadow.setBlurRadius(24)
        self._shadow.setOffset(0, 6)
        super().enterEvent(event)

    def leaveEvent(self, event) -> None:
        self._shadow.setBlurRadius(18)
        self._shadow.setOffset(0, 5)
        super().leaveEvent(event)

    def mousePressEvent(self, event) -> None:
        self._spring_click()
        super().mousePressEvent(event)

    def _spring_click(self) -> None:
        start = self.pos()
        down = QPoint(start.x(), start.y() + 2)
        a1 = QPropertyAnimation(self, b"pos", self)
        a1.setDuration(95)
        a1.setStartValue(start)
        a1.setEndValue(down)
        a1.setEasingCurve(QEasingCurve.Type.OutQuad)
        a2 = QPropertyAnimation(self, b"pos", self)
        a2.setDuration(200)
        a2.setStartValue(down)
        a2.setEndValue(start)
        a2.setEasingCurve(QEasingCurve.Type.OutBack)
        seq = QSequentialAnimationGroup(self)
        seq.addAnimation(a1)
        seq.addAnimation(a2)
        seq.start()
        self._anim_refs.append(seq)
