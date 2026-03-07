from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QHBoxLayout, QVBoxLayout

from ui.widgets.glass_card import GlassCard


class TimelineStep(GlassCard):
    def __init__(self, title: str, icon: str = "", parent=None) -> None:
        super().__init__(parent=parent)
        self.title = title
        self.icon = icon
        self._status = "pending"
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 10, 12, 10)
        root.setSpacing(6)
        top = QHBoxLayout()
        top.setSpacing(8)
        self.index_label = QLabel(str(icon or "--").strip())
        self.index_label.setObjectName("StepIndex")
        self.index_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.index_label.setProperty("tone", "neutral")
        self.badge = QLabel("대기")
        self.badge.setObjectName("Badge")
        self.badge.setProperty("tone", "pending")
        self.badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        top.addWidget(self.index_label, 0)
        top.addWidget(self.badge, 0)
        self.title_label = QLabel(title)
        self.title_label.setObjectName("TimelineTitle")
        self.title_label.setWordWrap(True)
        self.message_label = QLabel("-")
        self.message_label.setObjectName("TimelineBody")
        self.message_label.setWordWrap(True)
        root.addLayout(top)
        root.addWidget(self.title_label)
        root.addWidget(self.message_label)
        self.setMinimumHeight(88)

    def set_status(self, status: str, message: str = "") -> None:
        next_status = str(status or "pending").strip().lower()
        next_msg = str(message or "-")
        if self._status != next_status:
            self._status = next_status
            badge_text = {
                "success": "완료",
                "active": "진행",
                "pending": "대기",
                "error": "차단",
                "warning": "주의",
            }.get(self._status, self._status)
            self.badge.setText(badge_text)
            self.badge.setProperty("tone", self._status)
            self.badge.style().unpolish(self.badge)
            self.badge.style().polish(self.badge)
            tone = {
                "success": "success",
                "active": "active",
                "warning": "warning",
                "error": "error",
            }.get(self._status, "neutral")
            self.index_label.setProperty("tone", tone)
            self.index_label.style().unpolish(self.index_label)
            self.index_label.style().polish(self.index_label)
        if self.message_label.text() != next_msg:
            self.message_label.setText(next_msg)
        state = {
            "success": "success",
            "warn": "warning",
            "warning": "warning",
            "fail": "error",
            "error": "error",
            "running": "active",
            "active": "active",
            "pending": "",
        }.get(next_status, "")
        self.set_state(state)
