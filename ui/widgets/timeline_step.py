from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QHBoxLayout, QVBoxLayout, QWidget

from ui.widgets.glass_card import GlassCard


class TimelineStep(GlassCard):
    def __init__(self, title: str, icon: str = "", parent=None) -> None:
        super().__init__(parent=parent)
        self.title = title
        self.icon = icon
        self._status = "pending"
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 10, 12, 10)
        root.setSpacing(4)
        top = QHBoxLayout()
        self.title_label = QLabel(f"{icon} {title}".strip())
        self.title_label.setObjectName("Subtitle")
        self.title_label.setWordWrap(True)
        self.badge = QLabel("pending")
        self.badge.setObjectName("Badge")
        self.badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        top.addWidget(self.title_label, 1)
        top.addWidget(self.badge, 0)
        self.message_label = QLabel("-")
        self.message_label.setObjectName("ValueSmall")
        self.message_label.setWordWrap(True)
        root.addLayout(top)
        root.addWidget(self.message_label)
        self.setMinimumHeight(88)

    def set_status(self, status: str, message: str = "") -> None:
        next_status = str(status or "pending").strip().lower()
        next_msg = str(message or "-")
        if self._status != next_status:
            self._status = next_status
            self.badge.setText(self._status)
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
