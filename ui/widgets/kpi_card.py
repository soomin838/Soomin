from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QHBoxLayout, QLabel, QVBoxLayout

from ui.widgets.glass_card import GlassCard


class KpiCard(GlassCard):
    def __init__(self, title: str, icon: str, parent=None) -> None:
        super().__init__(parent=parent)
        self.setMinimumHeight(76)
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(12, 10, 12, 10)
        self.layout.setSpacing(6)

        top = QHBoxLayout()
        top.setSpacing(10)
        self.index_label = QLabel(str(icon or "--").strip())
        self.index_label.setObjectName("KpiIndex")
        self.index_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.index_label.setProperty("tone", "neutral")
        self.title_label = QLabel(title)
        self.title_label.setObjectName("KpiTitle")
        self.title_label.setWordWrap(False)
        top.addWidget(self.index_label, 0)
        top.addWidget(self.title_label, 1)

        self.value_label = QLabel("-")
        self.value_label.setObjectName("KpiValue")
        self.value_label.setWordWrap(False)
        self.sub_label = QLabel("")
        self.sub_label.setObjectName("KpiMeta")
        self.sub_label.setWordWrap(False)
        self.sub_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.layout.addLayout(top)
        self.layout.addWidget(self.value_label)
        self.layout.addWidget(self.sub_label)

    def set_value(self, value: str, sub: str = "", state: str = "") -> None:
        self.value_label.setText(str(value or "-"))
        self.sub_label.setText(str(sub or ""))
        tone = {
            "success": "good",
            "warning": "warning",
            "error": "danger",
            "active": "active",
        }.get(str(state or "").strip().lower(), "neutral")
        self.index_label.setProperty("tone", tone)
        self.index_label.style().unpolish(self.index_label)
        self.index_label.style().polish(self.index_label)
        self.set_state(state)
