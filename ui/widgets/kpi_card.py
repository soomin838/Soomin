from __future__ import annotations

from PySide6.QtWidgets import QLabel, QVBoxLayout

from ui.widgets.glass_card import GlassCard


class KpiCard(GlassCard):
    def __init__(self, title: str, icon: str, parent=None) -> None:
        super().__init__(parent=parent)
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(16, 14, 16, 14)
        self.layout.setSpacing(6)

        self.title_label = QLabel(f"{icon} {title}")
        self.title_label.setObjectName("Subtitle")
        self.title_label.setWordWrap(True)
        self.value_label = QLabel("-")
        self.value_label.setObjectName("Value")
        self.value_label.setWordWrap(True)
        self.sub_label = QLabel("")
        self.sub_label.setObjectName("ValueSmall")
        self.sub_label.setWordWrap(True)
        self.layout.addWidget(self.title_label)
        self.layout.addWidget(self.value_label)
        self.layout.addWidget(self.sub_label)

    def set_value(self, value: str, sub: str = "", state: str = "") -> None:
        self.value_label.setText(str(value or "-"))
        self.sub_label.setText(str(sub or ""))
        self.set_state(state)
