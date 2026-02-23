from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QTextEdit, QToolButton, QVBoxLayout

from ui.widgets.glass_card import GlassCard


class LogPanel(GlassCard):
    def __init__(self, parent=None) -> None:
        super().__init__(parent=parent)
        self._hide_error_lines = False
        self._entries: list[tuple[str, bool]] = []
        self._stage_labels: list[QLabel] = []
        self._phase_index = -1

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        top = QHBoxLayout()
        self.title = QLabel("실시간 콘솔")
        self.title.setObjectName("Subtitle")
        self.toggle_error_btn = QToolButton()
        self.toggle_error_btn.setText("에러 로그 접기")
        self.toggle_error_btn.clicked.connect(self.toggle_error_lines)
        self.auto_scroll = QCheckBox("자동 스크롤")
        self.auto_scroll.setChecked(True)
        top.addWidget(self.title)
        top.addStretch(1)
        top.addWidget(self.auto_scroll)
        top.addWidget(self.toggle_error_btn)
        root.addLayout(top)

        stage_row = QHBoxLayout()
        stage_row.setSpacing(6)
        for name in ["수집", "초안", "스토리", "SEO", "이미지", "HTML", "발행"]:
            dot = QLabel(name)
            dot.setProperty("stepState", "pending")
            dot.setObjectName("TimelineDot")
            dot.setMinimumHeight(24)
            dot.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self._stage_labels.append(dot)
            stage_row.addWidget(dot, 1)
        root.addLayout(stage_row)

        self.viewer = QTextEdit()
        self.viewer.setReadOnly(True)
        self.viewer.setMinimumHeight(160)
        root.addWidget(self.viewer, 1)

    def set_phase(self, phase_key: str) -> None:
        order = {
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
        idx = int(order.get(str(phase_key or "idle").lower(), 0))
        if idx == self._phase_index:
            return
        self._phase_index = idx
        for i, lab in enumerate(self._stage_labels):
            if i < idx:
                state = "done"
            elif i == idx:
                state = "active"
            else:
                state = "pending"
            lab.setProperty("stepState", state)
            lab.style().unpolish(lab)
            lab.style().polish(lab)

    def append_line(self, line: str) -> None:
        text = str(line or "")
        is_err = self._is_error_line(text)
        self._entries.append((text, is_err))
        if self._hide_error_lines and is_err:
            return
        self.viewer.append(text)
        if self.auto_scroll.isChecked():
            self.viewer.verticalScrollBar().setValue(self.viewer.verticalScrollBar().maximum())

    def set_entries(self, entries: list[tuple[str, bool]]) -> None:
        self._entries = list(entries)
        self._rebuild()

    def toggle_error_lines(self) -> None:
        self._hide_error_lines = not self._hide_error_lines
        self.toggle_error_btn.setText("에러 로그 펼치기" if self._hide_error_lines else "에러 로그 접기")
        self._rebuild()

    def _rebuild(self) -> None:
        self.viewer.clear()
        for line, is_err in self._entries:
            if self._hide_error_lines and is_err:
                continue
            self.viewer.append(line)

    def _is_error_line(self, line: str) -> bool:
        lower = str(line or "").lower()
        return "[오류]" in lower or " error:" in lower or "failed" in lower or "exception" in lower
