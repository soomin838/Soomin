from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QHBoxLayout, QLabel, QTextEdit, QVBoxLayout

from ui.widgets.glass_card import GlassCard


class LogPanel(GlassCard):
    def __init__(self, parent=None) -> None:
        super().__init__(parent=parent)
        self._hide_error_lines = False
        self._entries: list[tuple[str, bool]] = []
        self._phase_key = "idle"

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        top = QHBoxLayout()
        top.setSpacing(8)
        self.title = QLabel("실행 로그")
        self.title.setObjectName("PanelTitle")
        self.phase_chip = QLabel("대기")
        self.phase_chip.setObjectName("StatusChip")
        self.phase_chip.setProperty("tone", "neutral")
        self.phase_chip.setAlignment(Qt.AlignmentFlag.AlignCenter)
        top.addWidget(self.title, 1)
        top.addWidget(self.phase_chip, 0)
        root.addLayout(top)

        self.viewer = QTextEdit()
        self.viewer.setObjectName("LogViewer")
        self.viewer.setReadOnly(True)
        self.viewer.setMinimumHeight(150)
        root.addWidget(self.viewer, 1)

    def set_phase(self, phase_key: str) -> None:
        clean = str(phase_key or "idle").lower()
        if clean == self._phase_key:
            return
        self._phase_key = clean
        mapping = {
            "preflight": ("사전점검", "neutral"),
            "collect": ("수집", "neutral"),
            "select": ("수집", "neutral"),
            "draft": ("초안", "active"),
            "headline": ("앵글", "active"),
            "qa": ("QA", "active"),
            "visual": ("이미지", "active"),
            "schedule": ("예약", "warning"),
            "publish": ("발행", "good"),
            "indexing": ("후속반영", "good"),
            "done": ("완료", "good"),
            "error": ("오류", "danger"),
            "idle": ("대기", "neutral"),
        }
        text, tone = mapping.get(clean, (clean or "대기", "neutral"))
        self.phase_chip.setText(text)
        self.phase_chip.setProperty("tone", tone)
        self.phase_chip.style().unpolish(self.phase_chip)
        self.phase_chip.style().polish(self.phase_chip)

    def append_line(self, line: str) -> None:
        text = str(line or "")
        is_err = self._is_error_line(text)
        self._entries.append((text, is_err))
        if self._hide_error_lines and is_err:
            return
        self.viewer.append(text)
        bar = self.viewer.verticalScrollBar()
        bar.setValue(bar.maximum())

    def set_entries(self, entries: list[tuple[str, bool]]) -> None:
        self._entries = list(entries)
        self._rebuild()

    def toggle_error_lines(self) -> None:
        self._hide_error_lines = not self._hide_error_lines
        self._rebuild()

    def copy_all(self) -> None:
        clipboard = QApplication.clipboard()
        clipboard.setText(self.viewer.toPlainText())

    def _rebuild(self) -> None:
        self.viewer.clear()
        for line, is_err in self._entries:
            if self._hide_error_lines and is_err:
                continue
            self.viewer.append(line)
        bar = self.viewer.verticalScrollBar()
        bar.setValue(bar.maximum())

    def _is_error_line(self, line: str) -> bool:
        lower = str(line or "").lower()
        return (
            "[error]" in lower
            or "[오류]" in lower
            or " error:" in lower
            or "failed" in lower
            or "exception" in lower
            or "traceback" in lower
        )
