from __future__ import annotations

from html import escape

from PySide6.QtWidgets import QCheckBox, QDialog, QHBoxLayout, QLabel, QToolButton, QTextEdit, QVBoxLayout

from ui.widgets.glass_card import GlassCard


class LogsDialog(QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("RezeroAgent 로그")
        self.resize(960, 620)
        self._hide_error = False
        self._entries: list[tuple[str, bool]] = []

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(10)

        header = GlassCard()
        header_layout = QHBoxLayout(header)
        title = QLabel("실시간 로그")
        title.setObjectName("Title")
        self.toggle_error_btn = QToolButton()
        self.toggle_error_btn.setText("에러 접기")
        self.toggle_error_btn.clicked.connect(self._toggle_error)
        self.auto_scroll = QCheckBox("자동 스크롤")
        self.auto_scroll.setChecked(True)
        header_layout.addWidget(title)
        header_layout.addStretch(1)
        header_layout.addWidget(self.auto_scroll)
        header_layout.addWidget(self.toggle_error_btn)
        root.addWidget(header)

        body = GlassCard()
        body_layout = QVBoxLayout(body)
        self.viewer = QTextEdit()
        self.viewer.setReadOnly(True)
        body_layout.addWidget(self.viewer)
        root.addWidget(body, 1)

    def set_entries(self, entries: list[tuple[str, bool]]) -> None:
        self._entries = list(entries)
        self._rebuild()

    def append_line(self, line: str) -> None:
        text = str(line or "")
        is_err = self._is_error_line(text)
        self._entries.append((text, is_err))
        if self._hide_error and is_err:
            return
        self.viewer.append(self._line_html(text, is_err))
        if self.auto_scroll.isChecked():
            self.viewer.verticalScrollBar().setValue(self.viewer.verticalScrollBar().maximum())

    def _toggle_error(self) -> None:
        self._hide_error = not self._hide_error
        self.toggle_error_btn.setText("에러 펼치기" if self._hide_error else "에러 접기")
        self._rebuild()

    def _rebuild(self) -> None:
        chunks: list[str] = []
        for text, is_err in self._entries:
            if self._hide_error and is_err:
                continue
            chunks.append(self._line_html(text, is_err))
        self.viewer.setHtml("<br/>".join(chunks))
        if self.auto_scroll.isChecked():
            self.viewer.verticalScrollBar().setValue(self.viewer.verticalScrollBar().maximum())

    def _line_html(self, text: str, is_error: bool) -> str:
        color = "#ff9090" if is_error else "#d8e3ff"
        return f"<span style='color:{color};'>{escape(text)}</span>"

    def _is_error_line(self, line: str) -> bool:
        lower = str(line or "").lower()
        return "[오류]" in lower or " error:" in lower or "failed" in lower or "exception" in lower
