from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QFrame, QGridLayout, QLabel, QListWidget, QListWidgetItem, QPushButton, QVBoxLayout, QWidget


class DashboardView(QWidget):
    run_requested = Signal()
    refresh_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        header = QLabel("RezeroAgent V2")
        header.setProperty("title", True)
        subtitle = QLabel("단계별 상태와 최종 사유가 항상 보이는 V2 대시보드")
        subtitle.setProperty("muted", True)
        root.addWidget(header)
        root.addWidget(subtitle)

        button_row = QGridLayout()
        self.run_button = QPushButton("지금 실행")
        self.refresh_button = QPushButton("새로고침")
        self.run_button.clicked.connect(self.run_requested.emit)
        self.refresh_button.clicked.connect(self.refresh_requested.emit)
        button_row.addWidget(self.run_button, 0, 0)
        button_row.addWidget(self.refresh_button, 0, 1)
        root.addLayout(button_row)

        cards_frame = QFrame()
        cards_frame.setProperty("card", True)
        self.cards_layout = QGridLayout(cards_frame)
        self.cards_layout.setContentsMargins(16, 16, 16, 16)
        self.cards_layout.setHorizontalSpacing(12)
        self.cards_layout.setVerticalSpacing(12)
        root.addWidget(cards_frame)

        status_frame = QFrame()
        status_frame.setProperty("card", True)
        status_layout = QVBoxLayout(status_frame)
        self.status_label = QLabel("상태: idle")
        self.message_label = QLabel("대기 중")
        self.message_label.setProperty("muted", True)
        self.next_run_label = QLabel("다음 실행: -")
        self.final_summary_label = QLabel("최종 결과: 없음")
        self.final_summary_label.setWordWrap(True)
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.message_label)
        status_layout.addWidget(self.next_run_label)
        status_layout.addWidget(self.final_summary_label)
        root.addWidget(status_frame)

        lists_layout = QGridLayout()
        self.skip_list = QListWidget()
        self.publish_list = QListWidget()
        lists_layout.addWidget(self._wrap_list("최근 보류/건너뜀 사유", self.skip_list), 0, 0)
        lists_layout.addWidget(self._wrap_list("최근 발행 결과", self.publish_list), 0, 1)
        root.addLayout(lists_layout)

    def update_state(self, state) -> None:
        self.status_label.setText(f"상태: {state.current_status} / 단계: {state.current_stage}")
        self.message_label.setText(state.current_message or "대기 중")
        self.next_run_label.setText(f"다음 실행: {state.next_run_at or '-'}")
        summary = state.latest_final_summary or {}
        self.final_summary_label.setText(
            f"최종 결과: {summary.get('result', 'none')} | 사유: {summary.get('reason_code', '-') or '-'}"
        )
        while self.cards_layout.count():
            item = self.cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        for index, card in enumerate(state.dashboard_cards):
            frame = QFrame()
            frame.setProperty("card", True)
            layout = QVBoxLayout(frame)
            label = QLabel(card.label)
            label.setProperty("muted", True)
            value = QLabel(card.value)
            value.setProperty("title", True)
            layout.addWidget(label)
            layout.addWidget(value)
            self.cards_layout.addWidget(frame, 0, index)
        self.skip_list.clear()
        for reason in state.recent_skip_reasons:
            QListWidgetItem(reason, self.skip_list)
        self.publish_list.clear()
        for post in state.recent_posts[:6]:
            QListWidgetItem(f"[{post.status}] {post.title}", self.publish_list)

    def set_busy(self, busy: bool) -> None:
        self.run_button.setDisabled(bool(busy))

    def _wrap_list(self, title: str, widget) -> QFrame:
        frame = QFrame()
        frame.setProperty("card", True)
        layout = QVBoxLayout(frame)
        label = QLabel(title)
        layout.addWidget(label)
        layout.addWidget(widget)
        return frame
