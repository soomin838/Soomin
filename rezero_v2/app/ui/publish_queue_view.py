from __future__ import annotations

from PySide6.QtWidgets import QFrame, QLabel, QListWidget, QListWidgetItem, QSplitter, QVBoxLayout, QWidget


class PublishQueueView(QWidget):
    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        splitter = QSplitter()
        self.post_list = QListWidget()
        self.run_list = QListWidget()
        splitter.addWidget(self._wrap("최근 게시물", self.post_list))
        splitter.addWidget(self._wrap("최근 실행 결과", self.run_list))
        root.addWidget(splitter)

    def update_state(self, state) -> None:
        self.post_list.clear()
        for post in state.recent_posts:
            QListWidgetItem(f"[{post.status}] {post.title}", self.post_list)
        self.run_list.clear()
        for run in state.recent_runs:
            QListWidgetItem(f"[{run.result}] {run.reason_code} :: {run.selected_title}", self.run_list)

    def _wrap(self, title: str, widget) -> QFrame:
        frame = QFrame()
        frame.setProperty("card", True)
        layout = QVBoxLayout(frame)
        layout.addWidget(QLabel(title))
        layout.addWidget(widget)
        return frame
