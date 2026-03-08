from __future__ import annotations

from PySide6.QtWidgets import QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget


class RunHistoryView(QWidget):
    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["실행 ID", "결과", "사유", "제목", "유형"])
        self.table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self.table)

    def update_state(self, state) -> None:
        self.table.setRowCount(len(state.recent_runs))
        for row_index, row in enumerate(state.recent_runs):
            self.table.setItem(row_index, 0, QTableWidgetItem(row.run_id))
            self.table.setItem(row_index, 1, QTableWidgetItem(row.result))
            self.table.setItem(row_index, 2, QTableWidgetItem(row.reason_code))
            self.table.setItem(row_index, 3, QTableWidgetItem(row.selected_title))
            self.table.setItem(row_index, 4, QTableWidgetItem(row.content_type))
