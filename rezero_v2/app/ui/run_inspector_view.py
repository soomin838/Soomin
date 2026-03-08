from __future__ import annotations

from PySide6.QtWidgets import QFrame, QLabel, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget


class RunInspectorView(QWidget):
    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        self.summary = QLabel("최근 실행 정보가 없습니다.")
        self.summary.setWordWrap(True)
        root.addWidget(self.summary)
        frame = QFrame()
        frame.setProperty("card", True)
        layout = QVBoxLayout(frame)
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["단계", "상태", "사유", "ms"])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)
        root.addWidget(frame)

    def update_state(self, state) -> None:
        summary = state.latest_final_summary or {}
        self.summary.setText(
            f"run_id={summary.get('run_id', '-')}, result={summary.get('result', '-')}, "
            f"reason={summary.get('reason_code', '-')}, title={summary.get('selected_title', '-')}"
        )
        rows = list(state.inspector_stage_rows or [])
        self.table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            self.table.setItem(row_index, 0, QTableWidgetItem(str(row.get("stage_name", ""))))
            self.table.setItem(row_index, 1, QTableWidgetItem(str(row.get("status", ""))))
            self.table.setItem(row_index, 2, QTableWidgetItem(str(row.get("reason_code", ""))))
            self.table.setItem(row_index, 3, QTableWidgetItem(str(row.get("timing_ms", 0))))
