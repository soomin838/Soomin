from __future__ import annotations


def build_stylesheet() -> str:
    return """
QWidget {
  background: #f5f7fb;
  color: #142033;
  font-family: "Segoe UI", "Malgun Gothic";
  font-size: 11pt;
}
QMainWindow {
  background: #eef3f8;
}
QFrame[card="true"] {
  background: #ffffff;
  border: 1px solid #d7e0ea;
  border-radius: 18px;
}
QPushButton {
  background: #1e6df2;
  color: white;
  border: none;
  border-radius: 12px;
  padding: 10px 16px;
  font-weight: 600;
}
QPushButton:disabled {
  background: #9ab7ef;
}
QLineEdit, QPlainTextEdit, QSpinBox, QComboBox, QListWidget, QTreeWidget, QTableWidget {
  background: #fbfdff;
  border: 1px solid #d7e0ea;
  border-radius: 12px;
  padding: 6px;
}
QTabWidget::pane {
  border: none;
}
QTabBar::tab {
  background: #dfe8f3;
  border-radius: 10px;
  padding: 8px 14px;
  margin-right: 6px;
}
QTabBar::tab:selected {
  background: #1e6df2;
  color: white;
}
QLabel[muted="true"] {
  color: #5f6f86;
}
QLabel[title="true"] {
  font-size: 18pt;
  font-weight: 700;
}
"""
