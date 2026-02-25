from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFontMetrics
from PySide6.QtWidgets import QHBoxLayout, QLabel, QVBoxLayout, QWidget

from ui.widgets.motion_button import MotionButton


class TopNav(QWidget):
    refresh_clicked = Signal()
    settings_clicked = Signal()
    logs_clicked = Signal()
    help_clicked = Signal()
    theme_mode_changed = Signal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("TopNav")
        self.setMinimumHeight(96)

        row = QHBoxLayout(self)
        row.setContentsMargins(16, 12, 16, 12)
        row.setSpacing(10)

        brand = QVBoxLayout()
        brand.setSpacing(2)
        self.title_label = QLabel("RezeroAgent")
        self.title_label.setObjectName("Title")
        self.title_label.setWordWrap(False)
        self.blog_name = QLabel("블로그: -")
        self.blog_name.setObjectName("Subtitle")
        self.blog_name.setWordWrap(False)
        self.mascot_state = QLabel("◕‿◕")
        self.mascot_state.setObjectName("Subtitle")

        mascot_row = QHBoxLayout()
        mascot_row.setContentsMargins(0, 0, 0, 0)
        mascot_row.setSpacing(6)
        mascot_row.addWidget(self.blog_name)
        mascot_row.addWidget(self.mascot_state)
        mascot_row.addStretch(1)

        self.sub_label = QLabel("macOS 글래스 감성 자동화 스튜디오")
        self.sub_label.setObjectName("Subtitle")
        self.sub_label.setWordWrap(False)
        brand.addWidget(self.title_label)
        brand.addLayout(mascot_row)
        brand.addWidget(self.sub_label)
        row.addLayout(brand, 2)

        center = QHBoxLayout()
        center.setContentsMargins(0, 0, 0, 0)
        center.setSpacing(8)
        self.status_chip = QLabel("Idle")
        self.status_chip.setObjectName("StatusChip")
        self.countdown_label = QLabel("다음 실행까지 00:00:00")
        self.countdown_label.setObjectName("Subtitle")
        self.countdown_label.setWordWrap(False)
        center.addWidget(self.status_chip)
        center.addWidget(self.countdown_label)
        row.addLayout(center, 1)

        self.help_btn = MotionButton("도움말")
        self.help_btn.clicked.connect(self.help_clicked.emit)
        row.addWidget(self.help_btn)

        self.logs_btn = MotionButton("로그")
        self.logs_btn.clicked.connect(self.logs_clicked.emit)
        row.addWidget(self.logs_btn)

        self.settings_btn = MotionButton("설정")
        self.settings_btn.clicked.connect(self.settings_clicked.emit)
        row.addWidget(self.settings_btn)

        self.refresh_btn = MotionButton("새로고침")
        self.refresh_btn.clicked.connect(self.refresh_clicked.emit)
        row.addWidget(self.refresh_btn)

        self.theme_toggle_btn = MotionButton("라이트/다크")
        self.theme_toggle_btn.clicked.connect(self._toggle_theme_mode)
        row.addWidget(self.theme_toggle_btn)

        self._mode = "auto"
        self._full_blog_name = "블로그: -"
        self._compact = False
        self._apply_responsive_mode()

    def set_theme_mode(self, mode: str) -> None:
        clean = str(mode or "auto").strip().lower()
        self._mode = clean if clean in {"auto", "light", "dark"} else "auto"

    def set_status(self, status: str, countdown_text: str, blog_name: str = "") -> None:
        clean_status = str(status or "Idle")
        self.status_chip.setText(clean_status)
        self.countdown_label.setText(str(countdown_text or "다음 실행까지 00:00:00"))
        if blog_name:
            self._full_blog_name = f"블로그: {blog_name}"
        self._apply_blog_elide()

        lower = clean_status.lower()
        if "running" in lower:
            self.mascot_state.setText("૮₍ ˶ᵔ ᵕ ᵔ˶ ₎ა")
        elif "error" in lower:
            self.mascot_state.setText("( •︠ˍ•︡ )")
        elif "hold" in lower or "warning" in lower:
            self.mascot_state.setText("(｡•́︿•̀｡)")
        elif "success" in lower:
            self.mascot_state.setText("٩(ˊᗜˋ*)و")
        else:
            self.mascot_state.setText("◕‿◕")

    def _toggle_theme_mode(self) -> None:
        if self._mode in {"auto", "dark"}:
            self._mode = "light"
        else:
            self._mode = "dark"
        self.theme_mode_changed.emit(self._mode)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._apply_responsive_mode()

    def _apply_responsive_mode(self) -> None:
        compact = self.width() < 1300
        if compact != self._compact:
            self._compact = compact
            self.sub_label.setVisible(not compact)
            self.help_btn.setText("도움" if compact else "도움말")
            self.refresh_btn.setText("새로" if compact else "새로고침")
            self.theme_toggle_btn.setText("테마" if compact else "라이트/다크")
            self.setMinimumHeight(84 if compact else 96)
        self._apply_blog_elide()

    def _apply_blog_elide(self) -> None:
        text = str(self._full_blog_name or "블로그: -")
        width = max(120, self.blog_name.width() - 8)
        fm = QFontMetrics(self.blog_name.font())
        self.blog_name.setText(fm.elidedText(text, Qt.TextElideMode.ElideRight, width))
