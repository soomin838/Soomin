from __future__ import annotations

import webbrowser
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QIcon, QFont, QColor, QPainter, QBrush, QPixmap
from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLabel,
    QPushButton,
    QFrame,
    QTextEdit,
    QProgressBar,
    QSizePolicy,
    QApplication
)

from ui.canvas.mascot_canvas import MascotCanvas
from ui.dialogs.settings_dialog import SettingsDialog


class MainWindow(QMainWindow):
    def __init__(
        self,
        controller,
        root,
        settings_context,
        theme_manager,
        gemini_usage_dashboard,
    ):
        super().__init__()
        self.controller = controller
        self.root = root
        self.settings_context = settings_context
        self.theme_manager = theme_manager
        self.gemini_usage_dashboard = gemini_usage_dashboard

        # Load QSS Style
        try:
            with open(self.root / "ui" / "styles" / "neon_theme.qss", "r", encoding="utf-8") as f:
                self.setStyleSheet(f.read())
        except Exception:
            pass

        self.setWindowTitle("RezeroAgent 2.0 - Command Center")
        self.resize(1360, 800)
        self.setMinimumSize(1200, 720)

        # Apply specific font across app if needed
        self.font_family = "Inter"

        self._build_ui()
        
        self.ui_timer = QTimer(self)
        self.ui_timer.timeout.connect(self._sync_ui_state)
        self.ui_timer.start(1000)
        
        # Start immediately syncing
        self._sync_ui_state()

    def _build_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        # Fixed single-pane layout (No scrollbars)
        layout = QVBoxLayout(main_widget)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        # Header Row
        header_layout = QHBoxLayout()
        title_lbl = QLabel("Rezero 2.0: Command Center")
        title_lbl.setProperty("class", "HeroTitle")
        
        self.status_lbl = QLabel("STATUS: IDLE")
        self.status_lbl.setProperty("class", "StatusLabel")
        self.status_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        
        header_layout.addWidget(title_lbl)
        header_layout.addStretch()
        header_layout.addWidget(self.status_lbl)
        layout.addLayout(header_layout)

        # 4-Grid System (Controls, Analytics, Live Feed, AI Core)
        grid = QGridLayout()
        grid.setSpacing(16)
        
        # 1. CONTROLS (Top Left)
        ctrl_frame = QFrame()
        ctrl_frame.setObjectName("CommandCenter")
        ctrl_v = QVBoxLayout(ctrl_frame)
        ctrl_lbl = QLabel("COMMAND & OVERVIEW")
        ctrl_lbl.setObjectName("Subtitle")
        
        self.btn_run = QPushButton("▶ Run / Pause Engine")
        self.btn_run.setProperty("class", "PrimaryAction")
        self.btn_run.clicked.connect(self._toggle_run)
        self.btn_run.setFixedHeight(48)
        
        self.btn_force = QPushButton("⚡ 즉시 발행 (Publish Now)")
        self.btn_force.clicked.connect(self._force_run)
        self.btn_force.setFixedHeight(40)
        
        self.btn_settings = QPushButton("⚙ 시스템 설정 (Settings)")
        self.btn_settings.clicked.connect(self._open_settings)
        self.btn_settings.setFixedHeight(40)
        
        self.btn_dashboard = QPushButton("📊 구글 예산 대시보드")
        self.btn_dashboard.clicked.connect(lambda: webbrowser.open(self.gemini_usage_dashboard))
        self.btn_dashboard.setFixedHeight(40)

        ctrl_v.addWidget(ctrl_lbl)
        ctrl_v.addSpacing(16)
        ctrl_v.addWidget(self.btn_run)
        ctrl_v.addWidget(self.btn_force)
        ctrl_v.addWidget(self.btn_settings)
        ctrl_v.addWidget(self.btn_dashboard)
        ctrl_v.addStretch()

        # 2. ANALYTICS (Top Right)
        stat_frame = QFrame()
        stat_frame.setObjectName("CommandCenter")
        stat_v = QVBoxLayout(stat_frame)
        stat_lbl = QLabel("LIVE ANALYTICS")
        stat_lbl.setObjectName("Subtitle")
        
        self.usage_view = QLabel("로딩 중...")
        self.usage_view.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.usage_view.setWordWrap(True)
        self.usage_view.setObjectName("Value")
        
        self.timer_view = QLabel("다음 실행: -")
        self.timer_view.setAlignment(Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignRight)
        
        stat_v.addWidget(stat_lbl)
        stat_v.addSpacing(12)
        stat_v.addWidget(self.usage_view, stretch=1)
        stat_v.addWidget(self.timer_view)
        
        # 3. LIVE FEED (Bottom Left)
        feed_frame = QFrame()
        feed_frame.setObjectName("CommandCenter")
        feed_v = QVBoxLayout(feed_frame)
        feed_lbl = QLabel("LIVE FEED & LOGS")
        feed_lbl.setObjectName("Subtitle")
        
        self.log_viewer = QTextEdit()
        self.log_viewer.setObjectName("LogViewer")
        self.log_viewer.setReadOnly(True)
        # Using a fixed layout to prevent scroll where possible, but logs need scrolling eventually.
        self.log_viewer.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff) 
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setFixedHeight(6)

        feed_v.addWidget(feed_lbl)
        feed_v.addWidget(self.progress_bar)
        feed_v.addSpacing(8)
        feed_v.addWidget(self.log_viewer)
        
        # 4. AI CORE (Bottom Right)
        core_frame = QFrame()
        core_frame.setObjectName("CommandCenter")
        core_v = QVBoxLayout(core_frame)
        core_lbl = QLabel("AI CORE ORBIT")
        core_lbl.setObjectName("Subtitle")
        core_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.mascot_canvas = MascotCanvas()
        
        # Phase Subtitle
        self.phase_lbl = QLabel("Phase: IDLE")
        self.phase_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.phase_lbl.setProperty("class", "StatusLabel")

        core_v.addWidget(core_lbl)
        core_v.addWidget(self.mascot_canvas, stretch=1)
        core_v.addWidget(self.phase_lbl)
        
        # Assemble Grid
        # 0,0 | 0,1
        # 1,0 | 1,1
        grid.addWidget(ctrl_frame, 0, 0)
        grid.addWidget(stat_frame, 0, 1)
        grid.addWidget(feed_frame, 1, 0)
        grid.addWidget(core_frame, 1, 1)
        
        # Set proportions
        grid.setRowStretch(0, 1)
        grid.setRowStretch(1, 1)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 1)

        layout.addLayout(grid, stretch=1)

    def _sync_ui_state(self):
        ui = self.controller.get_ui_snapshot()
        mode = "RUNNING" if self.controller.running else "PAUSED"
        color = "#C4A1FF" if self.controller.running else "#FF8A8A"
        
        # Sync Status Text
        self.status_lbl.setText(f"ENGINE: {mode}")
        self.status_lbl.setStyleSheet(f"color: {color}; font-size: 14px; font-weight: 800;")
        
        # Sync Run Button
        if self.controller.running:
            self.btn_run.setText("⏸ Pause Engine")
            self.btn_run.setStyleSheet("")
        else:
            self.btn_run.setText("▶ Run Engine")
            self.btn_run.setStyleSheet("background-color: rgba(196, 161, 255, 0.15); border: 1px solid #C4A1FF; color: #E8DEFF;")
            
        # Sync Analytics
        status_raw = ui.get("status", "Idle")
        phase = ui.get("phase_key", "")
        pct = ui.get("phase_percent", 0)
        msg = ui.get("phase_message", "")
        
        self.timer_view.setText(f"{ui.get('next_run_text', '-')}")
        
        usage_txt = self.controller.usage_text()
        resume_txt = self.controller.resume_text()
        self.usage_view.setText(f"{usage_txt}\n\n[RESUME]: {resume_txt}")
        
        # Sync Logs (Append only if new msg)
        current_text = self.log_viewer.toPlainText()
        log_line = f"[{phase.upper()}] {msg}"
        if not current_text.endswith(log_line) and msg:
            self.log_viewer.append(log_line)
            # Auto scroll to bottom
            sb = self.log_viewer.verticalScrollBar()
            sb.setValue(sb.maximum())

        # Sync Progress & Mascot
        self.progress_bar.setValue(pct)
        self.phase_lbl.setText(f"Core: {phase.upper()} ({pct}%)")
        
        # Translate detailed status to mascot colors
        if not self.controller.running:
            self.mascot_canvas.set_state("paused", pct)
        elif status_raw in ["Error", "QuotaWait"] or phase == "error":
            self.mascot_canvas.set_state("error", pct)
        elif status_raw == "Running" or phase not in ["idle", ""]:
            self.mascot_canvas.set_state("running", pct)
        elif status_raw == "Success":
            self.mascot_canvas.set_state("success", pct)
        else:
            self.mascot_canvas.set_state("idle", pct)

    def _toggle_run(self):
        if self.controller.running:
            self.controller.stop()
        else:
            self.controller.start()
        self._sync_ui_state()

    def _force_run(self):
        # CRASH FIX: Do not call workflow.run_once on GUI thread!
        # Tell the controller's background loop to run immediately.
        self.controller.force_run = True
        self.controller.next_run_at = self.controller.tz.localize(self.controller.next_run_at.replace(tzinfo=None)) if self.controller.next_run_at.tzinfo is None else self.controller.next_run_at
        
        if not self.controller.running:
            self.controller.start()
            
        self.log_viewer.append(">>> 강제 실행 트리거가 활성화되었습니다. <<<")
        self._sync_ui_state()

    def _open_settings(self):
        from ui.dialogs.settings_dialog import SettingsDialog
        dlg = SettingsDialog(context=self.settings_context, on_saved=None, required_only=False)
        dlg.exec()
        self.controller.reload()
        self._sync_ui_state()
