from __future__ import annotations

import os
import threading
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

from PySide6.QtCore import QEasingCurve, QPoint, QPropertyAnimation, QSequentialAnimationGroup, QTimer, Qt, QUrl
from PySide6.QtGui import QBrush, QDesktopServices, QFont, QFontDatabase, QFontMetrics, QPalette, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QBoxLayout,
    QFileDialog,
    QFormLayout,
    QFrame,
    QGraphicsOpacityEffect,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from core.blog_pdf import export_blog_posts_pdf
from core.insights import GrowthInsights
from ui.canvas.mascot_canvas import MascotAssetManager, MascotCanvas
from ui.canvas.pipeline_canvas import PipelineCanvas
from ui.dialogs.logs_dialog import LogsDialog
from ui.dialogs.settings_dialog import SettingsDialog, SettingsDialogContext
from ui.theme.theme_manager import ThemeManager
from ui.widgets.glass_card import GlassCard
from ui.widgets.log_panel import LogPanel
from ui.widgets.motion_button import MotionButton
from ui.widgets.top_nav import TopNav


class ToastManager(QWidget):
    def __init__(self, parent: QWidget, open_logs_callback) -> None:
        super().__init__(parent)
        self.open_logs_callback = open_logs_callback
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False)
        self.setObjectName("ToastLayer")
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(8)
        self._toasts: list[QWidget] = []

    def show_toast(self, message: str, level: str = "info", with_logs_button: bool = False) -> None:
        card = GlassCard(parent=self)
        card.set_state("error" if level == "error" else ("warning" if level == "warning" else "active"))
        card.setFixedWidth(min(420, max(280, int(self.width() * 0.95))))
        row = QHBoxLayout(card)
        row.setContentsMargins(10, 8, 10, 8)
        row.setSpacing(8)
        msg = QLabel(str(message or ""))
        msg.setWordWrap(True)
        msg.setObjectName("ValueSmall")
        row.addWidget(msg, 1)
        if with_logs_button:
            btn = MotionButton("로그 보기")
            btn.clicked.connect(self.open_logs_callback)
            row.addWidget(btn)
        self._layout.addWidget(card, 0, Qt.AlignmentFlag.AlignRight)
        self._toasts.append(card)

        self._animate_toast_in(card)
        QTimer.singleShot(2600, lambda w=card: self._dismiss(w))

    def _animate_toast_in(self, toast: QWidget) -> None:
        start = toast.pos() + QPoint(36, 0)
        end = toast.pos()
        toast.move(start)
        anim = QPropertyAnimation(toast, b"pos", self)
        anim.setDuration(220)
        anim.setStartValue(start)
        anim.setEndValue(end)
        anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        anim.start()

    def _dismiss(self, toast: QWidget) -> None:
        if toast not in self._toasts:
            return
        self._toasts.remove(toast)
        toast.hide()
        toast.deleteLater()


class MainWindow(QMainWindow):
    def __init__(
        self,
        controller,
        root: Path,
        settings_context: SettingsDialogContext,
        theme_manager: ThemeManager,
        gemini_usage_dashboard: str,
    ) -> None:
        super().__init__()
        self.controller = controller
        self.root = root
        self.settings_context = settings_context
        self.theme_manager = theme_manager
        self.gemini_usage_dashboard = gemini_usage_dashboard
        self.logs_dialog: LogsDialog | None = None
        self._console_entries: list[tuple[str, bool]] = []
        self._animations: list[QPropertyAnimation | QSequentialAnimationGroup] = []
        self._last_log_emit_epoch = 0.0
        self._last_status_key = "idle"
        self._last_phase_key = "idle"
        self._mascot_frame = 0
        self._primary_action_mode = "run"

        self._insights = GrowthInsights(
            credentials_path=self.root / self.controller.settings.blogger.credentials_path,
            settings=self.controller.settings.integrations,
        )
        self._insights_snapshot: dict = {}
        self._insights_loading = False
        self._insights_lock = threading.Lock()
        self._next_insights_refresh_epoch = 0.0
        warmup = time.time() + 2.0
        self._next_usage_panel_refresh_epoch = warmup
        self._next_errors_panel_refresh_epoch = warmup
        self._next_schedule_panel_refresh_epoch = warmup
        self._force_heavy_refresh = False
        self._last_errors_signature = ""
        self._last_schedule_signature = ""
        self._bg_cache_key: tuple[str, str, int, int] | None = None

        self.setWindowTitle("RezeroAgent Studio")
        self.resize(1360, 900)
        self.setMinimumSize(980, 660)
        self._apply_cute_font()
        self._build_ui()
        self._bind_top_nav()
        self._build_mascot_assets()
        self._sync_animation_intensity()

        self.controller.start()
        self._append_console_line("[시작] RezeroAgent Studio UI 시작")

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._tick)
        self.timer.start(1000)

        self.mascot_timer = QTimer(self)
        self.mascot_timer.timeout.connect(self._animate_mascot_frame)
        self.mascot_timer.start(120)
        self._bg_apply_timer = QTimer(self)
        self._bg_apply_timer.setSingleShot(True)
        self._bg_apply_timer.timeout.connect(self._apply_background_art)

        self._animate_intro()
        self._tick()

    def _sync_animation_intensity(self) -> None:
        self.animation_intensity = str(self.theme_manager.animation_intensity() or "high").lower()
        if self.animation_intensity not in {"high", "medium", "off"}:
            self.animation_intensity = "high"

    def _apply_cute_font(self) -> None:
        app = QApplication.instance()
        if app is None:
            return
        font_dir = self.root / "storage" / "ui" / "fonts"
        font_dir.mkdir(parents=True, exist_ok=True)
        fallback_families = ["Segoe UI", "Malgun Gothic", "맑은 고딕", "Noto Sans CJK KR"]
        available = set(QFontDatabase.families())
        loaded_family = next((fam for fam in fallback_families if fam in available), "Segoe UI")
        for fp in sorted(font_dir.glob("*.ttf")) + sorted(font_dir.glob("*.otf")):
            fid = QFontDatabase.addApplicationFont(str(fp))
            if fid != -1:
                fams = QFontDatabase.applicationFontFamilies(fid)
                if fams:
                    candidate = QFont(fams[0], 10)
                    metrics = QFontMetrics(candidate)
                    # Guard against decorative fonts causing clipped text on compact layouts.
                    if metrics.height() <= 18 and metrics.lineSpacing() <= 19:
                        loaded_family = fams[0]
                        break
        app.setFont(QFont(loaded_family, 10))

    def _build_ui(self) -> None:
        self.app_root = QWidget(self)
        self.app_root.setObjectName("AppRoot")
        root = QVBoxLayout(self.app_root)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        self.top_nav = TopNav()
        self.top_nav.set_theme_mode(self.theme_manager.mode)
        root.addWidget(self.top_nav)

        self.content_row = QBoxLayout(QBoxLayout.Direction.LeftToRight)
        self.content_row.setSpacing(10)
        root.addLayout(self.content_row, 5)

        # Left column - Hero Mascot Central
        left = QWidget()
        left.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        left_col = QVBoxLayout(left)
        left_col.setContentsMargins(0, 0, 0, 0)
        left_col.setSpacing(20)

        self.hero_card = GlassCard(state="active")
        self.hero_card.setObjectName("HeroCard")
        hero_layout = QVBoxLayout(self.hero_card)
        hero_layout.setContentsMargins(30, 30, 30, 30)
        hero_layout.setSpacing(15)

        self.mascot_canvas = MascotCanvas()
        self.mascot_canvas.setMinimumSize(400, 320)
        hero_layout.addWidget(self.mascot_canvas, 0, Qt.AlignmentFlag.AlignCenter)

        self.hero_stage = QLabel("대기 중")
        self.hero_stage.setObjectName("Title")
        self.hero_stage.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.hero_stage.setStyleSheet("font-size: 34px; margin-top: 10px;")
        
        self.hero_task = QLabel("리지는 새로운 소식을 찾을 준비가 됐어요! ✨")
        self.hero_task.setObjectName("Subtitle")
        self.hero_task.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.hero_task.setWordWrap(True)
        self.hero_task.setStyleSheet("font-size: 18px; color: #3182f6; font-weight: 600;")

        self.hero_progress = QProgressBar()
        self.hero_progress.setRange(0, 100)
        self.hero_progress.setValue(0)
        self.hero_progress.setFixedHeight(12)
        
        self.hero_primary_btn = MotionButton("지금 시작하기 🚀")
        self.hero_primary_btn.setObjectName("PrimaryBtn")
        self.hero_primary_btn.setMinimumHeight(56)
        self.hero_primary_btn.clicked.connect(self._on_primary_action)

        hero_layout.addWidget(self.hero_stage)
        hero_layout.addWidget(self.hero_task)
        hero_layout.addSpacing(10)
        hero_layout.addWidget(self.hero_progress)
        hero_layout.addSpacing(20)
        hero_layout.addWidget(self.hero_primary_btn, 0, Qt.AlignmentFlag.AlignCenter)
        hero_layout.addStretch(1)

        left_col.addWidget(self.hero_card, 5)

        # Timeline integration into left below hero
        self.timeline_card = GlassCard()
        timeline_layout = QVBoxLayout(self.timeline_card)
        timeline_layout.setContentsMargins(20, 20, 20, 20)
        t_title = QLabel("현재 진행 워크플로우")
        t_title.setObjectName("ValueSmall")
        self.pipeline_canvas = PipelineCanvas()
        self.pipeline_canvas.setMinimumHeight(100)
        timeline_layout.addWidget(t_title)
        timeline_layout.addWidget(self.pipeline_canvas)
        left_col.addWidget(self.timeline_card, 2)

        self.left_col_widget = left
        self.content_row.addWidget(left, 5)

        # Right column - Summary & Logs
        right = QWidget()
        right.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        right_col = QVBoxLayout(right)
        right_col.setContentsMargins(0, 0, 0, 0)
        right_col.setSpacing(15)

        # Combined Info Panel (Usage + Schedule)
        self.info_panel = GlassCard()
        info_lay = QVBoxLayout(self.info_panel)
        info_lay.setContentsMargins(20, 20, 20, 20)
        info_title = QLabel("오늘의 리포트 📋")
        info_title.setObjectName("ValueSmall")
        self.usage_lines = QLabel("-")
        self.usage_lines.setObjectName("ValueSmall")
        self.usage_lines.setStyleSheet("color: #4e5968; font-weight: 400;")
        info_lay.addWidget(info_title)
        info_lay.addWidget(self.usage_lines)
        right_col.addWidget(self.info_panel, 3)

        self.error_panel = GlassCard(state="warning")
        err_layout = QVBoxLayout(self.error_panel)
        err_layout.setContentsMargins(20, 20, 20, 20)
        err_title = QLabel("알림 및 이슈")
        err_title.setObjectName("ValueSmall")
        self.error_cards_host = QWidget()
        self.error_cards_wrap = QVBoxLayout(self.error_cards_host)
        self.error_scroll = QScrollArea()
        self.error_scroll.setWidgetResizable(True)
        self.error_scroll.setWidget(self.error_cards_host)
        err_layout.addWidget(err_title)
        err_layout.addWidget(self.error_scroll, 1)
        right_col.addWidget(self.error_panel, 4)

        self.right_col_widget = right
        self.content_row.addWidget(right, 3)

        # Bottom bar
        self.bottom_bar = GlassCard()
        bottom_row = QHBoxLayout(self.bottom_bar)
        bottom_row.setContentsMargins(12, 10, 12, 10)
        bottom_row.setSpacing(8)

        self.dry_run_btn = MotionButton("드라이런: OFF")
        self.dry_run_btn.clicked.connect(self._toggle_dry_run)
        self.preview_btn = MotionButton("HTML 미리보기")
        self.preview_btn.clicked.connect(self._preview_html)
        bottom_row.addWidget(self.dry_run_btn)
        bottom_row.addWidget(self.preview_btn)
        bottom_row.addStretch(1)

        self.bottom_settings_btn = MotionButton("설정")
        self.bottom_settings_btn.clicked.connect(self.open_settings)
        self.bottom_exit_btn = MotionButton("종료")
        self.bottom_exit_btn.setObjectName("DangerBtn")
        self.bottom_exit_btn.clicked.connect(self.close)
        bottom_row.addWidget(self.bottom_settings_btn)
        bottom_row.addWidget(self.bottom_exit_btn)
        root.addWidget(self.bottom_bar)

        # Log panel
        self.log_panel = LogPanel()
        root.addWidget(self.log_panel, 2)

        self.root_scroll = QScrollArea(self)
        self.root_scroll.setWidgetResizable(True)
        self.root_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.root_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.root_scroll.setWidget(self.app_root)
        self.setCentralWidget(self.root_scroll)

        self.toast_manager = ToastManager(self.app_root, self.open_logs_dialog)
        self.toast_manager.resize(380, 260)
        self._apply_responsive_layout()
        self._position_toast_layer()

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._apply_responsive_layout()
        self._position_toast_layer()
        self._relayout_timeline_steps()
        self._bg_apply_timer.start(80)

    def _apply_responsive_layout(self) -> None:
        if not hasattr(self, "content_row"):
            return
        width = max(1, self.app_root.width())
        stacked = width < 1260
        target_dir = QBoxLayout.Direction.TopToBottom if stacked else QBoxLayout.Direction.LeftToRight
        if self.content_row.direction() != target_dir:
            self.content_row.setDirection(target_dir)
        if stacked:
            self.content_row.setStretch(0, 0)
            self.content_row.setStretch(1, 0)
            self.left_col_widget.setMinimumWidth(0)
            self.right_col_widget.setMinimumWidth(0)
        else:
            self.content_row.setStretch(0, 3)
            self.content_row.setStretch(1, 2)
            self.left_col_widget.setMinimumWidth(0)
            self.right_col_widget.setMinimumWidth(420)

    def _position_toast_layer(self) -> None:
        if not hasattr(self, "toast_manager"):
            return
        margin = 18
        w = min(420, max(280, int(self.app_root.width() * 0.30)))
        h = min(300, max(180, int(self.app_root.height() * 0.32)))
        x = max(0, self.app_root.width() - w - margin)
        y = max(0, self.app_root.height() - h - margin)
        self.toast_manager.setGeometry(x, y, w, h)

    def _bind_top_nav(self) -> None:
        self.top_nav.refresh_clicked.connect(self._on_refresh_clicked)
        self.top_nav.settings_clicked.connect(self.open_settings)
        self.top_nav.logs_clicked.connect(self.open_logs_dialog)
        self.top_nav.help_clicked.connect(self.open_help)
        self.top_nav.theme_mode_changed.connect(self._change_theme_mode)

    def _on_refresh_clicked(self) -> None:
        self._force_heavy_refresh = True
        self._tick(force=True)

    def _build_mascot_assets(self) -> None:
        self.mascot_assets = MascotAssetManager(
            runtime_root=self.root,
            pollinations_api_key="",
            pollinations_base_url="",
            allow_ui_api_calls=False,
        )
        self.mascot_canvas.set_asset_manager(self.mascot_assets)
        self.mascot_assets.prewarm()
        self._bg_cache_key = None
        self._apply_background_art()

    def _apply_background_art(self) -> None:
        if not hasattr(self, "app_root") or not hasattr(self, "mascot_assets"):
            return
        try:
            mode = self.theme_manager.resolved_mode()
            bg_path = self.mascot_assets.background_path(mode)
            if not bg_path.exists():
                return
            w = max(1, self.app_root.width())
            h = max(1, self.app_root.height())
            key = (mode, str(bg_path), max(1, w // 16), max(1, h // 16))
            if key == self._bg_cache_key:
                return
            pix = QPixmap(str(bg_path))
            if pix.isNull():
                return
            scaled = pix.scaled(
                w,
                h,
                Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                Qt.TransformationMode.SmoothTransformation,
            )
            palette = self.app_root.palette()
            palette.setBrush(QPalette.ColorRole.Window, QBrush(scaled))
            self.app_root.setAutoFillBackground(True)
            self.app_root.setPalette(palette)
            self._bg_cache_key = key
        except Exception:
            return

    def _tick(self, force: bool = False) -> None:
        self._schedule_insights_refresh()
        snap = self.controller.get_ui_snapshot()
        snap["insights_snapshot"] = dict(self._insights_snapshot or {})
        self.controller._ui_insights_snapshot = dict(self._insights_snapshot or {})

        msg = str(snap.get("last_message", "") or "")

        # Mascot-style messages
        status_map = {
            "Idle": "리지는 지금 꿀잠 자는 중... 💤",
            "Running": "리지가 열심히 소식을 분석하고 있어요! ✎",
            "Hold": "잠시만요! 리지가 생각을 정리하고 있어요. 🤔",
            "Error": "앗! 리지가 넘어졌어요. 확인이 필요해요! 🤕",
            "Success": "와아! 리지가 완벽하게 글을 완성했어요! ✨"
        }
        
        display_status = status_map.get(status, f"리지는 지금 {status} 중!")
        self.hero_stage.setText(display_status)
        
        rezy_talk = f"{phase}"
        if msg and msg != "Ready":
            rezy_talk += f" | {msg}"
        self.hero_task.setText(rezy_talk)
        
        self.hero_progress.setValue(percent)

        mode = "run"
        lower = status.lower()
        if "running" in lower:
            mode = "pause"
            self.hero_primary_btn.setText("잠시만 멈춰줘 ✋")
        elif "paused" in lower:
            mode = "resume"
            self.hero_primary_btn.setText("다시 시작해볼까? ▶")
        elif "hold" in lower or "error" in lower:
            mode = "resume"
            self.hero_primary_btn.setText("다시 해보자! ↻")
        else:
            self.hero_primary_btn.setText("뉴스 포스팅 시작하기 🚀")
        self._primary_action_mode = mode

        state = "active"
        if "error" in lower:
            state = "error"
        elif "hold" in lower:
            state = "warning"
        elif "success" in lower:
            state = "success"
        self.hero_card.set_state(state)

    def _format_countdown(self) -> str:
        now = datetime.now(self.controller.tz)
        delta = self.controller.next_run_at - now
        sec = max(0, int(delta.total_seconds()))
        hh = sec // 3600
        mm = (sec % 3600) // 60
        ss = sec % 60
        return f"다음 실행까지 {hh:02d}:{mm:02d}:{ss:02d}"

    def _apply_top_status(self, snap: dict) -> None:
        status = str(snap.get("status", "Idle") or "Idle")
        blog_name = str(getattr(self.controller.settings.blogger, "blog_id", "") or "-")
        self.top_nav.set_status(status, self._format_countdown(), blog_name=blog_name)

    def _apply_hero(self, snap: dict) -> None:
        status = str(snap.get("status", "Idle") or "Idle")
        phase = str(snap.get("phase_message", "대기 중") or "대기 중")
        percent = int(snap.get("phase_percent", 0) or 0)
        msg = str(snap.get("last_message", "") or "")

        # Mascot-style messages
        status_map = {
            "Idle": "리지는 지금 꿀잠 자는 중... 💤",
            "Running": "리지가 열심히 소식을 분석하고 있어요! ✎",
            "Hold": "잠시만요! 리지가 생각을 정리하고 있어요. 🤔",
            "Error": "앗! 리지가 넘어졌어요. 확인이 필요해요! 🤕",
            "Success": "와아! 리지가 완벽하게 글을 완성했어요! ✨"
        }
        
        display_status = status_map.get(status, f"리지는 지금 {status} 중!")
        self.hero_stage.setText(display_status)
        
        rezy_talk = f"{phase}"
        if msg and msg != "Ready":
            rezy_talk += f" | {msg}"
        self.hero_task.setText(rezy_talk)
        
        self.hero_progress.setValue(percent)

        mode = "run"
        lower = status.lower()
        if "running" in lower:
            mode = "pause"
            self.hero_primary_btn.setText("잠시만 멈춰줘 ✋")
        elif "paused" in lower:
            mode = "resume"
            self.hero_primary_btn.setText("다시 시작해볼까? ▶")
        elif "hold" in lower or "error" in lower:
            mode = "resume"
            self.hero_primary_btn.setText("다시 해보자! ↻")
        else:
            self.hero_primary_btn.setText("뉴스 포스팅 시작하기 🚀")
        self._primary_action_mode = mode

        state = "active"
        if "error" in lower:
            state = "error"
        elif "hold" in lower:
            state = "warning"
        elif "success" in lower:
            state = "success"
        self.hero_card.set_state(state)

    def _apply_timeline(self, snap: dict) -> None:
        status = str(snap.get("status", "idle") or "idle").lower()
        phase_key = str(snap.get("phase_key", "idle") or "idle")
        percent = int(snap.get("phase_percent", 0) or 0)
        self.pipeline_canvas.set_state(status, phase_key, percent, self._mascot_frame)
        self.log_panel.set_phase(phase_key)

        if status != self._last_status_key or phase_key != self._last_phase_key:
            if "error" in status:
                self._shake_widget(self.hero_card)
                self.toast_manager.show_toast("앗, 어딘가에서 문제가 생겼나봐요!", level="error", with_logs_button=True)
            elif "success" in status:
                self.toast_manager.show_toast("리지가 이번 글도 멋지게 마무리했어요!", level="info")
            self._force_heavy_refresh = True
            self._last_status_key = status
            self._last_phase_key = phase_key

    def _apply_errors_panel(self, snap: dict) -> None:
        raw_errors = list(snap.get("recent_errors", []) or [])
        errors = [raw_errors[i] for i in range(min(len(raw_errors), 4))]
        sig_parts = []
        for row in errors:
            r = dict(row or {})
            s = str(r.get("status", ""))
            c = str(r.get("created_at", ""))
            n = str(r.get("note", ""))
            n_short = n[:120] if len(n) > 120 else n
            sig_parts.append(f"{s}:{c}:{n_short}")
        
        signature = "|".join(sig_parts) or "__empty__"
        if signature == self._last_errors_signature:
            return
        self._last_errors_signature = signature
        self._clear_layout(self.error_cards_wrap)
        if not errors:
            empty = QLabel("리지가 발견한 문제가 없어요! 깔끔하네요 🌿")
            empty.setObjectName("ValueSmall")
            empty.setStyleSheet("padding: 20px; color: #8b95a1;")
            self.error_cards_wrap.addWidget(empty)
            self.error_panel.set_state("success")
            return

        self.error_panel.set_state("warning")
        for row in errors:
            card = GlassCard(state="warning")
            lay = QVBoxLayout(card)
            lay.setContentsMargins(15, 15, 15, 15)
            lay.setSpacing(10)

            r = dict(row or {})
            status = str(r.get("status", "error") or "error").upper()
            note = str(r.get("note", "") or "")
            ts_raw = str(r.get("created_at", "") or "")
            ts = ts_raw[:19].replace("T", " ") if len(ts_raw) >= 19 else ts_raw
            
            head = QLabel(f"🤕 {status} ({ts})")
            head.setObjectName("Subtitle")
            head.setStyleSheet("font-weight: 700; color: #ff4d94;")
            
            msg_text = note[:200] if len(note) > 200 else (note if note else "알 수 없는 오류")
            msg = QLabel(msg_text)
            msg.setWordWrap(True)
            msg.setObjectName("ValueSmall")
            msg.setStyleSheet("color: #4e5968;")

            actions = QHBoxLayout()
            btn_retry = MotionButton("다시 해보기")
            btn_retry.setMinimumHeight(36)
            btn_retry.clicked.connect(self.run_now)
            btn_logs = MotionButton("로그 보기", role="secondary")
            btn_logs.setMinimumHeight(36)
            btn_logs.clicked.connect(self.open_logs_dialog)
            
            actions.addWidget(btn_retry)
            actions.addWidget(btn_logs)

            lay.addWidget(head)
            lay.addWidget(msg)
            lay.addLayout(actions)
            self.error_cards_wrap.addWidget(card)

    def _apply_usage_panel(self, snap: dict) -> None:
        usage = dict(snap.get("usage_snapshot", {}) or {})
        calls = int(self.controller.workflow.logs.get_today_gemini_count())
        call_cap = int(self.controller.settings.budget.daily_gemini_call_limit)
        today_written = int(usage.get("today_written", 0))
        today_reserved = int(usage.get("today_reserved", 0))
        today_published = int(usage.get("today_published", 0))
        live_total = int(usage.get("blogger_live_total", 0))
        sched_total = int(usage.get("blogger_scheduled_total", 0))
        idx_count = int(usage.get("index_notified_today", 0))
        inspect_count = int(usage.get("inspection_checked_today", 0))
        img_passed = int(usage.get("image_pipeline_passed", 0))
        img_target = int(usage.get("image_pipeline_target", 0))

        queue_cap_days = max(1, int(self.controller.settings.publish.queue_horizon_hours / 24))
        queue_days = min(queue_cap_days, int(round(int(usage.get("scheduled_72h", 0)) / max(1, self.controller.settings.publish.daily_publish_cap))))

        lines = [
            f"Gemini API: {calls}/{call_cap}",
            f"오늘 생성/예약/게시: {today_written}/{today_reserved}/{today_published}",
            f"Blogger 누적(게시/예약): {live_total}/{sched_total}",
            f"Search Console(요청/검사): {idx_count}/{inspect_count}",
            f"이미지 파이프라인: {img_passed}/{img_target}",
            f"버퍼 게이지: {queue_days}/{queue_cap_days} days",
        ]
        self.usage_lines.setText("\n".join(lines))

    def _apply_schedule_panel(self, snap: dict) -> None:
        usage = dict(snap.get("usage_snapshot", {}) or {})
        raw_rows = list(usage.get("today_schedule_items", []) or [])
        try:
            if not raw_rows:
                snapshot = self.controller.workflow._blog_snapshot(force_refresh=False, allow_remote=False)  # noqa: SLF001
                raw_rows = list(snapshot.get("scheduled_items", []) or [])
        except Exception:
            pass

        rows = [raw_rows[i] for i in range(min(len(raw_rows), 10))]
        sig_parts = []
        for row in rows:
            r = dict(row or {})
            p = str(r.get("publish_at", ""))
            t_raw = str(r.get("title", ""))
            t = t_raw[:60] if len(t_raw) > 60 else t_raw
            u_raw = str(r.get("published_url", ""))
            u = u_raw[:60] if len(u_raw) > 60 else u_raw
            sig_parts.append(f"{p}:{t}:{u}")
        
        signature = "|".join(sig_parts) or "__empty__"
        if signature == self._last_schedule_signature:
            return
        self._last_schedule_signature = signature
        self._clear_layout(self.schedule_rows)
        if not rows:
            empty = QLabel("오늘 리지가 배달할 소식이 아직 없어요! 📬")
            empty.setObjectName("ValueSmall")
            empty.setStyleSheet("padding: 20px; color: #8b95a1;")
            self.schedule_rows.addWidget(empty)
            return

        for row in rows:
            box = GlassCard()
            lay = QHBoxLayout(box)
            lay.setContentsMargins(12, 10, 12, 10)
            lay.setSpacing(10)

            r = dict(row or {})
            title = str(r.get("title", "Untitled"))
            dt_raw = str(r.get("publish_at", ""))
            dt_text = dt_raw.replace("T", " ")[:16] if len(dt_raw) >= 16 else dt_raw
            
            is_pub = bool(str(r.get("published_url", "") or "").strip())
            status_tag = "✅ 게시됨" if is_pub else "⏳ 예약됨"
            
            short_title = title[:60] if len(title) > 60 else title
            txt = QLabel(f"{status_tag}\n{dt_text} | {short_title}...")
            txt.setObjectName("ValueSmall")
            txt.setWordWrap(True)
            txt.setStyleSheet("color: #333d4b; font-weight: 500;")
            lay.addWidget(txt, 1)

            url = str(r.get("published_url", "") or "").strip()
            if not url:
                url = str(r.get("source_url", "") or "").strip()
            
            if url:
                open_btn = MotionButton("보기", role="secondary")
                open_btn.setFixedWidth(60)
                open_btn.clicked.connect(lambda _=False, u=url: QDesktopServices.openUrl(QUrl(u)))
                lay.addWidget(open_btn)
            
            self.schedule_rows.addWidget(box)

    def _apply_recent_log_line(self, snap: dict) -> None:
        now_epoch = time.time()
        if now_epoch - self._last_log_emit_epoch < 60.0:
            return
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {snap.get('status', '')}: {snap.get('last_message', '')}"
        self._append_console_line(line)
        self._last_log_emit_epoch = now_epoch

    def _append_console_line(self, line: str) -> None:
        txt = str(line or "")
        is_err = self._is_error_log_line(txt)
        self._console_entries.append((txt, is_err))
        self.log_panel.append_line(txt)
        if self.logs_dialog is not None:
            self.logs_dialog.append_line(txt)

    def _is_error_log_line(self, line: str) -> bool:
        lower = str(line or "").lower()
        return "[오류]" in lower or " error:" in lower or "failed" in lower or "exception" in lower

    def _animate_intro(self) -> None:
        if self.animation_intensity == "off":
            return
        # Layout-managed widgets must not be moved directly; opacity-only intro avoids overlap/clipping.
        fx = QGraphicsOpacityEffect(self.app_root)
        self.app_root.setGraphicsEffect(fx)
        fx.setOpacity(0.0)
        anim = QPropertyAnimation(fx, b"opacity", self)
        anim.setDuration(220 if self.animation_intensity == "medium" else 320)
        anim.setStartValue(0.0)
        anim.setEndValue(1.0)
        anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        anim.finished.connect(lambda: self.app_root.setGraphicsEffect(None))
        anim.start()
        self._animations.append(anim)

    def _shake_widget(self, widget: QWidget) -> None:
        if self.animation_intensity == "off":
            return
        start = widget.pos()
        amp = 8 if self.animation_intensity == "high" else 4
        seq = QSequentialAnimationGroup(self)
        for dx in [amp, -amp, amp // 2, -(amp // 2), 0]:
            a = QPropertyAnimation(widget, b"pos", self)
            a.setDuration(42)
            a.setStartValue(widget.pos())
            a.setEndValue(start + QPoint(dx, 0))
            a.setEasingCurve(QEasingCurve.Type.OutQuad)
            seq.addAnimation(a)
        seq.start()
        self._animations.append(seq)

    def _animate_mascot_frame(self) -> None:
        self._mascot_frame = (self._mascot_frame + 1) % 200
        self.mascot_canvas.set_state(self.controller.last_status, self._mascot_frame)

    def _schedule_insights_refresh(self) -> None:
        if self._insights_loading:
            return
        now_epoch = time.time()
        if now_epoch < self._next_insights_refresh_epoch:
            return
        self._insights_loading = True

        def worker() -> None:
            snap = {}
            try:
                snap = self._insights.fetch_snapshot()
            except Exception as exc:
                snap = {
                    "adsense_status": f"error: {exc}",
                    "analytics_status": f"error: {exc}",
                    "search_console_status": f"error: {exc}",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            with self._insights_lock:
                self._insights_snapshot = snap
                interval_min = max(3, int(getattr(self.controller.settings.integrations, "refresh_minutes", 15) or 15))
                self._next_insights_refresh_epoch = time.time() + (interval_min * 60)
                self._insights_loading = False

        threading.Thread(target=worker, daemon=True).start()

    def _change_theme_mode(self, mode: str) -> None:
        self.theme_manager.set_mode(mode)
        self.controller.theme_mode = self.theme_manager.mode
        self.top_nav.set_theme_mode(self.theme_manager.mode)
        self._build_mascot_assets()
        self._bg_cache_key = None
        self._force_heavy_refresh = True
        self._next_errors_panel_refresh_epoch = 0.0
        self._next_usage_panel_refresh_epoch = 0.0
        self._next_schedule_panel_refresh_epoch = 0.0
        self._append_console_line(f"[테마] 변경: {self.theme_manager.mode}")

    def _on_primary_action(self) -> None:
        if self._primary_action_mode == "pause":
            self.pause()
        elif self._primary_action_mode == "resume":
            self.resume()
        else:
            self.run_now()

    def run_now(self) -> None:
        self.controller.force_run = True
        self._append_console_line("[동작] 즉시 실행 요청")

    def pause(self) -> None:
        self.controller.stop()
        self.controller.last_status = "Paused"
        self._append_console_line("[동작] 에이전트 일시정지")

    def resume(self) -> None:
        self.controller.start()
        self.controller.last_status = "Running"
        self._append_console_line("[동작] 에이전트 재개")

    def _toggle_dry_run(self) -> None:
        raw = self.settings_context.settings_path
        try:
            import yaml

            txt = raw.read_text(encoding="utf-8")
            data = dict(yaml.safe_load(txt) or {})
            budget = dict(data.setdefault("budget", {}))
            now_flag = bool(budget.get("dry_run", False))
            budget["dry_run"] = not now_flag
            data["budget"] = budget
            
            raw.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
            self.controller.reload()
            
            status_text = "실제 포스팅 모드" if not budget["dry_run"] else "연습 모드 (Dry-run)"
            self._append_console_line(f"[설정] 모드 변경: {status_text}")
        except Exception as exc:
            QMessageBox.warning(self, "설정 저장 실패", str(exc))

    def _preview_html(self) -> None:
        try:
            snap = self.controller.workflow.get_resume_snapshot(force_refresh=True)
            if not bool(snap.get("exists", False)):
                QMessageBox.information(self, "HTML 미리보기", "중단 문서가 없어 미리볼 HTML이 없습니다.")
                return
            stage = str(snap.get("stage", ""))
            title = str(snap.get("title", ""))
            QMessageBox.information(self, "HTML 미리보기", f"중단 문서\n단계: {stage}\n제목: {title}")
        except Exception as exc:
            QMessageBox.warning(self, "HTML 미리보기", f"미리보기 실패: {exc}")

    def open_help(self) -> None:
        readme = self.root / "README.md"
        if readme.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(readme)))
        else:
            webbrowser.open("https://github.com/")

    def open_settings(self) -> None:
        dlg = SettingsDialog(context=self.settings_context, on_saved=self.on_settings_saved, required_only=False)
        dlg.exec()

    def on_settings_saved(self) -> None:
        self._append_console_line("[설정] 적용 중...")
        self.hero_task.setText("설정 적용 중...")

        def worker() -> None:
            error = ""
            try:
                self.controller.reload()
            except Exception as exc:
                error = str(exc)
            QTimer.singleShot(0, lambda e=error: self._finish_settings_reload(e))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_settings_reload(self, error: str = "") -> None:
        if error:
            QMessageBox.warning(self, "설정 적용 실패", error)
            self._append_console_line(f"[오류] 설정 적용 실패: {error}")
            return
        self._insights = GrowthInsights(
            credentials_path=self.root / self.controller.settings.blogger.credentials_path,
            settings=self.controller.settings.integrations,
        )
        self._next_insights_refresh_epoch = 0.0
        self._next_errors_panel_refresh_epoch = 0.0
        self._next_usage_panel_refresh_epoch = 0.0
        self._next_schedule_panel_refresh_epoch = 0.0
        self._last_errors_signature = ""
        self._last_schedule_signature = ""
        self._force_heavy_refresh = True
        self._sync_animation_intensity()
        self._build_mascot_assets()
        self._append_console_line("[설정] 설정 파일을 다시 불러왔습니다.")

    def open_logs_dialog(self) -> None:
        if self.logs_dialog is None:
            self.logs_dialog = LogsDialog(self)
        self.logs_dialog.set_entries(self._console_entries)
        self.logs_dialog.show()
        self.logs_dialog.raise_()
        self.logs_dialog.activateWindow()

    def open_usage_dashboard(self) -> None:
        try:
            webbrowser.open(self.gemini_usage_dashboard)
        except Exception:
            pass

    def export_blog_pdf(self) -> None:
        out, _ = QFileDialog.getSaveFileName(
            self,
            "블로그 PDF 저장",
            str((Path(os.getenv("USERPROFILE", str(Path.home()))) / "Desktop" / "RezeroAgent_BlogExport.pdf")),
            "PDF Files (*.pdf)",
        )
        if not out:
            return
        out_path = Path(out)
        if out_path.suffix.lower() != ".pdf":
            out_path = out_path.with_suffix(".pdf")
        try:
            publisher = self.controller.workflow.publisher
            posts = publisher.fetch_posts_for_export(statuses=["live", "scheduled"], limit=30, include_bodies=True)
            if not posts:
                QMessageBox.information(self, "PDF 추출", "추출할 글이 없습니다.")
                return
            saved = export_blog_posts_pdf(
                posts=posts,
                output_path=out_path,
                blog_id=self.controller.settings.blogger.blog_id,
                source_label="Blogger Live API",
            )
            QMessageBox.information(self, "PDF 추출", f"완료: {saved}")
            self._append_console_line(f"[완료] 블로그 PDF 추출: {saved}")
        except Exception as exc:
            QMessageBox.warning(self, "PDF 추출", str(exc))
            self._append_console_line(f"[오류] 블로그 PDF 추출 실패: {exc}")

    def _clear_layout(self, layout: QVBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            child_layout = item.layout()
            if widget is not None:
                widget.deleteLater()
            if child_layout is not None:
                self._clear_layout(child_layout)  # type: ignore[arg-type]

    def closeEvent(self, event) -> None:
        self.controller.stop()
        super().closeEvent(event)
