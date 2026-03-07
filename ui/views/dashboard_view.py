import os
import sys
import webbrowser
from datetime import datetime

from PySide6.QtCore import QEasingCurve, QPropertyAnimation, Qt, QTimer
from PySide6.QtGui import QClipboard
from PySide6.QtWidgets import (
    QApplication,
    QBoxLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from ui.widgets.glass_card import GlassCard
from ui.widgets.log_panel import LogPanel


class SectionCard(GlassCard):
    def __init__(self, title: str, copy: str = "", parent=None) -> None:
        super().__init__(parent=parent)
        self.setObjectName("SectionCard")
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 15, 16, 15)
        root.setSpacing(12)

        header = QVBoxLayout()
        header.setSpacing(4)
        self.title_label = QLabel(title)
        self.title_label.setObjectName("PanelTitle")
        self.copy_label = QLabel(copy)
        self.copy_label.setObjectName("PanelBody")
        self.copy_label.setWordWrap(True)
        header.addWidget(self.title_label)
        if copy:
            header.addWidget(self.copy_label)
        root.addLayout(header)

        self.body = QVBoxLayout()
        self.body.setSpacing(12)
        root.addLayout(self.body, 1)


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
        self._last_log_msg = ""
        self._timeline_keys = ["collect", "draft", "qa", "visual", "schedule", "publish"]
        self._wrap_refresh_pending = False

        self.theme_manager.apply()

        self.setWindowTitle("RezeroAgent - 운영 대시보드")
        self.resize(1360, 760)
        self.setMinimumSize(1220, 720)
        self._layout_mode = ""

        self._build_ui()

        self._progress_anim = QPropertyAnimation(self.progress_bar, b"value", self)
        self._progress_anim.setDuration(420)
        self._progress_anim.setEasingCurve(QEasingCurve.Type.OutCubic)

        self.ui_timer = QTimer(self)
        self.ui_timer.timeout.connect(self._sync_ui_state)
        self.ui_timer.start(1000)
        self._sync_ui_state()

    def _build_ui(self):
        shell = QWidget()
        shell.setObjectName("RootShell")
        self.shell = shell
        self.setCentralWidget(shell)

        outer = QVBoxLayout(shell)
        outer.setContentsMargins(14, 12, 14, 12)
        outer.setSpacing(10)
        self.outer_content = outer

        outer.addWidget(self._build_top_nav(), 0)

        self.command_pane = self._build_command_rail()
        self.focus_pane = self._build_focus_column()
        self.command_pane.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.focus_pane.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        self.support_pane = QWidget()
        self.support_pane.setObjectName("DashboardPane")
        self.support_pane.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        support_root = QVBoxLayout(self.support_pane)
        support_root.setContentsMargins(0, 0, 0, 0)
        support_root.setSpacing(10)
        support_root.addWidget(self.command_pane, 1)

        self.board = QWidget()
        self.board.setObjectName("DashboardBoard")
        self.board.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.board_layout = QGridLayout(self.board)
        self.board_layout.setContentsMargins(0, 0, 0, 0)
        self.board_layout.setHorizontalSpacing(10)
        self.board_layout.setVerticalSpacing(10)
        outer.addWidget(self.board, 1)
        self._apply_responsive_layout(force=True)

    def _build_top_nav(self) -> QFrame:
        bar = QFrame()
        bar.setObjectName("TopNav")
        bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        bar.setMinimumHeight(72)
        bar.setMaximumHeight(94)
        self.top_nav = bar
        self.top_nav_layout = QBoxLayout(QBoxLayout.Direction.LeftToRight, bar)
        self.top_nav_layout.setContentsMargins(0, 0, 0, 0)
        self.top_nav_layout.setSpacing(12)

        brand = QVBoxLayout()
        brand.setSpacing(0)
        eyebrow = QLabel("REZEROAGENT")
        eyebrow.setObjectName("HeroEyebrow")
        self.top_brand_title = QLabel("발행 관제실")
        self.top_brand_title.setObjectName("TopBrandTitle")
        self.top_brand_title.setWordWrap(True)
        self.top_brand_title.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.top_brand_copy = QLabel("실행·이미지·예약 상태를 요약합니다.")
        self.top_brand_copy.setObjectName("TopBrandCopy")
        self.top_brand_copy.setWordWrap(True)
        self.top_brand_copy.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        brand.addWidget(eyebrow)
        brand.addWidget(self.top_brand_title)
        brand.addWidget(self.top_brand_copy)

        self.header_chip_grid = QGridLayout()
        self.header_chip_grid.setHorizontalSpacing(8)
        self.header_chip_grid.setVerticalSpacing(8)
        self.engine_chip = self._make_chip("엔진 대기", "neutral")
        self.local_chip = self._make_chip("이미지 미확인", "neutral")
        self.queue_chip = self._make_chip("예약 0건", "neutral")
        self.header_chips = [
            self.engine_chip,
            self.local_chip,
            self.queue_chip,
        ]

        left = QVBoxLayout()
        left.setSpacing(6)
        left.addLayout(brand)
        left.addLayout(self.header_chip_grid)
        self.top_nav_layout.addLayout(left, 1)

        meta = QVBoxLayout()
        meta.setSpacing(2)
        build_value = getattr(self.controller, "running_version", "unknown")
        build_short = build_value[:18] + "..." if len(build_value) > 18 else build_value
        self.version_chip = QLabel(f"빌드 {build_short}")
        self.version_chip.setObjectName("HeaderMeta")
        meta.addStretch(1)
        meta.addWidget(self.version_chip, 0, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignBottom)
        self.top_nav_layout.addLayout(meta)
        return bar

    def _build_command_rail(self) -> QWidget:
        pane = QWidget()
        pane.setObjectName("DashboardPane")
        root = QVBoxLayout(pane)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)
        root.addWidget(self._build_mission_card(), 1)
        return pane

    def _build_focus_column(self) -> QWidget:
        pane = QWidget()
        pane.setObjectName("DashboardPane")
        root = QVBoxLayout(pane)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)
        root.addWidget(self._build_hero_card(), 5)
        root.addWidget(self._build_log_section(), 4)
        return pane

    def _build_insight_column(self) -> QWidget:
        pane = QWidget()
        pane.setObjectName("DashboardPane")
        root = QVBoxLayout(pane)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)
        root.addWidget(self._build_metrics_card(), 4)
        root.addWidget(self._build_watch_card(), 3)
        return pane

    def _build_mission_card(self) -> GlassCard:
        card = GlassCard()
        card.setObjectName("SideCard")
        card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        root = QVBoxLayout(card)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        eyebrow = QLabel("CONTROL")
        eyebrow.setObjectName("HeroEyebrow")
        title = QLabel("바로 실행")
        title.setObjectName("PanelTitle")
        root.addWidget(eyebrow)
        root.addWidget(title)

        action_grid = QGridLayout()
        action_grid.setHorizontalSpacing(8)
        action_grid.setVerticalSpacing(8)
        self.action_grid = action_grid

        self.btn_run = QPushButton("엔진 정지")
        self.btn_run.setObjectName("PrimaryBtn")
        self.btn_run.clicked.connect(self._toggle_run)
        self.btn_force = QPushButton("즉시 발행")
        self.btn_force.setObjectName("AccentBtn")
        self.btn_force.clicked.connect(self._force_run)
        self.btn_settings = QPushButton("설정")
        self.btn_settings.setObjectName("GhostBtn")
        self.btn_settings.clicked.connect(self._open_settings)

        self.action_buttons = [self.btn_run, self.btn_force, self.btn_settings]
        root.addLayout(action_grid)

        self.command_summary = QLabel("소스, 로컬, 예약 상태를 한 줄로 확인합니다.")
        self.command_summary.setObjectName("PanelBody")
        self.command_summary.setWordWrap(True)
        self.command_summary.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.command_summary.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        root.addWidget(self.command_summary)

        self.mission_chip_grid = QGridLayout()
        self.mission_chip_grid.setHorizontalSpacing(8)
        self.mission_chip_grid.setVerticalSpacing(8)
        self.phase_badge = self._make_chip("단계 대기", "neutral")
        self.next_run_badge = self._make_chip("다음 실행 -", "neutral")
        self.resume_badge = self._make_chip("재개 없음", "good")
        root.addWidget(self._make_divider())
        root.addLayout(self.mission_chip_grid)

        self.metric_grid = QGridLayout()
        self.metric_grid.setHorizontalSpacing(10)
        self.metric_grid.setVerticalSpacing(10)
        self.metric_rows = {
            "api": self._make_metric_pair("API 사용"),
            "output": self._make_metric_pair("오늘 발행"),
            "queue": self._make_metric_pair("예약 큐"),
            "image": self._make_metric_pair("이미지"),
        }
        metric_items = [
            self.metric_rows["api"],
            self.metric_rows["output"],
            self.metric_rows["queue"],
            self.metric_rows["image"],
        ]
        for idx, pair in enumerate(metric_items):
            self.metric_grid.addWidget(pair[0], idx, 0)
            self.metric_grid.addWidget(pair[1], idx, 1)
        root.addWidget(self._make_divider())
        root.addLayout(self.metric_grid)

        self.schedule_title = QLabel("다음 예약")
        self.schedule_title.setObjectName("DataLabel")
        root.addWidget(self._make_divider())
        root.addWidget(self.schedule_title)

        self.schedule_lines = []
        for _ in range(2):
            line = QLabel("예약된 글이 없습니다.")
            line.setObjectName("ScheduleLine")
            line.setWordWrap(False)
            line.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
            line.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
            self.schedule_lines.append(line)
            root.addWidget(line)

        self.alert_title = QLabel("최근 경고")
        self.alert_title.setObjectName("DataLabel")
        root.addWidget(self._make_divider())
        root.addWidget(self.alert_title)

        self.alert_lines = []
        for _ in range(2):
            line = QLabel("경고 없음")
            line.setObjectName("AlertLine")
            line.setWordWrap(False)
            line.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
            line.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
            self.alert_lines.append(line)
            root.addWidget(line)
        return card

    def _build_control_card(self) -> SectionCard:
        card = SectionCard("Actions", "Manual controls stay grouped here so they do not crowd the main run narrative.")
        card.setObjectName("ControlCard")

        action_grid = QGridLayout()
        action_grid.setHorizontalSpacing(10)
        action_grid.setVerticalSpacing(10)
        self.action_grid = action_grid

        self.btn_run = QPushButton("Pause engine")
        self.btn_run.setObjectName("PrimaryBtn")
        self.btn_run.clicked.connect(self._toggle_run)
        self.btn_force = QPushButton("Force publish")
        self.btn_force.setObjectName("AccentBtn")
        self.btn_force.clicked.connect(self._force_run)
        self.btn_settings = QPushButton("Settings")
        self.btn_settings.setObjectName("GhostBtn")
        self.btn_settings.clicked.connect(self._open_settings)
        self.btn_dashboard = QPushButton("Usage dashboard")
        self.btn_dashboard.setObjectName("GhostBtn")
        self.btn_dashboard.clicked.connect(self._open_usage_dashboard)
        self.btn_theme = QPushButton("Cycle theme")
        self.btn_theme.setObjectName("GhostBtn")
        self.btn_theme.clicked.connect(self._cycle_theme)
        self.btn_restart = QPushButton("Restart app")
        self.btn_restart.setObjectName("GhostBtn")
        self.btn_restart.clicked.connect(self._restart_app)

        buttons = [
            self.btn_run,
            self.btn_force,
            self.btn_settings,
            self.btn_dashboard,
            self.btn_theme,
            self.btn_restart,
        ]
        self.action_buttons = list(buttons)
        for idx, btn in enumerate(self.action_buttons):
            action_grid.addWidget(btn, idx // 2, idx % 2)
        card.body.addLayout(action_grid)

        self.action_hint = QLabel("Use force publish only when the queue or schedule really needs an interrupt.")
        self.action_hint.setObjectName("PanelBody")
        self.action_hint.setWordWrap(True)
        self.action_hint.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        card.body.addWidget(self.action_hint)
        return card

    def _build_hero_card(self) -> GlassCard:
        card = GlassCard()
        card.setObjectName("HeroCard")
        card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        root = QVBoxLayout(card)
        root.setContentsMargins(14, 12, 14, 12)
        root.setSpacing(8)

        self.hero_header_container = QWidget()
        self.hero_header = QBoxLayout(QBoxLayout.Direction.LeftToRight, self.hero_header_container)
        self.hero_header.setContentsMargins(0, 0, 0, 0)
        self.hero_header.setSpacing(8)
        title_box = QVBoxLayout()
        title_box.setSpacing(4)
        eyebrow = QLabel("현재 실행")
        eyebrow.setObjectName("HeroEyebrow")
        self.hero_title = QLabel("발행 엔진 대기 중")
        self.hero_title.setObjectName("HeroTitle")
        self.hero_title.setWordWrap(True)
        self.hero_title.setMinimumHeight(0)
        self.hero_title.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        self.hero_title.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.hero_body = QLabel("실행과 발행 타이밍 요약")
        self.hero_body.setObjectName("HeroBody")
        self.hero_body.setWordWrap(True)
        self.hero_body.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        self.hero_body.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        title_box.addWidget(eyebrow)
        title_box.addWidget(self.hero_title)
        self.focus_note = QLabel("현재 단계 메모")
        self.focus_note.setObjectName("PanelBody")
        self.focus_note.setWordWrap(True)
        self.focus_note.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.focus_note.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        self.hero_header.addLayout(title_box, 4)
        root.addWidget(self.hero_header_container)

        progress_wrap = QVBoxLayout()
        progress_wrap.setSpacing(6)
        meter_row = QHBoxLayout()
        self.phase_value = QLabel("대기")
        self.phase_value.setObjectName("PanelValue")
        self.phase_value.setWordWrap(True)
        self.phase_value.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.phase_value.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        self.phase_detail = QLabel("다음 예약 실행을 기다리는 중입니다.")
        self.phase_detail.setObjectName("PanelBody")
        self.phase_detail.setWordWrap(True)
        self.phase_detail.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.phase_detail.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        self.progress_percent = QLabel("0%")
        self.progress_percent.setObjectName("DataLabel")
        meter_row.addWidget(self.phase_value, 1)
        meter_row.addWidget(self.progress_percent, 0)
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(12)
        progress_wrap.addLayout(meter_row)
        progress_wrap.addWidget(self.progress_bar)
        progress_wrap.addWidget(self.phase_detail)
        root.addLayout(progress_wrap)
        root.addWidget(self.hero_body)

        footer = QGridLayout()
        footer.setHorizontalSpacing(10)
        footer.setVerticalSpacing(6)
        self.hero_footer_grid = footer
        self.hero_stat_source = self._make_stat_block("소스")
        self.hero_stat_output = self._make_stat_block("오늘 발행")
        self.hero_stat_visual = self._make_stat_block("이미지")
        self.hero_stat_llm = self._make_stat_block("로컬 모델")
        footer.addLayout(self.hero_stat_source[0], 0, 0)
        footer.addLayout(self.hero_stat_output[0], 0, 1)
        footer.addLayout(self.hero_stat_visual[0], 0, 2)
        footer.addLayout(self.hero_stat_llm[0], 0, 3)
        root.addLayout(footer)
        return card

    def _build_kpi_grid(self) -> QGridLayout:
        grid = QGridLayout()
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(10)

        self.kpi_api = KpiCard("API 사용", "01")
        self.kpi_output = KpiCard("오늘 발행", "02")
        self.kpi_queue = KpiCard("예약 큐", "03")
        self.kpi_image = KpiCard("이미지", "04")
        self.kpi_cards = [
            self.kpi_api,
            self.kpi_output,
            self.kpi_queue,
            self.kpi_image,
        ]
        for idx, card in enumerate(self.kpi_cards):
            grid.addWidget(card, idx // 2, idx % 2)
        return grid

    def _build_log_section(self) -> LogPanel:
        self.log_panel = LogPanel()
        self.log_panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Ignored)
        return self.log_panel

    def _build_schedule_card(self) -> SectionCard:
        card = SectionCard("예약 현황", "")
        card.setObjectName("ScheduleCard")

        self.schedule_title = QLabel("Next scheduled window")
        self.schedule_title.setObjectName("DataLabel")
        card.body.addWidget(self.schedule_title)

        self.schedule_lines = []
        for _ in range(4):
            line = QLabel("No scheduled posts loaded.")
            line.setObjectName("ScheduleLine")
            line.setWordWrap(True)
            self.schedule_lines.append(line)
            card.body.addWidget(line)

        self.schedule_utility = QBoxLayout(QBoxLayout.Direction.LeftToRight)
        self.schedule_utility.setSpacing(10)
        self.btn_clear_drafts = QPushButton("Clear WIP")
        self.btn_clear_drafts.setObjectName("DangerBtn")
        self.btn_clear_drafts.clicked.connect(self._discard_all_drafts)
        self.btn_copy_logs = QPushButton("Copy logs")
        self.btn_copy_logs.setObjectName("GhostBtn")
        self.btn_copy_logs.clicked.connect(self._copy_logs)
        self.schedule_utility.addWidget(self.btn_clear_drafts)
        self.schedule_utility.addWidget(self.btn_copy_logs)
        card.body.addLayout(self.schedule_utility)
        return card

    def _build_timeline_card(self) -> SectionCard:
        card = SectionCard("파이프라인", "")
        card.setObjectName("TimelineCard")
        self.timeline_steps = {
            "collect": TimelineStep("수집", "A1"),
            "draft": TimelineStep("초안", "A2"),
            "qa": TimelineStep("QA", "A3"),
            "visual": TimelineStep("이미지", "A4"),
            "schedule": TimelineStep("예약", "A5"),
            "publish": TimelineStep("발행", "A6"),
        }
        self.timeline_grid = QGridLayout()
        self.timeline_grid.setHorizontalSpacing(12)
        self.timeline_grid.setVerticalSpacing(12)
        for key in self._timeline_keys:
            self.timeline_grid.addWidget(self.timeline_steps[key], 0, 0)
        card.body.addLayout(self.timeline_grid)
        return card

    def _build_metrics_card(self) -> SectionCard:
        card = SectionCard("핵심 지표", "")
        card.setObjectName("MetricsCard")
        card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.metrics_grid = self._build_kpi_grid()
        card.body.addLayout(self.metrics_grid)
        return card

    def _build_watch_card(self) -> SectionCard:
        card = SectionCard("주의 항목", "")
        card.setObjectName("WatchCard")
        card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.orbit_detail = QLabel("이미지 실패나 재개 이슈가 있으면 이 카드에서 먼저 보여줍니다.")
        self.orbit_detail.setObjectName("PanelBody")
        self.orbit_detail.setWordWrap(True)
        self.orbit_detail.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.orbit_detail.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.alert_title = QLabel("최근 경고")
        self.alert_title.setObjectName("DataLabel")
        card.body.addWidget(self.alert_title)
        self.alert_lines = []
        for _ in range(2):
            line = QLabel("경고 없음")
            line.setObjectName("AlertLine")
            line.setWordWrap(False)
            line.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
            line.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
            self.alert_lines.append(line)
            card.body.addWidget(line)
        return card

    def _make_chip(self, text: str, tone: str = "neutral") -> QLabel:
        chip = QLabel(text)
        chip.setObjectName("StatusChip")
        chip.setProperty("tone", tone)
        chip.setWordWrap(True)
        chip.setAlignment(Qt.AlignmentFlag.AlignCenter)
        chip.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        return chip

    def _make_metric_pair(self, label_text: str):
        label = QLabel(label_text)
        label.setObjectName("SummaryLabel")
        value = QLabel("-")
        value.setObjectName("SummaryValue")
        value.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return label, value

    def _make_divider(self) -> QFrame:
        line = QFrame()
        line.setObjectName("SectionDivider")
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFixedHeight(1)
        return line

    def _make_stat_block(self, label_text: str):
        wrapper = QVBoxLayout()
        wrapper.setSpacing(4)
        label = QLabel(label_text)
        label.setObjectName("MetaLabel")
        value = QLabel("-")
        value.setObjectName("MetaValue")
        value.setWordWrap(True)
        value.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        wrapper.addWidget(label)
        wrapper.addWidget(value)
        return wrapper, value

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._apply_responsive_layout()

    def _apply_responsive_layout(self, force: bool = False) -> None:
        width = max(1, self.width())
        if width >= 1450:
            mode = "wide"
        elif width >= 1260:
            mode = "balanced"
        else:
            mode = "compact"
        if (not force) and mode == self._layout_mode:
            self._refresh_dynamic_heights()
            self._schedule_dynamic_height_refresh()
            return
        self._layout_mode = mode

        self._clear_layout(self.board_layout)
        self.board_layout.addWidget(self.focus_pane, 0, 0)
        self.board_layout.addWidget(self.support_pane, 0, 1)
        self.board_layout.setHorizontalSpacing(12 if mode == "wide" else 10)
        if mode == "wide":
            self.board_layout.setColumnStretch(0, 8)
            self.board_layout.setColumnStretch(1, 4)
        elif mode == "balanced":
            self.board_layout.setColumnStretch(0, 7)
            self.board_layout.setColumnStretch(1, 4)
        else:
            self.board_layout.setColumnStretch(0, 7)
            self.board_layout.setColumnStretch(1, 4)
        self.board_layout.setColumnStretch(2, 0)

        self.top_nav_layout.setDirection(QBoxLayout.Direction.LeftToRight)
        self.top_nav_layout.setSpacing(12 if mode == "wide" else 10)
        self.top_brand_copy.setVisible(mode != "compact")
        self.top_nav.setMaximumHeight(94 if mode == "wide" else 90 if mode == "balanced" else 86)
        self.hero_header.setDirection(
            QBoxLayout.Direction.LeftToRight if mode == "wide" else QBoxLayout.Direction.TopToBottom
        )

        self._reflow_header_chips(mode)
        self._reflow_mission_chips(mode)
        self._reflow_action_grid(mode)
        self._reflow_hero_stats(mode)
        self._apply_type_scale(mode)
        self._refresh_dynamic_heights()
        self._schedule_dynamic_height_refresh()

    def _clear_layout(self, layout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            if item.widget() is not None:
                item.widget().setParent(None)

    def _reflow_header_chips(self, mode: str) -> None:
        self._clear_layout(self.header_chip_grid)
        positions = [(0, 0), (0, 1), (0, 2)] if mode == "wide" else [(0, 0), (0, 1), (1, 0)]
        for chip, (row, col) in zip(self.header_chips, positions):
            self.header_chip_grid.addWidget(chip, row, col)
        for col in range(3):
            self.header_chip_grid.setColumnStretch(col, 0)

    def _reflow_mission_chips(self, mode: str) -> None:
        self._clear_layout(self.mission_chip_grid)
        if mode == "wide":
            self.mission_chip_grid.addWidget(self.phase_badge, 0, 0)
            self.mission_chip_grid.addWidget(self.next_run_badge, 0, 1)
            self.mission_chip_grid.addWidget(self.resume_badge, 1, 0, 1, 2)
            return
        self.mission_chip_grid.addWidget(self.phase_badge, 0, 0)
        self.mission_chip_grid.addWidget(self.next_run_badge, 1, 0)
        self.mission_chip_grid.addWidget(self.resume_badge, 2, 0)

    def _reflow_action_grid(self, mode: str) -> None:
        self._clear_layout(self.action_grid)
        columns = 2 if mode == "compact" else 3
        for idx, btn in enumerate(self.action_buttons):
            self.action_grid.addWidget(btn, idx // columns, idx % columns)

    def _reflow_hero_stats(self, mode: str) -> None:
        self._clear_layout(self.hero_footer_grid)
        blocks = [
            self.hero_stat_source[0],
            self.hero_stat_output[0],
            self.hero_stat_visual[0],
            self.hero_stat_llm[0],
        ]
        for idx, block in enumerate(blocks):
            self.hero_footer_grid.addLayout(block, 0, idx)

    def _reflow_timeline_grid(self, mode: str) -> None:
        if not hasattr(self, "timeline_grid"):
            return
        self._clear_layout(self.timeline_grid)
        placements = {
            "collect": (0, 0),
            "draft": (0, 1),
            "qa": (0, 2),
            "visual": (1, 0),
            "schedule": (1, 1),
            "publish": (1, 2),
        }
        for key in self._timeline_keys:
            row, col = placements[key]
            self.timeline_grid.addWidget(self.timeline_steps[key], row, col)

    def _reflow_metrics_grid(self, mode: str) -> None:
        if not hasattr(self, "metrics_grid"):
            return
        self._clear_layout(self.metrics_grid)
        columns = 2
        for idx, card in enumerate(self.kpi_cards):
            self.metrics_grid.addWidget(card, idx // columns, idx % columns)

    def _apply_type_scale(self, mode: str) -> None:
        scale_map = {
            "wide": {"brand": 22, "hero": 22, "phase": 19, "meta": 11, "body": 11, "chip": 10, "button": 10, "summary": 14, "stat": 12},
            "balanced": {"brand": 20, "hero": 20, "phase": 18, "meta": 10, "body": 10, "chip": 9, "button": 10, "summary": 13, "stat": 11},
            "compact": {"brand": 18, "hero": 18, "phase": 16, "meta": 9, "body": 9, "chip": 9, "button": 9, "summary": 12, "stat": 10},
        }
        scale = scale_map.get(mode, scale_map["wide"])
        self._set_point_size(self.top_brand_title, scale["brand"])
        self._set_point_size(self.hero_title, scale["hero"])
        self._set_point_size(self.phase_value, scale["phase"])
        self._set_point_size(self.top_brand_copy, scale["body"])
        self._set_point_size(self.hero_body, scale["body"])
        self._set_point_size(self.command_summary, scale["body"])
        self._set_point_size(self.phase_detail, scale["body"])
        self._set_point_size(self.schedule_title, scale["meta"])
        self._set_point_size(self.alert_title, scale["meta"])
        self._set_point_size(self.progress_percent, scale["meta"])
        for chip in getattr(self, "header_chips", []):
            self._set_point_size(chip, scale["chip"])
        for chip in [self.version_chip, self.phase_badge, self.next_run_badge, self.resume_badge]:
            self._set_point_size(chip, scale["chip"] if isinstance(chip, QLabel) and chip.objectName() == "StatusChip" else scale["meta"])
        for value in [
            self.hero_stat_source[1],
            self.hero_stat_output[1],
            self.hero_stat_visual[1],
            self.hero_stat_llm[1],
        ]:
            self._set_point_size(value, scale["stat"])
        for label in self.schedule_lines + self.alert_lines:
            self._set_point_size(label, scale["body"])
        for btn in self.action_buttons:
            font = btn.font()
            font.setPointSize(int(scale["button"]))
            btn.setFont(font)
            btn.updateGeometry()
        for pair in getattr(self, "metric_rows", {}).values():
            self._set_point_size(pair[0], scale["meta"])
            self._set_point_size(pair[1], scale["summary"])

    def _refresh_dynamic_heights(self) -> None:
        changed = False
        for label in self.findChildren(QLabel):
            if not label.wordWrap():
                continue
            if self._fit_wrapped_label(label):
                changed = True
        if changed:
            self.board_layout.invalidate()
            self.board_layout.activate()
            self.outer_content.invalidate()
            self.outer_content.activate()
            self.board.adjustSize()
            self.shell.updateGeometry()

    def _fit_wrapped_label(self, label: QLabel, padding: int = 8) -> bool:
        if not label.isVisible():
            return False
        margins = label.contentsMargins()
        width = max(0, label.width() - margins.left() - margins.right())
        if width <= 1:
            width = max(0, label.contentsRect().width())
        if width <= 1:
            return False
        base_min = int(label.property("baseMinHeight") or 0)
        if not base_min:
            base_min = max(0, label.minimumHeight())
            label.setProperty("baseMinHeight", base_min)
        rect = label.fontMetrics().boundingRect(
            0,
            0,
            width,
            10000,
            int(Qt.TextFlag.TextWordWrap),
            label.text(),
        )
        needed = max(base_min, rect.height() + margins.top() + margins.bottom() + padding)
        if label.minimumHeight() == needed:
            return False
        label.setMinimumHeight(needed)
        label.updateGeometry()
        return True

    def _schedule_dynamic_height_refresh(self) -> None:
        if self._wrap_refresh_pending:
            return
        self._wrap_refresh_pending = True
        QTimer.singleShot(0, self._flush_dynamic_height_refresh)

    def _flush_dynamic_height_refresh(self) -> None:
        self._wrap_refresh_pending = False
        self._refresh_dynamic_heights()

    def _set_point_size(self, widget: QLabel, points: int) -> None:
        font = widget.font()
        font.setPointSize(int(points))
        widget.setFont(font)
        widget.updateGeometry()

    def _sync_ui_state(self):
        self.theme_manager.check_auto_theme_update()
        self._apply_responsive_layout()
        ui = self.controller.get_ui_snapshot()
        usage = dict(ui.get("usage_snapshot", {}) or {})
        recent_errors = list(ui.get("recent_errors", []) or [])
        status_raw = str(ui.get("status", "Idle") or "Idle")
        phase = str(ui.get("phase_key", "idle") or "idle").lower()
        pct = int(ui.get("phase_percent", 0) or 0)
        msg = str(ui.get("phase_message", "") or "")

        self._set_chip(self.engine_chip, f"엔진 {self._engine_label(status_raw)}", self._engine_tone(status_raw))
        queue_72h = int(usage.get("scheduled_72h", 0) or 0)
        self._set_chip(self.queue_chip, f"예약 {queue_72h}건", "neutral" if queue_72h > 0 else "warning")

        local_llm_ready = bool(usage.get("local_llm_ready", False))
        local_llm_used = bool(usage.get("local_llm_used_last_run", False))
        local_tone = "good" if local_llm_used or local_llm_ready else "warning"
        local_text = "로컬 사용" if local_llm_used else ("로컬 준비" if local_llm_ready else "백업 모드")
        self._set_chip(self.local_chip, local_text, local_tone)
        build_value = getattr(self.controller, "running_version", "unknown")
        build_short = build_value[:18] + "..." if len(build_value) > 18 else build_value
        self.version_chip.setText(f"빌드 {build_short}")

        self.hero_title.setText(self._headline_for(status_raw, phase, usage))
        self.hero_title.updateGeometry()
        self.hero_body.setText(self._compact_text(self._summary_for(ui, usage), self._mode_limit(72, 62, 52)))
        self.phase_value.setText(self._phase_display(phase))
        self.phase_detail.setText(self._compact_text(msg or str(ui.get("last_message", "") or "단계 메모가 아직 없습니다."), self._mode_limit(68, 58, 48)))
        self.progress_percent.setText(f"{pct}%")
        self._fit_wrapped_label(self.hero_title, padding=10)
        self._animate_progress(pct)

        phase_tone = "good" if pct >= 100 and phase in {"done", "publish", "indexing"} else "neutral"
        self._set_chip(self.phase_badge, f"단계 {self._phase_display(phase)}", phase_tone)
        self._set_chip(self.next_run_badge, f"다음 {self._compact_text(str(ui.get('next_run_text', '-') or '-'), self._mode_limit(20, 18, 15))}", "neutral")
        resume_text = str(ui.get("resume_text", "") or "")
        resume_tone = "warning" if resume_text != "없음" and resume_text.lower() != "none" else "good"
        resume_clean = "없음" if resume_tone == "good" else self._compact_text(resume_text, self._mode_limit(16, 14, 12))
        self._set_chip(self.resume_badge, f"재개 {resume_clean}", resume_tone)

        source = str(usage.get("source", "local") or "local")
        today_posts = int(usage.get("today_posts", 0) or 0)
        today_runs = int(usage.get("today_runs", 0) or 0)
        today_scheduled = int(usage.get("today_scheduled", 0) or 0)
        scheduled_total = int(usage.get("blogger_scheduled_total", 0) or 0)
        image_target = int(usage.get("image_pipeline_target", 0) or 0)
        image_state = str(usage.get("image_pipeline_status", "unknown") or "unknown")
        timezone_name = str(usage.get("publish_timezone", "") or "")
        local_reason = str(usage.get("local_llm_reason", "") or "")

        api_calls = self._get_today_api_calls()
        call_cap = int(self.controller.settings.budget.daily_gemini_call_limit)
        self.metric_rows["api"][1].setText(f"{api_calls}/{call_cap}")
        self.metric_rows["output"][1].setText(f"발행 {today_posts}건")
        self.metric_rows["queue"][1].setText(f"72시간 {queue_72h}건")
        self.metric_rows["image"][1].setText(f"{self._image_state_label(image_state)} · {image_target}장")
        self.command_summary.setText(
            f"소스 {('Blogger' if source == 'blogger' else '로컬')} · 실행 {today_runs}회 · "
            f"표준시 {timezone_name or '-'} · 로컬 {self._compact_text(local_reason or local_text, self._mode_limit(28, 24, 18))}"
        )

        self.hero_stat_source[1].setText("Blogger 연동" if source == "blogger" else "로컬 추정")
        self.hero_stat_output[1].setText(f"발행 {today_posts}건 / 예약 {today_scheduled}건")
        self.hero_stat_visual[1].setText(f"{self._image_state_label(image_state)} · 목표 {image_target}")
        self.hero_stat_llm[1].setText(self._compact_text(local_reason or local_text, self._mode_limit(28, 24, 18)))

        self.schedule_title.setText(f"다음 예약 · {queue_72h}건")
        items = list(usage.get("today_schedule_items", []) or [])[:2]
        if not items:
            items = self._safe_schedule_fallback(limit=2)
        self._render_schedule(items)

        self.log_panel.set_phase(phase)
        if msg and msg != self._last_log_msg:
            timestamp = datetime.now().strftime("%H:%M:%S")
            line = f"[{timestamp}] [{phase.upper()}] {msg}"
            self.log_panel.append_line(line)
            self._last_log_msg = msg

        self._render_watchlist(recent_errors, usage)
        self._fit_wrapped_label(self.command_summary, padding=10)
        self._refresh_dynamic_heights()
        self._schedule_dynamic_height_refresh()

        if self.controller.running:
            self.btn_run.setText("엔진 정지")
        else:
            self.btn_run.setText("엔진 시작")

    def _render_schedule(self, items):
        title_limit = self._mode_limit(40, 34, 30)
        for idx, label in enumerate(self.schedule_lines):
            if idx < len(items):
                row = items[idx] or {}
                slot = self._format_publish_time(str(row.get("publish_at", "") or ""))
                title = self._compact_text(str(row.get("title", "") or "제목 없음"), title_limit)
                label.setText(f"{slot}  {title}")
            else:
                label.setText("예정된 예약이 없습니다.")

    def _render_watchlist(self, recent_errors, usage):
        lines = []
        alert_limit = self._mode_limit(42, 36, 32)
        for row in recent_errors[:2]:
            status = self._watch_status_label(str((row or {}).get("status", "hold") or "hold"))
            title = str((row or {}).get("title", "") or (row or {}).get("message", "") or (row or {}).get("error", "") or "문제 감지")
            lines.append(f"{status} · {self._compact_text(title, alert_limit)}")
        if not lines:
            image_message = str(usage.get("image_pipeline_message", "") or "")
            lines = [
                f"이미지 · {self._compact_text(image_message or '최근 이미지 경고 없음', alert_limit)}",
                f"재개 · {self._compact_text(str(usage.get('resume_title', '') or '중단된 초안 없음'), alert_limit)}",
            ]
        self.alert_title.setText(f"최근 경고 {len(lines)}건")
        for idx, label in enumerate(self.alert_lines):
            label.setText(lines[idx] if idx < len(lines) else "경고 없음")

    def _update_timeline(self, phase: str, status_raw: str, message: str):
        order = {
            "collect": 0,
            "select": 0,
            "preflight": 0,
            "draft": 1,
            "headline": 1,
            "qa": 2,
            "visual": 3,
            "schedule": 4,
            "publish": 5,
            "indexing": 5,
            "done": 5,
            "idle": -1,
            "error": -1,
        }
        current = order.get(phase, -1)
        failure = status_raw in {"Error", "QuotaWait", "Hold"} or phase == "error"
        for idx, key in enumerate(self._timeline_keys):
            if failure and idx == max(current, 0):
                status = "error"
            elif idx < current:
                status = "success"
            elif idx == current:
                status = "active"
            else:
                status = "pending"
            note = self._message_for_timeline(key, phase, message)
            self.timeline_steps[key].set_status(status, note)

    def _message_for_timeline(self, key: str, phase: str, message: str) -> str:
        if key == "collect":
            return "소스 수집과 주제 선별"
        if key == "draft":
            return message if phase in {"draft", "headline"} else "초안 작성과 앵글 정리"
        if key == "qa":
            return message if phase == "qa" else "분량, 톤, 구조 점검"
        if key == "visual":
            return message if phase == "visual" else "썸네일과 본문 이미지 준비"
        if key == "schedule":
            return message if phase == "schedule" else "예약 슬롯 배치"
        if key == "publish":
            return message if phase in {"publish", "indexing", "done"} else "Blogger 반영과 후속 확인"
        return "-"

    def _headline_for(self, status_raw: str, phase: str, usage: dict) -> str:
        if not self.controller.running:
            return "엔진 일시정지"
        if status_raw in {"Error", "QuotaWait", "Hold"}:
            return "파이프라인 점검 필요"
        if phase not in {"idle", ""}:
            return f"{self._phase_display(phase)} 진행 중"
        queued = int(usage.get("scheduled_72h", 0) or 0)
        return f"대기 중 · 예약 {queued}건"

    def _summary_for(self, ui: dict, usage: dict) -> str:
        source = "Blogger 연동" if str(usage.get("source", "local") or "local") == "blogger" else "로컬 추정"
        image_state = self._image_state_label(str(usage.get("image_pipeline_status", "unknown") or "unknown"))
        llm_reason = self._compact_text(str(usage.get("local_llm_reason", "") or "로컬 경로 메모 없음"), 42)
        next_run = str(ui.get("next_run_text", "-") or "-")
        return f"소스 {source} · 이미지 {image_state} · 로컬 {llm_reason} · 다음 {next_run}"

    def _set_chip(self, label: QLabel, text: str, tone: str):
        label.setText(text)
        label.setProperty("tone", tone)
        label.style().unpolish(label)
        label.style().polish(label)

    def _engine_label(self, status_raw: str) -> str:
        mapping = {
            "Idle": "대기",
            "Running": "실행중",
            "Paused": "정지",
            "Error": "오류",
            "QuotaWait": "쿼타대기",
            "Success": "정상",
            "Skipped": "건너뜀",
            "Hold": "보류",
        }
        return mapping.get(status_raw, str(status_raw or "대기"))

    def _engine_tone(self, status_raw: str) -> str:
        if status_raw in {"Error", "QuotaWait", "Hold"}:
            return "danger"
        if status_raw == "Success":
            return "good"
        if status_raw == "Paused":
            return "warning"
        return "neutral"

    def _theme_label(self) -> str:
        mapping = {
            "auto": "자동",
            "dark": "다크",
            "light": "라이트",
        }
        return mapping.get(str(self.theme_manager.mode or "auto").lower(), str(self.theme_manager.mode or "auto"))

    def _image_state_label(self, state: str) -> str:
        mapping = {
            "ready": "준비",
            "success": "정상",
            "passed": "정상",
            "repairing": "보정중",
            "idle": "대기",
            "unknown": "미확인",
            "failed": "실패",
            "error": "오류",
            "blocked": "차단",
        }
        return mapping.get(str(state or "unknown").strip().lower(), str(state or "미확인").upper())

    def _watch_status_label(self, state: str) -> str:
        mapping = {
            "hold": "보류",
            "warning": "주의",
            "watch": "주의",
            "error": "오류",
            "failed": "실패",
            "success": "정상",
        }
        return mapping.get(str(state or "hold").strip().lower(), str(state or "hold"))

    def _phase_display(self, phase: str) -> str:
        mapping = {
            "preflight": "사전점검",
            "collect": "수집",
            "select": "수집",
            "draft": "초안",
            "headline": "앵글",
            "qa": "QA",
            "visual": "이미지",
            "schedule": "예약",
            "publish": "발행",
            "indexing": "후속반영",
            "done": "완료",
            "idle": "대기",
            "error": "오류",
        }
        return mapping.get(str(phase or "idle").lower(), str(phase or "idle"))

    def _animate_progress(self, target: int) -> None:
        target = max(0, min(100, int(target)))
        if self.progress_bar.value() == target:
            return
        self._progress_anim.stop()
        self._progress_anim.setStartValue(self.progress_bar.value())
        self._progress_anim.setEndValue(target)
        self._progress_anim.start()

    def _get_today_api_calls(self) -> int:
        try:
            return int(self.controller.workflow.logs.get_today_gemini_count())
        except Exception:
            return 0

    def _safe_schedule_fallback(self, limit: int = 5) -> list[dict]:
        try:
            return list(self.controller.workflow.get_today_schedule_items(limit=limit, allow_remote=False) or [])
        except Exception:
            return []

    def _format_publish_time(self, iso_text: str) -> str:
        raw = str(iso_text or "").strip()
        if not raw:
            return "--:--"
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(self.controller.tz).strftime("%H:%M")
        except Exception:
            return raw[:16]

    def _compact_text(self, text: str, limit: int) -> str:
        clean = " ".join(str(text or "").split())
        if len(clean) <= limit:
            return clean
        return clean[: max(0, limit - 3)] + "..."

    def _mode_limit(self, wide: int, balanced: int, compact: int) -> int:
        mode = str(self._layout_mode or "wide")
        if mode == "compact":
            return compact
        if mode == "balanced":
            return balanced
        return wide

    def _copy_logs(self):
        clipboard: QClipboard = QApplication.clipboard()
        header = f"--- REZERO LOG SNAPSHOT {datetime.now().isoformat()} ---\n"
        clipboard.setText(header + self.log_panel.viewer.toPlainText())

    def _open_usage_dashboard(self):
        if self.gemini_usage_dashboard:
            webbrowser.open(self.gemini_usage_dashboard)

    def _restart_app(self):
        QApplication.quit()
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def _toggle_run(self):
        if self.controller.running:
            self.controller.stop()
        else:
            self.controller.start()
        self._sync_ui_state()

    def _force_run(self):
        self.controller.force_run = True
        self.controller.next_run_at = datetime.now(self.controller.tz)
        if not self.controller.running:
            self.controller.start()
        self.log_panel.append_line(f"[{datetime.now().strftime('%H:%M:%S')}] [시스템] 즉시 발행 요청")
        self._sync_ui_state()

    def _cycle_theme(self):
        modes = ["auto", "dark", "light"]
        current = str(self.theme_manager.mode or "auto").lower()
        try:
            next_mode = modes[(modes.index(current) + 1) % len(modes)]
        except ValueError:
            next_mode = "dark"
        self.theme_manager.set_mode(next_mode)
        self.controller.theme_mode = self.theme_manager.mode
        self._sync_ui_state()

    def _open_settings(self):
        from ui.dialogs.settings_dialog import SettingsDialog

        dlg = SettingsDialog(context=self.settings_context, on_saved=None, required_only=False)
        dlg.exec()
        self.controller.reload()
        self._sync_ui_state()

    def _discard_all_drafts(self):
        reply = QMessageBox.question(
            self,
            "Discard WIP drafts",
            "Delete all in-progress drafts and resume artifacts? This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        result = self.controller.discard_all_wip_drafts()
        count = int(result.get("deleted_count", 0) or 0)
        self.log_panel.append_line(f"[{datetime.now().strftime('%H:%M:%S')}] [SYSTEM] Deleted {count} WIP draft(s).")
        QMessageBox.information(self, "Drafts removed", f"Deleted {count} WIP draft(s).")
