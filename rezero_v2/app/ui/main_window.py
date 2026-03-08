from __future__ import annotations

from PySide6.QtWidgets import QMainWindow, QTabWidget, QWidget, QVBoxLayout

from rezero_v2.app.ui.dashboard_view import DashboardView
from rezero_v2.app.ui.publish_queue_view import PublishQueueView
from rezero_v2.app.ui.run_history_view import RunHistoryView
from rezero_v2.app.ui.run_inspector_view import RunInspectorView
from rezero_v2.app.ui.settings_view import SettingsView
from rezero_v2.app.ui.style import build_stylesheet


class V2MainWindow(QMainWindow):
    def __init__(self, *, run_controller, settings_controller, publish_controller) -> None:
        super().__init__()
        self.run_controller = run_controller
        self.settings_controller = settings_controller
        self.publish_controller = publish_controller
        self.setWindowTitle("RezeroAgent V2")
        self.resize(1320, 860)
        self.setStyleSheet(build_stylesheet())

        central = QWidget()
        central_layout = QVBoxLayout(central)
        self.tabs = QTabWidget()
        self.dashboard_view = DashboardView()
        self.run_inspector_view = RunInspectorView()
        self.publish_queue_view = PublishQueueView()
        self.run_history_view = RunHistoryView()
        self.settings_view = SettingsView()
        self.tabs.addTab(self.dashboard_view, "대시보드")
        self.tabs.addTab(self.run_inspector_view, "실행 검사")
        self.tabs.addTab(self.publish_queue_view, "게시 큐")
        self.tabs.addTab(self.run_history_view, "실행 이력")
        self.tabs.addTab(self.settings_view, "설정")
        central_layout.addWidget(self.tabs)
        self.setCentralWidget(central)

        self.dashboard_view.run_requested.connect(self.run_controller.run_once_async)
        self.dashboard_view.refresh_requested.connect(self.run_controller.refresh_state)
        self.settings_view.save_requested.connect(self.settings_controller.save_config)
        self.run_controller.state_changed.connect(self._apply_state)
        self.run_controller.busy_changed.connect(self.dashboard_view.set_busy)
        self.settings_controller.config_changed.connect(self.settings_view.load_config)

        self.settings_view.load_config(self.settings_controller.load_config())
        self._apply_state(self.run_controller.refresh_state())

    def _apply_state(self, state) -> None:
        self.dashboard_view.update_state(state)
        self.run_inspector_view.update_state(state)
        self.publish_queue_view.update_state(state)
        self.run_history_view.update_state(state)
