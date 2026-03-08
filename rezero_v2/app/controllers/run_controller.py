from __future__ import annotations

import threading
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QObject, Signal

from rezero_v2.app.state.app_state import V2AppState
from rezero_v2.core.orchestrator.run_engine import RunEngine
from rezero_v2.stores.app_settings_store import AppSettingsStore


class V2RunController(QObject):
    state_changed = Signal(object)
    run_finished = Signal(object)
    busy_changed = Signal(bool)
    progress_event = Signal(object)

    def __init__(self, root: Path, settings_path: Path, *, engine: RunEngine | None = None) -> None:
        super().__init__()
        self.root = Path(root).resolve()
        self.settings_path = Path(settings_path).resolve()
        self.settings_store = AppSettingsStore(self.settings_path)
        self.engine = engine or RunEngine(self.root, self.settings_path, progress_hook=self._on_progress)
        self._busy = False
        self._worker: threading.Thread | None = None
        self._current_status = 'idle'
        self._current_stage = 'idle'
        self._current_message = '대기 중'
        self._latest_stage_rows: list[dict] = []
        self.refresh_state()

    def refresh_state(self) -> V2AppState:
        settings = self.settings_store.load_app_settings()
        latest = self.engine.run_store.latest_final_summary()
        recent_runs = self.engine.run_store.list_recent_runs(limit=20)
        recent_posts = self.engine.publish_store.list_recent_posts(limit=20)
        today_mix = self.engine.publish_store.get_daily_counts(datetime.now().astimezone().date().isoformat())
        state = V2AppState.build(
            latest_final_summary=latest,
            recent_runs=recent_runs,
            recent_posts=recent_posts,
            today_mix=today_mix,
            current_status=self._current_status,
            current_stage=self._current_stage,
            current_message=self._current_message,
            schedule_hours=float(getattr(settings.schedule, 'interval_hours', 2.4) or 2.4),
            inspector_stage_rows=list(self._latest_stage_rows),
        )
        self.state_changed.emit(state)
        return state

    def run_once_sync(self, *, force_content_type: str | None = None, dry_run: bool | None = None):
        self._busy = True
        self.busy_changed.emit(True)
        try:
            result = self.engine.run_once(force_content_type=force_content_type, dry_run=dry_run)
            self._current_status = result.summary.result
            self._current_stage = result.summary.final_stage
            self._current_message = result.summary.human_message
            self._latest_stage_rows = list(result.stage_results)
            self.refresh_state()
            self.run_finished.emit(result)
            return result
        finally:
            self._busy = False
            self.busy_changed.emit(False)

    def run_once_async(self, *, force_content_type: str | None = None, dry_run: bool | None = None) -> None:
        if self._busy:
            return

        def _worker() -> None:
            self.run_once_sync(force_content_type=force_content_type, dry_run=dry_run)

        self._worker = threading.Thread(target=_worker, name='rezero-v2-runner', daemon=True)
        self._worker.start()

    def _on_progress(self, event: dict) -> None:
        event_type = str(event.get('type', '') or '')
        if event_type == 'stage':
            self._current_status = str(event.get('status', '') or 'running')
            self._current_stage = str(event.get('stage_name', '') or 'stage')
            self._current_message = str(event.get('human_message', '') or '')
        elif event_type == 'run_start':
            self._current_status = 'running'
            self._current_stage = 'allocation'
            self._current_message = f"{event.get('slot_type', '')} 슬롯 실행을 시작합니다."
        elif event_type == 'final':
            summary = event.get('summary', {}) or {}
            self._current_status = str(summary.get('result', '') or 'done')
            self._current_stage = str(summary.get('final_stage', '') or 'done')
            self._current_message = str(summary.get('human_message', '') or '')
        self.progress_event.emit(event)
