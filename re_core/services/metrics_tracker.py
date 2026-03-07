import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

class MetricsTrackerService:
    """
    Tracks performance, timings, and runtime metrics for posts being generated.
    Supports phase tracking, heartbeats, and persistent logging.
    """
    def __init__(self, log_path: Optional[Path] = None):
        self.metrics: dict[str, Any] = {}
        self.log_path = log_path
        self.run_id = uuid.uuid4().hex[:12]
        self._start_mono = time.perf_counter()
        self._current_phase: Optional[str] = None
        self._phase_start_mono: float = 0.0
        self._phase_last_message: str = ""
        self._phase_last_percent: int = 0
        self._last_heartbeat_mono: float = 0.0

    def _append_log(self, event: str, payload: Optional[dict] = None):
        if not self.log_path:
            return
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "run_id": self.run_id,
        }
        if payload:
            row.update(payload)
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def start_run(self, manual_trigger: bool = False, meta: Optional[dict] = None):
        self._append_log("run_start", {
            "manual_trigger": manual_trigger,
            **(meta or {})
        })

    def track_progress(self, phase: str, message: str, percent: int, heartbeat_sec: float = 5.0):
        now = time.perf_counter()
        phase = phase.strip() or "idle"
        
        if self._current_phase != phase:
            if self._current_phase:
                self.end_phase("phase_change")
            self._current_phase = phase
            self._phase_start_mono = now
            self._phase_last_message = message
            self._phase_last_percent = percent
            self._last_heartbeat_mono = now
            self._append_log("phase_start", {"phase": phase, "message": message, "percent": percent})
            return

        # Heartbeat logic
        if (now - self._last_heartbeat_mono) >= heartbeat_sec:
            elapsed_ms = int((now - self._phase_start_mono) * 1000)
            self._append_log("phase_heartbeat", {
                "phase": phase,
                "elapsed_ms": elapsed_ms,
                "message": message,
                "percent": percent
            })
            self._last_heartbeat_mono = now
        
        self._phase_last_message = message
        self._phase_last_percent = percent

    def end_phase(self, reason: str = "finished"):
        if not self._current_phase:
            return
        elapsed_ms = int((time.perf_counter() - self._phase_start_mono) * 1000)
        self._append_log("phase_end", {
            "phase": self._current_phase,
            "duration_ms": elapsed_ms,
            "reason": reason,
            "last_message": self._phase_last_message,
            "last_percent": self._phase_last_percent
        })
        self._current_phase = None

    def finish_run(self, status: str, message: str = ""):
        self.end_phase("run_end")
        total_ms = int((time.perf_counter() - self._start_mono) * 1000)
        self._append_log("run_end", {
            "status": status,
            "message": message,
            "total_ms": total_ms
        })

    def record_event(self, event: str, payload: Optional[dict] = None):
        self._append_log(event, payload)
