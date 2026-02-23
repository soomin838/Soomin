from __future__ import annotations

import argparse
import base64
import ctypes
import io
import importlib.util
import math
import json
import os
import platform
import random
import re
import shutil
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path
import yaml
from zoneinfo import ZoneInfo

from core.onboarding import has_missing_required
from core.preflight import validate_runtime_settings
from core.qa_logger import QALogger, classify_error
from core.settings import load_settings
from core.workflow import AgentWorkflow

if getattr(sys, "frozen", False):
    BUNDLE_ROOT = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
else:
    BUNDLE_ROOT = Path(__file__).resolve().parent

ROOT = Path(os.getenv("APPDATA", str(Path.home()))) / "RezeroAgent"
SETTINGS_PATH = ROOT / "config" / "settings.yaml"
CLIENT_SECRETS_PATH = ROOT / "config" / "client_secrets.json"
BLOGGER_TOKEN_PATH = ROOT / "config" / "blogger_token.json"
GEMINI_RATE_LIMIT_DOC = "https://ai.google.dev/gemini-api/docs/rate-limits"
GEMINI_PRICING_DOC = "https://ai.google.dev/gemini-api/docs/pricing"
GEMINI_QUOTA_CONSOLE = "https://console.cloud.google.com/apis/api/generativelanguage.googleapis.com/quotas"
GEMINI_USAGE_DASHBOARD = "https://aistudio.google.com/usage"


def _read_version_lines(path: Path) -> dict[str, str]:
    try:
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return {}
    meta: dict[str, str] = {}
    for line in str(raw or "").splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = str(k or "").strip()
        val = str(v or "").strip()
        if key:
            meta[key] = val
    return meta


def resolve_running_version() -> str:
    candidates: list[Path] = []
    try:
        exe_dir = Path(sys.executable).resolve().parent
        candidates.append(exe_dir / "version.txt")
    except Exception:
        pass
    candidates.extend(
        [
            ROOT / "version.txt",
            BUNDLE_ROOT / "version.txt",
            Path(__file__).resolve().parent / "version.txt",
        ]
    )
    seen: set[str] = set()
    for p in candidates:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        meta = _read_version_lines(p)
        if not meta:
            continue
        commit = str(meta.get("commit", "") or "").strip()
        build_date = str(meta.get("build_date", "") or "").strip()
        if commit and build_date:
            return f"{commit[:7]} ({build_date})"
        if commit:
            return commit[:7]
        if build_date:
            return build_date
    return "unknown"


def safe_tz(tz_name: str):
    """Return a valid tzinfo even when zone database is missing."""
    try:
        return ZoneInfo(tz_name)
    except Exception:
        if tz_name.upper() == "UTC":
            return timezone.utc
        try:
            return datetime.now().astimezone().tzinfo or timezone.utc
        except Exception:
            return timezone.utc


def initialize_runtime_home() -> None:
    (ROOT / "config").mkdir(parents=True, exist_ok=True)
    (ROOT / "storage" / "logs").mkdir(parents=True, exist_ok=True)
    (ROOT / "storage" / "temp_images").mkdir(parents=True, exist_ok=True)
    (ROOT / "storage" / "sessions").mkdir(parents=True, exist_ok=True)
    (ROOT / "storage" / "seeds").mkdir(parents=True, exist_ok=True)
    (ROOT / "storage" / "references").mkdir(parents=True, exist_ok=True)
    (ROOT / "patches").mkdir(parents=True, exist_ok=True)

    src_settings = BUNDLE_ROOT / "config" / "settings.yaml"
    if not SETTINGS_PATH.exists() and src_settings.exists():
        shutil.copy2(src_settings, SETTINGS_PATH)

    for rel in [
        "storage/seeds/seeds.json",
        "storage/seeds/topics.jsonl",
        "storage/references/quality_automation_manual.txt",
        "storage/references/writing_patterns_playbook.txt",
        "patches/runtime_patch.py.example",
    ]:
        src = BUNDLE_ROOT / rel
        dst = ROOT / rel
        if src.exists() and not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)

    # One-time migration: legacy topics.jsonl -> seeds.json (array).
    seeds_json = ROOT / "storage" / "seeds" / "seeds.json"
    legacy_jsonl = ROOT / "storage" / "seeds" / "topics.jsonl"
    if (not seeds_json.exists()) and legacy_jsonl.exists():
        rows = []
        for line in legacy_jsonl.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                rows.append(item)
        if rows:
            seeds_json.write_text(
                json.dumps(rows, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )


class AgentController:
    def __init__(self) -> None:
        self.running_version = resolve_running_version()
        self.settings = load_settings(SETTINGS_PATH)
        self._auto_tune_runtime_limits()
        self.workflow = AgentWorkflow(ROOT, self.settings)
        self.qa = QALogger(ROOT / "storage" / "logs" / "qa_runtime.jsonl")
        self.workflow.set_progress_hook(self._on_workflow_progress)
        self._apply_runtime_patch_if_present()
        self.lock = threading.Lock()
        self.tz = safe_tz(self.settings.timezone)
        self.scheduler_state_path = ROOT / "storage" / "logs" / "run_schedule_state.json"
        self.scheduler_slots: list[datetime] = []
        self._schedule_refresh_interval_sec = 600
        self._last_schedule_refresh_epoch = 0.0
        self.running = False
        self.force_run = False
        self.next_run_at = datetime.now(self.tz)
        self.last_status = "Idle"
        self.last_message = "Ready"
        self.last_error = ""
        self.thread: threading.Thread | None = None
        self._started = False
        self.phase_key = "idle"
        self.phase_message = "대기"
        self.phase_percent = 0
        self._phase_trace: list[dict] = []
        self._phase_started_at: float | None = None
        self._phase_last_key: str = "idle"
        self._run_started_at_monotonic: float | None = None
        self.theme_mode: str = "auto"
        self._ui_insights_snapshot: dict = {}
        self._ui_usage_cache: dict = {}
        self._ui_usage_loading = False
        self._ui_usage_next_refresh_epoch = 0.0
        self._ui_errors_cache: list[dict] = []
        self._ui_errors_next_refresh_epoch = time.time() + 2.0
        self.qa.write(
            "update_regression",
            "controller_initialized",
            {
                "runtime_root": str(ROOT),
                "settings_path": str(SETTINGS_PATH),
                "free_mode": bool(self.settings.budget.free_mode),
                "dry_run": bool(self.settings.budget.dry_run),
                "model": self.settings.gemini.model,
            },
        )
        self.qa.write(
            "runtime",
            "running_version",
            {
                "version": self.running_version,
                "frozen": bool(getattr(sys, "frozen", False)),
                "executable": str(getattr(sys, "executable", "")),
            },
        )
        self._bootstrap_rolling_scheduler()
        # Warm usage snapshot asynchronously (non-blocking for UI startup).
        self._maybe_refresh_ui_usage_async(force=True)

    def compute_next_run(self) -> datetime:
        now = datetime.now(self.tz)
        schedule = self.settings.schedule
        min_h = float(getattr(schedule, "min_interval_hours", 1.0))
        max_h = float(getattr(schedule, "max_interval_hours", 4.5))
        base_h = float(getattr(schedule, "interval_hours", 2.4))
        if min_h > max_h:
            min_h, max_h = max_h, min_h
        base_h = max(min_h, min(max_h, base_h))

        # Human-like irregular wake cycle: mostly near base, occasionally wide.
        if random.random() < 0.72:
            sampled_h = random.triangular(min_h, max_h, base_h)
        else:
            sampled_h = random.uniform(min_h, max_h)
        sampled_sec = int(sampled_h * 3600)
        micro_jitter_sec = random.randint(-120, 180)
        delay_sec = max(60, sampled_sec + micro_jitter_sec)
        return now + timedelta(seconds=delay_sec)

    def _rolling_horizon_hours(self) -> int:
        # Execution scheduler keeps a rolling multi-day window but stays adaptive.
        try:
            configured = int(getattr(self.settings.publish, "queue_horizon_hours", 72) or 72)
        except Exception:
            configured = 72
        return max(24, min(72, configured))

    def _schedule_target_slot_count(self, horizon_hours: int) -> int:
        try:
            base_h = float(getattr(self.settings.schedule, "interval_hours", 2.4) or 2.4)
        except Exception:
            base_h = 2.4
        slots = int(math.ceil(float(horizon_hours) / max(0.6, base_h)))
        return max(8, min(96, slots))

    def _parse_scheduler_dt(self, value: str) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self.tz)
        return dt.astimezone(self.tz).replace(microsecond=0)

    def _load_scheduler_state(self) -> bool:
        path = self.scheduler_state_path
        if not path.exists():
            return False
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return False
        now = datetime.now(self.tz)
        out: list[datetime] = []
        for item in list(raw.get("slots", []) or [])[:400]:
            dt = self._parse_scheduler_dt(str(item))
            if dt is not None and dt > now + timedelta(seconds=30):
                out.append(dt)
        if not out:
            next_raw = str(raw.get("next_run_at", "") or "").strip()
            dt = self._parse_scheduler_dt(next_raw)
            if dt is not None and dt > now + timedelta(seconds=30):
                out.append(dt)
        out.sort()
        self.scheduler_slots = out
        if out:
            self.next_run_at = out[0]
        return bool(out)

    def _save_scheduler_state(self) -> None:
        try:
            self.scheduler_state_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "timezone": self.settings.timezone,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "horizon_hours": self._rolling_horizon_hours(),
                "next_run_at": self.next_run_at.isoformat(),
                "slots": [dt.isoformat() for dt in self.scheduler_slots[:200]],
            }
            self.scheduler_state_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _scheduler_mode(self, now_local: datetime) -> str:
        # Blogger live state drives execution density (catch-up vs cool-down).
        try:
            snap_fn = getattr(self.workflow, "_blog_snapshot", None)
            if not callable(snap_fn):
                return "steady"
            # Startup/reload latency guard:
            # do not force a remote Blogger call on UI startup path.
            snap = snap_fn(force_refresh=False, allow_remote=False)
            now_utc = now_local.astimezone(timezone.utc)
            end_utc = now_utc + timedelta(hours=self._rolling_horizon_hours())
            scheduled = list((snap or {}).get("scheduled_items", []) or [])
            count = 0
            for row in scheduled:
                dt = self._parse_scheduler_dt(str((row or {}).get("publish_at", "") or ""))
                if dt is None:
                    continue
                dt_utc = dt.astimezone(timezone.utc)
                if now_utc < dt_utc <= end_utc:
                    count += 1
            daily_cap = max(1, int(getattr(self.settings.publish, "daily_publish_cap", 5) or 5))
            target = max(4, int(math.ceil(daily_cap * (self._rolling_horizon_hours() / 24.0))))
            if count < max(2, int(target * 0.70)):
                return "catchup"
            if count >= int(target * 1.25):
                return "cooldown"
        except Exception:
            return "steady"
        return "steady"

    def _sample_delay_seconds(self, mode: str = "steady") -> int:
        schedule = self.settings.schedule
        min_h = float(getattr(schedule, "min_interval_hours", 1.0) or 1.0)
        max_h = float(getattr(schedule, "max_interval_hours", 4.5) or 4.5)
        base_h = float(getattr(schedule, "interval_hours", 2.4) or 2.4)
        if min_h > max_h:
            min_h, max_h = max_h, min_h
        base_h = max(min_h, min(max_h, base_h))

        if mode == "catchup":
            preferred = max(min_h, min(base_h, min_h + (base_h - min_h) * 0.35))
        elif mode == "cooldown":
            preferred = min(max_h, max(base_h, base_h + (max_h - base_h) * 0.65))
        else:
            preferred = base_h

        if random.random() < 0.78:
            sampled_h = random.triangular(min_h, max_h, preferred)
        else:
            sampled_h = random.uniform(min_h, max_h)

        delay_sec = int(sampled_h * 3600) + random.randint(-180, 240)
        return max(300, delay_sec)

    def _rebuild_scheduler_slots(
        self,
        *,
        keep_existing: bool = True,
        now_local: datetime | None = None,
        force_mode: str | None = None,
    ) -> None:
        now_local = now_local or datetime.now(self.tz)
        horizon_h = self._rolling_horizon_hours()
        horizon_end = now_local + timedelta(hours=horizon_h)
        target_count = self._schedule_target_slot_count(horizon_h)

        slots: list[datetime] = []
        if keep_existing:
            for dt in self.scheduler_slots:
                if dt > now_local + timedelta(seconds=30):
                    slots.append(dt.astimezone(self.tz).replace(microsecond=0))
            slots.sort()

        mode = force_mode or self._scheduler_mode(now_local)
        anchor = slots[-1] if slots else now_local
        loops = 0
        while ((len(slots) < target_count) or (not slots or slots[-1] < horizon_end)) and loops < 360:
            loops += 1
            nxt = anchor + timedelta(seconds=self._sample_delay_seconds(mode))
            if nxt <= now_local + timedelta(minutes=1):
                nxt = now_local + timedelta(minutes=1)
            if slots and nxt <= slots[-1]:
                nxt = slots[-1] + timedelta(minutes=1)
            nxt = nxt.replace(microsecond=0)
            slots.append(nxt)
            anchor = nxt

        self.scheduler_slots = slots
        if slots:
            self.next_run_at = slots[0]
        else:
            self.next_run_at = self.compute_next_run()
        self._save_scheduler_state()

    def _bootstrap_rolling_scheduler(self) -> None:
        loaded = self._load_scheduler_state()
        self._rebuild_scheduler_slots(keep_existing=loaded, now_local=datetime.now(self.tz))
        self._last_schedule_refresh_epoch = time.time()
        self.qa.write(
            "soak_test",
            "rolling_scheduler_bootstrapped",
            {
                "loaded_state": bool(loaded),
                "slots": int(len(self.scheduler_slots)),
                "horizon_h": int(self._rolling_horizon_hours()),
                "next_run_at": self.next_run_at.isoformat(),
            },
        )

    def _maybe_refresh_scheduler(self, force: bool = False) -> None:
        now_epoch = time.time()
        if not force and (now_epoch - self._last_schedule_refresh_epoch) < self._schedule_refresh_interval_sec:
            return
        self._rebuild_scheduler_slots(keep_existing=True, now_local=datetime.now(self.tz))
        self._last_schedule_refresh_epoch = now_epoch

    def _advance_scheduler_after_run(self, *, manual_trigger: bool, now_local: datetime | None = None) -> None:
        now_local = now_local or datetime.now(self.tz)
        if manual_trigger:
            # Manual run should not consume a future planned slot.
            self.scheduler_slots = [
                dt for dt in self.scheduler_slots
                if dt > now_local + timedelta(seconds=30)
            ]
        else:
            self.scheduler_slots = [
                dt for dt in self.scheduler_slots
                if dt > now_local + timedelta(minutes=2)
            ]
        self._rebuild_scheduler_slots(keep_existing=True, now_local=now_local)

    def _apply_backoff_next_run(self, wait_minutes: int, now_local: datetime | None = None) -> None:
        now_local = now_local or datetime.now(self.tz)
        wait = max(1, int(wait_minutes))
        forced = (now_local + timedelta(minutes=wait)).replace(microsecond=0)
        kept = [dt for dt in self.scheduler_slots if dt > forced + timedelta(seconds=30)]
        self.scheduler_slots = [forced] + kept
        self._rebuild_scheduler_slots(keep_existing=True, now_local=now_local)

    def start(self) -> None:
        if self._started:
            self.running = True
            return
        self.running = True
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()
        self._started = True

    def stop(self) -> None:
        self.running = False
        self._save_scheduler_state()

    def reload(self) -> None:
        with self.lock:
            self.settings = load_settings(SETTINGS_PATH)
            self._auto_tune_runtime_limits()
            self.workflow = AgentWorkflow(ROOT, self.settings)
            self.workflow.set_progress_hook(self._on_workflow_progress)
            self._apply_runtime_patch_if_present()
            self.tz = safe_tz(self.settings.timezone)
            self._bootstrap_rolling_scheduler()
            self.qa.write(
                "update_regression",
                "settings_reloaded",
                {
                    "free_mode": bool(self.settings.budget.free_mode),
                    "dry_run": bool(self.settings.budget.dry_run),
                    "model": self.settings.gemini.model,
                    "target_images": int(self.settings.visual.target_images_per_post),
                },
            )

    def _apply_runtime_patch_if_present(self) -> None:
        patch_path = ROOT / "patches" / "runtime_patch.py"
        if not patch_path.exists():
            return
        try:
            module_name = f"rezero_runtime_patch_{int(patch_path.stat().st_mtime)}"
            spec = importlib.util.spec_from_file_location(module_name, str(patch_path))
            if spec is None or spec.loader is None:
                raise RuntimeError("patch spec load failed")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            apply_fn = getattr(mod, "apply", None)
            if not callable(apply_fn):
                raise RuntimeError("runtime_patch.py missing apply(controller, workflow, settings, root)")
            apply_fn(controller=self, workflow=self.workflow, settings=self.settings, root=ROOT)
            self.qa.write(
                "update_regression",
                "runtime_patch_applied",
                {"patch_file": str(patch_path)},
            )
        except Exception as exc:
            self.qa.write(
                "fault_injection",
                "runtime_patch_failed",
                {"patch_file": str(patch_path), "error": str(exc)},
            )

    def _on_workflow_progress(self, phase: str, message: str, percent: int) -> None:
        phase_key = phase or "idle"
        phase_msg = message or ""
        phase_pct = max(0, min(100, int(percent)))
        self.phase_key = phase_key
        self.phase_message = phase_msg
        self.phase_percent = phase_pct
        self._trace_phase_transition(phase_key, phase_msg, phase_pct)

    def _begin_run_trace(self) -> None:
        now_mono = time.monotonic()
        now_iso = datetime.now(timezone.utc).isoformat()
        self._run_started_at_monotonic = now_mono
        self._phase_started_at = now_mono
        self._phase_last_key = "run_init"
        self._phase_trace = [
            {
                "phase": "run_init",
                "message": "워크플로우 시작",
                "percent": 1,
                "at_utc": now_iso,
            }
        ]

    def _trace_phase_transition(self, phase: str, message: str, percent: int) -> None:
        now_mono = time.monotonic()
        now_iso = datetime.now(timezone.utc).isoformat()
        if not self._phase_trace:
            self._phase_trace = [
                {
                    "phase": str(phase or "idle"),
                    "message": str(message or ""),
                    "percent": int(percent),
                    "at_utc": now_iso,
                }
            ]
            self._phase_started_at = now_mono
            self._phase_last_key = str(phase or "idle")
            return

        current_key = str(self._phase_last_key or "idle")
        next_key = str(phase or "idle")
        if current_key == next_key:
            # Keep most recent message/percent for current phase.
            self._phase_trace[-1]["message"] = str(message or "")
            self._phase_trace[-1]["percent"] = int(percent)
            return

        if self._phase_started_at is not None:
            dur = max(0.0, now_mono - self._phase_started_at)
            self._phase_trace[-1]["duration_sec"] = round(dur, 2)
        self._phase_trace.append(
            {
                "phase": next_key,
                "message": str(message or ""),
                "percent": int(percent),
                "at_utc": now_iso,
            }
        )
        self._phase_started_at = now_mono
        self._phase_last_key = next_key

    def _finalize_run_trace(self) -> dict:
        now_mono = time.monotonic()
        if self._phase_trace and self._phase_started_at is not None:
            if "duration_sec" not in self._phase_trace[-1]:
                self._phase_trace[-1]["duration_sec"] = round(
                    max(0.0, now_mono - self._phase_started_at), 2
                )
        totals: dict[str, float] = {}
        for row in self._phase_trace:
            phase = str(row.get("phase", "") or "").strip() or "unknown"
            try:
                dur = float(row.get("duration_sec", 0.0) or 0.0)
            except Exception:
                dur = 0.0
            totals[phase] = round(float(totals.get(phase, 0.0)) + dur, 2)
        run_elapsed = 0.0
        if self._run_started_at_monotonic is not None:
            run_elapsed = round(max(0.0, now_mono - self._run_started_at_monotonic), 2)
        slowest = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:5]
        return {
            "run_elapsed_sec_trace": run_elapsed,
            "phase_totals_sec": totals,
            "phase_slowest_top5": [{"phase": k, "sec": v} for k, v in slowest],
            "phase_trace": self._phase_trace[-30:],
        }

    def loop(self) -> None:
        backoff_minutes = 1
        self._maybe_refresh_scheduler(force=True)

        while True:
            if not self.running:
                time.sleep(1)
                continue

            self._maybe_refresh_scheduler(force=False)
            now = datetime.now(self.tz)
            if not (self.force_run or now >= self.next_run_at):
                time.sleep(2)
                continue

            try:
                run_started = time.time()
                with self.lock:
                    is_manual_trigger = bool(self.force_run)
                    self.force_run = False
                    self.last_status = "Running"
                    self.last_message = "Agent workflow executing..."
                    self.phase_key = "run_init"
                    self.phase_message = "워크플로우 시작"
                    self.phase_percent = 1
                    self._begin_run_trace()
                    self.qa.write(
                        "soak_test",
                        "run_started",
                        {
                            "next_run_at": self.next_run_at.isoformat(),
                            "manual_trigger": bool(is_manual_trigger),
                            "free_mode": bool(self.settings.budget.free_mode),
                            "dry_run": bool(self.settings.budget.dry_run),
                            "model": self.settings.gemini.model,
                        },
                    )
                    result = self.workflow.run_once(manual_trigger=is_manual_trigger)
                    self.last_status = result.status
                    self.last_message = result.message
                    if result.status.lower() in {"success", "skipped", "hold"}:
                        self.phase_key = "idle"
                        self.phase_message = "다음 예약 대기"
                        self.phase_percent = 100 if result.status.lower() == "success" else max(self.phase_percent, 85)
                    elapsed = round(time.time() - run_started, 2)
                    trace_payload = self._finalize_run_trace()
                    self.qa.write(
                        "publish_regression",
                        "run_finished",
                        {
                            "status": result.status,
                            "message": result.message,
                            "manual_trigger": bool(is_manual_trigger),
                            "duration_sec": elapsed,
                            "next_run_at": self.next_run_at.isoformat(),
                            "phase_slowest_top5": trace_payload.get("phase_slowest_top5", []),
                            "phase_totals_sec": trace_payload.get("phase_totals_sec", {}),
                            "phase_trace_tail": trace_payload.get("phase_trace", []),
                            "run_elapsed_sec_trace": trace_payload.get("run_elapsed_sec_trace", 0.0),
                        },
                    )
                backoff_minutes = 1
                self._advance_scheduler_after_run(
                    manual_trigger=is_manual_trigger,
                    now_local=datetime.now(self.tz),
                )
            except Exception as exc:
                self.last_status = "Error"
                self.last_error = str(exc)
                self.last_message = f"Error: {exc}"
                self.phase_key = "error"
                self.phase_message = "오류 대응/재시도 대기"
                self._trace_phase_transition("error", self.phase_message, max(1, int(self.phase_percent or 1)))
                self._notify_failure(f"Agent run failed: {exc}")
                quota_wait = self._compute_quota_wait_minutes(str(exc))
                if quota_wait is not None:
                    sleep_for = quota_wait
                    self.last_status = "QuotaWait"
                    self.last_message = f"Quota exceeded. Auto-retry in {sleep_for} minutes."
                    self.phase_key = "quota_wait"
                    self.phase_message = "쿼터 회복 대기"
                else:
                    sleep_for = min(
                        backoff_minutes,
                        int(self.settings.schedule.max_retry_backoff_minutes),
                    )
                self._apply_backoff_next_run(sleep_for, now_local=now)
                trace_payload = self._finalize_run_trace()
                self.qa.write(
                    "fault_injection",
                    "run_failed",
                    {
                        "error": str(exc),
                        "error_class": classify_error(str(exc)),
                        "retry_after_minutes": int(sleep_for),
                        "next_run_at": self.next_run_at.isoformat(),
                        "phase_slowest_top5": trace_payload.get("phase_slowest_top5", []),
                        "phase_totals_sec": trace_payload.get("phase_totals_sec", {}),
                    },
                )
                if quota_wait is None:
                    backoff_minutes *= 2
                else:
                    backoff_minutes = 1

    def status_text(self) -> str:
        now = datetime.now(self.tz)
        remaining = self.next_run_at - now
        minutes = max(int(remaining.total_seconds() // 60), 0)
        hours, mins = divmod(minutes, 60)
        status_map = {
            "Idle": "대기",
            "Running": "실행중",
            "Paused": "일시정지",
            "Error": "오류",
            "QuotaWait": "쿼터대기",
            "Success": "성공",
            "Skipped": "건너뜀",
            "Hold": "보류",
        }
        status = status_map.get(self.last_status, self.last_status)
        return f"{status} | 다음 실행까지 {hours}시간 {mins}분"

    def usage_text(self) -> str:
        try:
            calls = int(self.workflow.logs.get_today_gemini_count())
            usage = self.workflow.get_usage_snapshot()
            posts = int(usage.get("today_posts", 0))
            runs = int(usage.get("today_runs", 0))
            today_scheduled = int(usage.get("today_scheduled", 0))
            queue_72h = int(usage.get("scheduled_72h", 0))
            source = str(usage.get("source", "local"))
            resume_exists = bool(usage.get("resume_exists", False))
            resume_stage = str(usage.get("resume_stage", "") or "")
            local_llm_ready = bool(usage.get("local_llm_ready", False))
            local_llm_used = bool(usage.get("local_llm_used_last_run", False))
            local_llm_reason = str(usage.get("local_llm_reason", "") or "")
            keywords = self.workflow.get_today_global_keywords()
        except Exception:
            calls = 0
            posts = 0
            runs = 0
            today_scheduled = 0
            queue_72h = 0
            source = "local"
            resume_exists = False
            resume_stage = ""
            local_llm_ready = False
            local_llm_used = False
            local_llm_reason = "unknown"
            keywords = []
        call_cap = int(self.settings.budget.daily_gemini_call_limit)
        post_cap = int(self.settings.budget.daily_post_limit)
        publish_cap = int(self.settings.publish.daily_publish_cap)
        queue_cap = int(self.settings.publish.target_queue_size)
        rec_cap = self._recommended_call_cap(self.settings.gemini.model)
        source_label = "Blogger 실시간" if source == "blogger" else "로컬 추정"
        kw_line = ", ".join(keywords[:3]) if keywords else "-"
        resume_line = "없음"
        if resume_exists:
            resume_line = f"있음({resume_stage or 'unknown'})"
        llm_state = "used" if local_llm_used else ("ready" if local_llm_ready else "fallback")
        llm_reason = (local_llm_reason or "-")[:42]
        return (
            f"모델: {self.settings.gemini.model}\n"
            f"API: {calls}/{call_cap} (권장 {rec_cap})\n"
            f"게시(일캡)/예약(일캡): {posts}/{publish_cap} | {today_scheduled}/{post_cap}\n"
            f"{int(self.settings.publish.queue_horizon_hours)}h 큐: {queue_72h}/{queue_cap} ({source_label})\n"
            f"로컬성공: {runs} | 재개: {resume_line}\n"
            f"Local LLM: {llm_state} ({llm_reason})\n"
            f"키워드: {kw_line}"
        )

    def resume_text(self) -> str:
        try:
            snap = self.workflow.get_resume_snapshot(force_refresh=False, allow_remote=False)
        except Exception:
            snap = {}
        exists = bool(snap.get("exists", False))
        if not exists:
            return "없음"
        stage = str(snap.get("stage", "") or "unknown")
        updated = str(snap.get("updated", "") or "")
        title = str(snap.get("title", "") or "").strip()
        short = title[:44] + ("..." if len(title) > 44 else "")
        if updated:
            return f"있음 | {stage} | {short}"
        return f"있음 | {stage} | {short}"

    def get_ui_snapshot(self) -> dict:
        now_epoch = time.time()
        self._maybe_refresh_ui_usage_async(force=False)
        if now_epoch >= self._ui_errors_next_refresh_epoch:
            try:
                self._ui_errors_cache = self.workflow.logs.get_recent_runs(
                    days=3,
                    limit=10,
                    statuses=["error", "hold"],
                )
            except Exception:
                self._ui_errors_cache = []
            self._ui_errors_next_refresh_epoch = now_epoch + 3.0
        usage = dict(self._ui_usage_cache or {})
        recent_errors = list(self._ui_errors_cache or [])
        return {
            "status": str(self.last_status or ""),
            "phase_key": str(self.phase_key or "idle"),
            "phase_message": str(self.phase_message or ""),
            "phase_percent": int(self.phase_percent or 0),
            "next_run_text": self.status_text(),
            "last_message": str(self.last_message or ""),
            "resume_text": self.resume_text(),
            "usage_snapshot": usage,
            "recent_posts": [],
            "recent_errors": recent_errors,
            "insights_snapshot": dict(self._ui_insights_snapshot or {}),
            "theme_mode": str(self.theme_mode or "auto"),
        }

    def _maybe_refresh_ui_usage_async(self, force: bool = False) -> None:
        now_epoch = time.time()
        if self._ui_usage_loading:
            return
        if not force and now_epoch < self._ui_usage_next_refresh_epoch:
            return
        self._ui_usage_loading = True

        def worker() -> None:
            next_epoch = time.time() + 8.0
            data: dict = dict(self._ui_usage_cache or {})
            acquired = False
            try:
                acquired = self.lock.acquire(blocking=False)
                if not acquired:
                    next_epoch = time.time() + 1.0
                    return
                data = self.workflow.get_usage_snapshot(allow_remote=False)
            except Exception:
                pass
            finally:
                if acquired:
                    self.lock.release()
                self._ui_usage_cache = dict(data or {})
                self._ui_usage_next_refresh_epoch = next_epoch
                self._ui_usage_loading = False

        threading.Thread(target=worker, daemon=True).start()

    def _recommended_call_cap(self, model: str) -> int:
        m = (model or "").lower()
        if "lite" in m:
            return 20
        if "pro" in m:
            return 50
        if "1.5-flash" in m:
            return 50
        if "2.0-flash" in m or "2.5-flash" in m or "flash" in m:
            return 50
        return 50

    def _auto_tune_runtime_limits(self) -> None:
        # Policy: apply one-time migrations only. Never re-overwrite user-selected limits on reload.
        try:
            raw = _load_yaml(SETTINGS_PATH)
            if not isinstance(raw, dict):
                return
            changed = False
            migration_target = 1
            try:
                current_ver = int(_nested_get(raw, "runtime.policy_migration_version") or "0")
            except Exception:
                current_ver = 0
            if current_ver >= migration_target:
                return

            rec = self._recommended_call_cap(self.settings.gemini.model)
            try:
                cur_cap = int(_nested_get(raw, "budget.daily_gemini_call_limit") or "0")
            except Exception:
                cur_cap = 0
            if cur_cap <= 0:
                _nested_set(raw, "budget.daily_gemini_call_limit", rec)
                changed = True
            try:
                cur_day_calls = int(_nested_get(raw, "gemini.max_calls_per_day") or "0")
            except Exception:
                cur_day_calls = 0
            if cur_day_calls <= 0:
                _nested_set(raw, "gemini.max_calls_per_day", rec)
                changed = True
            try:
                cur_run_calls = int(_nested_get(raw, "gemini.max_calls_per_run") or "0")
            except Exception:
                cur_run_calls = 0
            if cur_run_calls <= 0:
                _nested_set(raw, "gemini.max_calls_per_run", 4)
                changed = True

            if (_nested_get(raw, "sources.seeds_path") or "").strip().endswith("topics.jsonl"):
                _nested_set(raw, "sources.seeds_path", "storage/seeds/seeds.json")
                changed = True

            # Align legacy timezone default with US-target publish policy.
            legacy_tz = (_nested_get(raw, "timezone") or "").strip()
            if legacy_tz in {"", "UTC", "Asia/Seoul", "America/Los_Angeles"}:
                _nested_set(raw, "timezone", "America/New_York")
                changed = True

            cur_backend = (_nested_get(raw, "publish.image_hosting_backend") or "").strip().lower()
            if cur_backend in {"", "gcs", "drive"}:
                _nested_set(raw, "publish.image_hosting_backend", "blogger_media")
                changed = True

            cur_text_model = (_nested_get(raw, "gemini.model") or "").strip()
            if not cur_text_model:
                _nested_set(raw, "gemini.model", "gemini-2.0-flash")
                changed = True

            gemini_raw = raw.get("gemini", {}) if isinstance(raw.get("gemini", {}), dict) else {}
            cur_fb_models = gemini_raw.get("fallback_models")
            if not isinstance(cur_fb_models, list) or len(cur_fb_models) < 3:
                _nested_set(
                    raw,
                    "gemini.fallback_models",
                    ["gemini-2.0-flash", "gemini-2.5-flash-lite", "gemini-2.5-flash"],
                )
                changed = True

            cur_prompt_model = (_nested_get(raw, "visual.gemini_prompt_model") or "").strip()
            if not cur_prompt_model:
                _nested_set(raw, "visual.gemini_prompt_model", "gemini-2.0-flash")
                changed = True
            cur_img_model = (_nested_get(raw, "visual.gemini_image_model") or "").strip()
            if cur_img_model in {"", "gemini-2.0-flash-exp", "imagen-3.0", "gemini-2.5-flash-image"}:
                _nested_set(raw, "visual.gemini_image_model", "models/imagen-3.0-generate-001")
                changed = True

            try:
                cur_target_images = int(_nested_get(raw, "visual.target_images_per_post") or "0")
            except Exception:
                cur_target_images = 0
            if cur_target_images <= 0:
                _nested_set(raw, "visual.target_images_per_post", 2)
                changed = True

            try:
                cur_img_gap = int(_nested_get(raw, "visual.image_request_interval_seconds") or "0")
            except Exception:
                cur_img_gap = 0
            if cur_img_gap <= 0:
                _nested_set(raw, "visual.image_request_interval_seconds", 20)
                changed = True

            provider = (_nested_get(raw, "visual.image_provider") or "").strip().lower()
            if provider != "pollinations":
                _nested_set(raw, "visual.image_provider", "pollinations")
                changed = True
            pollinations_raw = (
                raw.get("visual", {}) if isinstance(raw.get("visual", {}), dict) else {}
            )
            if "pollinations_enabled" not in pollinations_raw:
                _nested_set(raw, "visual.pollinations_enabled", True)
                changed = True
            if str(pollinations_raw.get("pollinations_thumbnail_model", "")).strip() != "gptimage":
                _nested_set(raw, "visual.pollinations_thumbnail_model", "gptimage")
                changed = True
            if str(pollinations_raw.get("pollinations_content_model", "")).strip() != "gptimage":
                _nested_set(raw, "visual.pollinations_content_model", "gptimage")
                changed = True
            if "thumbnail_ocr_verify" not in pollinations_raw:
                _nested_set(raw, "visual.thumbnail_ocr_verify", False)
                changed = True
            if str(_nested_get(raw, "visual.enable_gemini_image_generation") or "").strip().lower() not in {"false", "0", "no", "off"}:
                _nested_set(raw, "visual.enable_gemini_image_generation", False)
                changed = True

            try:
                cur_interval = float(_nested_get(raw, "schedule.interval_hours") or "0")
            except Exception:
                cur_interval = 0.0
            if cur_interval <= 0:
                _nested_set(raw, "schedule.interval_hours", 2.4)
                changed = True
            try:
                min_interval = float(_nested_get(raw, "schedule.min_interval_hours") or "0")
            except Exception:
                min_interval = 0.0
            try:
                max_interval = float(_nested_get(raw, "schedule.max_interval_hours") or "0")
            except Exception:
                max_interval = 0.0
            if min_interval <= 0:
                _nested_set(raw, "schedule.min_interval_hours", 1.0)
                changed = True
            if max_interval <= 0:
                _nested_set(raw, "schedule.max_interval_hours", 4.5)
                changed = True

            # Queue defaults are rolling 5-day buffer values and only migrated when missing/invalid.
            try:
                cur_horizon = int(_nested_get(raw, "publish.queue_horizon_hours") or "0")
            except Exception:
                cur_horizon = 0
            if cur_horizon in {72, 2160}:
                _nested_set(raw, "publish.queue_horizon_hours", 120)
                changed = True
                cur_horizon = 120
            if cur_horizon <= 0:
                _nested_set(raw, "publish.queue_horizon_hours", 120)
                changed = True
            try:
                cur_queue_target = int(_nested_get(raw, "publish.target_queue_size") or "0")
            except Exception:
                cur_queue_target = 0
            try:
                dcap = int(_nested_get(raw, "publish.daily_publish_cap") or "5")
            except Exception:
                dcap = 5
            expected_target = max(5, dcap * 5)
            if cur_queue_target in {18, 180}:
                _nested_set(raw, "publish.target_queue_size", expected_target)
                changed = True
                cur_queue_target = expected_target
            if cur_queue_target <= 0:
                _nested_set(raw, "publish.target_queue_size", expected_target)
                changed = True

            legacy_qa_retry = (_nested_get(raw, "quality.qa_retry_max_passes") or "").strip()
            if legacy_qa_retry == "24":
                _nested_set(raw, "quality.qa_retry_max_passes", 0)
                changed = True

            legacy_indexing = (_nested_get(raw, "indexing.enabled") or "").strip().lower()
            if legacy_indexing in {"", "false", "0", "no", "off"}:
                _nested_set(raw, "indexing.enabled", True)
                changed = True

            try:
                cur_indexing_quota = int(_nested_get(raw, "indexing.daily_quota") or "0")
            except Exception:
                cur_indexing_quota = 0
            if cur_indexing_quota <= 0:
                _nested_set(raw, "indexing.daily_quota", 200)
                changed = True

            _nested_set(raw, "runtime.policy_migration_version", migration_target)
            changed = True

            if changed:
                _save_yaml(SETTINGS_PATH, raw)
                self.settings = load_settings(SETTINGS_PATH)
        except Exception:
            pass

    def _notify_failure(self, message: str) -> None:
        if platform.system().lower() != "windows":
            return
        try:
            ctypes.windll.user32.MessageBoxW(0, message, "RezeroAgent 오류", 0x10)
        except Exception:
            pass

    def _compute_quota_wait_minutes(self, message: str) -> int | None:
        msg = (message or "").lower()
        marker = re.search(r"\[temp_429_retry_min=(\d+)\]", msg)
        if marker:
            try:
                return max(20, min(30, int(marker.group(1))))
            except Exception:
                return 25
        if "[daily_quota_exceeded]" in msg:
            # Daily exhaustion should end current attempt; let normal scheduler take over.
            return None
        has_429 = "429" in msg
        has_quota = "quota" in msg or "quota exceeded" in msg
        has_retry_hint = "retry in" in msg
        if not (has_429 or has_quota):
            return None
        if not (has_quota or has_retry_hint):
            # Generic rate-limit without quota exhaustion -> use normal backoff path.
            return None
        # Example payload often includes: "Please retry in 37.601568004s."
        retry_sec = None
        m = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", msg)
        if m:
            try:
                retry_sec = float(m.group(1))
            except Exception:
                retry_sec = None
        base_minutes = math.ceil((retry_sec or 0.0) / 60.0)
        # Temporary 429 policy: retry in 20~30 minutes.
        return max(20, min(30, base_minutes + 20))


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _save_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _nested_get(data: dict, dotted: str) -> str:
    cur = data
    parts = dotted.split(".")
    for p in parts[:-1]:
        cur = cur.get(p, {}) if isinstance(cur, dict) else {}
    val = cur.get(parts[-1], "") if isinstance(cur, dict) else ""
    return str(val)


def _nested_set(data: dict, dotted: str, value) -> None:
    cur = data
    parts = dotted.split(".")
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def _is_valid_gemini_key(value: str) -> bool:
    # Typical Google API key pattern.
    return bool(re.fullmatch(r"AIza[0-9A-Za-z_-]{20,}", value.strip()))


def _is_valid_pollinations_key(value: str) -> bool:
    # Pollinations secret key pattern (example: sk_xxx...)
    return bool(re.fullmatch(r"sk_[0-9A-Za-z_-]{16,}", value.strip()))


def _is_valid_blogger_blog_id(value: str) -> bool:
    # Blogger blogId is a numeric identifier.
    return bool(re.fullmatch(r"\d{8,30}", value.strip()))


def _validate_blogger_token_file(path_value: str) -> tuple[bool, str]:
    p = Path(path_value)
    if not p.is_absolute():
        p = ROOT / p
    if not p.exists():
        return False, "blogger_token.json 파일을 찾을 수 없습니다."
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return False, "blogger_token.json 파일 형식이 올바른 JSON이 아닙니다."
    required = ["client_id", "client_secret", "refresh_token", "token_uri"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return False, f"blogger_token.json 필수 항목 누락: {', '.join(missing)}"
    return True, ""


def run_cli(force_once: bool) -> None:
    print(f"Running version: {resolve_running_version()}")
    controller = AgentController()
    if force_once:
        controller.force_run = True
    controller.start()

    print("RezeroAgent CLI 모드가 시작되었습니다. 종료하려면 Ctrl+C를 누르세요.")
    try:
        while True:
            print(controller.status_text())
            time.sleep(30)
    except KeyboardInterrupt:
        controller.stop()


def run_qt(force_once: bool, setup_only: bool) -> int:
    print(f"Running version: {resolve_running_version()}")
    mutex_handle = None
    if platform.system().lower() == "windows":
        try:
            ERROR_ALREADY_EXISTS = 183
            mutex_name = "Global\\RezeroAgentStudioSingleton"
            mutex_handle = ctypes.windll.kernel32.CreateMutexW(None, False, mutex_name)
            if mutex_handle and ctypes.windll.kernel32.GetLastError() == ERROR_ALREADY_EXISTS:
                ctypes.windll.user32.MessageBoxW(
                    0,
                    "RezeroAgent가 이미 실행 중입니다.\n기존 창을 사용해 주세요.",
                    "RezeroAgent",
                    0x40,
                )
                return 0
        except Exception:
            mutex_handle = None

    try:
        from PySide6.QtWidgets import QApplication, QMessageBox
    except Exception as exc:
        print(f"PySide6 import failed: {exc}")
        print("Install with: pip install PySide6")
        return 1

    from ui.dialogs.settings_dialog import SettingsDialog, SettingsDialogContext
    from ui.theme.theme_manager import ThemeManager
    from ui.views.dashboard_view import MainWindow

    app = QApplication(sys.argv)
    theme_manager = ThemeManager(app=app, runtime_root=ROOT)
    theme_manager.apply()

    settings_context = SettingsDialogContext(
        root=ROOT,
        settings_path=SETTINGS_PATH,
        client_secrets_path=CLIENT_SECRETS_PATH,
        blogger_token_path=BLOGGER_TOKEN_PATH,
        gemini_usage_dashboard=GEMINI_USAGE_DASHBOARD,
        gemini_rate_limit_doc=GEMINI_RATE_LIMIT_DOC,
        gemini_pricing_doc=GEMINI_PRICING_DOC,
        gemini_quota_console=GEMINI_QUOTA_CONSOLE,
    )

    qa_boot = QALogger(ROOT / "storage" / "logs" / "qa_runtime.jsonl")
    qa_boot.write(
        "runtime",
        "running_version",
        {
            "version": resolve_running_version(),
            "frozen": bool(getattr(sys, "frozen", False)),
            "executable": str(getattr(sys, "executable", "")),
        },
    )

    if setup_only:
        dlg = SettingsDialog(context=settings_context, on_saved=None, required_only=False)
        return 0 if dlg.exec() else 1

    if has_missing_required(SETTINGS_PATH):
        dlg = SettingsDialog(context=settings_context, on_saved=None, required_only=True)
        if not dlg.exec() or has_missing_required(SETTINGS_PATH):
            return 1

    preflight_errors = validate_runtime_settings(ROOT, load_settings(SETTINGS_PATH))
    if preflight_errors:
        qa_boot.write(
            "update_regression",
            "preflight_failed",
            {"errors": preflight_errors[:8]},
        )
        box = QMessageBox()
        box.setIcon(QMessageBox.Warning)
        box.setWindowTitle("설정 점검 필요")
        box.setText("실행 전 설정 오류가 있습니다.")
        box.setInformativeText("\n".join(f"- {e}" for e in preflight_errors[:8]))
        box.exec()
        return 1
    qa_boot.write("update_regression", "preflight_passed", {"status": "ok"})

    controller = AgentController()
    if force_once:
        controller.force_run = True
    controller.theme_mode = theme_manager.mode

    w = MainWindow(
        controller=controller,
        root=ROOT,
        settings_context=settings_context,
        theme_manager=theme_manager,
        gemini_usage_dashboard=GEMINI_USAGE_DASHBOARD,
    )
    w.show()
    code = app.exec()
    if mutex_handle:
        try:
            ctypes.windll.kernel32.CloseHandle(mutex_handle)
        except Exception:
            pass
    return code


def main() -> None:
    initialize_runtime_home()
    parser = argparse.ArgumentParser(description="RezeroAgent 윈도우 실행기")
    parser.add_argument("--setup", action="store_true", help="설정 UI 실행")
    parser.add_argument("--once", action="store_true", help="시작 시 즉시 1회 실행")
    parser.add_argument("--cli", action="store_true", help="터미널 모드 실행")
    args = parser.parse_args()

    if args.cli:
        run_cli(force_once=args.once)
        return

    raise SystemExit(run_qt(force_once=args.once, setup_only=args.setup))


if __name__ == "__main__":
    main()

