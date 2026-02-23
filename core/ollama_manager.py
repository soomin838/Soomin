from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
import webbrowser
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from .settings import LocalLLMSettings


class OllamaManager:
    def __init__(
        self,
        root: Path,
        settings: LocalLLMSettings,
        log_path: Path | None = None,
    ) -> None:
        self.root = root
        self.settings = settings
        self.base_url = str(getattr(settings, "base_url", "http://127.0.0.1:11434") or "http://127.0.0.1:11434").rstrip("/")
        self.log_path = log_path or (root / "storage" / "logs" / "ollama_manager.jsonl")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._exe_path_cache: Path | None = None
        self._owned_server_proc: subprocess.Popen | None = None

    def _log(self, event: str, payload: dict[str, Any] | None = None) -> None:
        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "").strip(),
            "base_url": self.base_url,
            "model": str(getattr(self.settings, "model", "") or ""),
        }
        if payload:
            row.update(payload)
        try:
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            return

    def _default_install_candidates(self) -> list[Path]:
        local_app = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
        return [
            local_app / "Programs" / "Ollama" / "ollama.exe",
            Path(r"C:\Program Files\Ollama\ollama.exe"),
            Path(r"C:\Program Files (x86)\Ollama\ollama.exe"),
        ]

    def find_ollama_exe(self) -> Path | None:
        if self._exe_path_cache and self._exe_path_cache.exists():
            return self._exe_path_cache
        which_hit = shutil.which("ollama")
        if which_hit:
            p = Path(which_hit)
            if p.exists():
                self._exe_path_cache = p
                return p
        for p in self._default_install_candidates():
            if p.exists():
                self._exe_path_cache = p
                return p
        return None

    # Backward-compatible alias.
    def find_ollama_executable(self) -> Path | None:
        return self.find_ollama_exe()

    def is_installed(self) -> bool:
        return self.find_ollama_exe() is not None

    def _server_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["OLLAMA_NUM_PARALLEL"] = str(max(1, int(getattr(self.settings, "num_parallel", 1) or 1)))
        env["OLLAMA_MAX_LOADED_MODELS"] = str(max(1, int(getattr(self.settings, "max_loaded_models", 1) or 1)))
        return env

    def _request_timeout(self) -> int:
        try:
            return max(10, int(getattr(self.settings, "request_timeout_sec", 60) or 60))
        except Exception:
            return 60

    def ping_server(self, timeout: int = 2) -> bool:
        ok, _ = self.ping(timeout=timeout)
        return ok

    def ping(self, timeout: int | None = None) -> tuple[bool, str]:
        req_timeout = timeout if timeout is not None else min(8, self._request_timeout())
        try:
            r = requests.get(f"{self.base_url}/api/version", timeout=req_timeout)
            if r.status_code == 200:
                return True, "ok"
            # compatibility fallback endpoint
            r2 = requests.get(f"{self.base_url}/api/tags", timeout=req_timeout)
            if r2.status_code == 200:
                return True, "ok"
            return False, f"http_{r.status_code}"
        except Exception as exc:
            return False, str(exc)

    def install_if_needed(self) -> tuple[bool, str]:
        if self.is_installed():
            return True, "installed"
        if not bool(getattr(self.settings, "install_if_missing", True)):
            return False, "install_disabled"

        url = "https://ollama.com/download/OllamaSetup.exe"
        installer_path = self.root / "storage" / "temp_images" / "OllamaSetup.exe"
        installer_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._log("ollama_install_download_start", {"url": url, "target": str(installer_path)})
            with requests.get(url, timeout=90, stream=True, allow_redirects=True) as r:
                r.raise_for_status()
                with installer_path.open("wb") as fh:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            fh.write(chunk)
        except Exception as exc:
            self._log("ollama_install_download_failed", {"error": str(exc)})
            return False, f"download_failed:{exc}"

        silent_arg_sets = [
            ["/VERYSILENT", "/SUPPRESSMSGBOXES", "/NORESTART"],
            ["/S"],
            ["/quiet"],
            ["/qn"],
        ]
        for args in silent_arg_sets:
            try:
                self._log("ollama_install_exec", {"args": args})
                proc = subprocess.run(
                    [str(installer_path), *args],
                    cwd=str(installer_path.parent),
                    env=self._server_env(),
                    timeout=420,
                    check=False,
                )
                self._log("ollama_install_exec_done", {"args": args, "returncode": int(proc.returncode)})
                time.sleep(2)
                if self.is_installed():
                    self._log("ollama_install_success", {"mode": "silent", "args": args})
                    return True, "installed"
            except Exception as exc:
                self._log("ollama_install_exec_failed", {"args": args, "error": str(exc)})
                continue

        # Non-silent fallback: launch installer for manual finish and open docs page.
        try:
            subprocess.Popen([str(installer_path)], cwd=str(installer_path.parent), env=self._server_env())
            webbrowser.open("https://ollama.com/download")
            self._log(
                "ollama_install_manual_started",
                {"installer_path": str(installer_path)},
            )
            return False, "manual_install_started"
        except Exception as exc:
            self._log("ollama_install_manual_failed", {"error": str(exc)})
            return False, f"manual_install_failed:{exc}"

    def start_server_hidden(self) -> subprocess.Popen | None:
        exe = self.find_ollama_exe()
        if exe is None:
            self._log("ollama_serve_start_failed", {"error": "ollama_not_installed"})
            return None
        try:
            creationflags = 0
            if os.name == "nt":
                creationflags = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
            proc = subprocess.Popen(
                [str(exe), "serve"],
                cwd=str(exe.parent),
                env=self._server_env(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
            )
            self._owned_server_proc = proc
            self._log("ollama_serve_started", {"exe": str(exe), "pid": int(getattr(proc, "pid", 0) or 0)})
            return proc
        except Exception as exc:
            self._log("ollama_serve_start_failed", {"error": str(exc), "exe": str(exe)})
            return None

    def ensure_server_running(self) -> tuple[bool, str]:
        ok, reason = self.ping()
        if ok:
            return True, "server_ready"
        exe = self.find_ollama_exe()
        if exe is None:
            return False, "ollama_not_installed"
        proc = self.start_server_hidden()
        if proc is None:
            return False, "serve_start_failed"
        deadline = time.time() + 10
        while time.time() < deadline:
            ok, _ = self.ping()
            if ok:
                return True, "server_ready"
            time.sleep(0.5)
        return False, "server_not_ready_timeout"

    def _list_models_text(self) -> tuple[bool, str]:
        exe = self.find_ollama_exe()
        if exe is None:
            return False, "ollama_not_installed"
        try:
            proc = subprocess.run(
                [str(exe), "list"],
                capture_output=True,
                text=True,
                env=self._server_env(),
                timeout=120,
                check=False,
            )
            text = (proc.stdout or "") + "\n" + (proc.stderr or "")
            return proc.returncode == 0, text
        except Exception as exc:
            return False, str(exc)

    def _has_model(self, model: str) -> bool:
        ok, text = self._list_models_text()
        if not ok:
            return False
        lower = text.lower()
        target = str(model or "").strip().lower()
        if not target:
            return False
        return bool(re.search(rf"(^|\s){re.escape(target)}(\s|$)", lower, flags=re.MULTILINE))

    def pull_model_if_needed(self) -> tuple[bool, str]:
        model = str(getattr(self.settings, "model", "qwen2.5:3b") or "qwen2.5:3b").strip()
        if not model:
            return False, "model_empty"
        if self._has_model(model):
            return True, "model_ready"
        if not bool(getattr(self.settings, "pull_model_if_missing", True)):
            return False, "pull_disabled"
        exe = self.find_ollama_exe()
        if exe is None:
            return False, "ollama_not_installed"
        try:
            self._log("ollama_pull_start", {"model": model})
            proc = subprocess.run(
                [str(exe), "pull", model],
                capture_output=True,
                text=True,
                env=self._server_env(),
                timeout=1800,
                check=False,
            )
            self._log(
                "ollama_pull_done",
                {
                    "model": model,
                    "returncode": int(proc.returncode),
                    "stdout": str(proc.stdout or "")[:600],
                    "stderr": str(proc.stderr or "")[:600],
                },
            )
            if proc.returncode != 0:
                return False, f"pull_failed_rc_{proc.returncode}"
            if not self._has_model(model):
                return False, "pull_done_but_model_not_listed"
            return True, "model_ready"
        except Exception as exc:
            self._log("ollama_pull_failed", {"model": model, "error": str(exc)})
            return False, f"pull_failed:{exc}"

    def ensure_model_available(self, model: str) -> bool:
        target = str(model or "").strip() or str(getattr(self.settings, "model", "qwen2.5:3b") or "qwen2.5:3b")
        try:
            if self._has_model(target):
                return True
            if not bool(getattr(self.settings, "pull_model_if_missing", True)):
                return False
            exe = self.find_ollama_exe()
            if exe is None:
                return False
            proc = subprocess.run(
                [str(exe), "pull", target],
                capture_output=True,
                text=True,
                env=self._server_env(),
                timeout=1800,
                check=False,
            )
            self._log(
                "ollama_pull_done",
                {
                    "model": target,
                    "returncode": int(proc.returncode),
                    "stdout": str(proc.stdout or "")[:600],
                    "stderr": str(proc.stderr or "")[:600],
                },
            )
            return proc.returncode == 0 and self._has_model(target)
        except Exception as exc:
            self._log("ollama_pull_failed", {"model": target, "error": str(exc)})
            return False

    def shutdown_server_if_owned(self) -> None:
        proc = self._owned_server_proc
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=4)
                except Exception:
                    proc.kill()
            self._log("ollama_serve_stopped", {"pid": int(getattr(proc, "pid", 0) or 0)})
        except Exception as exc:
            self._log("ollama_serve_stop_failed", {"error": str(exc)})
        finally:
            self._owned_server_proc = None

    def ensure_ready(self) -> tuple[bool, str]:
        installed, install_reason = self.install_if_needed()
        if not installed:
            return False, install_reason
        server_ok, server_reason = self.ensure_server_running()
        if not server_ok:
            return False, server_reason
        model_name = str(getattr(self.settings, "model", "qwen2.5:3b") or "qwen2.5:3b").strip()
        if not self.ensure_model_available(model_name):
            return False, "model_not_ready"
        return True, "ready"

    def status_snapshot(self) -> dict[str, Any]:
        ok_ping, ping_reason = self.ping()
        return {
            "enabled": bool(getattr(self.settings, "enabled", False)),
            "installed": self.is_installed(),
            "server_ok": bool(ok_ping),
            "server_reason": ping_reason,
            "model": str(getattr(self.settings, "model", "") or ""),
            "settings": asdict(self.settings),
        }
