from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

from ui.resources.pathing import resolve_qss, resolve_theme_qss


@dataclass
class ThemeState:
    mode: str = "dark"
    animation_intensity: str = "high"


class ThemeManager:
    def __init__(self, app: QApplication, runtime_root: Path) -> None:
        self.app = app
        self.runtime_root = runtime_root
        self.pref_path = runtime_root / "storage" / "ui" / "preferences.json"
        self.pref_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = ThemeState()
        self.load_preferences()

    @property
    def mode(self) -> str:
        return self.state.mode

    def load_preferences(self) -> None:
        if not self.pref_path.exists():
            self.state = ThemeState(mode="dark", animation_intensity="high")
            return
        try:
            payload = json.loads(self.pref_path.read_text(encoding="utf-8"))
            mode = str((payload or {}).get("theme_mode", "dark") or "dark").strip().lower()
            if mode not in {"auto", "light", "dark"}:
                mode = "dark"
            intensity = str((payload or {}).get("animation_intensity", "high") or "high").strip().lower()
            if intensity not in {"high", "medium", "off"}:
                intensity = "high"
            self.state = ThemeState(mode=mode, animation_intensity=intensity)
        except Exception:
            self.state = ThemeState(mode="dark", animation_intensity="high")

    def save_preferences(self) -> None:
        payload = {}
        if self.pref_path.exists():
            try:
                payload = json.loads(self.pref_path.read_text(encoding="utf-8")) or {}
            except Exception:
                payload = {}
        payload["theme_mode"] = self.state.mode
        payload["animation_intensity"] = self.state.animation_intensity
        self.pref_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def detect_os_dark(self) -> bool:
        import platform
        if platform.system() == "Windows":
            try:
                import winreg
                key = winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize"
                )
                value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
                return value == 0
            except Exception:
                pass
        palette: QPalette = self.app.palette()
        base = palette.color(QPalette.ColorRole.Window)
        return self._is_dark_color(base)

    def _is_dark_color(self, color: QColor) -> bool:
        return int(color.lightness()) < 128

    def resolved_mode(self) -> str:
        if self.state.mode == "auto":
            return "dark" if self.detect_os_dark() else "light"
        return self.state.mode

    def set_mode(self, mode: str) -> None:
        clean = str(mode or "dark").strip().lower()
        self.state.mode = clean if clean in {"auto", "light", "dark"} else "dark"
        self.save_preferences()
        self.apply()

    def set_animation_intensity(self, intensity: str) -> None:
        clean = str(intensity or "high").strip().lower()
        if clean not in {"high", "medium", "off"}:
            clean = "high"
        self.state.animation_intensity = clean
        self.save_preferences()

    def animation_intensity(self) -> str:
        return self.state.animation_intensity

    def _load_qss(self) -> str:
        parts: list[str] = []

        # Always load neon_theme.qss as the base layer
        base_qss = resolve_qss("neon_theme.qss")
        if base_qss.exists():
            parts.append(base_qss.read_text(encoding="utf-8"))

        # Overlay mode-specific QSS on top
        mode_qss = resolve_theme_qss(self.resolved_mode())
        if mode_qss.exists():
            parts.append(mode_qss.read_text(encoding="utf-8"))

        if parts:
            return "\n".join(parts)

        # Backward compatibility fallback
        files = [resolve_qss("theme.qss"), resolve_qss("glass.qss")]
        for path in files:
            if path.exists():
                parts.append(path.read_text(encoding="utf-8"))
        return "\n".join(parts)

    def check_auto_theme_update(self) -> bool:
        if self.state.mode != "auto":
            return False
        current_resolved = "dark" if self.detect_os_dark() else "light"
        if getattr(self, "_last_applied_mode", "") != current_resolved:
            self.apply()
            return True
        return False

    def apply(self) -> None:
        self._last_applied_mode = "dark" if self.detect_os_dark() else "light" if self.state.mode == "auto" else self.state.mode
        raw = self._load_qss()
        if not raw:
            return
        self.app.setStyleSheet(raw)

