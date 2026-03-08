from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, Signal

from rezero_v2.stores.app_settings_store import AppSettingsStore, V2RuntimeConfig


class V2SettingsController(QObject):
    config_changed = Signal(object)
    save_failed = Signal(str)

    def __init__(self, root: Path, settings_path: Path) -> None:
        super().__init__()
        self.root = Path(root).resolve()
        self.settings_path = Path(settings_path).resolve()
        self.store = AppSettingsStore(self.settings_path)

    def load_config(self) -> V2RuntimeConfig:
        return self.store.load_v2_config()

    def save_config(self, config: V2RuntimeConfig) -> None:
        try:
            self.store.save_v2_config(config)
            self.config_changed.emit(config)
        except Exception as exc:
            self.save_failed.emit(str(exc))
