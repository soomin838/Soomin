from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject

from rezero_v2.stores.publish_store import PublishStore
from rezero_v2.stores.run_store import RunStore


class V2PublishController(QObject):
    def __init__(self, root: Path, settings_path: Path) -> None:
        super().__init__()
        self.root = Path(root).resolve()
        self.db_path = self.root / "storage" / "v2" / "rezero_v2.sqlite3"
        self.publish_store = PublishStore(self.db_path)
        self.run_store = RunStore(self.db_path)

    def load_recent_posts(self, limit: int = 20) -> list[dict]:
        return self.publish_store.list_recent_posts(limit=limit)

    def load_recent_runs(self, limit: int = 20) -> list[dict]:
        return self.run_store.list_recent_runs(limit=limit)
