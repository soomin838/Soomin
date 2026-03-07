from __future__ import annotations

import sys
from pathlib import Path


def bundle_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[2]))
    return Path(__file__).resolve().parents[2]


def runtime_root_from_appdata(app_name: str = "RezeroAgent") -> Path:
    import os

    return Path(os.getenv("APPDATA", str(Path.home()))) / app_name


def resolve_resource(*parts: str) -> Path:
    return bundle_root().joinpath(*parts)


def resolve_qss(name: str) -> Path:
    return resolve_resource("ui", "styles", name)


def resolve_asset(*parts: str) -> Path:
    return resolve_resource("ui", "assets", *parts)


def resolve_theme_qss(mode: str) -> Path:
    clean = str(mode or "dark").strip().lower()
    if clean not in {"light", "dark"}:
        clean = "dark"
    return resolve_resource("ui", "themes", f"{clean}.qss")
