from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.settings import load_settings
from re_core.visual import ImageAsset
from re_core.workflow import AgentWorkflow


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    settings = load_settings(root / "config" / "settings.yaml")
    workflow = AgentWorkflow(root, settings)

    keep_path = root / "assets" / "news_pack_cache" / "smoke_keep.txt"
    temp_path = root / "storage" / "temp_images" / "smoke_delete.txt"
    keep_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    keep_path.write_text("keep", encoding="utf-8")
    temp_path.write_text("delete", encoding="utf-8")

    images = [
        ImageAsset(path=keep_path, alt="", source_kind="news_pack", source_url="", anchor_text="", license_note=""),
        ImageAsset(path=temp_path, alt="", source_kind="temp", source_url="", anchor_text="", license_note=""),
    ]
    removed = workflow._cleanup_local_image_files(images)  # noqa: SLF001

    keep_exists = keep_path.exists()
    temp_exists = temp_path.exists()
    status = keep_exists and (not temp_exists)
    print(
        "news_cache_cleanup_smoke",
        {
            "removed": int(removed),
            "keep_exists": bool(keep_exists),
            "temp_exists": bool(temp_exists),
            "ok": bool(status),
        },
    )

    try:
        if keep_path.exists():
            keep_path.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
    except Exception:
        pass

    return 0 if status else 1


if __name__ == "__main__":
    raise SystemExit(main())
