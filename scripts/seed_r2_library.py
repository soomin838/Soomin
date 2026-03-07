from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.r2_uploader import R2Config, upload_file
from re_core.settings import load_settings


def main() -> int:
    root = ROOT
    settings = load_settings(root / "config" / "settings.yaml")
    r2 = getattr(settings.publish, "r2", None)
    cfg = R2Config(
        endpoint_url=str(getattr(r2, "endpoint_url", "") or "").strip(),
        bucket=str(getattr(r2, "bucket", "") or "").strip(),
        access_key_id=str(getattr(r2, "access_key_id", "") or "").strip(),
        secret_access_key=str(getattr(r2, "secret_access_key", "") or "").strip(),
        public_base_url=str(getattr(r2, "public_base_url", "") or "").strip(),
        prefix=str(getattr(r2, "prefix", "library") or "library").strip() or "library",
        cache_control=str(getattr(r2, "cache_control", "public, max-age=31536000, immutable") or "public, max-age=31536000, immutable").strip(),
    )
    required = [
        ("endpoint_url", cfg.endpoint_url),
        ("bucket", cfg.bucket),
        ("access_key_id", cfg.access_key_id),
        ("secret_access_key", cfg.secret_access_key),
        ("public_base_url", cfg.public_base_url),
    ]
    missing = [k for k, v in required if not str(v).strip()]
    if missing:
        raise RuntimeError(f"r2_missing_config:{','.join(missing)}")

    lib_root = (root / "assets" / "library").resolve()
    items: list[dict] = []
    for category_dir in sorted([p for p in lib_root.iterdir() if p.is_dir()]):
        category = category_dir.name.strip().lower()
        for ext in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
            for path in sorted(category_dir.glob(ext)):
                if not path.is_file():
                    continue
                url = upload_file(root=root, cfg=cfg, file_path=path, category=category)
                rel = str(path.resolve().relative_to(root)).replace("\\", "/")
                items.append(
                    {
                        "local_path": rel,
                        "category": category,
                        "r2_url": str(url),
                        "uploaded_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                print(f"[seed_r2] {rel} -> {url}")

    manifest_path = (root / "storage" / "state" / "r2_library_manifest.json").resolve()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[seed_r2] wrote manifest: {manifest_path} ({len(items)} items)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
