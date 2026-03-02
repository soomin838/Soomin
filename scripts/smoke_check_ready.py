from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.settings import load_settings  # noqa: E402


def main() -> int:
    fails: list[str] = []
    warns: list[str] = []
    checks: list[str] = []

    cfg_path = ROOT / "config" / "settings.yaml"
    if not cfg_path.exists():
        fails.append("missing config/settings.yaml")
    else:
        checks.append("config/settings.yaml exists")

    settings = load_settings(cfg_path)

    backend = str(getattr(settings.publish, "image_hosting_backend", "") or "").strip().lower()
    if backend != "r2":
        fails.append(f"publish.image_hosting_backend is '{backend}' (expected 'r2')")
    else:
        checks.append("publish.image_hosting_backend == r2")

    r2 = getattr(settings.publish, "r2", None)
    required_r2 = {
        "public_base_url": str(getattr(r2, "public_base_url", "") or "").strip(),
        "bucket": str(getattr(r2, "bucket", "") or "").strip(),
        "access_key_id": str(getattr(r2, "access_key_id", "") or "").strip(),
        "secret_access_key": str(getattr(r2, "secret_access_key", "") or "").strip(),
    }
    for key, value in required_r2.items():
        if not value:
            fails.append(f"publish.r2.{key} is empty")
    if all(required_r2.values()):
        checks.append("publish.r2 required fields are non-empty")

    visual_enable = bool(getattr(settings.visual, "enable_gemini_image_generation", False))
    if visual_enable:
        fails.append("visual.enable_gemini_image_generation must be false")
    else:
        checks.append("visual.enable_gemini_image_generation == false")

    canonical_host = str(getattr(settings.internal_links, "canonical_internal_host", "") or "").strip()
    if not canonical_host:
        warns.append("internal_links.canonical_internal_host is empty (fallback host inference will be used)")
    else:
        checks.append("internal_links.canonical_internal_host set")

    for rel in ("storage/logs", "storage/state", "storage/reports"):
        path = ROOT / rel
        try:
            path.mkdir(parents=True, exist_ok=True)
            checks.append(f"{rel} directory writable")
        except Exception as exc:
            fails.append(f"{rel} directory not writable: {exc}")

    print("SMOKE CHECK READY")
    for item in checks:
        print(f"PASS: {item}")
    for item in warns:
        print(f"WARN: {item}")
    for item in fails:
        print(f"FAIL: {item}")

    if fails:
        print("RESULT: FAIL")
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
