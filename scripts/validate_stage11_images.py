from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.settings import load_settings  # noqa: E402
from re_core.visual_diagnostics import diagnose_visual_settings  # noqa: E402


def _tail_lines(path: Path, limit: int = 20) -> list[str]:
    try:
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return lines[-max(1, int(limit)) :]
    except Exception:
        return []


def _print_log_probe(path: Path) -> None:
    print(f"log_file: {path}")
    if not path.exists():
        print("  exists: False")
        return
    print("  exists: True")
    tail = _tail_lines(path, limit=20)
    if not tail:
        print("  tail(20): []")
        return
    print("  tail(20):")
    for line in tail:
        print(f"    {line}")


def main() -> int:
    settings_path = ROOT / "config" / "settings.yaml"
    settings = load_settings(settings_path)
    report = diagnose_visual_settings(settings, ROOT)

    print("VISUAL DIAGNOSTICS")
    print(f"can_attempt_generation: {bool(report.get('can_attempt_generation', False))}")
    print(f"blockers: {list(report.get('blockers', []) or [])}")
    print(f"recommend_fix: {list(report.get('recommend_fix', []) or [])}")
    print("snapshot:")
    snapshot_keys = [
        "visual.image_provider",
        "visual.enable_gemini_image_generation",
        "visual.target_images_per_post",
        "visual.max_banner_images",
        "visual.max_inline_images",
        "gemini.api_key_present",
        "publish.min_images_required",
        "publish.max_images_per_post",
        "publish.thumbnail_preflight_only",
        "publish.thumbnail_preflight_max_cycles",
        "images_block_present_in_raw",
        "visual_block_present_in_raw",
        "images_block_runtime_override_suspected",
        "settings_warnings_log_path",
    ]
    for key in snapshot_keys:
        print(f"  {key}: {report.get(key)}")
    print("  settings_warnings_tail:")
    for line in list(report.get("settings_warnings_tail", []) or []):
        print(f"    {line}")

    print("thumbnail_and_visual_logs:")
    log_paths = [
        ROOT / "storage" / "logs" / "thumbnail_gate.jsonl",
        ROOT / "storage" / "logs" / "visual_pipeline.jsonl",
        ROOT / "storage" / "logs" / "workflow_perf.jsonl",
    ]
    for p in log_paths:
        _print_log_probe(p)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

