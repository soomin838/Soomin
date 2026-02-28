from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.settings import load_settings
from core.workflow import AgentWorkflow


def main() -> int:
    root = ROOT
    settings = load_settings(root / "config" / "settings.yaml")
    wf = AgentWorkflow(root, settings)

    summary_payload = {
        "short_summary": "Windows 11 Bluetooth keeps disconnecting after an update. "
        "The article provides ordered fixes with expected results and fallback actions.",
        "primary_issue_phrase": "windows 11 bluetooth keeps disconnecting after update",
        "device_family": "windows",
        "feature": "bluetooth",
        "must_include_terms": ["windows 11", "bluetooth", "after update", "fix"],
    }
    recent_titles = [
        "Windows 11 Bluetooth keeps disconnecting after update: 5 safe fixes",
        "How to fix Bluetooth disconnecting on Windows 11 after update",
    ]
    candidates = [
        "Device not working? 3 fixes that actually work",
        "Windows 11 Bluetooth keeps disconnecting after update: 5 safe fixes",
        "Windows 11 Bluetooth not working after update: 5 fixes to try first",
        "Windows 11 Bluetooth error after update: what to try next",
    ]
    best, reason = wf._choose_best_unique_title(  # noqa: SLF001
        candidates=candidates,
        summary_payload=summary_payload,
        recent_titles=recent_titles,
    )
    print(f"best={best}")
    print(f"reason={reason}")
    if not best:
        print("FAIL: no title selected")
        return 1
    lowered = best.lower()
    if "fixes that actually work" in lowered or "device not working" in lowered:
        print("FAIL: banned template selected")
        return 1
    if "windows" not in lowered or "bluetooth" not in lowered:
        print("FAIL: missing device/feature specificity")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
