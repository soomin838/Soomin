from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def _stage_scripts(scripts_dir: Path, stage_no: int) -> list[Path]:
    pattern = re.compile(rf"^validate_stage{stage_no}(?:_.+)?\.py$", flags=re.IGNORECASE)
    matches = [p for p in scripts_dir.glob("validate_stage*.py") if pattern.fullmatch(p.name)]
    if not matches:
        return []
    exact = [p for p in matches if p.name.lower() == f"validate_stage{stage_no}.py"]
    extra = [p for p in matches if p not in exact]
    return sorted(exact) + sorted(extra)


def main() -> int:
    scripts_dir = ROOT / "scripts"
    found: list[Path] = []
    for stage_no in range(1, 23):
        found.extend(_stage_scripts(scripts_dir, stage_no))
    stable_script = scripts_dir / "validate_stable_clickbait_sanitizer.py"
    if stable_script.exists():
        found.append(stable_script)
    if not found:
        print("No validate scripts found.")
        return 1

    print("STABILITY SUITE START")
    for path in found:
        cmd = [sys.executable, str(path)]
        print(f"[RUN ] {path.name}")
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )
        if proc.stdout:
            print(proc.stdout.rstrip())
        if proc.returncode != 0:
            if proc.stderr:
                print(proc.stderr.rstrip())
            print(f"[FAIL] {path.name} (exit={proc.returncode})")
            return proc.returncode
        print(f"[PASS] {path.name}")

    print("ALL PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
