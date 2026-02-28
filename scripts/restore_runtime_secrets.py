from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.secret_backup import restore_runtime_secrets


def main() -> int:
    result = restore_runtime_secrets(ROOT, from_latest=True)
    print(result)
    return 0 if str(result.get("status", "")).startswith("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
