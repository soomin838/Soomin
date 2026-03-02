from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.publish_ledger import PublishLedger, make_ledger_key  # noqa: E402


def _ledger_path() -> Path:
    return ROOT / "storage" / "ledger" / "_validate_stage4.jsonl"


def validate_record_and_exists(path: Path) -> str:
    ledger = PublishLedger(path=path, ttl_days=90)
    key = make_ledger_key(
        event_id="event-001",
        cluster_id="cluster-a",
        facet="impact",
        blog_id="blog-test",
    )
    ok = ledger.record(
        {
            "key": key,
            "event_id": "event-001",
            "cluster_id": "cluster-a",
            "facet": "impact",
            "blog_id": "blog-test",
            "title": "Ledger validation title",
            "source_url": "https://example.com/source",
        }
    )
    if not ok:
        raise AssertionError("Case1 failed: record returned False.")
    if not ledger.exists(key):
        raise AssertionError("Case1 failed: exists(key) should be True after record.")
    return key


def validate_duplicate_block(path: Path, key: str) -> None:
    ledger = PublishLedger(path=path, ttl_days=90)
    if not ledger.exists(key):
        raise AssertionError("Case2 failed: duplicate key should be blocked (exists=True).")


def validate_ttl_expiry(path: Path) -> str:
    old_key = make_ledger_key(
        event_id="event-old",
        cluster_id="cluster-old",
        facet="impact",
        blog_id="blog-test",
    )
    old_row = {
        "key": old_key,
        "event_id": "event-old",
        "cluster_id": "cluster-old",
        "facet": "impact",
        "blog_id": "blog-test",
        "created_at_utc": (datetime.now(timezone.utc) - timedelta(days=91)).isoformat(),
        "title": "Old record",
        "source_url": "https://example.com/old",
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(old_row, ensure_ascii=False) + "\n")
    ledger = PublishLedger(path=path, ttl_days=90)
    if ledger.exists(old_key):
        raise AssertionError("Case3 failed: 91-day old record should be expired.")
    return old_key


def validate_fail_closed_read_error() -> bool:
    blocked_path = ROOT / "storage" / "ledger" / "_validate_stage4_blocked"
    blocked_path.mkdir(parents=True, exist_ok=True)
    ledger = PublishLedger(path=blocked_path, ttl_days=90)
    probe_key = make_ledger_key(
        event_id="event-probe",
        cluster_id="cluster-probe",
        facet="impact",
        blog_id="blog-test",
    )
    result = ledger.exists(probe_key)
    if not result:
        raise AssertionError("Case4 failed: read-error path must fail closed (exists=True).")
    try:
        shutil.rmtree(blocked_path)
    except Exception:
        pass
    return result


def main() -> int:
    path = _ledger_path()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        key = validate_record_and_exists(path)
        validate_duplicate_block(path, key)
        old_key = validate_ttl_expiry(path)
        fail_closed = validate_fail_closed_read_error()
        print("Case 1 OK: record -> exists(key)=True")
        print(f"  key={key}")
        print("Case 2 OK: duplicate key is blocked by exists=True")
        print(f"  duplicate_key={key}")
        print("Case 3 OK: TTL(90d) excludes 91-day old record")
        print(f"  expired_key={old_key}")
        print("Case 4 OK: read error path is fail-closed")
        print(f"  fail_closed={fail_closed}")
        print("Stage-4 publish ledger validation passed.")
        return 0
    finally:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

