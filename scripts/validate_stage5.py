from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from re_core.watchdog import Watchdog, utc_now  # noqa: E402


def _state_path() -> Path:
    return ROOT / "storage" / "state" / "_validate_stage5_watchdog.json"


def _settings() -> dict:
    return {
        "max_same_hard_failure_streak": 3,
        "max_event_wallclock_minutes": 20,
        "max_event_total_attempts": 6,
        "max_global_holds_per_hour": 12,
        "max_provider_530_streak": 6,
        "max_provider_429_streak": 4,
        "backoff_on_provider_failure_minutes": {
            "http_530": [30, 60, 120],
            "http_429": [5, 15, 30],
        },
        "retry_reset_on_success": True,
    }


def validate_hard_failure_abort(path: Path) -> str:
    wd = Watchdog(state_path=path, enabled=True, settings=_settings())
    event_id = "evt-hard-failure"
    for _ in range(3):
        wd.begin_event(event_id)
        wd.register_hard_failure(event_id, "qa_below_threshold")
    abort, reason = wd.should_abort_event(event_id)
    if not abort:
        raise AssertionError("Case1 failed: hard failure streak did not trigger abort.")
    return reason


def validate_provider_429_backoff(path: Path) -> list[int]:
    wd = Watchdog(state_path=path, enabled=True, settings=_settings())
    event_id = "evt-provider-429"
    out: list[int] = []
    for _ in range(3):
        wd.begin_event(event_id)
        wd.register_provider_failure(event_id, 429)
        val = wd.compute_backoff_minutes(event_id, 429)
        out.append(int(val or 0))
    if out != [5, 15, 30]:
        raise AssertionError(f"Case2 failed: expected [5,15,30], got {out}")
    return out


def validate_provider_530_backoff(path: Path) -> list[int]:
    wd = Watchdog(state_path=path, enabled=True, settings=_settings())
    event_id = "evt-provider-530"
    out: list[int] = []
    for _ in range(3):
        wd.begin_event(event_id)
        wd.register_provider_failure(event_id, 530)
        val = wd.compute_backoff_minutes(event_id, 530)
        out.append(int(val or 0))
    if out != [30, 60, 120]:
        raise AssertionError(f"Case3 failed: expected [30,60,120], got {out}")
    return out


def validate_wallclock_abort(path: Path) -> str:
    wd = Watchdog(state_path=path, enabled=True, settings=_settings())
    event_id = "evt-wallclock"
    wd.begin_event(event_id)
    first_seen = utc_now() - timedelta(minutes=25)
    row = dict((wd._state.get("events", {}) or {}).get(event_id, {}) or {})  # noqa: SLF001
    row["first_seen_utc"] = first_seen.isoformat()
    wd._state.setdefault("events", {})[event_id] = row  # noqa: SLF001
    wd.save()
    abort, reason = wd.should_abort_event(event_id)
    if not abort:
        raise AssertionError("Case4 failed: wallclock timeout did not trigger abort.")
    return reason


def validate_global_hold_limit(path: Path) -> tuple[bool, bool, bool]:
    settings = _settings()
    settings["max_global_holds_per_hour"] = 2
    wd = Watchdog(state_path=path, enabled=True, settings=settings)
    a1, _ = wd.should_hold_global()
    a2, _ = wd.should_hold_global()
    a3, _ = wd.should_hold_global()
    if a1 or a2 or (not a3):
        raise AssertionError(f"Case5 failed: expected [False, False, True], got {[a1, a2, a3]}")
    return a1, a2, a3


def main() -> int:
    path = _state_path()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass

    try:
        hard_reason = validate_hard_failure_abort(path)
        b429 = validate_provider_429_backoff(path)
        b530 = validate_provider_530_backoff(path)
        wall_reason = validate_wallclock_abort(path)
        global_seq = validate_global_hold_limit(path)
        print("Case 1 OK: hard_failure_streak triggers abort")
        print(f"  reason={hard_reason}")
        print("Case 2 OK: provider 429 backoff sequence")
        print(f"  backoff_429={b429}")
        print("Case 3 OK: provider 530 backoff sequence")
        print(f"  backoff_530={b530}")
        print("Case 4 OK: wallclock limit triggers abort")
        print(f"  reason={wall_reason}")
        print("Case 5 OK: global holds/hour limit works")
        print(f"  sequence={list(global_seq)}")
        print("Stage-5 watchdog validation passed.")
        return 0
    finally:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

