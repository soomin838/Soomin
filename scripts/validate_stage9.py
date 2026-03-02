from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.content_entropy import check_entropy  # noqa: E402


SETTINGS = {
    "enabled": True,
    "trigram_max_ratio": 0.05,
    "starter_max_repeats": 3,
    "duplicate_h2_max": 0,
    "max_rewrite_attempts": 1,
}


def validate_trigram_fail() -> float:
    repeated = "alpha beta gamma delta epsilon."
    html = "<h2>Quick Take</h2><p>" + " ".join([repeated for _ in range(30)]) + "</p>"
    result = check_entropy(html, SETTINGS)
    ratio = float(result.get("trigram_ratio", 0.0) or 0.0)
    if ratio <= 0.05:
        raise AssertionError(f"Case1 failed: expected trigram_ratio > 0.05, got {ratio:.4f}")
    if bool(result.get("ok", True)):
        raise AssertionError("Case1 failed: high trigram repetition should fail entropy check.")
    return ratio


def validate_starter_repeat_fail() -> tuple[int, str]:
    html = (
        "<h2>What Happened</h2>"
        "<p>"
        "Users should always check release notes before rollout. "
        "Users should always compare policy defaults before rollout. "
        "Users should always verify admin roles before rollout. "
        "Users should always test one pilot update before rollout. "
        "Users should always monitor logs after rollout."
        "</p>"
    )
    result = check_entropy(html, SETTINGS)
    count = int(result.get("max_starter_count", 0) or 0)
    starter = str(result.get("max_starter", "") or "")
    if count < 4:
        raise AssertionError(f"Case2 failed: expected starter repeats >= 4, got {count}")
    if bool(result.get("ok", True)):
        raise AssertionError("Case2 failed: repeated sentence starters should fail entropy check.")
    return count, starter


def validate_duplicate_h2_fail() -> int:
    html = (
        "<h2>What Happened</h2><p>Sentence one with mixed wording.</p>"
        "<h2>What Happened</h2><p>Sentence two with different detail.</p>"
    )
    result = check_entropy(html, SETTINGS)
    dup = int(result.get("duplicate_h2", 0) or 0)
    if dup <= 0:
        raise AssertionError("Case3 failed: duplicate H2 count should be > 0.")
    if bool(result.get("ok", True)):
        raise AssertionError("Case3 failed: duplicate H2 should fail entropy check.")
    return dup


def validate_normal_ok() -> dict:
    html = (
        "<h2>Quick Take</h2>"
        "<p>Windows admins received a security update and reviewed deployment windows carefully.</p>"
        "<h2>What Happened</h2>"
        "<p>Microsoft documented new guidance, while Apple and Google published separate compatibility notes.</p>"
        "<h2>What To Do Now</h2>"
        "<ul><li>Check version coverage.</li><li>Validate pilot users.</li><li>Track rollback signals.</li></ul>"
        "<h2>Sources</h2>"
        "<ul><li><a href=\"https://msrc.microsoft.com\">msrc.microsoft.com</a></li></ul>"
    )
    result = check_entropy(html, SETTINGS)
    if not bool(result.get("ok", False)):
        raise AssertionError(f"Case4 failed: normal article should pass, got reasons={result.get('reasons', [])}")
    return result


def main() -> int:
    ratio = validate_trigram_fail()
    starter_count, starter = validate_starter_repeat_fail()
    dup_h2 = validate_duplicate_h2_fail()
    ok_result = validate_normal_ok()
    print("Case 1 OK: high trigram repetition triggers entropy fail")
    print(f"  trigram_ratio={ratio:.4f}")
    print("Case 2 OK: repeated sentence starter triggers entropy fail")
    print(f"  max_starter_count={starter_count}, starter={starter}")
    print("Case 3 OK: duplicate H2 title triggers entropy fail")
    print(f"  duplicate_h2={dup_h2}")
    print("Case 4 OK: normal article passes entropy check")
    print(
        "  metrics="
        + f"trigram={float(ok_result.get('trigram_ratio', 0.0)):.4f},"
        + f"starter={int(ok_result.get('max_starter_count', 0) or 0)},"
        + f"duplicate_h2={int(ok_result.get('duplicate_h2', 0) or 0)}"
    )
    print("Stage-9 content entropy validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
