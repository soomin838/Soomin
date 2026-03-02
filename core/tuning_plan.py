from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .ops_report import read_run_metrics, summarize
from .settings import load_settings


def _nested_set(obj: dict[str, Any], keys: list[str], value: Any) -> None:
    cur: dict[str, Any] = obj
    for key in keys[:-1]:
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = {}
        cur = cur[key]
    cur[keys[-1]] = value


def _risk_rank(level: str) -> int:
    table = {"low": 1, "medium": 2, "high": 3}
    return int(table.get(str(level or "low").strip().lower(), 1))


def _pick_higher_risk(current: str, candidate: str) -> str:
    if _risk_rank(candidate) > _risk_rank(current):
        return str(candidate or "low").strip().lower()
    return str(current or "low").strip().lower()


def _reason_count(summary: dict, prefix: str) -> int:
    counter = dict(summary.get("reason_counter", {}) or {})
    total = 0
    for code, count in counter.items():
        if str(code or "").startswith(str(prefix or "")):
            total += int(count or 0)
    return int(total)


def _r2_fail_count(summary: dict) -> int:
    counter = dict(summary.get("reason_counter", {}) or {})
    total = 0
    for code, count in counter.items():
        token = str(code or "")
        if ("r2_upload" in token) or token.startswith("r2_") or ("missing_r2_config" in token):
            total += int(count or 0)
    return int(total)


def build_tuning_plan(root: Path, days: int = 7) -> dict:
    repo_root = Path(root).resolve()
    window_days = max(1, int(days or 7))
    metrics_path = (repo_root / "storage" / "logs" / "run_metrics.jsonl").resolve()
    settings_path = (repo_root / "config" / "settings.yaml").resolve()

    metrics = read_run_metrics(metrics_path, days=window_days)
    summary = summarize(metrics)
    settings = load_settings(settings_path)

    total_runs = int(summary.get("total_runs", 0) or 0)
    success_count = int(summary.get("success_count", 0) or 0)
    success_rate = float(success_count / total_runs) if total_runs else 0.0
    ctr_risk_ratio = float(summary.get("ctr_risk_ratio", 0.0) or 0.0)
    entropy_fail_count = int(_reason_count(summary, "entropy_fail"))
    internal_links_failed = int(dict(summary.get("reason_counter", {}) or {}).get("internal_links_failed", 0) or 0)
    r2_fail_count = int(_r2_fail_count(summary))

    patch: dict[str, Any] = {}
    why: list[str] = []
    risk = "low"

    # 1) CTR risk
    if ctr_risk_ratio >= 0.30:
        current_target = int(getattr(getattr(settings, "visual", None), "target_images_per_post", 5) or 5)
        current_inline = int(getattr(getattr(settings, "visual", None), "max_inline_images", 4) or 4)
        _nested_set(patch, ["visual", "target_images_per_post"], min(current_target + 1, 7))
        _nested_set(patch, ["visual", "max_inline_images"], min(current_inline + 1, 6))
        why.append("CTR risk 높음 -> 이미지 밀도 상향")
        risk = _pick_higher_risk(risk, "low")

    # 2) entropy fail frequency
    if entropy_fail_count >= max(2, int(total_runs * 0.15)):
        current_ratio = float(getattr(getattr(settings, "entropy_check", None), "trigram_max_ratio", 0.05) or 0.05)
        _nested_set(patch, ["entropy_check", "trigram_max_ratio"], min(current_ratio + 0.01, 0.08))
        why.append("entropy_fail 높음 -> 임계값 소폭 완화")
        risk = _pick_higher_risk(risk, "medium")

    # 3) internal_links_failed detected
    if internal_links_failed > 0:
        current_threshold = float(getattr(getattr(settings, "internal_links", None), "overlap_threshold", 0.4) or 0.4)
        _nested_set(patch, ["internal_links", "overlap_threshold"], max(current_threshold - 0.05, 0.25))
        why.append("internal_links_failed -> 후보 부족 가능성, threshold 완화")
        risk = _pick_higher_risk(risk, "medium")

    # 4) r2 failures
    if r2_fail_count > 0:
        _nested_set(patch, ["publish", "min_images_required"], 0)
        why.append("R2 fail -> 이미지 필수 조건 완화로 발행 중단 방지")
        risk = _pick_higher_risk(risk, "low")

    plan = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "window_days": int(window_days),
        "signals": {
            "ctr_risk_ratio": float(ctr_risk_ratio),
            "entropy_fail_count": int(entropy_fail_count),
            "internal_links_failed": int(internal_links_failed),
            "r2_fail_count": int(r2_fail_count),
            "success_rate": float(success_rate),
        },
        "patch": patch,
        "why": list(why),
        "risk": str(risk or "low"),
    }
    return plan


def render_tuning_plan_md(plan: dict) -> str:
    payload = dict(plan or {})
    signals = dict(payload.get("signals", {}) or {})
    patch = dict(payload.get("patch", {}) or {})
    why = list(payload.get("why", []) or [])
    risk = str(payload.get("risk", "low") or "low")

    lines: list[str] = []
    lines.append("# Tuning Plan")
    lines.append("")
    lines.append(f"- Generated at (UTC): `{str(payload.get('generated_at_utc', '') or '-')}`")
    lines.append(f"- Window days: `{int(payload.get('window_days', 7) or 7)}`")
    lines.append("")
    lines.append("## Signals")
    lines.append("")
    lines.append("| Signal | Value |")
    lines.append("|---|---:|")
    lines.append(f"| ctr_risk_ratio | {float(signals.get('ctr_risk_ratio', 0.0) or 0.0):.3f} |")
    lines.append(f"| entropy_fail_count | {int(signals.get('entropy_fail_count', 0) or 0)} |")
    lines.append(f"| internal_links_failed | {int(signals.get('internal_links_failed', 0) or 0)} |")
    lines.append(f"| r2_fail_count | {int(signals.get('r2_fail_count', 0) or 0)} |")
    lines.append(f"| success_rate | {float(signals.get('success_rate', 0.0) or 0.0) * 100:.1f}% |")
    lines.append("")

    lines.append("## Proposed Patch")
    lines.append("")
    lines.append("```yaml")
    lines.append(yaml.safe_dump({"patch": patch}, sort_keys=False, allow_unicode=True).rstrip())
    lines.append("```")
    lines.append("")

    lines.append("## Why")
    lines.append("")
    if why:
        for item in why:
            lines.append(f"- {str(item)}")
    else:
        lines.append("- No change proposed in this window.")
    lines.append("")

    lines.append("## Risk")
    lines.append("")
    lines.append(f"- `{risk}`")
    lines.append("")
    return "\n".join(lines)
