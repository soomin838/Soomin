from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _parse_utc(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_reason_codes(value: Any) -> list[str]:
    if isinstance(value, list):
        tokens = [str(x).strip().lower() for x in value if str(x).strip()]
    else:
        tokens = [
            re.sub(r"[^a-z0-9:_\-\.=]+", "", x.strip().lower())
            for x in re.split(r"[\s\|,;]+", str(value or ""))
            if str(x).strip()
        ]
    out: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        clean = str(tok or "").strip("._-")
        if not clean:
            continue
        if clean in seen:
            continue
        seen.add(clean)
        out.append(clean[:80])
    return out


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "y"}


def read_run_metrics(path: Path, days: int) -> list[dict]:
    p = Path(path).resolve()
    if not p.exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(days or 7)))
    out: list[dict[str, Any]] = []
    try:
        with p.open("r", encoding="utf-8") as fh:
            for line in fh:
                row = str(line or "").strip()
                if not row:
                    continue
                try:
                    parsed = json.loads(row)
                except Exception:
                    continue
                if not isinstance(parsed, dict):
                    continue
                ts = _parse_utc(str(parsed.get("ts_utc", "") or ""))
                if ts is None:
                    continue
                if ts < cutoff:
                    continue
                reason_codes = _normalize_reason_codes(parsed.get("reason_codes", []))
                out.append(
                    {
                        "ts_utc": ts.isoformat(),
                        "run_id": str(parsed.get("run_id", "") or "").strip(),
                        "status": str(parsed.get("status", "") or "").strip().lower(),
                        "reason_codes": reason_codes,
                        "topic_cluster": str(parsed.get("topic_cluster", "") or "default").strip().lower() or "default",
                        "focus_keywords": [str(x).strip().lower() for x in (parsed.get("focus_keywords", []) or []) if str(x).strip()][:6],
                        "seo_slug": str(parsed.get("seo_slug", "") or "").strip(),
                        "title": str(parsed.get("title", "") or "").strip(),
                        "published_url": str(parsed.get("published_url", "") or "").strip(),
                        "publish_at_utc": str(parsed.get("publish_at_utc", "") or "").strip(),
                        "images_count": _to_int(parsed.get("images_count", 0), 0),
                        "internal_links_count": _to_int(parsed.get("internal_links_count", 0), 0),
                        "related_links_count": _to_int(parsed.get("related_links_count", 0), 0),
                        "ctr_risk_low_visual_density": _to_bool(parsed.get("ctr_risk_low_visual_density", False)),
                        "entropy_ok": _to_bool(parsed.get("entropy_ok", True)),
                    }
                )
    except Exception:
        return []
    out.sort(key=lambda x: str(x.get("ts_utc", "")))
    return out


def summarize(metrics: list[dict]) -> dict:
    rows = list(metrics or [])
    total = len(rows)
    status_counter: Counter[str] = Counter()
    reason_counter: Counter[str] = Counter()
    topic_total: defaultdict[str, int] = defaultdict(int)
    topic_success: defaultdict[str, int] = defaultdict(int)
    sum_images = 0
    sum_internal = 0
    sum_related = 0
    ctr_risk_count = 0
    entropy_ok_count = 0

    for row in rows:
        status = str((row or {}).get("status", "") or "").strip().lower()
        if status not in {"success", "skipped", "hold", "failed"}:
            status = "failed"
        status_counter[status] += 1
        topic = str((row or {}).get("topic_cluster", "") or "default").strip().lower() or "default"
        topic_total[topic] += 1
        if status == "success":
            topic_success[topic] += 1
        for code in _normalize_reason_codes((row or {}).get("reason_codes", [])):
            reason_counter[code] += 1
        sum_images += _to_int((row or {}).get("images_count", 0), 0)
        sum_internal += _to_int((row or {}).get("internal_links_count", 0), 0)
        sum_related += _to_int((row or {}).get("related_links_count", 0), 0)
        if _to_bool((row or {}).get("ctr_risk_low_visual_density", False)):
            ctr_risk_count += 1
        if _to_bool((row or {}).get("entropy_ok", True)):
            entropy_ok_count += 1

    topic_breakdown: list[dict[str, Any]] = []
    for topic, t_total in topic_total.items():
        t_success = int(topic_success.get(topic, 0))
        rate = float(t_success / t_total) if t_total else 0.0
        topic_breakdown.append(
            {
                "topic_cluster": topic,
                "total": int(t_total),
                "success": int(t_success),
                "success_rate": rate,
            }
        )
    topic_breakdown.sort(key=lambda x: (-int(x.get("total", 0)), str(x.get("topic_cluster", ""))))

    top_reason_codes = [
        {"reason_code": code, "count": int(count)}
        for code, count in reason_counter.most_common(10)
    ]

    first_ts = str(rows[0].get("ts_utc", "")) if rows else ""
    last_ts = str(rows[-1].get("ts_utc", "")) if rows else ""
    avg_images = float(sum_images / total) if total else 0.0
    avg_internal = float(sum_internal / total) if total else 0.0
    avg_related = float(sum_related / total) if total else 0.0

    return {
        "total_runs": int(total),
        "success_count": int(status_counter.get("success", 0)),
        "skipped_count": int(status_counter.get("skipped", 0)),
        "hold_count": int(status_counter.get("hold", 0)),
        "failed_count": int(status_counter.get("failed", 0)),
        "top_reason_codes": top_reason_codes,
        "topic_breakdown": topic_breakdown,
        "avg_images_count": avg_images,
        "avg_internal_links_count": avg_internal,
        "avg_related_links_count": avg_related,
        "ctr_risk_ratio": float(ctr_risk_count / total) if total else 0.0,
        "entropy_ok_ratio": float(entropy_ok_count / total) if total else 0.0,
        "period_start_utc": first_ts,
        "period_end_utc": last_ts,
        "reason_counter": {k: int(v) for k, v in reason_counter.items()},
    }


def _build_recommendations(summary: dict) -> list[str]:
    total = int(summary.get("total_runs", 0) or 0)
    reasons = dict(summary.get("reason_counter", {}) or {})
    recs: list[str] = []

    ctr_ratio = float(summary.get("ctr_risk_ratio", 0.0) or 0.0)
    if ctr_ratio >= 0.30:
        recs.append("`ctr_risk_low_visual_density` 비율이 높습니다. `visual.target_images_per_post` 상향을 검토하세요.")

    entropy_fail_count = sum(int(v) for k, v in reasons.items() if str(k).startswith("entropy_fail"))
    if entropy_fail_count >= max(2, int(total * 0.15)):
        recs.append("`entropy_fail` 빈도가 높습니다. Stage-9 임계값 또는 문장 다양성 후처리 강화를 검토하세요.")

    internal_links_failed_count = int(reasons.get("internal_links_failed", 0))
    if internal_links_failed_count > 0:
        recs.append("`internal_links_failed`가 감지되었습니다. `canonical_internal_host`와 내부 링크 풀 파일을 점검하세요.")

    r2_fail_count = sum(
        int(v)
        for k, v in reasons.items()
        if ("r2_upload" in str(k)) or str(k).startswith("r2_") or ("missing_r2_config" in str(k))
    )
    if r2_fail_count > 0:
        recs.append("R2 관련 실패 토큰이 있습니다. `publish.r2` 설정값과 권한(키/버킷/public URL)을 점검하세요.")

    if not recs:
        recs.append("뚜렷한 경고 패턴이 없습니다. 현재 운영 정책을 유지하고 추세만 모니터링하세요.")
    return recs


def render_markdown(summary: dict, metrics: list[dict]) -> str:
    s = dict(summary or {})
    top_reasons = list(s.get("top_reason_codes", []) or [])
    topics = list(s.get("topic_breakdown", []) or [])
    recs = _build_recommendations(s)

    lines: list[str] = []
    lines.append("# Weekly Operations Report")
    lines.append("")
    lines.append(f"- Period start (UTC): `{str(s.get('period_start_utc', '') or '-')}`")
    lines.append(f"- Period end (UTC): `{str(s.get('period_end_utc', '') or '-')}`")
    lines.append(f"- Total runs: `{int(s.get('total_runs', 0) or 0)}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| success | {int(s.get('success_count', 0) or 0)} |")
    lines.append(f"| skipped | {int(s.get('skipped_count', 0) or 0)} |")
    lines.append(f"| hold | {int(s.get('hold_count', 0) or 0)} |")
    lines.append(f"| failed | {int(s.get('failed_count', 0) or 0)} |")
    lines.append(f"| avg images_count | {float(s.get('avg_images_count', 0.0) or 0.0):.2f} |")
    lines.append(f"| avg internal_links_count | {float(s.get('avg_internal_links_count', 0.0) or 0.0):.2f} |")
    lines.append(f"| avg related_links_count | {float(s.get('avg_related_links_count', 0.0) or 0.0):.2f} |")
    lines.append(f"| ctr_risk_low_visual_density ratio | {float(s.get('ctr_risk_ratio', 0.0) or 0.0) * 100:.1f}% |")
    lines.append(f"| entropy_ok ratio | {float(s.get('entropy_ok_ratio', 0.0) or 0.0) * 100:.1f}% |")
    lines.append("")

    lines.append("## Top reason codes")
    lines.append("")
    lines.append("| reason_code | count |")
    lines.append("|---|---:|")
    if top_reasons:
        for row in top_reasons:
            lines.append(f"| `{str((row or {}).get('reason_code', '') or '')}` | {int((row or {}).get('count', 0) or 0)} |")
    else:
        lines.append("| `-` | 0 |")
    lines.append("")

    lines.append("## Topic breakdown")
    lines.append("")
    lines.append("| topic_cluster | total | success | success_rate |")
    lines.append("|---|---:|---:|---:|")
    if topics:
        for row in topics:
            rate = float((row or {}).get("success_rate", 0.0) or 0.0) * 100.0
            lines.append(
                f"| `{str((row or {}).get('topic_cluster', '') or 'default')}` | "
                f"{int((row or {}).get('total', 0) or 0)} | "
                f"{int((row or {}).get('success', 0) or 0)} | "
                f"{rate:.1f}% |"
            )
    else:
        lines.append("| `default` | 0 | 0 | 0.0% |")
    lines.append("")

    lines.append("## Recommendations")
    lines.append("")
    for rec in recs:
        lines.append(f"- {rec}")
    lines.append("")
    lines.append(f"_Rows analyzed: {len(metrics or [])}_")
    lines.append("")
    return "\n".join(lines)


def write_csv(metrics: list[dict], path: Path) -> None:
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ts_utc",
        "run_id",
        "status",
        "topic_cluster",
        "seo_slug",
        "title",
        "images_count",
        "internal_links_count",
        "related_links_count",
        "ctr_risk_low_visual_density",
        "entropy_ok",
        "reason_codes",
    ]
    with p.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in (metrics or []):
            writer.writerow(
                {
                    "ts_utc": str((row or {}).get("ts_utc", "") or ""),
                    "run_id": str((row or {}).get("run_id", "") or ""),
                    "status": str((row or {}).get("status", "") or ""),
                    "topic_cluster": str((row or {}).get("topic_cluster", "") or "default"),
                    "seo_slug": str((row or {}).get("seo_slug", "") or ""),
                    "title": str((row or {}).get("title", "") or ""),
                    "images_count": _to_int((row or {}).get("images_count", 0), 0),
                    "internal_links_count": _to_int((row or {}).get("internal_links_count", 0), 0),
                    "related_links_count": _to_int((row or {}).get("related_links_count", 0), 0),
                    "ctr_risk_low_visual_density": bool((row or {}).get("ctr_risk_low_visual_density", False)),
                    "entropy_ok": bool((row or {}).get("entropy_ok", True)),
                    "reason_codes": "|".join(_normalize_reason_codes((row or {}).get("reason_codes", []))),
                }
            )
