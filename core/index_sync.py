from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .asset_store import PostsIndexStore
from .publisher import Publisher


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BloggerIndexSync:
    def __init__(
        self,
        publisher: Publisher,
        posts_index: PostsIndexStore,
        sync_settings: Any,
        root: Path,
    ) -> None:
        self.publisher = publisher
        self.posts_index = posts_index
        self.settings = sync_settings
        self.root = Path(root)
        log_rel = str(getattr(self.settings, "log_path", "storage/logs/sync_blogger.jsonl") or "storage/logs/sync_blogger.jsonl")
        self.log_path = self.root / log_rel
        self.state_path = self.root / "storage" / "state" / "sync_state.json"

    def should_run(self, force: bool = False) -> tuple[bool, str]:
        if force:
            return True, "forced"
        if not bool(getattr(self.settings, "enabled", True)):
            return False, "disabled"
        if not bool(getattr(self.settings, "run_on_startup", True)):
            return False, "startup_disabled"
        interval_h = max(1, int(getattr(self.settings, "interval_hours", 24) or 24))
        state = self._load_state()
        last_raw = str(state.get("last_sync_utc", "") or "").strip()
        if not last_raw:
            return True, "first_run"
        try:
            last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            elapsed_h = (datetime.now(timezone.utc) - last_dt.astimezone(timezone.utc)).total_seconds() / 3600.0
            if elapsed_h >= float(interval_h):
                return True, f"interval_elapsed:{elapsed_h:.2f}h"
            return False, f"interval_guard:{elapsed_h:.2f}h<{interval_h}h"
        except Exception:
            return True, "invalid_state_recover"

    def sync_with_blogger(self, force: bool = False) -> dict[str, Any]:
        run_ok, reason = self.should_run(force=force)
        now_iso = _utc_now_iso()
        report: dict[str, Any] = {
            "ts_utc": now_iso,
            "phase": "sync_with_blogger",
            "reason": reason,
            "counts": {
                "blogger_live": 0,
                "blogger_scheduled": 0,
                "local_before": 0,
                "added": 0,
                "updated": 0,
                "soft_deleted": 0,
                "purged": 0,
                "local_after": 0,
            },
            "examples": {"added": [], "updated": [], "soft_deleted": []},
            "status": "skipped" if not run_ok else "ok",
        }
        if not run_ok:
            self._write_report(report)
            return report

        include_statuses = [
            str(x or "").strip().lower()
            for x in (getattr(self.settings, "include_statuses", ["live", "scheduled"]) or [])
            if str(x or "").strip()
        ]
        include_statuses = [s for s in include_statuses if s in {"live", "scheduled"}]
        if not include_statuses:
            include_statuses = ["live", "scheduled"]

        remote_by_status: dict[str, list[dict[str, Any]]] = {}
        try:
            for status in include_statuses:
                rows = self.fetch_blogger_posts(status=status)
                remote_by_status[status] = rows
                report["counts"][f"blogger_{status}"] = len(rows)
            diff = self.apply_diff(
                blogger_live=remote_by_status.get("live", []),
                blogger_scheduled=remote_by_status.get("scheduled", []),
                include_statuses=include_statuses,
                now_iso=now_iso,
            )
            report["counts"].update(diff.get("counts", {}))
            report["examples"].update(diff.get("examples", {}))
            report["status"] = "ok"
            self._save_state(
                {
                    "last_sync_utc": now_iso,
                    "last_counts": report.get("counts", {}),
                }
            )
        except Exception as exc:
            report["status"] = "error"
            report["error"] = str(exc)[:500]

        self._write_report(report)
        return report

    def fetch_blogger_posts(self, status: str) -> list[dict[str, Any]]:
        target_status = str(status or "").strip().lower()
        if target_status not in {"live", "scheduled"}:
            return []
        creds = self.publisher._oauth_credentials()  # noqa: SLF001
        self.publisher._ensure_valid_token(creds)  # noqa: SLF001
        max_pages = max(1, int(getattr(self.settings, "max_pages", 20) or 20))
        max_results = max(1, min(500, int(getattr(self.settings, "max_results_per_status", 500) or 500)))
        rows = self.publisher._list_posts_by_status(  # noqa: SLF001
            creds=creds,
            status=target_status,
            start_utc=None,
            end_utc=None,
            max_pages=max_pages,
            fetch_bodies=False,
            max_results=max_results,
        )
        out: list[dict[str, Any]] = []
        for row in rows:
            norm = self.normalize_blogger_row(row=row, status=target_status)
            if norm:
                out.append(norm)
        return out

    def normalize_blogger_row(self, row: dict[str, Any], status: str) -> dict[str, Any]:
        post_id = str((row or {}).get("id", "") or "").strip()
        if not post_id:
            return {}
        raw_url = str((row or {}).get("url", "") or "").strip()
        url = self.publisher._normalize_public_url(raw_url)  # noqa: SLF001
        strict_url = bool(getattr(self.settings, "strict_url_validation", True))
        if strict_url:
            if not url:
                return {}
            parsed = urlparse(url)
            if str(parsed.scheme or "").lower() not in {"http", "https"}:
                return {}
            if not str(parsed.netloc or "").strip():
                return {}

        title = re.sub(r"\s+", " ", str((row or {}).get("title", "") or "")).strip()
        published_at = str((row or {}).get("published", "") or (row or {}).get("updated", "") or "").strip()
        if not published_at:
            published_at = _utc_now_iso()
        device_type = self._infer_device_type(f"{title}\n{url}")
        cluster_id = self._infer_cluster_id(title)
        return {
            "post_id": post_id,
            "url": url,
            "title": title,
            "published_at": published_at,
            "summary": title,
            "focus_keywords": "",
            "cluster_id": cluster_id,
            "device_type": device_type,
            "word_count": len(re.findall(r"[A-Za-z0-9']+", title)),
            "status": str(status or "live").strip().lower() or "live",
            "deleted_at": None,
            "last_seen_at": _utc_now_iso(),
            "source": "blogger",
        }

    def apply_diff(
        self,
        blogger_live: list[dict[str, Any]],
        blogger_scheduled: list[dict[str, Any]],
        include_statuses: list[str],
        now_iso: str,
    ) -> dict[str, Any]:
        local_rows = self.posts_index.fetch_all()
        local_map = {str(r.get("post_id", "") or "").strip(): r for r in local_rows if str(r.get("post_id", "") or "").strip()}

        remote_map: dict[str, dict[str, Any]] = {}
        for row in blogger_scheduled:
            pid = str(row.get("post_id", "") or "").strip()
            if pid:
                remote_map[pid] = dict(row)
        for row in blogger_live:
            pid = str(row.get("post_id", "") or "").strip()
            if pid:
                remote_map[pid] = dict(row)  # live overrides scheduled if duplicated

        added_ids: list[str] = []
        updated_ids: list[str] = []
        for pid, remote in remote_map.items():
            local = local_map.get(pid)
            if local is None:
                self.posts_index.upsert_post(**remote)
                added_ids.append(pid)
                continue

            changed = False
            for key in ("url", "title", "published_at", "status"):
                if str(local.get(key, "") or "") != str(remote.get(key, "") or ""):
                    changed = True
                    break
            if str(local.get("deleted_at", "") or "").strip():
                changed = True
            if changed:
                merged = dict(remote)
                merged["summary"] = str(local.get("summary", "") or remote.get("summary", "") or "")
                merged["focus_keywords"] = str(local.get("focus_keywords", "") or "")
                merged["cluster_id"] = str(local.get("cluster_id", "") or remote.get("cluster_id", "general") or "general")
                merged["device_type"] = str(local.get("device_type", "") or remote.get("device_type", "windows") or "windows")
                merged["word_count"] = int(local.get("word_count", 0) or remote.get("word_count", 0) or 0)
                merged["deleted_at"] = None
                merged["last_seen_at"] = now_iso
                self.posts_index.upsert_post(**merged)
                updated_ids.append(pid)
            else:
                # keep stable row but refresh heartbeat.
                self.posts_index.upsert_post(
                    post_id=pid,
                    url=str(local.get("url", "") or ""),
                    title=str(local.get("title", "") or ""),
                    published_at=str(local.get("published_at", "") or now_iso),
                    summary=str(local.get("summary", "") or ""),
                    focus_keywords=str(local.get("focus_keywords", "") or ""),
                    cluster_id=str(local.get("cluster_id", "") or "general"),
                    device_type=str(local.get("device_type", "") or "windows"),
                    word_count=int(local.get("word_count", 0) or 0),
                    status=str(local.get("status", "") or "live"),
                    deleted_at=None,
                    last_seen_at=now_iso,
                    source=str(local.get("source", "") or "blogger"),
                )

        active_local_ids = {
            str(r.get("post_id", "") or "").strip()
            for r in local_rows
            if str(r.get("post_id", "") or "").strip()
            and str(r.get("status", "") or "").strip().lower() in set(include_statuses)
            and not str(r.get("deleted_at", "") or "").strip()
        }
        remote_ids = set(remote_map.keys())
        local_only_ids = sorted(active_local_ids - remote_ids)
        soft_deleted = self.posts_index.soft_delete_posts(local_only_ids, deleted_at=now_iso)
        purged = self.posts_index.purge_deleted(int(getattr(self.settings, "purge_deleted_after_days", 7) or 7))

        return {
            "counts": {
                "local_before": len(local_rows),
                "added": len(added_ids),
                "updated": len(updated_ids),
                "soft_deleted": int(soft_deleted),
                "purged": int(purged),
                "local_after": int(self.posts_index.count()),
            },
            "examples": {
                "added": added_ids[:5],
                "updated": updated_ids[:5],
                "soft_deleted": local_only_ids[:5],
            },
        }

    def _load_state(self) -> dict[str, Any]:
        path = self.state_path
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            return {}

    def _save_state(self, payload: dict[str, Any]) -> None:
        path = self.state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_report(self, report: dict[str, Any]) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(report, ensure_ascii=False) + "\n")

    def _infer_device_type(self, text: str) -> str:
        low = str(text or "").lower()
        if any(x in low for x in ("iphone", "ios", "ipad")):
            return "iphone"
        if any(x in low for x in ("galaxy", "android", "samsung")):
            return "galaxy"
        if any(x in low for x in ("mac", "macbook", "macos")):
            return "mac"
        return "windows"

    def _infer_cluster_id(self, text: str) -> str:
        low = str(text or "").lower()
        if any(x in low for x in ("wifi", "network", "internet", "router")):
            return "network"
        if any(x in low for x in ("audio", "sound", "speaker", "microphone", "mic")):
            return "audio"
        if any(x in low for x in ("bluetooth", "pairing")):
            return "connectivity"
        if any(x in low for x in ("update", "upgrade", "patch")):
            return "update"
        return "general"
