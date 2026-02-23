from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    _GOOGLE_CLIENT_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover - optional dependency
    Request = None
    Credentials = None
    build = None
    _GOOGLE_CLIENT_IMPORT_ERROR = str(exc)


@dataclass
class InsightsSettingsView:
    enabled: bool = True
    adsense_enabled: bool = True
    analytics_enabled: bool = True
    search_console_enabled: bool = True
    ga4_property_id: str = ""
    search_console_site_url: str = ""


class GrowthInsights:
    def __init__(self, credentials_path: Path, settings: InsightsSettingsView) -> None:
        self.credentials_path = Path(credentials_path)
        self.settings = settings

    def fetch_snapshot(self) -> dict:
        out = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "adsense_today_usd": 0.0,
            "adsense_7d_usd": 0.0,
            "ga4_active_users_today": 0.0,
            "ga4_pageviews_today": 0.0,
            "sc_clicks_today": 0.0,
            "sc_impressions_today": 0.0,
            "adsense_status": "disabled",
            "analytics_status": "disabled",
            "search_console_status": "disabled",
        }
        if not bool(getattr(self.settings, "enabled", True)):
            return out

        try:
            creds = self._oauth_credentials()
        except Exception as exc:
            err = f"oauth_error: {exc}"
            out["adsense_status"] = err
            out["analytics_status"] = err
            out["search_console_status"] = err
            return out

        if bool(getattr(self.settings, "adsense_enabled", True)):
            try:
                today_val, week_val = self._fetch_adsense(creds)
                out["adsense_today_usd"] = today_val
                out["adsense_7d_usd"] = week_val
                out["adsense_status"] = "ok"
            except Exception as exc:
                out["adsense_status"] = f"error: {exc}"

        if bool(getattr(self.settings, "analytics_enabled", True)):
            try:
                users, views = self._fetch_analytics(creds)
                out["ga4_active_users_today"] = users
                out["ga4_pageviews_today"] = views
                out["analytics_status"] = "ok"
            except Exception as exc:
                out["analytics_status"] = f"error: {exc}"

        if bool(getattr(self.settings, "search_console_enabled", True)):
            try:
                clicks, impressions = self._fetch_search_console(creds)
                out["sc_clicks_today"] = clicks
                out["sc_impressions_today"] = impressions
                out["search_console_status"] = "ok"
            except Exception as exc:
                out["search_console_status"] = f"error: {exc}"
        return out

    def _oauth_credentials(self):
        if Request is None or Credentials is None or build is None:
            raise RuntimeError(f"google_api_client_missing:{_GOOGLE_CLIENT_IMPORT_ERROR}")
        if not self.credentials_path.exists():
            raise RuntimeError(f"token_missing:{self.credentials_path}")
        # Keep token's original scope set to avoid invalid_scope refresh errors.
        creds = Credentials.from_authorized_user_file(str(self.credentials_path))
        if getattr(creds, "expired", False) or not getattr(creds, "token", None):
            try:
                creds.refresh(Request())
            except Exception as exc:
                msg = str(exc or "").lower()
                if "invalid_scope" in msg:
                    raise RuntimeError(
                        "oauth_scope_mismatch: 설정 > Google 로그인에서 토큰을 다시 연결하세요."
                    ) from exc
                raise
        return creds

    def _fetch_adsense(self, creds) -> tuple[float, float]:
        service = build("adsense", "v2", credentials=creds, cache_discovery=False)
        accounts = (service.accounts().list(pageSize=1).execute() or {}).get("accounts", []) or []
        if not accounts:
            raise RuntimeError("no_adsense_account")
        account_name = str(accounts[0].get("name", "")).strip()
        if not account_name:
            raise RuntimeError("adsense_account_name_missing")

        today = (
            service.accounts()
            .reports()
            .generate(
                account=account_name,
                dateRange="TODAY",
                metrics=["ESTIMATED_EARNINGS"],
            )
            .execute()
        )
        week = (
            service.accounts()
            .reports()
            .generate(
                account=account_name,
                dateRange="LAST_7_DAYS",
                metrics=["ESTIMATED_EARNINGS"],
            )
            .execute()
        )
        return self._extract_first_metric(today), self._extract_first_metric(week)

    def _fetch_analytics(self, creds) -> tuple[float, float]:
        property_id = re.sub(r"[^0-9]", "", str(getattr(self.settings, "ga4_property_id", "") or "").strip())
        if not property_id:
            raise RuntimeError("ga4_property_id_missing")
        service = build("analyticsdata", "v1beta", credentials=creds, cache_discovery=False)
        response = (
            service.properties()
            .runReport(
                property=f"properties/{property_id}",
                body={
                    "dateRanges": [{"startDate": "today", "endDate": "today"}],
                    "metrics": [{"name": "activeUsers"}, {"name": "screenPageViews"}],
                },
            )
            .execute()
        )
        rows = response.get("rows", []) or []
        if not rows:
            return 0.0, 0.0
        metric_values = (rows[0] or {}).get("metricValues", []) or []
        users = self._safe_float((metric_values[0] or {}).get("value", "0")) if len(metric_values) > 0 else 0.0
        views = self._safe_float((metric_values[1] or {}).get("value", "0")) if len(metric_values) > 1 else 0.0
        return users, views

    def _fetch_search_console(self, creds) -> tuple[float, float]:
        site_url = str(getattr(self.settings, "search_console_site_url", "") or "").strip()
        if not site_url:
            raise RuntimeError("search_console_site_url_missing")
        service = build("searchconsole", "v1", credentials=creds, cache_discovery=False)
        day = datetime.now(timezone.utc).date().isoformat()
        response = (
            service.searchanalytics()
            .query(
                siteUrl=site_url,
                body={"startDate": day, "endDate": day, "rowLimit": 1},
            )
            .execute()
        )
        rows = response.get("rows", []) or []
        if not rows:
            return 0.0, 0.0
        row = rows[0] or {}
        return self._safe_float(row.get("clicks", 0)), self._safe_float(row.get("impressions", 0))

    def _extract_first_metric(self, payload: dict) -> float:
        # AdSense v2 can return:
        # - rows: [{"cells":[{"value":"0.01"}]}]
        # - totals: {"cells":[{"value":"0.01"}]}
        # - averages: {"cells":[{"value":"0.01"}]}
        # For empty-range responses, rows/totals may be absent, while date fields
        # still include numeric year/month/day. We must never treat those as revenue.
        for key in ("totals", "rows", "averages"):
            node = payload.get(key, None)
            candidates = node if isinstance(node, list) else [node] if isinstance(node, dict) else []
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                for cell_key in ("cells", "metricValues", "values"):
                    vals = item.get(cell_key, [])
                    if not isinstance(vals, list):
                        continue
                    for v in vals:
                        raw = v.get("value") if isinstance(v, dict) else v
                        num = self._safe_float(raw)
                        if num is not None:
                            return num
        return 0.0

    def _safe_float(self, value) -> float:
        try:
            return float(str(value).replace(",", "").strip())
        except Exception:
            return 0.0
