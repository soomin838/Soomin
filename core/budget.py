from __future__ import annotations

from dataclasses import dataclass

from .logstore import LogStore
from .settings import BudgetSettings


@dataclass
class BudgetCheck:
    ok: bool
    reason: str


class BudgetGuard:
    def __init__(self, settings: BudgetSettings, logs: LogStore) -> None:
        self.settings = settings
        self.logs = logs

    def can_run(
        self,
        today_posts: int | None = None,
        enforce_post_limit: bool = True,
    ) -> BudgetCheck:
        if not self.settings.free_mode:
            return BudgetCheck(True, "free mode disabled")

        posts = int(today_posts) if today_posts is not None else self.logs.get_today_success_posts()
        calls = self.logs.get_today_gemini_count()

        if enforce_post_limit and posts >= self.settings.daily_post_limit:
            return BudgetCheck(False, f"daily post limit reached ({posts})")
        if calls >= self.settings.daily_gemini_call_limit:
            return BudgetCheck(False, f"daily gemini call limit reached ({calls})")
        return BudgetCheck(True, "ok")
