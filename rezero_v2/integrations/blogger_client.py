from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from rezero_v2.core.domain.publish_result import PublishArtifact


class BloggerClient:
    def __init__(self, *, credentials_path: Path, blog_id: str, dry_run: bool = False) -> None:
        self.credentials_path = Path(credentials_path).resolve()
        self.blog_id = str(blog_id or '').strip()
        self.dry_run = bool(dry_run)

    def publish_post(self, *, title: str, html: str, labels: list[str] | None = None) -> PublishArtifact:
        now = datetime.now(timezone.utc).isoformat()
        if self.dry_run or (not self.blog_id) or (not self.credentials_path.exists()):
            return PublishArtifact(status='scheduled', post_id='dry-run', post_url=f"dry-run://{title[:40]}", published_at_utc=now)
        creds = Credentials.from_authorized_user_file(str(self.credentials_path))
        service = build('blogger', 'v3', credentials=creds, cache_discovery=False)
        response = service.posts().insert(blogId=self.blog_id, body={'title': str(title or ''), 'content': str(html or ''), 'labels': list(labels or [])}, isDraft=False).execute()
        return PublishArtifact(status='published', post_id=str(response.get('id', '') or ''), post_url=str(response.get('url', '') or ''), published_at_utc=str(response.get('published', '') or now))
