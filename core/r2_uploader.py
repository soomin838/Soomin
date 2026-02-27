from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import mimetypes

import boto3


@dataclass
class R2Config:
    endpoint_url: str
    bucket: str
    access_key_id: str
    secret_access_key: str
    public_base_url: str
    prefix: str = "library"
    cache_control: str = "public, max-age=31536000, immutable"


def _guess_content_type(p: Path) -> str:
    ct, _ = mimetypes.guess_type(str(p))
    return ct or "application/octet-stream"


def _fingerprint(p: Path) -> str:
    st = p.stat()
    return f"{p.resolve()}|{st.st_size}|{int(st.st_mtime)}"


def _cache_path(root: Path) -> Path:
    return (root / "storage" / "state" / "r2_upload_cache.json").resolve()


def _load_cache(root: Path) -> dict:
    cp = _cache_path(root)
    try:
        return json.loads(cp.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(root: Path, data: dict) -> None:
    cp = _cache_path(root)
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def upload_file(root: Path, cfg: R2Config, file_path: Path, category: str) -> str:
    file_path = Path(file_path).resolve()
    cache = _load_cache(root)
    key_fp = _fingerprint(file_path)
    cached = cache.get(key_fp)
    if isinstance(cached, str) and cached.startswith("https://"):
        return cached

    h = hashlib.sha1(key_fp.encode("utf-8")).hexdigest()[:12]
    ext = file_path.suffix.lower() or ".bin"
    prefix = (cfg.prefix or "library").strip("/")
    safe_stem = file_path.stem[:40]
    category = (category or "generic").strip().lower()

    object_key = f"{prefix}/{category}/{safe_stem}_{h}{ext}"

    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        region_name="auto",
    )

    extra = {
        "ContentType": _guess_content_type(file_path),
        "CacheControl": cfg.cache_control,
    }
    s3.upload_file(str(file_path), cfg.bucket, object_key, ExtraArgs=extra)

    public_url = cfg.public_base_url.rstrip("/") + "/" + object_key
    cache[key_fp] = public_url
    _save_cache(root, cache)
    return public_url

