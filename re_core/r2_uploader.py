from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import mimetypes
import os
import tempfile

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


def _is_dry_run() -> bool:
    return str(os.getenv("R2_DRY_RUN", "") or "").strip().lower() in {"1", "true", "yes", "on"}


def _cache_disabled() -> bool:
    return str(os.getenv("R2_CACHE_DISABLED", "") or "").strip().lower() in {"1", "true", "yes", "on"}


def _dryrun_url(cfg: R2Config, category: str, filename_hint: str) -> str:
    prefix = (cfg.prefix or "library").strip("/")
    cat = re_sub_nonword(str(category or "generic").strip().lower()) or "generic"
    name = re_sub_nonword(str(filename_hint or "image").strip().lower()) or "image"
    token = hashlib.sha1(f"{cat}|{name}".encode("utf-8", errors="ignore")).hexdigest()[:12]
    object_key = f"{prefix}/dryrun/{cat}/{name}_{token}.png"
    return cfg.public_base_url.rstrip("/") + "/" + object_key


def re_sub_nonword(value: str) -> str:
    out = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in str(value or ""))
    out = out.strip("_")
    while "__" in out:
        out = out.replace("__", "_")
    return out[:60]


def upload_file(root: Path, cfg: R2Config, file_path: Path, category: str) -> str:
    file_path = Path(file_path).resolve()
    if _is_dry_run():
        return _dryrun_url(cfg, category=category, filename_hint=file_path.stem)

    key_fp = _fingerprint(file_path)
    use_cache = not _cache_disabled()
    cache: dict = {}
    if use_cache:
        cache = _load_cache(root)
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
    if use_cache:
        cache[key_fp] = public_url
        _save_cache(root, cache)
    return public_url


def upload_bytes(
    root: Path,
    cfg: R2Config,
    content: bytes,
    filename_hint: str,
    category: str,
    content_type: str,
) -> str:
    if _is_dry_run():
        return _dryrun_url(cfg, category=category, filename_hint=filename_hint)

    ct = str(content_type or "").strip().lower()
    ext = ".bin"
    if "png" in ct:
        ext = ".png"
    elif "jpeg" in ct or "jpg" in ct:
        ext = ".jpg"
    elif "webp" in ct:
        ext = ".webp"
    elif "gif" in ct:
        ext = ".gif"

    safe_hint = re_sub_nonword(filename_hint) or "image"
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix=f"rz_{safe_hint[:24]}_") as tmp:
            tmp.write(content or b"")
            tmp.flush()
            tmp_path = Path(tmp.name).resolve()
        return upload_file(
            root=root,
            cfg=cfg,
            file_path=tmp_path,
            category=category,
        )
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

