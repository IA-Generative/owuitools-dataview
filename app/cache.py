"""Cache fichiers téléchargés sur disque avec TTL et taille max."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

from app.config import settings

CACHE_DIR = Path("/app/cache") if Path("/app/cache").exists() else Path("cache")


def _ensure_cache_dir() -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR


def cache_key(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def _meta_path(key: str) -> Path:
    return _ensure_cache_dir() / f"{key}.meta"


def _data_path(key: str) -> Path:
    return _ensure_cache_dir() / f"{key}.data"


def get(url: str) -> bytes | None:
    key = cache_key(url)
    meta_p = _meta_path(key)
    data_p = _data_path(key)

    if not meta_p.exists() or not data_p.exists():
        return None

    meta = json.loads(meta_p.read_text())
    if time.time() - meta["timestamp"] > settings.CACHE_TTL_SECONDS:
        meta_p.unlink(missing_ok=True)
        data_p.unlink(missing_ok=True)
        return None

    return data_p.read_bytes()


def get_meta(url: str) -> dict | None:
    key = cache_key(url)
    meta_p = _meta_path(key)
    if not meta_p.exists():
        return None
    return json.loads(meta_p.read_text())


def get_last_modified(url: str) -> str | None:
    key = cache_key(url)
    meta_p = _meta_path(key)
    if not meta_p.exists():
        return None
    meta = json.loads(meta_p.read_text())
    return meta.get("last_modified")


def put(url: str, data: bytes, last_modified: str | None = None,
        content_type: str | None = None, effective_url: str | None = None) -> None:
    _cleanup_if_needed(len(data))
    key = cache_key(url)
    meta = {
        "url": url,
        "timestamp": time.time(),
        "size": len(data),
        "last_modified": last_modified,
        "content_type": content_type,
        "effective_url": effective_url,
    }
    _meta_path(key).write_text(json.dumps(meta))
    _data_path(key).write_bytes(data)


def invalidate(url: str) -> None:
    key = cache_key(url)
    _meta_path(key).unlink(missing_ok=True)
    _data_path(key).unlink(missing_ok=True)


def _cleanup_if_needed(incoming_size: int) -> None:
    cache_dir = _ensure_cache_dir()
    max_bytes = settings.CACHE_MAX_SIZE_MB * 1024 * 1024

    entries: list[tuple[float, Path, Path]] = []
    total = incoming_size

    for meta_p in cache_dir.glob("*.meta"):
        data_p = cache_dir / meta_p.name.replace(".meta", ".data")
        if not data_p.exists():
            meta_p.unlink(missing_ok=True)
            continue
        meta = json.loads(meta_p.read_text())
        total += meta.get("size", 0)
        entries.append((meta.get("timestamp", 0), meta_p, data_p))

    if total <= max_bytes:
        return

    # Supprimer les plus anciens d'abord
    entries.sort(key=lambda e: e[0])
    for ts, meta_p, data_p in entries:
        if total <= max_bytes:
            break
        meta = json.loads(meta_p.read_text())
        total -= meta.get("size", 0)
        meta_p.unlink(missing_ok=True)
        data_p.unlink(missing_ok=True)
