from __future__ import annotations

from pathlib import Path
import hashlib
import os
import tempfile
from typing import Any
from urllib.parse import urlparse

import httpx


DEFAULT_CACHE_DIR = Path("runtime/storage_cache")


def _safe_suffix_from_uri(uri: str) -> str:
    parsed = urlparse(uri)
    name = Path(parsed.path).name
    suffix = Path(name).suffix
    if len(suffix) > 15:
        return ""
    return suffix


def _cache_path_for_uri(uri: str, cache_dir: Path) -> Path:
    digest = hashlib.sha256(uri.encode("utf-8")).hexdigest()
    suffix = _safe_suffix_from_uri(uri)
    return cache_dir / f"{digest}{suffix}"


def _parse_s3_uri(uri: str) -> tuple[str, str] | None:
    parsed = urlparse(uri)
    if parsed.scheme.lower() != "s3":
        return None
    bucket = parsed.netloc.strip()
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        return None
    return bucket, key


def _create_s3_client() -> Any:
    try:
        import boto3  # type: ignore
    except Exception as exc:
        raise RuntimeError("boto3_not_installed") from exc

    endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL")
    region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if region_name:
        kwargs["region_name"] = region_name
    return boto3.client("s3", **kwargs)


def is_supported_storage_uri(uri: str) -> bool:
    normalized = str(uri).strip()
    if not normalized:
        return False

    parsed = urlparse(normalized)
    scheme = parsed.scheme.lower()

    if scheme in {"http", "https"}:
        return True

    if scheme == "s3":
        if _parse_s3_uri(normalized) is None:
            return False
        try:
            _create_s3_client()
        except Exception:
            return False
        return True

    if scheme == "file":
        candidate = Path(parsed.path)
    else:
        candidate = Path(normalized)

    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate.exists()


def resolve_storage_uri(
    uri: str,
    *,
    cache_dir: Path | None = None,
    timeout_seconds: float = 60.0,
) -> Path | None:
    normalized = str(uri).strip()
    if not normalized:
        return None

    parsed = urlparse(normalized)
    scheme = parsed.scheme.lower()

    if scheme == "file":
        candidate = Path(parsed.path)
        if not candidate.is_absolute():
            candidate = (Path.cwd() / candidate).resolve()
        return candidate if candidate.exists() else None

    if scheme in {"http", "https"}:
        target_root = cache_dir or DEFAULT_CACHE_DIR
        target_root.mkdir(parents=True, exist_ok=True)
        target = _cache_path_for_uri(normalized, target_root)
        if target.exists() and target.stat().st_size > 0:
            return target

        fd, tmp_name = tempfile.mkstemp(prefix="http_obj_", dir=str(target_root))
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
                response = client.get(normalized)
                if response.status_code >= 400:
                    return None
                tmp.write_bytes(response.content)
            tmp.replace(target)
            return target
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    if scheme == "s3":
        parsed_s3 = _parse_s3_uri(normalized)
        if parsed_s3 is None:
            return None
        bucket, key = parsed_s3

        target_root = cache_dir or DEFAULT_CACHE_DIR
        target_root.mkdir(parents=True, exist_ok=True)
        target = _cache_path_for_uri(normalized, target_root)
        if target.exists() and target.stat().st_size > 0:
            return target

        try:
            client = _create_s3_client()
        except Exception:
            return None

        fd, tmp_name = tempfile.mkstemp(prefix="s3_obj_", dir=str(target_root))
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            client.download_file(bucket, key, str(tmp))
            if not tmp.exists() or tmp.stat().st_size == 0:
                return None
            tmp.replace(target)
            return target
        except Exception:
            return None
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    candidate = Path(normalized)
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate if candidate.exists() else None
