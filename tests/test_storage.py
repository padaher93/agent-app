from __future__ import annotations

from pathlib import Path

import agent_app_dataset.storage as storage


class _FakeHTTPResponse:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


class _FakeHTTPClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str):
        return _FakeHTTPResponse(200, f"payload:{url}".encode("utf-8"))


class _FakeS3Client:
    def download_file(self, bucket: str, key: str, filename: str) -> None:
        Path(filename).write_bytes(f"{bucket}/{key}".encode("utf-8"))


def test_resolve_storage_uri_local_path(tmp_path: Path) -> None:
    sample = tmp_path / "sample.pdf"
    sample.write_bytes(b"%PDF-1.7\nsample")

    resolved = storage.resolve_storage_uri(str(sample))
    assert resolved == sample


def test_resolve_storage_uri_http_downloads_to_cache(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(storage.httpx, "Client", _FakeHTTPClient)
    cache_dir = tmp_path / "cache"

    resolved = storage.resolve_storage_uri("https://example.com/file.pdf", cache_dir=cache_dir)
    assert resolved is not None
    assert resolved.exists() is True
    assert resolved.read_bytes() == b"payload:https://example.com/file.pdf"

    # second call should reuse cache
    resolved_again = storage.resolve_storage_uri("https://example.com/file.pdf", cache_dir=cache_dir)
    assert resolved_again == resolved


def test_resolve_storage_uri_s3_downloads_to_cache(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(storage, "_create_s3_client", lambda: _FakeS3Client())
    cache_dir = tmp_path / "cache"

    resolved = storage.resolve_storage_uri("s3://bucket-a/folder/file.xlsx", cache_dir=cache_dir)
    assert resolved is not None
    assert resolved.exists() is True
    assert resolved.read_text(encoding="utf-8") == "bucket-a/folder/file.xlsx"


def test_is_supported_storage_uri_rejects_unknown_scheme() -> None:
    assert storage.is_supported_storage_uri("gcs://bucket/object") is False

