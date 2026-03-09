from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from agent_app_dataset.inbound_gateway import create_gateway_app
from agent_app_dataset.internal_api import create_app


class _FakeS3Client:
    def put_object(self, **kwargs) -> None:  # pragma: no cover - simple test double
        return None


def _security_headers() -> dict[str, str]:
    return {
        "X-Internal-Token": "internal-secret",
        "X-Forwarded-Proto": "https",
    }


def test_internal_api_strict_profile_requires_security_and_openai(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="strict_runtime_profile_requires"):
        create_app(
            db_path=tmp_path / "runtime" / "api.sqlite3",
            labels_dir=tmp_path / "labels",
            events_log_path=tmp_path / "runtime" / "events.jsonl",
            runtime_profile="prod",
        )


def test_internal_api_strict_profile_rejects_non_llm_extraction(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    sample_pdf = tmp_path / "borrower_update.pdf"
    sample_pdf.write_bytes(b"%PDF-1.7\nsample")

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        internal_token="internal-secret",
        require_https=True,
        runtime_profile="prod",
    )
    client = TestClient(app)

    ingest = client.post(
        "/internal/v1/packages:ingest",
        headers=_security_headers(),
        json={
            "sender_email": "ops@borrower.com",
            "source_email_id": "email_strict_001",
            "deal_id": "deal_strict",
            "period_end_date": "2026-01-31",
            "received_at": "2026-03-06T12:00:00+00:00",
            "files": [
                {
                    "file_id": "file_strict_01",
                    "source_id": "src_strict_01",
                    "doc_type": "PDF",
                    "filename": "borrower_update.pdf",
                    "storage_uri": str(sample_pdf),
                    "checksum": "strict_checksum_01",
                    "pages_or_sheets": 1,
                }
            ],
            "variant_tags": [],
            "quality_flags": [],
        },
    )
    assert ingest.status_code == 200
    package_id = ingest.json()["package_id"]

    process = client.post(
        f"/internal/v1/packages/{package_id}:process",
        headers={**_security_headers(), "X-Workspace-Id": "ws_default"},
        json={"async_mode": False, "max_retries": 2, "extraction_mode": "eval"},
    )
    assert process.status_code == 400
    assert process.json()["detail"] == "extraction_mode_not_allowed_in_strict_profile"


def test_internal_api_strict_profile_rejects_unresolvable_storage(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        internal_token="internal-secret",
        require_https=True,
        runtime_profile="staging",
    )
    client = TestClient(app)

    ingest = client.post(
        "/internal/v1/packages:ingest",
        headers=_security_headers(),
        json={
            "sender_email": "ops@borrower.com",
            "source_email_id": "email_strict_002",
            "deal_id": "deal_strict",
            "period_end_date": "2026-01-31",
            "received_at": "2026-03-06T12:00:00+00:00",
            "files": [
                {
                    "file_id": "file_strict_02",
                    "source_id": "src_strict_02",
                    "doc_type": "PDF",
                    "filename": "borrower_update.pdf",
                    "storage_uri": "gcs://placeholder/borrower_update.pdf",
                    "checksum": "strict_checksum_02",
                    "pages_or_sheets": 1,
                }
            ],
            "variant_tags": [],
            "quality_flags": [],
        },
    )
    assert ingest.status_code == 400
    assert ingest.json()["detail"].startswith("unsupported_storage_uri:")


def test_gateway_strict_profile_requires_postmark_and_secrets() -> None:
    with pytest.raises(RuntimeError, match="strict_runtime_profile_violation"):
        create_gateway_app(
            internal_api_base="http://127.0.0.1:8080",
            runtime_profile="prod",
            internal_api_token="internal-secret",
            internal_api_require_https=True,
            postmark_server_token="pm-token",
            outbound_email_mode="none",
        )


def test_gateway_strict_profile_disables_non_postmark_ingress() -> None:
    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        runtime_profile="prod",
        internal_api_token="internal-secret",
        internal_api_require_https=True,
        postmark_server_token="pm-token",
        outbound_email_mode="postmark",
        outbound_postmark_server_token="pm-outbound-token",
        attachment_storage_mode="s3",
        attachment_storage_s3_bucket="patricius-inbound",
        attachment_storage_s3_client=_FakeS3Client(),
    )
    client = TestClient(app)

    direct = client.post(
        "/inbound/v1/email",
        json={
            "email": {
                "from": "ops@borrower.com",
                "source_email_id": "email_direct_001",
                "attachments": [
                    {
                        "file_id": "file_01",
                        "source_id": "src_01",
                        "doc_type": "PDF",
                        "filename": "update.pdf",
                        "storage_uri": "/tmp/update.pdf",
                        "checksum": "abc123",
                        "pages_or_sheets": 1,
                    }
                ],
            },
            "process": {"async_mode": True, "max_retries": 2, "extraction_mode": "llm"},
        },
    )
    assert direct.status_code == 404
    assert direct.json()["detail"] == "endpoint_disabled_in_strict_profile"

    mailgun = client.post("/inbound/v1/providers/mailgun", data={"sender": "ops@borrower.com"})
    assert mailgun.status_code == 404
    assert mailgun.json()["detail"] == "endpoint_disabled_in_strict_profile"

    sendgrid = client.post("/inbound/v1/providers/sendgrid", data={"from": "ops@borrower.com"})
    assert sendgrid.status_code == 404
    assert sendgrid.json()["detail"] == "endpoint_disabled_in_strict_profile"
