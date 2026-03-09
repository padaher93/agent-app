from __future__ import annotations

import base64
import hashlib
import hmac
import json
from pathlib import Path
import time

from fastapi.testclient import TestClient

import agent_app_dataset.inbound_gateway as inbound_gateway
from agent_app_dataset.inbound_gateway import create_gateway_app


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


class _FakeHttpClient:
    calls: list[tuple[str, dict, dict]] = []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, json: dict, headers: dict | None = None):
        _FakeHttpClient.calls.append((url, json, headers or {}))
        if url.endswith("/internal/v1/packages:ingest"):
            return _FakeResponse(200, {"package_id": "pkg_int_001", "created": True})
        if url.endswith("/auth/v1/onboarding:ensure"):
            return _FakeResponse(
                200,
                {
                    "email": "ops@borrower.com",
                    "created": True,
                    "needs_password_setup": True,
                    "magic_link_url": "https://app.patrici.us/app/?magic_token=test-token",
                    "expires_at": "2026-03-06T12:30:00+00:00",
                },
            )
        return _FakeResponse(200, {"package_id": "pkg_int_001", "status": "processing"})


def _reset_calls() -> None:
    _FakeHttpClient.calls = []


class _FakeNotifier:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def send_onboarding_email(self, *, to_email: str, magic_link_url: str, package_id: str, sender_email: str):
        self.calls.append(
            {
                "to_email": to_email,
                "magic_link_url": magic_link_url,
                "package_id": package_id,
                "sender_email": sender_email,
            }
        )

        class _Result:
            @staticmethod
            def as_dict() -> dict:
                return {"sent": True, "provider": "test", "reason": "", "message_id": "msg_123"}

        return _Result()


class _FakeS3Uploader:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_object(self, **kwargs) -> None:
        self.calls.append(kwargs)


def test_inbound_gateway_accepts_payload_and_triggers_ingest_and_process(monkeypatch) -> None:
    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FakeHttpClient)
    _reset_calls()
    notifier = _FakeNotifier()

    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        inbound_token="secret",
        notifier=notifier,
    )
    client = TestClient(app)

    payload = {
        "email": {
            "from": "ops@borrower.com",
            "source_email_id": "email_gateway_001",
            "deal_id": "deal_gateway",
            "period_end_date": "2026-01-31",
            "received_at": "2026-03-06T12:00:00+00:00",
            "attachments": [
                {
                    "file_id": "file_01",
                    "source_id": "src_01",
                    "doc_type": "PDF",
                    "filename": "update.pdf",
                    "storage_uri": "s3://files/update.pdf",
                    "checksum": "abc123",
                    "pages_or_sheets": 4,
                }
            ],
        },
        "process": {
            "async_mode": True,
            "max_retries": 2,
            "extraction_mode": "runtime",
        },
    }

    unauthorized = client.post("/inbound/v1/email", json=payload)
    assert unauthorized.status_code == 401

    response = client.post(
        "/inbound/v1/email",
        headers={"X-Inbound-Token": "secret"},
        json=payload,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert body["package"]["package_id"] == "pkg_int_001"
    assert body["processing"]["status"] == "processing"
    assert "onboarding" in body
    assert body["notification"]["sent"] is True
    assert len(notifier.calls) == 1
    assert notifier.calls[0]["to_email"] == "ops@borrower.com"

    assert len(_FakeHttpClient.calls) == 3


def test_postmark_provider_endpoint_persists_supported_attachments(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FakeHttpClient)
    _reset_calls()

    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        postmark_server_token="pm_secret",
        attachments_dir=tmp_path / "inbound_files",
    )
    client = TestClient(app)

    pdf_bytes = b"%PDF-1.7\nproxy"
    payload = {
        "MessageID": "postmark_msg_001",
        "Date": "2026-03-06T12:00:00Z",
        "FromFull": {"Email": "ops@borrower.com"},
        "Attachments": [
            {
                "Name": "borrower_update.pdf",
                "ContentType": "application/pdf",
                "Content": base64.b64encode(pdf_bytes).decode("utf-8"),
            },
            {
                "Name": "ignore.txt",
                "ContentType": "text/plain",
                "Content": base64.b64encode(b"not used").decode("utf-8"),
            },
        ],
    }

    unauthorized = client.post("/inbound/v1/providers/postmark", json=payload)
    assert unauthorized.status_code == 401

    response = client.post(
        "/inbound/v1/providers/postmark",
        headers={"X-Postmark-Server-Token": "pm_secret"},
        json=payload,
    )
    assert response.status_code == 200

    ingest_call = [item for item in _FakeHttpClient.calls if item[0].endswith(":ingest")][0]
    ingest_payload = ingest_call[1]
    assert ingest_payload["sender_email"] == "ops@borrower.com"
    assert ingest_payload["source_email_id"] == "postmark_msg_001"
    assert ingest_payload["quality_flags"] == ["unsupported_attachments:1"]

    files = ingest_payload["files"]
    assert len(files) == 1
    assert files[0]["doc_type"] == "PDF"
    assert files[0]["checksum"] == hashlib.sha256(pdf_bytes).hexdigest()
    assert Path(files[0]["storage_uri"]).exists()


def test_postmark_provider_endpoint_supports_s3_attachment_storage(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FakeHttpClient)
    _reset_calls()
    fake_s3 = _FakeS3Uploader()

    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        postmark_server_token="pm_secret",
        attachment_storage_mode="s3",
        attachment_storage_s3_bucket="patricius-inbound",
        attachment_storage_s3_client=fake_s3,
        attachments_dir=tmp_path / "inbound_files",
    )
    client = TestClient(app)

    pdf_bytes = b"%PDF-1.7\nproxy"
    payload = {
        "MessageID": "postmark_msg_002",
        "Date": "2026-03-06T12:00:00Z",
        "FromFull": {"Email": "ops@borrower.com"},
        "Attachments": [
            {
                "Name": "borrower_update.pdf",
                "ContentType": "application/pdf",
                "Content": base64.b64encode(pdf_bytes).decode("utf-8"),
            },
        ],
    }

    response = client.post(
        "/inbound/v1/providers/postmark",
        headers={"X-Postmark-Server-Token": "pm_secret"},
        json=payload,
    )
    assert response.status_code == 200
    assert len(fake_s3.calls) == 1
    assert fake_s3.calls[0]["Bucket"] == "patricius-inbound"

    ingest_call = [item for item in _FakeHttpClient.calls if item[0].endswith(":ingest")][0]
    ingest_payload = ingest_call[1]
    assert ingest_payload["files"][0]["storage_uri"].startswith("s3://patricius-inbound/")


def test_mailgun_provider_endpoint_validates_hmac_and_ingests(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FakeHttpClient)
    _reset_calls()

    signing_key = "mailgun_signing_key"
    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        mailgun_signing_key=signing_key,
        attachments_dir=tmp_path / "inbound_files",
    )
    client = TestClient(app)

    timestamp = str(int(time.time()))
    token = "abc123"
    signature = hmac.new(
        signing_key.encode("utf-8"),
        f"{timestamp}{token}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    form_data = {
        "sender": "ops@borrower.com",
        "Message-Id": "mailgun_msg_001",
        "timestamp": timestamp,
        "token": token,
        "signature": signature,
    }

    bad_sig = dict(form_data)
    bad_sig["signature"] = "invalid"
    unauthorized = client.post(
        "/inbound/v1/providers/mailgun",
        data=bad_sig,
        files={"attachment-1": ("borrower_update.xlsx", b"xlsx-bytes", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert unauthorized.status_code == 401

    response = client.post(
        "/inbound/v1/providers/mailgun",
        data=form_data,
        files={"attachment-1": ("borrower_update.xlsx", b"xlsx-bytes", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert response.status_code == 200

    ingest_call = [item for item in _FakeHttpClient.calls if item[0].endswith(":ingest")][0]
    ingest_payload = ingest_call[1]
    assert ingest_payload["source_email_id"] == "mailgun_msg_001"
    assert ingest_payload["files"][0]["doc_type"] == "XLSX"
    assert Path(ingest_payload["files"][0]["storage_uri"]).exists()


def test_sendgrid_provider_endpoint_checks_token(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FakeHttpClient)
    _reset_calls()

    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        sendgrid_inbound_token="sg_secret",
        attachments_dir=tmp_path / "inbound_files",
    )
    client = TestClient(app)

    unauthorized = client.post(
        "/inbound/v1/providers/sendgrid",
        data={"from": "ops@borrower.com", "message_id": "sg_msg_001"},
        files={"attachment1": ("borrower_update.pdf", b"pdf", "application/pdf")},
    )
    assert unauthorized.status_code == 401

    response = client.post(
        "/inbound/v1/providers/sendgrid",
        headers={"X-Sendgrid-Inbound-Token": "sg_secret"},
        data={"from": "ops@borrower.com", "message_id": "sg_msg_001"},
        files={"attachment1": ("borrower_update.pdf", b"pdf", "application/pdf")},
    )
    assert response.status_code == 200

    ingest_call = [item for item in _FakeHttpClient.calls if item[0].endswith(":ingest")][0]
    ingest_payload = ingest_call[1]
    assert ingest_payload["source_email_id"] == "sg_msg_001"
    assert ingest_payload["files"][0]["doc_type"] == "PDF"


def test_mailgun_rejects_stale_timestamp(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FakeHttpClient)
    _reset_calls()

    signing_key = "mailgun_signing_key"
    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        mailgun_signing_key=signing_key,
        attachments_dir=tmp_path / "inbound_files",
        mailgun_signature_tolerance_seconds=10,
    )
    client = TestClient(app)

    old_timestamp = str(int(time.time()) - 3600)
    token = "abc123"
    signature = hmac.new(
        signing_key.encode("utf-8"),
        f"{old_timestamp}{token}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    response = client.post(
        "/inbound/v1/providers/mailgun",
        data={
            "sender": "ops@borrower.com",
            "Message-Id": "mailgun_msg_stale",
            "timestamp": old_timestamp,
            "token": token,
            "signature": signature,
        },
        files={"attachment-1": ("borrower_update.xlsx", b"xlsx-bytes", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "stale_mailgun_signature"


def test_gateway_writes_dlq_on_internal_failure(monkeypatch, tmp_path: Path) -> None:
    class _FailingHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url: str, json: dict, headers: dict | None = None):
            if url.endswith("/internal/v1/packages:ingest"):
                return _FakeResponse(500, {"detail": "down"})
            return _FakeResponse(200, {"status": "ok"})

    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FailingHttpClient)
    dlq_path = tmp_path / "inbound_dlq.jsonl"

    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        inbound_token="secret",
        dlq_path=dlq_path,
    )
    client = TestClient(app)

    payload = {
        "email": {
            "from": "ops@borrower.com",
            "source_email_id": "email_gateway_dlq",
            "attachments": [
                {
                    "file_id": "file_01",
                    "source_id": "src_01",
                    "doc_type": "PDF",
                    "filename": "update.pdf",
                    "storage_uri": "s3://files/update.pdf",
                    "checksum": "abc123",
                    "pages_or_sheets": 4,
                }
            ],
        },
        "process": {"async_mode": True, "max_retries": 2, "extraction_mode": "runtime"},
    }

    response = client.post(
        "/inbound/v1/email",
        headers={"X-Inbound-Token": "secret"},
        json=payload,
    )
    assert response.status_code == 502
    assert dlq_path.exists() is True

    lines = dlq_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["ingest_payload"]["sender_email"] == "ops@borrower.com"
