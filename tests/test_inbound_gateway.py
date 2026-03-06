from __future__ import annotations

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
    def __init__(self, *args, **kwargs):
        self.calls: list[tuple[str, dict]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, json: dict):
        self.calls.append((url, json))
        if url.endswith("/internal/v1/packages:ingest"):
            return _FakeResponse(200, {"package_id": "pkg_int_001", "created": True})
        return _FakeResponse(200, {"package_id": "pkg_int_001", "status": "processing"})


def test_inbound_gateway_accepts_payload_and_triggers_ingest_and_process(monkeypatch) -> None:
    monkeypatch.setattr(inbound_gateway.httpx, "Client", _FakeHttpClient)

    app = create_gateway_app(
        internal_api_base="http://127.0.0.1:8080",
        inbound_token="secret",
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
