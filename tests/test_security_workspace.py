from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.internal_api import create_app


def _ingest_payload(workspace_id: str) -> dict:
    return {
        "workspace_id": workspace_id,
        "sender_email": "ops@borrower.com",
        "source_email_id": "email_ws_001",
        "deal_id": "deal_ws",
        "period_end_date": "2026-01-31",
        "received_at": "2026-03-06T12:00:00+00:00",
        "files": [
            {
                "file_id": "file_ws_01",
                "source_id": "src_ws_01",
                "doc_type": "PDF",
                "filename": "borrower_update.pdf",
                "storage_uri": "s3://phase_ws/borrower_update.pdf",
                "checksum": "abc123workspace",
                "pages_or_sheets": 1,
            }
        ],
        "variant_tags": [],
        "quality_flags": [],
    }


def test_workspace_isolation_on_read_endpoints(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    ingest = client.post("/internal/v1/packages:ingest", json=_ingest_payload("ws_alpha"))
    assert ingest.status_code == 200
    package_id = ingest.json()["package_id"]

    allowed = client.get(
        f"/internal/v1/packages/{package_id}",
        headers={"X-Workspace-Id": "ws_alpha"},
    )
    assert allowed.status_code == 200
    assert allowed.json()["workspace_id"] == "ws_alpha"

    blocked = client.get(
        f"/internal/v1/packages/{package_id}",
        headers={"X-Workspace-Id": "ws_beta"},
    )
    assert blocked.status_code == 404


def test_internal_token_and_https_guard(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        internal_token="secret",
        require_https=True,
    )
    client = TestClient(app)

    missing_token = client.get("/internal/v1/health")
    assert missing_token.status_code == 401

    missing_https = client.get(
        "/internal/v1/health",
        headers={"X-Internal-Token": "secret"},
    )
    assert missing_https.status_code == 400

    allowed = client.get(
        "/internal/v1/health",
        headers={
            "X-Internal-Token": "secret",
            "X-Forwarded-Proto": "https",
        },
    )
    assert allowed.status_code == 200
