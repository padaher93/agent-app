from __future__ import annotations

from agent_app_dataset.email_adapter import normalized_email_to_ingest_request


def test_normalized_email_adapter_builds_ingest_payload() -> None:
    payload = {
        "from": "borrower@example.com",
        "message_id": "email_abc",
        "deal_id": "deal_abc",
        "period_end_date": "2025-12-31",
        "received_at": "2026-03-06T12:00:00+00:00",
        "attachments": [
            {
                "file_id": "file_01",
                "source_id": "src_01",
                "doc_type": "PDF",
                "filename": "report.pdf",
                "storage_uri": "s3://x/report.pdf",
                "checksum": "deadbeef",
                "pages_or_sheets": 12,
            }
        ],
    }

    out = normalized_email_to_ingest_request(payload)
    assert out["sender_email"] == "borrower@example.com"
    assert out["source_email_id"] == "email_abc"
    assert out["deal_id"] == "deal_abc"
    assert out["files"][0]["file_id"] == "file_01"
