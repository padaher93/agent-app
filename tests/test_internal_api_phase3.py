from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.constants import STARTER_CONCEPT_IDS
from agent_app_dataset.internal_api import create_app
from agent_app_dataset.io_utils import write_json


def _sample_ingest_payload() -> dict:
    return {
        "sender_email": "ops@borrower.com",
        "source_email_id": "email_phase3_001",
        "deal_id": "deal_phase3",
        "period_end_date": "2025-12-31",
        "received_at": "2026-03-06T12:00:00+00:00",
        "files": [
            {
                "file_id": "file_phase3_01",
                "source_id": "src_phase3_01",
                "doc_type": "PDF",
                "filename": "borrower_update.pdf",
                "storage_uri": "s3://phase3/borrower_update.pdf",
                "checksum": "abc123phase3checksum",
                "pages_or_sheets": 12,
            }
        ],
        "variant_tags": ["phase3_test"],
        "quality_flags": [],
    }


def _write_label(labels_dir: Path, package_id: str, deal_id: str, period_end_date: str) -> None:
    rows = []
    for concept_id in STARTER_CONCEPT_IDS:
        rows.append(
            {
                "trace_id": f"tr_{package_id}_{concept_id}",
                "concept_id": concept_id,
                "period_end_date": period_end_date,
                "raw_value_text": "$100.00",
                "normalized_value": 100.0,
                "unit_currency": "USD",
                "expected_status": "verified",
                "labeler_confidence": 0.99,
                "flags": [],
                "normalization": {
                    "raw_scale": "absolute",
                    "normalized_scale": "absolute",
                    "currency_conversion_applied": False,
                },
                "evidence": {
                    "doc_id": "file_phase3_01",
                    "doc_name": "borrower_update.pdf",
                    "page_or_sheet": "Page 1",
                    "locator_type": "paragraph",
                    "locator_value": "p1:l1",
                    "source_snippet": "Revenue total: 100.00",
                },
            }
        )

    payload = {
        "schema_version": "1.0",
        "package_id": package_id,
        "deal_id": deal_id,
        "period_end_date": period_end_date,
        "dictionary_version": "v1.0",
        "labeling": {
            "primary_labeler": "qa",
            "reviewer": "qa_reviewer",
            "adjudication_required": False,
        },
        "rows": rows,
    }
    write_json(labels_dir / f"{package_id}.ground_truth.json", payload)


def test_ingest_is_idempotent(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    payload = _sample_ingest_payload()
    first = client.post("/internal/v1/packages:ingest", json=payload)
    second = client.post("/internal/v1/packages:ingest", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200

    first_body = first.json()
    second_body = second.json()
    assert first_body["package_id"] == second_body["package_id"]
    assert first_body["created"] is True
    assert second_body["created"] is False


def test_process_sync_and_query_delta_and_trace(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    ingest_resp = client.post("/internal/v1/packages:ingest", json=_sample_ingest_payload())
    package_id = ingest_resp.json()["package_id"]

    _write_label(
        labels_dir=labels_dir,
        package_id=package_id,
        deal_id="deal_phase3",
        period_end_date="2025-12-31",
    )

    process_resp = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": False, "max_retries": 2},
    )
    assert process_resp.status_code == 200
    assert process_resp.json()["status"] == "completed"

    pkg = client.get(f"/internal/v1/packages/{package_id}")
    assert pkg.status_code == 200
    assert pkg.json()["status"] == "completed"

    delta = client.get(f"/internal/v1/deals/deal_phase3/periods/{package_id}/delta")
    assert delta.status_code == 200
    rows = delta.json()["rows"]
    assert len(rows) == len(STARTER_CONCEPT_IDS)

    trace_id = rows[0]["trace_id"]
    trace = client.get(f"/internal/v1/traces/{trace_id}")
    assert trace.status_code == 200
    assert trace.json()["trace_id"] == trace_id


def test_async_process_status_lifecycle(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    ingest_resp = client.post("/internal/v1/packages:ingest", json=_sample_ingest_payload())
    package_id = ingest_resp.json()["package_id"]

    accepted = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": True, "max_retries": 2},
    )
    assert accepted.status_code == 200
    assert accepted.json()["status"] == "processing"

    final_status = None
    for _ in range(20):
        current = client.get(f"/internal/v1/packages/{package_id}")
        final_status = current.json()["status"]
        if final_status in {"completed", "needs_review", "failed"}:
            break
        time.sleep(0.05)

    assert final_status in {"completed", "needs_review"}
