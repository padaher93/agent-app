from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.constants import STARTER_CONCEPT_IDS
from agent_app_dataset.internal_api import create_app
from agent_app_dataset.io_utils import write_json


def _sample_ingest_payload(source_email_id: str, period_end_date: str, received_at: str) -> dict:
    return {
        "sender_email": "ops@borrower.com",
        "source_email_id": source_email_id,
        "deal_id": "deal_phase4",
        "period_end_date": period_end_date,
        "received_at": received_at,
        "files": [
            {
                "file_id": "file_phase4_01",
                "source_id": "src_phase4_01",
                "doc_type": "XLSX",
                "filename": "borrower_update.xlsx",
                "storage_uri": "s3://phase4/borrower_update.xlsx",
                "checksum": f"checksum-{source_email_id}",
                "pages_or_sheets": 6,
            }
        ],
        "variant_tags": ["phase4_test"],
        "quality_flags": [],
    }


def _write_label(
    labels_dir: Path,
    package_id: str,
    deal_id: str,
    period_end_date: str,
    flagged_concept_id: str | None = None,
) -> None:
    rows = []
    for concept_id in STARTER_CONCEPT_IDS:
        confidence = 0.99
        if flagged_concept_id and concept_id == flagged_concept_id:
            confidence = 0.85

        rows.append(
            {
                "trace_id": f"tr_{package_id}_{concept_id}",
                "concept_id": concept_id,
                "period_end_date": period_end_date,
                "raw_value_text": "$100.00",
                "normalized_value": 100.0,
                "unit_currency": "USD",
                "expected_status": "candidate_flagged" if confidence < 0.9 else "verified",
                "labeler_confidence": confidence,
                "flags": [],
                "normalization": {
                    "raw_scale": "absolute",
                    "normalized_scale": "absolute",
                    "currency_conversion_applied": False,
                },
                "evidence": {
                    "doc_id": "file_phase4_01",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet1",
                    "locator_type": "cell",
                    "locator_value": "C12",
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


def _make_ui_dir(tmp_path: Path) -> Path:
    ui_dir = tmp_path / "ui"
    ui_dir.mkdir(parents=True, exist_ok=True)
    (ui_dir / "index.html").write_text("<html><body>phase4 ui</body></html>", encoding="utf-8")
    return ui_dir


def test_phase4_deals_events_and_resolution(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        ui_dir=_make_ui_dir(tmp_path),
    )
    client = TestClient(app)

    baseline_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_sample_ingest_payload(
            source_email_id="email_phase4_001",
            period_end_date="2025-12-31",
            received_at="2026-03-06T12:00:00+00:00",
        ),
    )
    baseline_pkg = baseline_ingest.json()["package_id"]

    followup_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_sample_ingest_payload(
            source_email_id="email_phase4_002",
            period_end_date="2026-01-31",
            received_at="2026-03-06T12:30:00+00:00",
        ),
    )
    followup_pkg = followup_ingest.json()["package_id"]

    _write_label(labels_dir, baseline_pkg, "deal_phase4", "2025-12-31")
    _write_label(
        labels_dir,
        followup_pkg,
        "deal_phase4",
        "2026-01-31",
        flagged_concept_id="ebitda_adjusted",
    )

    baseline_process = client.post(
        f"/internal/v1/packages/{baseline_pkg}:process",
        json={"async_mode": False, "max_retries": 2},
    )
    assert baseline_process.status_code == 200

    followup_process = client.post(
        f"/internal/v1/packages/{followup_pkg}:process",
        json={"async_mode": False, "max_retries": 0},
    )
    assert followup_process.status_code == 200
    assert followup_process.json()["status"] == "needs_review"

    deals_resp = client.get("/internal/v1/deals")
    assert deals_resp.status_code == 200
    deals = deals_resp.json()["deals"]
    assert len(deals) == 1
    assert deals[0]["deal_id"] == "deal_phase4"
    assert deals[0]["period_count"] == 2

    periods_resp = client.get("/internal/v1/deals/deal_phase4/periods")
    assert periods_resp.status_code == 200
    periods = periods_resp.json()["periods"]
    assert len(periods) == 2
    assert periods[0]["package_id"] == followup_pkg

    package_resp = client.get(f"/internal/v1/packages/{followup_pkg}?include_manifest=true")
    assert package_resp.status_code == 200
    assert package_resp.json()["package_manifest"]["package_id"] == followup_pkg

    package_events = client.get(f"/internal/v1/packages/{followup_pkg}/events")
    assert package_events.status_code == 200
    assert package_events.json()["count"] > 0
    assert package_events.json()["integrity_ok"] is True

    delta_resp = client.get(f"/internal/v1/deals/deal_phase4/periods/{followup_pkg}/delta")
    assert delta_resp.status_code == 200
    rows = delta_resp.json()["rows"]
    flagged = [row for row in rows if row["status"] == "candidate_flagged"]
    assert flagged

    trace_id = flagged[0]["trace_id"]
    trace_events_before = client.get(f"/internal/v1/traces/{trace_id}/events")
    assert trace_events_before.status_code == 200
    before_count = trace_events_before.json()["count"]

    resolve_resp = client.post(
        f"/internal/v1/traces/{trace_id}:resolve",
        json={
            "resolver": "operator",
            "selected_evidence": {
                "doc_id": "file_phase4_01",
                "locator_type": "cell",
                "locator_value": "C12",
            },
            "note": "manual review",
        },
    )
    assert resolve_resp.status_code == 200
    assert resolve_resp.json()["status"] == "verified"
    assert resolve_resp.json()["package_status"] == "completed"

    trace_resp = client.get(f"/internal/v1/traces/{trace_id}")
    assert trace_resp.status_code == 200
    assert trace_resp.json()["row"]["status"] == "verified"
    assert trace_resp.json()["row"]["resolved_by_user"] is True

    delta_after = client.get(f"/internal/v1/deals/deal_phase4/periods/{followup_pkg}/delta")
    resolved_row = next(row for row in delta_after.json()["rows"] if row["trace_id"] == trace_id)
    assert resolved_row["status"] == "verified"

    trace_events_after = client.get(f"/internal/v1/traces/{trace_id}/events")
    assert trace_events_after.status_code == 200
    assert trace_events_after.json()["count"] > before_count
    assert any(event["event_type"] == "user_resolved" for event in trace_events_after.json()["events"])


def test_app_mount_redirects_to_ui(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        ui_dir=_make_ui_dir(tmp_path),
    )
    client = TestClient(app)

    root = client.get("/", follow_redirects=False)
    assert root.status_code in {302, 307}
    assert root.headers["location"] == "/app/"

    app_home = client.get("/app/")
    assert app_home.status_code == 200
    assert "phase4 ui" in app_home.text


def test_repo_ui_assets_are_served(tmp_path: Path) -> None:
    repo_ui_dir = Path(__file__).resolve().parents[1] / "src" / "agent_app_dataset" / "ui"

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        ui_dir=repo_ui_dir,
    )
    client = TestClient(app)

    index_resp = client.get("/app/")
    assert index_resp.status_code == 200
    assert "Patricius Review Console" in index_resp.text

    css_resp = client.get("/app/app.css")
    assert css_resp.status_code == 200
    assert "workspace" in css_resp.text

    js_resp = client.get("/app/app.js")
    assert js_resp.status_code == 200
    assert "materialityForRow" in js_resp.text
