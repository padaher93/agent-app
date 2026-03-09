from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.internal_api import create_app


def _ingest_payload(*, deal_id: str, source_email_id: str, workspace_id: str = "ws_default") -> dict:
    return {
        "workspace_id": workspace_id,
        "sender_email": "ops@borrower.com",
        "source_email_id": source_email_id,
        "deal_id": deal_id,
        "period_end_date": "2026-01-31",
        "received_at": "2026-03-06T12:00:00+00:00",
        "files": [
            {
                "file_id": f"file_{source_email_id}",
                "source_id": f"src_{source_email_id}",
                "doc_type": "PDF",
                "filename": "borrower_update.pdf",
                "storage_uri": "s3://phase4/borrower_update.pdf",
                "checksum": f"checksum_{source_email_id}",
                "pages_or_sheets": 2,
            }
        ],
    }


def test_deal_rename_archive_and_package_reassignment(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    first = client.post("/internal/v1/packages:ingest", json=_ingest_payload(deal_id="deal_alpha", source_email_id="email_001"))
    second = client.post("/internal/v1/packages:ingest", json=_ingest_payload(deal_id="deal_beta", source_email_id="email_002"))

    assert first.status_code == 200
    assert second.status_code == 200

    package_id = first.json()["package_id"]

    rename = client.patch(
        "/internal/v1/deals/deal_alpha",
        json={"display_name": "Alpha Core"},
    )
    assert rename.status_code == 200
    assert rename.json()["display_name"] == "Alpha Core"

    archive = client.delete(
        "/internal/v1/deals/deal_beta",
        headers={"X-Role": "Owner"},
    )
    assert archive.status_code == 200
    assert archive.json()["status"] == "archived"

    deals = client.get("/internal/v1/deals")
    assert deals.status_code == 200
    ids = [item["deal_id"] for item in deals.json()["deals"]]
    assert "deal_alpha" in ids
    assert "deal_beta" not in ids

    alpha_entry = next(item for item in deals.json()["deals"] if item["deal_id"] == "deal_alpha")
    assert alpha_entry["display_name"] == "Alpha Core"

    reassign = client.post(
        f"/internal/v1/packages/{package_id}:reassign",
        headers={"X-Role": "Operator"},
        json={
            "target_deal_id": "deal_gamma",
            "actor": "test",
            "note": "move package",
        },
    )
    assert reassign.status_code == 200
    assert reassign.json()["deal_id"] == "deal_gamma"
    assert reassign.json()["source_deal_id"] == "deal_alpha"

    gamma_periods = client.get("/internal/v1/deals/deal_gamma/periods")
    assert gamma_periods.status_code == 200
    assert gamma_periods.json()["count"] == 1

    alpha_periods = client.get("/internal/v1/deals/deal_alpha/periods")
    assert alpha_periods.status_code == 200
    assert alpha_periods.json()["count"] == 0


def test_reassign_blocks_cross_workspace_target_deal(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    src = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(deal_id="deal_source", source_email_id="email_100", workspace_id="ws_default"),
    )
    assert src.status_code == 200
    package_id = src.json()["package_id"]

    other = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(deal_id="deal_target", source_email_id="email_200", workspace_id="ws_other"),
    )
    assert other.status_code == 200

    conflict = client.post(
        f"/internal/v1/packages/{package_id}:reassign",
        headers={"X-Role": "Owner", "X-Workspace-Id": "ws_default"},
        json={"target_deal_id": "deal_target", "actor": "test", "note": "cross workspace"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == "target_deal_workspace_conflict"


def test_create_deal_and_list_empty_periods(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    created = client.post(
        "/internal/v1/deals",
        headers={"X-Workspace-Id": "ws_default"},
        json={
            "display_name": "Alderon Credit Partners",
            "template_id": "tpl_fixed_starter_v1",
            "concept_overrides": [
                {"concept_id": "revenue_total", "selected": True},
                {"concept_id": "ebitda_reported", "selected": True},
            ],
        },
    )
    assert created.status_code == 200
    deal_id = created.json()["deal_id"]
    assert deal_id.startswith("deal_")
    assert created.json()["period_count"] == 0
    assert created.json()["forwarding_address"] == "inbound@patrici.us"
    assert "PDF/XLSX" in created.json()["quick_instruction"]
    assert created.json()["template_id"] == "tpl_fixed_starter_v1"
    assert len(created.json()["concept_overrides"]) == 2
    assert created.json()["concept_overrides"][0]["selected"] is True

    deals = client.get("/internal/v1/deals", headers={"X-Workspace-Id": "ws_default"})
    assert deals.status_code == 200
    deal_rows = deals.json()["deals"]
    ids = [item["deal_id"] for item in deal_rows]
    assert deal_id in ids
    created_row = next(item for item in deal_rows if item["deal_id"] == deal_id)
    assert created_row["template_id"] == "tpl_fixed_starter_v1"
    assert created_row["forwarding_address"] == "inbound@patrici.us"

    periods = client.get(f"/internal/v1/deals/{deal_id}/periods", headers={"X-Workspace-Id": "ws_default"})
    assert periods.status_code == 200
    assert periods.json()["count"] == 0


def test_create_deal_rejects_when_no_variable_selected(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    created = client.post(
        "/internal/v1/deals",
        headers={"X-Workspace-Id": "ws_default"},
        json={
            "display_name": "Threshold Missing Deal",
            "concept_overrides": [
                {"concept_id": "revenue_total", "selected": False},
            ],
        },
    )
    assert created.status_code == 400
    assert created.json()["detail"] == "at_least_one_concept_required"
