from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.internal_api import create_app
from tools.seed_delta_review_db import seed_delta_review


def test_seeded_delta_review_demo_surfaces_deterministic_review_reasons(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "internal_api.sqlite3"
    events_log = tmp_path / "runtime" / "agent_events.jsonl"
    docs_dir = tmp_path / "runtime" / "seed_docs"

    summary = seed_delta_review(
        db_path=db_path,
        events_log=events_log,
        workspace_id="ws_default",
        deal_id="deal_delta_reason_demo",
        deal_name="Alderon Credit Partners",
        docs_dir=docs_dir,
    )

    app = create_app(
        db_path=db_path,
        labels_dir=tmp_path / "labels",
        events_log_path=events_log,
    )
    client = TestClient(app)

    response = client.get(
        f"/internal/v1/deals/{summary['deal_id']}/periods/{summary['current_package_id']}/review_queue"
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["product_mode"] == "first_package_intake"
    by_metric = {item["metric_key"]: item for item in payload["items"]}

    assert by_metric["revenue_total"]["review_reason_code"] == "source_conflict_across_rows"
    assert by_metric["revenue_total"]["review_reason_label"] == "Source conflict across rows"

    assert by_metric["ebitda_adjusted"]["review_reason_code"] == "exact_row_header_missing"
    assert by_metric["ebitda_adjusted"]["review_reason_label"] == "Exact row header missing"

    assert by_metric["ebitda_reported"]["review_reason_code"] == "candidate_from_pdf_text_only"
    assert by_metric["ebitda_reported"]["review_reason_label"] == "Extracted from PDF text only"

    assert by_metric["cash_and_equivalents"]["review_reason_code"] == "requirement_grounding_unavailable"
    assert by_metric["cash_and_equivalents"]["review_reason_label"] == "Requirement grounding unavailable"
    assert "requirement" not in str(by_metric["cash_and_equivalents"]["headline"]).lower()
    assert "required" not in str(by_metric["cash_and_equivalents"]["headline"]).lower()
    distinct_labels = {
        str(by_metric["revenue_total"]["review_reason_label"]),
        str(by_metric["ebitda_adjusted"]["review_reason_label"]),
        str(by_metric["ebitda_reported"]["review_reason_label"]),
        str(by_metric["cash_and_equivalents"]["review_reason_label"]),
    }
    assert len([label for label in distinct_labels if label.strip()]) >= 3

    for item in payload["items"]:
        label = str(item.get("review_reason_label") or "").lower()
        detail = str(item.get("review_reason_detail") or "").lower()
        assert "review-tier confirmation required" not in label
        assert "analyst confirmation required" not in label
        assert "candidate needs confirmation" not in label
        assert "review-tier confirmation required" not in detail
        assert "analyst confirmation required" not in detail
        assert "candidate needs confirmation" not in detail
