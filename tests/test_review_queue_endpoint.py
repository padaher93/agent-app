from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.auth import hash_token
from agent_app_dataset.constants import STARTER_CONCEPT_IDS
from agent_app_dataset.internal_api import create_app
from agent_app_dataset.internal_store import InternalStore
from agent_app_dataset.io_utils import write_json


def _ingest_payload(*, deal_id: str, source_email_id: str, period_end_date: str, received_at: str) -> dict:
    return {
        "workspace_id": "ws_default",
        "sender_email": "ops@borrower.com",
        "source_email_id": source_email_id,
        "deal_id": deal_id,
        "period_end_date": period_end_date,
        "received_at": received_at,
        "files": [
            {
                "file_id": f"file_{source_email_id}",
                "source_id": f"src_{source_email_id}",
                "doc_type": "XLSX",
                "filename": "borrower_update.xlsx",
                "storage_uri": "s3://phase4/borrower_update.xlsx",
                "checksum": f"checksum_{source_email_id}",
                "pages_or_sheets": 4,
            }
        ],
    }


def _build_label_payload(
    *,
    package_id: str,
    deal_id: str,
    period_end_date: str,
    base_values: dict[str, float],
    overrides: dict[str, dict] | None = None,
) -> dict:
    rows = []
    updates = overrides or {}
    for concept_id in STARTER_CONCEPT_IDS:
        value = base_values.get(concept_id, 1000.0)
        row = {
            "trace_id": f"tr_{package_id}_{concept_id}",
            "concept_id": concept_id,
            "period_end_date": period_end_date,
            "raw_value_text": f"{value}",
            "normalized_value": value,
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
                "doc_id": f"file_{package_id}",
                "doc_name": "borrower_update.xlsx",
                "page_or_sheet": "Sheet: Coverage",
                "locator_type": "cell",
                "locator_value": "B10",
                "source_snippet": f"{concept_id} from borrower package.",
            },
        }
        row.update(updates.get(concept_id, {}))
        rows.append(row)

    return {
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


def test_review_queue_shapes_and_ranks_items(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_queue"
    baseline_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="baseline",
            period_end_date="2025-06-30",
            received_at="2025-07-05T12:00:00+00:00",
        ),
    )
    current_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="current",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert baseline_ingest.status_code == 200
    assert current_ingest.status_code == 200

    baseline_pkg = baseline_ingest.json()["package_id"]
    current_pkg = current_ingest.json()["package_id"]

    baseline_values = {
        "revenue_total": 12_500_000,
        "ebitda_adjusted": 2_610_000,
        "net_income": 980_000,
        "cash_and_equivalents": 1_480_000,
    }
    current_values = {
        "revenue_total": 12_450_000,
        "ebitda_adjusted": 2_450_000,
        "net_income": 0,
        "cash_and_equivalents": 0,
    }

    baseline_label = _build_label_payload(
        package_id=baseline_pkg,
        deal_id=deal_id,
        period_end_date="2025-06-30",
        base_values=baseline_values,
    )
    current_label = _build_label_payload(
        package_id=current_pkg,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values=current_values,
        overrides={
            "net_income": {
                "raw_value_text": "",
                "normalized_value": None,
                "labeler_confidence": 0.20,
                "flags": ["missing_schedule"],
                "requirement_anchor": {
                    "doc_id": f"file_{current_pkg}_requirements",
                    "doc_name": "credit_reporting_requirements.pdf",
                    "page_or_sheet": "Page 3",
                    "locator_type": "paragraph",
                    "locator_value": "p3:l7",
                    "source_snippet": "Borrower must provide Net Income with each quarterly reporting package.",
                    "required_concept_id": "net_income",
                    "required_concept_label": "Net Income",
                    "obligation_type": "reporting_requirement",
                    "source_role": "credit_reporting_schedule",
                    "trace_id": f"tr_{current_pkg}_net_income_req",
                },
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "paragraph",
                    "locator_value": "unresolved:not_found",
                    "source_snippet": "Net income missing from package.",
                },
            },
            "cash_and_equivalents": {
                "raw_value_text": "",
                "normalized_value": None,
                "labeler_confidence": 0.21,
                "flags": ["missing_schedule"],
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "paragraph",
                    "locator_value": "unresolved:not_found",
                    "source_snippet": "Cash schedule missing from package.",
                },
            },
            "revenue_total": {
                "labeler_confidence": 0.88,
                "flags": ["currency_inconsistency"],
                "source_anchors": [
                    {
                        "anchor_id": f"tr_{current_pkg}_revenue_total:cand:1",
                        "doc_id": f"file_{current_pkg}",
                        "doc_name": "borrower_update.xlsx",
                        "page_or_sheet": "Sheet: Coverage",
                        "locator_type": "cell",
                        "locator_value": "B10",
                        "source_snippet": "Revenue total 12,450,000 per coverage sheet.",
                        "raw_value_text": "12450000",
                        "normalized_value": 12450000,
                        "source_role": "coverage_sheet",
                        "confidence": 0.95,
                    },
                    {
                        "anchor_id": f"tr_{current_pkg}_revenue_total:cand:2",
                        "doc_id": f"file_{current_pkg}_memo",
                        "doc_name": "management_memo.xlsx",
                        "page_or_sheet": "Sheet: Memo",
                        "locator_type": "cell",
                        "locator_value": "D14",
                        "source_snippet": "Revenue total 12,150,000 in management memo.",
                        "raw_value_text": "12150000",
                        "normalized_value": 12150000,
                        "source_role": "management_memo",
                        "confidence": 0.93,
                    },
                ],
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Page 1",
                    "locator_type": "paragraph",
                    "locator_value": "p1:l4",
                    "source_snippet": "Revenue shown in inconsistent units across files.",
                },
            },
        },
    )
    write_json(labels_dir / f"{baseline_pkg}.ground_truth.json", baseline_label)
    write_json(labels_dir / f"{current_pkg}.ground_truth.json", current_label)

    baseline_process = client.post(
        f"/internal/v1/packages/{baseline_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    current_process = client.post(
        f"/internal/v1/packages/{current_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    assert baseline_process.status_code == 200
    assert current_process.status_code == 200

    response = client.get(f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue")
    assert response.status_code == 200
    payload = response.json()

    assert payload["product_mode"] == "delta_review"
    assert payload["product_state"]["screen_mode"] == "delta_review"
    assert payload["product_state"]["has_baseline"] is True
    assert payload["periods"]["comparison_basis"] == "prior_verified_period"
    assert payload["screen_taxonomy"]["section_order"] == [
        "blockers",
        "review_signals",
        "verified_changes",
    ]
    items = payload["items"]
    assert items
    assert items[0]["display_group"] == "blockers"
    assert payload["summary"]["blockers"] == sum(1 for item in items if item["display_group"] == "blockers")
    assert payload["summary"]["review_signals"] == sum(
        1 for item in items if item["display_group"] == "review_signals"
    )
    assert payload["summary"]["verified_changes"] == sum(
        1 for item in items if item["display_group"] == "verified_changes"
    )

    by_metric = {item["metric_key"]: item for item in items}
    for item in items:
        label = str(item.get("review_reason_label") or "").lower()
        detail = str(item.get("review_reason_detail") or "").lower()
        assert "review-tier confirmation required" not in label
        assert "analyst review lane" not in detail

    assert "net_income" in by_metric
    assert by_metric["net_income"]["headline"] in {
        "Net Income missing from current package",
        "Net income is missing from current package",
    }
    assert by_metric["net_income"]["case_mode"] == "investigation_missing_required_reporting"
    assert by_metric["net_income"]["concept_maturity"] == "grounded"
    assert by_metric["net_income"]["trust_tier"] == "grounded"
    assert by_metric["net_income"]["authority_level"] == "document_grounded"
    assert by_metric["net_income"]["workspace_mode"] == "delta_review"
    assert by_metric["net_income"]["review_required"] is True
    assert by_metric["net_income"]["display_group"] == "blockers"
    assert by_metric["net_income"]["proof_state"] == "missing_source"
    assert by_metric["net_income"]["proof_compare_mode"] == "baseline_current_plus_requirement"
    assert by_metric["net_income"]["obligation_grounding_state"] == "grounded"
    assert by_metric["net_income"]["requirement_anchor"]["doc_name"] == "credit_reporting_requirements.pdf"
    assert "required support" in by_metric["net_income"]["grounded_implication"].lower()
    missing_draft = by_metric["net_income"]["draft_borrower_query"]
    assert missing_draft["subject"].startswith("Reporting support request")
    assert "credit_reporting_requirements.pdf" in missing_draft["body"]
    assert "Net Income" in missing_draft["body"]
    assert by_metric["net_income"]["primary_action"]["id"] == "request_borrower_update"
    action_ids = [action["id"] for action in by_metric["net_income"]["recommended_actions"]]
    assert action_ids[:3] == [
        "request_borrower_update",
        "confirm_alternate_source",
        "mark_item_received",
    ]
    assert [action["id"] for action in by_metric["net_income"]["secondary_actions"]] == [
        "confirm_alternate_source"
    ]
    assert [action["id"] for action in by_metric["net_income"]["overflow_actions"]] == [
        "mark_item_received",
        "view_reporting_requirement",
        "copy_borrower_draft",
        "view_review_history",
    ]

    assert "revenue_total" in by_metric
    assert "source conflict" in by_metric["revenue_total"]["headline"].lower()
    assert by_metric["revenue_total"]["case_mode"] == "review_possible_source_conflict"
    assert by_metric["revenue_total"]["concept_maturity"] == "review"
    assert by_metric["revenue_total"]["trust_tier"] == "review"
    assert by_metric["revenue_total"]["authority_level"] == "analyst_confirmation_required"
    assert by_metric["revenue_total"]["workspace_mode"] == "investigation_mode"
    assert by_metric["revenue_total"]["review_required"] is True
    assert by_metric["revenue_total"]["display_group"] == "blockers"
    assert by_metric["revenue_total"]["proof_state"] == "conflict_detected"
    assert by_metric["revenue_total"]["proof_compare_mode"] == "source_vs_source"
    assert by_metric["revenue_total"]["review_reason_code"] == "source_conflict_across_rows"
    assert by_metric["revenue_total"]["review_reason_label"] == "Source conflict across rows"
    assert "confidence" not in str(by_metric["revenue_total"]["review_reason_detail"]).lower()
    assert "%" not in str(by_metric["revenue_total"]["review_reason_detail"])
    assert [action["id"] for action in by_metric["revenue_total"]["secondary_actions"]] == [
        "view_source_evidence"
    ]
    assert [action["id"] for action in by_metric["revenue_total"]["overflow_actions"]] == [
        "prepare_borrower_follow_up",
        "draft_analyst_note",
        "mark_expected_noise",
        "dismiss_after_review",
        "view_review_history",
    ]
    assert len(by_metric["revenue_total"]["competing_anchors"]) == 2
    revenue_subline = str(by_metric["revenue_total"]["subline"])
    assert "12,450,000" in revenue_subline
    assert "12,150,000" in revenue_subline
    anchor_a, anchor_b = by_metric["revenue_total"]["competing_anchors"]
    assert anchor_a["doc_id"] != anchor_b["doc_id"]
    assert anchor_a["preview_url"] != anchor_b["preview_url"]
    assert f"/files/{anchor_a['doc_id']}/evidence-preview" in anchor_a["preview_url"]
    assert "locator_type=cell" in anchor_a["preview_url"]
    assert "locator_value=B10" in anchor_a["preview_url"]
    assert f"/files/{anchor_b['doc_id']}/evidence-preview" in anchor_b["preview_url"]
    assert "locator_type=cell" in anchor_b["preview_url"]
    assert "locator_value=D14" in anchor_b["preview_url"]
    revenue_implication = by_metric["revenue_total"]["grounded_implication"].lower()
    assert "confirm source evidence" in revenue_implication
    assert "conflicting sources" in revenue_implication or "analyst confirmation" in revenue_implication
    assert "section" not in revenue_implication
    draft = by_metric["revenue_total"]["draft_borrower_query"]
    assert draft["subject"].startswith(
        "Source of record confirmation requested"
    )
    assert "borrower_update.xlsx" in draft["body"]
    assert "management_memo.xlsx" in draft["body"]
    assert "12,450,000" in draft["body"]
    assert "12,150,000" in draft["body"]

    assert "cash_and_equivalents" in by_metric
    assert by_metric["cash_and_equivalents"]["concept_maturity"] == "review"
    assert by_metric["cash_and_equivalents"]["trust_tier"] == "review"
    assert by_metric["cash_and_equivalents"]["case_mode"] in {
        "review_possible_missing_reporting_item",
        "review_possible_requirement",
    }
    assert by_metric["cash_and_equivalents"]["workspace_mode"] == "investigation_mode"
    assert by_metric["cash_and_equivalents"]["primary_action"]["id"] == "review_possible_requirement"
    assert by_metric["cash_and_equivalents"]["display_group"] == "review_signals"
    assert "reporting gap" in by_metric["cash_and_equivalents"]["grounded_implication"].lower() or (
        "analyst confirmation" in by_metric["cash_and_equivalents"]["grounded_implication"].lower()
    )

    assert "ebitda_adjusted" in by_metric
    assert by_metric["ebitda_adjusted"]["concept_maturity"] == "review"
    assert by_metric["ebitda_adjusted"]["trust_tier"] == "review"
    assert by_metric["ebitda_adjusted"]["case_mode"] == "review_possible_material_change"
    assert by_metric["ebitda_adjusted"]["review_reason_code"] == "variance_above_materiality_policy"
    assert by_metric["ebitda_adjusted"]["review_reason_label"] == "Variance exceeds materiality policy"
    assert "minor-variance policy" in str(by_metric["ebitda_adjusted"]["review_reason_detail"]).lower()
    assert "confirm source evidence" in by_metric["ebitda_adjusted"]["grounded_implication"].lower()
    assert by_metric["ebitda_adjusted"]["display_group"] == "review_signals"
    assert by_metric["ebitda_adjusted"]["case_certainty"] == "review_signal"
    assert by_metric["net_income"]["review_reason_code"] is None

    for item in items:
        if item["display_group"] == "verified_changes":
            assert item["case_certainty"] in {"grounded_fact", "confirmed_current_extraction"}
        if item["case_certainty"] in {"review_signal", "candidate_only", "conflict_detected", "missing_source"}:
            assert item["display_group"] != "verified_changes"

    # Trust ordering: grounded lane appears before review lane.
    net_income_rank = by_metric["net_income"]["rank"]
    revenue_rank = by_metric["revenue_total"]["rank"]
    assert net_income_rank < revenue_rank
    assert "total_assets" not in by_metric
    assert "accounts_receivable_total" not in by_metric


def test_review_queue_first_package_intake_mode_uses_intake_language(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_intake_mode"
    ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="intake_only",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert ingest.status_code == 200
    package_id = ingest.json()["package_id"]

    label = _build_label_payload(
        package_id=package_id,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={"net_income": 910_000, "revenue_total": 12_300_000},
        overrides={
            "net_income": {
                "expected_status": "unresolved",
                "labeler_confidence": 0.42,
                "evidence": {
                    "doc_id": f"file_{package_id}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "cell",
                    "locator_value": "B9",
                    "source_snippet": "Net Income 910000 from current package table.",
                },
            }
        },
    )
    write_json(labels_dir / f"{package_id}.ground_truth.json", label)

    process = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    assert process.status_code == 200

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{package_id}/review_queue")
    assert queue.status_code == 200
    payload = queue.json()

    assert payload["product_mode"] == "first_package_intake"
    assert payload["product_state"]["screen_mode"] == "first_package_intake"
    assert payload["product_state"]["has_baseline"] is False
    assert payload["product_state"]["comparison_ready"] is False
    assert payload["periods"]["baseline"] is None
    assert payload["periods"]["comparison_basis"] == "none"
    assert "material_changes" not in payload["summary"]
    assert "verified" not in payload["summary"]
    assert isinstance(payload["summary"]["confirmed_findings"], int)
    assert isinstance(payload["summary"]["review_signals"], int)
    assert payload["screen_taxonomy"]["summary_keys"] == [
        "blockers",
        "review_signals",
        "confirmed_findings",
    ]
    assert payload["screen_taxonomy"]["section_order"] == [
        "blockers",
        "review_signals",
        "confirmed_findings",
    ]

    by_metric = {item["metric_key"]: item for item in payload["items"]}
    items = payload["items"]
    counts_by_group = {
        "blockers": sum(1 for item in items if item["display_group"] == "blockers"),
        "review_signals": sum(1 for item in items if item["display_group"] == "review_signals"),
        "confirmed_findings": sum(1 for item in items if item["display_group"] == "confirmed_findings"),
    }
    assert payload["summary"]["blockers"] == counts_by_group["blockers"]
    assert payload["summary"]["review_signals"] == counts_by_group["review_signals"]
    assert payload["summary"]["confirmed_findings"] == counts_by_group["confirmed_findings"]
    assert payload["summary"]["total"] == len(items)

    for item in items:
        if item["display_group"] == "confirmed_findings":
            assert item["case_certainty"] in {"confirmed_current_extraction", "grounded_fact"}

    net_income = by_metric["net_income"]
    assert net_income["screen_mode"] == "first_package_intake"
    assert net_income["case_mode"] == "investigation_candidate_only"
    assert net_income["workspace_mode"] == "first_package_intake"
    assert net_income["concept_maturity"] == "grounded"
    assert net_income["case_certainty"] == "candidate_only"
    assert net_income["case_certainty_label"] == "Candidate only"
    assert net_income["display_group"] == "blockers"
    assert net_income["proof_compare_mode"] in {"current_only", "current_vs_candidate", "current_plus_requirement"}
    assert not net_income["proof_compare_mode"].startswith("baseline_")
    assert net_income["primary_action"]["id"] == "confirm_alternate_source"
    assert "versus prior period" not in net_income["headline"].lower()
    assert "→" not in net_income["subline"]
    assert "current package value" in net_income["subline"].lower()
    assert "evidence is sufficient" not in net_income["grounded_implication"].lower()
    assert net_income["grounded_implication"] == "Net Income is candidate-only and cannot be relied on yet."


def test_review_queue_first_package_intake_review_signal_is_investigation_framed(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_intake_review_signal"
    ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="intake_review_signal",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert ingest.status_code == 200
    package_id = ingest.json()["package_id"]

    label = _build_label_payload(
        package_id=package_id,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={"ebitda_adjusted": 2_620_000},
        overrides={
            "ebitda_adjusted": {
                "expected_status": "candidate_flagged",
                "labeler_confidence": 0.58,
                "evidence": {
                    "doc_id": f"file_{package_id}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "cell",
                    "locator_value": "B11",
                    "source_snippet": "EBITDA adjusted candidate 2,620,000 pending verification.",
                },
            }
        },
    )
    write_json(labels_dir / f"{package_id}.ground_truth.json", label)

    process = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    assert process.status_code == 200

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{package_id}/review_queue")
    assert queue.status_code == 200
    payload = queue.json()
    by_metric = {item["metric_key"]: item for item in payload["items"]}
    ebitda_adjusted = by_metric["ebitda_adjusted"]

    assert payload["product_mode"] == "first_package_intake"
    assert payload["summary"]["review_signals"] == sum(
        1 for item in payload["items"] if item["display_group"] == "review_signals"
    )
    assert ebitda_adjusted["screen_mode"] == "first_package_intake"
    assert ebitda_adjusted["workspace_mode"] == "investigation_mode"
    assert ebitda_adjusted["concept_maturity"] == "review"
    assert ebitda_adjusted["display_group"] == "blockers"
    assert ebitda_adjusted["case_mode"] in {
        "review_possible_requirement",
        "review_possible_missing_reporting_item",
    }
    assert ebitda_adjusted["case_certainty"] == "candidate_only"
    assert ebitda_adjusted["review_reason_code"] == "requirement_grounding_unavailable"
    assert ebitda_adjusted["review_reason_label"] == "Requirement grounding unavailable"
    assert "confidence" not in str(ebitda_adjusted["review_reason_detail"]).lower()
    assert "%" not in str(ebitda_adjusted["review_reason_detail"])
    assert ebitda_adjusted["proof_compare_mode"] in {"current_only", "current_vs_candidate", "current_plus_requirement"}
    assert "possible" in ebitda_adjusted["headline"].lower()
    assert "requirement" not in ebitda_adjusted["headline"].lower()
    assert "required" not in ebitda_adjusted["headline"].lower()
    assert "possible change" not in ebitda_adjusted["headline"].lower()
    assert "versus prior period" not in ebitda_adjusted["headline"].lower()
    assert "→" not in ebitda_adjusted["subline"]
    assert "current package" in ebitda_adjusted["subline"].lower() or "candidate only" in ebitda_adjusted["subline"].lower()
    implication = ebitda_adjusted["grounded_implication"].lower()
    assert (
        "confirm source evidence" in implication
        or "needs confirmation" in implication
        or "review package evidence" in implication
    )
    assert "evidence is sufficient" not in implication
    assert ebitda_adjusted["primary_action"]["id"] == "confirm_source_of_record"


def test_intake_verified_review_rows_are_review_signals_not_confirmed(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_intake_verified_review_rows"
    ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="intake_verified_review_rows",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert ingest.status_code == 200
    package_id = ingest.json()["package_id"]

    label = _build_label_payload(
        package_id=package_id,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={
            "net_income": 900_000,
            "ebitda_adjusted": 2_620_000,
            "ebitda_reported": 2_450_000,
            "revenue_total": 12_300_000,
        },
    )
    write_json(labels_dir / f"{package_id}.ground_truth.json", label)

    process = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    assert process.status_code == 200

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{package_id}/review_queue")
    assert queue.status_code == 200
    payload = queue.json()
    by_metric = {item["metric_key"]: item for item in payload["items"]}

    assert payload["product_mode"] == "first_package_intake"
    assert by_metric["ebitda_adjusted"]["case_mode"] == "review_possible_material_change"
    assert by_metric["ebitda_reported"]["case_mode"] == "review_possible_material_change"
    assert by_metric["revenue_total"]["case_mode"] == "review_possible_material_change"
    assert by_metric["ebitda_adjusted"]["display_group"] == "review_signals"
    assert by_metric["ebitda_reported"]["display_group"] == "review_signals"
    assert by_metric["revenue_total"]["display_group"] == "review_signals"
    assert by_metric["ebitda_adjusted"]["case_certainty"] == "review_signal"
    assert by_metric["net_income"]["display_group"] == "confirmed_findings"
    assert by_metric["net_income"]["case_certainty"] == "confirmed_current_extraction"

    summary = payload["summary"]
    assert summary["review_signals"] == sum(1 for item in payload["items"] if item["display_group"] == "review_signals")
    assert summary["confirmed_findings"] == sum(
        1 for item in payload["items"] if item["display_group"] == "confirmed_findings"
    )
    assert summary["review_signals"] >= 3


def test_review_queue_prefers_persisted_extraction_reason_when_available(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_intake_extraction_reason"
    ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="intake_extraction_reason",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert ingest.status_code == 200
    package_id = ingest.json()["package_id"]

    label = _build_label_payload(
        package_id=package_id,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={"net_income": 910_000, "ebitda_adjusted": 2_620_000},
        overrides={
            "ebitda_adjusted": {
                "expected_status": "candidate_flagged",
                "labeler_confidence": 0.84,
                "extraction_reason_code": "exact_row_header_missing",
                "extraction_reason_label": "Exact row header missing",
                "extraction_reason_detail": "Candidate value was found without a precise structured row locator.",
                "uncertainty_source": "package_extraction",
                "source_modality": "table_cell",
                "match_basis": "label_variant_match",
                "candidate_count": 2,
                "evidence": {
                    "doc_id": f"file_{package_id}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "cell",
                    "locator_value": "B11",
                    "source_snippet": "EBITDA adjusted candidate pending verification.",
                },
            }
        },
    )
    write_json(labels_dir / f"{package_id}.ground_truth.json", label)

    process = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    assert process.status_code == 200

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{package_id}/review_queue")
    assert queue.status_code == 200
    payload = queue.json()
    by_metric = {item["metric_key"]: item for item in payload["items"]}
    ebitda_adjusted = by_metric["ebitda_adjusted"]

    assert ebitda_adjusted["case_mode"] in {"review_possible_requirement", "review_possible_missing_reporting_item"}
    assert ebitda_adjusted["review_reason_code"] == "exact_row_header_missing"
    assert ebitda_adjusted["review_reason_label"] == "Exact row header missing"
    assert "locator" in str(ebitda_adjusted["review_reason_detail"]).lower()
    assert ebitda_adjusted["review_reason_code"] != "requirement_grounding_unavailable"

def test_list_deals_humanizes_display_name_when_metadata_is_internal_id(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id="deal_alderon",
            source_email_id="humanize_deal",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert ingest.status_code == 200

    deals = client.get("/internal/v1/deals")
    assert deals.status_code == 200
    rows = {row["deal_id"]: row for row in deals.json()["deals"]}
    assert rows["deal_alderon"]["display_name"] == "Deal Alderon"


def test_review_queue_does_not_emit_deterministic_conflict_from_blocker_string_only(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_conflict_guard"
    ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="single",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert ingest.status_code == 200
    package_id = ingest.json()["package_id"]

    label = _build_label_payload(
        package_id=package_id,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={"revenue_total": 12_400_000},
        overrides={
            "revenue_total": {
                "labeler_confidence": 0.85,
                "flags": ["currency_inconsistency"],
                "evidence": {
                    "doc_id": f"file_{package_id}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "cell",
                    "locator_value": "B10",
                    "source_snippet": "Revenue appears with potential unit mismatch marker.",
                },
            }
        },
    )
    write_json(labels_dir / f"{package_id}.ground_truth.json", label)

    process = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    assert process.status_code == 200

    response = client.get(f"/internal/v1/deals/{deal_id}/periods/{package_id}/review_queue")
    assert response.status_code == 200
    payload = response.json()
    revenue = next(item for item in payload["items"] if item["metric_key"] == "revenue_total")
    assert revenue["case_mode"] != "investigation_conflict"
    assert revenue["proof_state"] != "conflict_detected"


def test_missing_required_reporting_requires_grounded_obligation_anchor(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_missing_req_guard"
    baseline_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="baseline_guard",
            period_end_date="2025-06-30",
            received_at="2025-07-05T12:00:00+00:00",
        ),
    )
    current_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="current_guard",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert baseline_ingest.status_code == 200
    assert current_ingest.status_code == 200
    baseline_pkg = baseline_ingest.json()["package_id"]
    current_pkg = current_ingest.json()["package_id"]

    baseline_label = _build_label_payload(
        package_id=baseline_pkg,
        deal_id=deal_id,
        period_end_date="2025-06-30",
        base_values={"net_income": 910_000},
    )
    current_label = _build_label_payload(
        package_id=current_pkg,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={"net_income": 0},
        overrides={
            "net_income": {
                "raw_value_text": "",
                "normalized_value": None,
                "labeler_confidence": 0.23,
                "flags": ["missing_schedule"],
                # Ungrounded: missing snippet and unresolved locator.
                "requirement_anchor": {
                    "doc_id": "file_reporting_requirements",
                    "doc_name": "credit_reporting_requirements.pdf",
                    "page_or_sheet": "Page 3",
                    "locator_type": "paragraph",
                    "locator_value": "unresolved:not_found",
                    "source_snippet": "",
                    "required_concept_id": "net_income",
                    "required_concept_label": "Net Income",
                },
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "paragraph",
                    "locator_value": "unresolved:not_found",
                    "source_snippet": "Net income missing from package.",
                },
            }
        },
    )
    write_json(labels_dir / f"{baseline_pkg}.ground_truth.json", baseline_label)
    write_json(labels_dir / f"{current_pkg}.ground_truth.json", current_label)

    baseline_process = client.post(
        f"/internal/v1/packages/{baseline_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    current_process = client.post(
        f"/internal/v1/packages/{current_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    )
    assert baseline_process.status_code == 200
    assert current_process.status_code == 200

    response = client.get(f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue")
    assert response.status_code == 200
    payload = response.json()
    net_income = next(item for item in payload["items"] if item["metric_key"] == "net_income")

    assert net_income["case_mode"] != "investigation_missing_required_reporting"
    assert net_income["case_mode"] == "investigation_missing_source"
    assert net_income["obligation_grounding_state"] == "not_grounded"
    assert "Requirement anchor:" not in net_income["grounded_implication"]


def test_review_tier_feedback_is_persisted_and_grounded_items_are_rejected(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_review_feedback"
    baseline_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="baseline_feedback",
            period_end_date="2025-06-30",
            received_at="2025-07-05T12:00:00+00:00",
        ),
    )
    current_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="current_feedback",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert baseline_ingest.status_code == 200
    assert current_ingest.status_code == 200
    baseline_pkg = baseline_ingest.json()["package_id"]
    current_pkg = current_ingest.json()["package_id"]

    baseline_label = _build_label_payload(
        package_id=baseline_pkg,
        deal_id=deal_id,
        period_end_date="2025-06-30",
        base_values={
            "revenue_total": 12_500_000,
            "net_income": 980_000,
        },
    )
    current_label = _build_label_payload(
        package_id=current_pkg,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={
            "revenue_total": 12_450_000,
            "net_income": 0,
        },
        overrides={
            "net_income": {
                "raw_value_text": "",
                "normalized_value": None,
                "labeler_confidence": 0.2,
                "flags": ["missing_schedule"],
                "requirement_anchor": {
                    "doc_id": f"file_{current_pkg}_requirements",
                    "doc_name": "credit_reporting_requirements.pdf",
                    "page_or_sheet": "Page 3",
                    "locator_type": "paragraph",
                    "locator_value": "p3:l7",
                    "source_snippet": "Borrower must provide Net Income with each quarterly reporting package.",
                    "required_concept_id": "net_income",
                    "required_concept_label": "Net Income",
                },
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "paragraph",
                    "locator_value": "unresolved:not_found",
                    "source_snippet": "Net income missing from package.",
                },
            },
            "revenue_total": {
                "labeler_confidence": 0.88,
                "flags": ["currency_inconsistency"],
                "source_anchors": [
                    {
                        "anchor_id": f"tr_{current_pkg}_revenue_total:cand:1",
                        "doc_id": f"file_{current_pkg}",
                        "doc_name": "borrower_update.xlsx",
                        "page_or_sheet": "Sheet: Coverage",
                        "locator_type": "cell",
                        "locator_value": "B10",
                        "source_snippet": "Revenue total 12,450,000 per coverage sheet.",
                        "raw_value_text": "12450000",
                        "normalized_value": 12450000,
                        "source_role": "coverage_sheet",
                        "confidence": 0.95,
                    },
                    {
                        "anchor_id": f"tr_{current_pkg}_revenue_total:cand:2",
                        "doc_id": f"file_{current_pkg}_memo",
                        "doc_name": "management_memo.xlsx",
                        "page_or_sheet": "Sheet: Memo",
                        "locator_type": "cell",
                        "locator_value": "D14",
                        "source_snippet": "Revenue total 12,150,000 in management memo.",
                        "raw_value_text": "12150000",
                        "normalized_value": 12150000,
                        "source_role": "management_memo",
                        "confidence": 0.93,
                    },
                ],
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Page 1",
                    "locator_type": "paragraph",
                    "locator_value": "p1:l4",
                    "source_snippet": "Revenue shown in inconsistent units across files.",
                },
            },
        },
    )
    write_json(labels_dir / f"{baseline_pkg}.ground_truth.json", baseline_label)
    write_json(labels_dir / f"{current_pkg}.ground_truth.json", current_label)

    assert client.post(
        f"/internal/v1/packages/{baseline_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    ).status_code == 200
    assert client.post(
        f"/internal/v1/packages/{current_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    ).status_code == 200

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue")
    assert queue.status_code == 200
    items = {item["metric_key"]: item for item in queue.json()["items"]}
    revenue = items["revenue_total"]
    net_income = items["net_income"]
    assert revenue["concept_maturity"] == "review"
    assert net_income["concept_maturity"] == "grounded"

    feedback_post = client.post(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{revenue['id']}:feedback",
        json={
            "action_id": "mark_expected_noise",
            "outcome": "expected_noise",
            "actor": "operator_ui",
            "note": "Known memo variance for draft package.",
            "metadata": {"source": "unit_test"},
        },
    )
    assert feedback_post.status_code == 200
    feedback_payload = feedback_post.json()["feedback"]
    assert feedback_payload["item_id"] == revenue["id"]
    assert feedback_payload["concept_id"] == "revenue_total"
    assert feedback_payload["concept_maturity"] == "review"
    assert feedback_payload["outcome"] == "expected_noise"

    feedback_list = client.get(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/feedback",
        params={"item_id": revenue["id"]},
    )
    assert feedback_list.status_code == 200
    assert feedback_list.json()["count"] >= 1
    latest = feedback_list.json()["feedback"][0]
    assert latest["item_id"] == revenue["id"]
    assert latest["outcome"] == "expected_noise"
    assert latest["action_id"] == "mark_expected_noise"

    grounded_feedback = client.post(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{net_income['id']}:feedback",
        json={
            "action_id": "dismiss_after_review",
            "outcome": "dismissed",
            "actor": "operator_ui",
        },
    )
    assert grounded_feedback.status_code == 400
    assert grounded_feedback.json()["detail"] == "feedback_requires_review_tier_item"


def test_borrower_draft_workflow_events_are_persisted(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_draft_events"
    baseline_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="baseline_draft_events",
            period_end_date="2025-06-30",
            received_at="2025-07-05T12:00:00+00:00",
        ),
    )
    current_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="current_draft_events",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert baseline_ingest.status_code == 200
    assert current_ingest.status_code == 200
    baseline_pkg = baseline_ingest.json()["package_id"]
    current_pkg = current_ingest.json()["package_id"]

    baseline_label = _build_label_payload(
        package_id=baseline_pkg,
        deal_id=deal_id,
        period_end_date="2025-06-30",
        base_values={"net_income": 980_000},
    )
    current_label = _build_label_payload(
        package_id=current_pkg,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={"net_income": 0},
        overrides={
            "net_income": {
                "raw_value_text": "",
                "normalized_value": None,
                "labeler_confidence": 0.20,
                "flags": ["missing_schedule"],
                "requirement_anchor": {
                    "doc_id": f"file_{current_pkg}_requirements",
                    "doc_name": "credit_reporting_requirements.pdf",
                    "page_or_sheet": "Page 3",
                    "locator_type": "paragraph",
                    "locator_value": "p3:l7",
                    "source_snippet": "Borrower must provide Net Income with each quarterly reporting package.",
                    "required_concept_id": "net_income",
                    "required_concept_label": "Net Income",
                },
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "paragraph",
                    "locator_value": "unresolved:not_found",
                    "source_snippet": "Net income missing from package.",
                },
            }
        },
    )
    write_json(labels_dir / f"{baseline_pkg}.ground_truth.json", baseline_label)
    write_json(labels_dir / f"{current_pkg}.ground_truth.json", current_label)

    assert client.post(
        f"/internal/v1/packages/{baseline_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    ).status_code == 200
    assert client.post(
        f"/internal/v1/packages/{current_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    ).status_code == 200

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue")
    assert queue.status_code == 200
    items = {item["metric_key"]: item for item in queue.json()["items"]}
    net_income = items["net_income"]
    assert net_income["case_mode"] == "investigation_missing_required_reporting"
    assert net_income["concept_maturity"] == "grounded"

    post_opened = client.post(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{net_income['id']}:draft_event",
        json={
            "event_type": "draft_opened",
            "actor": "operator_ui",
            "subject": "Reporting support request — Net Income",
            "draft_text": "Please share Net Income support for the current package.",
            "metadata": {"source_action": "request_borrower_update"},
        },
    )
    assert post_opened.status_code == 200

    post_edited = client.post(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{net_income['id']}:draft_event",
        json={
            "event_type": "draft_edited",
            "actor": "operator_ui",
            "subject": "Reporting support request — Net Income",
            "draft_text": "Please share authoritative Net Income support for period 2025-09-30.",
            "metadata": {"source_field": "text"},
        },
    )
    assert post_edited.status_code == 200

    post_prepared = client.post(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{net_income['id']}:draft_event",
        json={
            "event_type": "draft_prepared",
            "actor": "operator_ui",
            "subject": "Reporting support request — Net Income",
            "draft_text": "Please share authoritative Net Income support for period 2025-09-30.",
            "metadata": {"source_action": "mark_follow_up_prepared"},
        },
    )
    assert post_prepared.status_code == 200

    events_response = client.get(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/draft_events",
        params={"item_id": net_income["id"]},
    )
    assert events_response.status_code == 200
    payload = events_response.json()
    assert payload["count"] >= 3
    events = payload["events"]
    event_types = {event["event_type"] for event in events}
    assert {"draft_opened", "draft_edited", "draft_prepared"} <= event_types
    latest = events[0]
    assert latest["item_id"] == net_income["id"]
    assert latest["concept_id"] == "net_income"
    assert latest["concept_maturity"] == "grounded"


def test_analyst_note_is_persisted_and_reopenable(tmp_path: Path) -> None:
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "runtime" / "api.sqlite3"

    app = create_app(
        db_path=db_path,
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    deal_id = "deal_analyst_notes"
    baseline_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="baseline_analyst_note",
            period_end_date="2025-06-30",
            received_at="2025-07-05T12:00:00+00:00",
        ),
    )
    current_ingest = client.post(
        "/internal/v1/packages:ingest",
        json=_ingest_payload(
            deal_id=deal_id,
            source_email_id="current_analyst_note",
            period_end_date="2025-09-30",
            received_at="2025-10-05T12:00:00+00:00",
        ),
    )
    assert baseline_ingest.status_code == 200
    assert current_ingest.status_code == 200
    baseline_pkg = baseline_ingest.json()["package_id"]
    current_pkg = current_ingest.json()["package_id"]

    baseline_label = _build_label_payload(
        package_id=baseline_pkg,
        deal_id=deal_id,
        period_end_date="2025-06-30",
        base_values={"ebitda_adjusted": 2_610_000, "net_income": 980_000},
    )
    current_label = _build_label_payload(
        package_id=current_pkg,
        deal_id=deal_id,
        period_end_date="2025-09-30",
        base_values={"ebitda_adjusted": 2_450_000, "net_income": 980_000},
        overrides={
            "ebitda_adjusted": {
                "expected_status": "candidate_flagged",
                "labeler_confidence": 0.61,
                "flags": ["row_header_not_exact"],
                "extraction_reason_code": "exact_row_header_missing",
                "extraction_reason_label": "Exact row header missing",
                "evidence": {
                    "doc_id": f"file_{current_pkg}",
                    "doc_name": "borrower_update.xlsx",
                    "page_or_sheet": "Sheet: Coverage",
                    "locator_type": "paragraph",
                    "locator_value": "p1:l5",
                    "source_snippet": "Adjusted EBITDA candidate found but exact row header could not be anchored.",
                },
            }
        },
    )
    write_json(labels_dir / f"{baseline_pkg}.ground_truth.json", baseline_label)
    write_json(labels_dir / f"{current_pkg}.ground_truth.json", current_label)

    assert client.post(
        f"/internal/v1/packages/{baseline_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    ).status_code == 200
    assert client.post(
        f"/internal/v1/packages/{current_pkg}:process",
        json={"async_mode": False, "max_retries": 0, "extraction_mode": "eval"},
    ).status_code == 200

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue")
    assert queue.status_code == 200
    by_metric = {item["metric_key"]: item for item in queue.json()["items"]}
    target = by_metric["ebitda_adjusted"]
    assert target["concept_maturity"] == "review"
    assert target["case_certainty"] in {"review_signal", "candidate_only"}

    put_create = client.put(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{target['id']}/analyst_note",
        json={
            "actor": "operator_ui",
            "subject": "EBITDA extraction review",
            "note_text": "Needs confirmation against signed borrower package table.",
            "memo_ready": True,
            "export_ready": False,
            "metadata": {"source_action": "draft_analyst_note"},
        },
    )
    assert put_create.status_code == 200
    created_note = put_create.json()["note"]
    assert created_note["item_id"] == target["id"]
    assert created_note["concept_id"] == "ebitda_adjusted"
    assert created_note["concept_maturity"] == "review"
    assert created_note["case_mode"] == target["case_mode"]
    assert created_note["subject"] == "EBITDA extraction review"
    assert created_note["memo_ready"] is True
    assert created_note["export_ready"] is False
    assert created_note["author"] == "operator_ui"

    get_note = client.get(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{target['id']}/analyst_note"
    )
    assert get_note.status_code == 200
    fetched = get_note.json()["note"]
    assert fetched is not None
    assert fetched["note_id"] == created_note["note_id"]
    assert fetched["note_text"] == "Needs confirmation against signed borrower package table."
    assert fetched["memo_ready"] is True
    assert fetched["export_ready"] is False

    put_update = client.put(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{target['id']}/analyst_note",
        json={
            "actor": "operator_ui",
            "subject": "EBITDA extraction review",
            "note_text": "Confirmed source mismatch; escalate only if borrower cannot provide exact row support.",
            "memo_ready": True,
            "export_ready": True,
            "metadata": {"source_action": "save_analyst_note"},
        },
    )
    assert put_update.status_code == 200
    updated = put_update.json()["note"]
    assert updated["note_id"] == created_note["note_id"]
    assert updated["note_text"].startswith("Confirmed source mismatch")
    assert updated["memo_ready"] is True
    assert updated["export_ready"] is True
    assert updated["updated_at"] >= updated["created_at"]

    # When a real authenticated session exists, note author should use session identity.
    store = InternalStore(db_path)
    session_token = "session_token_analyst_note_test"
    store.create_session(
        email="analyst@patricius.test",
        token_hash=hash_token(session_token),
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=4)).isoformat(),
    )
    put_with_auth = client.put(
        f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue/items/{target['id']}/analyst_note",
        json={
            "actor": "operator_ui",
            "subject": "EBITDA extraction review",
            "note_text": "Session-attributed analyst note update.",
            "memo_ready": True,
            "export_ready": True,
            "metadata": {"source_action": "save_analyst_note"},
        },
        headers={"Authorization": f"Bearer {session_token}"},
    )
    assert put_with_auth.status_code == 200
    attributed = put_with_auth.json()["note"]
    assert attributed["author"] == "analyst@patricius.test"
    assert attributed["note_text"] == "Session-attributed analyst note update."
