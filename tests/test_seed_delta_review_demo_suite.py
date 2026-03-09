from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.internal_api import create_app
from agent_app_dataset.internal_store import InternalStore
from tools.seed_delta_review_db import seed_canonical_demo_deals, seed_northstar_followup_package


def test_seed_canonical_demo_deals_exposes_intake_and_delta_modes(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "internal_api.sqlite3"
    events_log = tmp_path / "runtime" / "agent_events.jsonl"
    docs_dir = tmp_path / "runtime" / "seed_docs"
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    summary = seed_canonical_demo_deals(
        db_path=db_path,
        events_log=events_log,
        workspace_id="ws_default",
        docs_dir=docs_dir,
    )
    intake = summary["intake_demo"]
    delta = summary["delta_demo"]

    assert intake["deal_id"] == "deal_alderon"
    assert intake["product_mode"] == "first_package_intake"
    assert delta["deal_id"] == "deal_northstar"
    assert delta["product_mode"] == "delta_review"
    assert delta["baseline_package_id"] == "deal_northstar_period_2025_06_30"
    assert delta["current_package_id"] == "deal_northstar_period_2025_09_30"

    app = create_app(
        db_path=db_path,
        labels_dir=labels_dir,
        events_log_path=events_log,
    )
    client = TestClient(app)

    intake_queue = client.get(
        f"/internal/v1/deals/{intake['deal_id']}/periods/{intake['current_package_id']}/review_queue"
    )
    assert intake_queue.status_code == 200
    intake_payload = intake_queue.json()
    assert intake_payload["product_mode"] == "first_package_intake"
    assert intake_payload["periods"]["baseline"] is None

    delta_queue = client.get(
        f"/internal/v1/deals/{delta['deal_id']}/periods/{delta['current_package_id']}/review_queue"
    )
    assert delta_queue.status_code == 200
    delta_payload = delta_queue.json()
    assert delta_payload["product_mode"] == "delta_review"
    assert isinstance(delta_payload["periods"]["baseline"], dict)
    assert delta_payload["summary"]["blockers"] == 0
    assert delta_payload["summary"]["review_signals"] >= 1
    assert delta_payload["summary"]["verified_changes"] >= 1
    assert delta_payload["screen_taxonomy"]["section_order"] == [
        "blockers",
        "review_signals",
        "verified_changes",
    ]

    by_metric = {item["metric_key"]: item for item in delta_payload["items"]}
    assert by_metric["net_income"]["case_mode"] == "verified_review"
    assert by_metric["net_income"]["concept_maturity"] == "grounded"
    assert by_metric["net_income"]["display_group"] == "verified_changes"
    assert by_metric["revenue_total"]["proof_state"] == "verified"
    assert by_metric["revenue_total"]["review_reason_code"] is None
    assert by_metric["revenue_total"]["materiality_outcome"] == "auto_verified_minor_variance"
    assert by_metric["revenue_total"]["case_mode"] == "verified_review"
    assert by_metric["revenue_total"]["primary_action"]["id"] == "view_source_evidence"
    assert by_metric["revenue_total"]["materiality_policy"]["pct_minor_variance_max"] == 0.75
    assert by_metric["ebitda_adjusted"]["proof_state"] == "verified"
    assert by_metric["ebitda_adjusted"]["review_reason_code"] is None
    assert by_metric["ebitda_adjusted"]["display_group"] == "review_signals"
    assert by_metric["ebitda_adjusted"]["materiality_outcome"] == "review_signal"
    assert by_metric["ebitda_adjusted"]["primary_action"]["id"] == "confirm_source_of_record"
    assert not any(item["display_group"] == "blockers" for item in delta_payload["items"])
    assert by_metric["ebitda_reported"]["display_group"] == "verified_changes"
    assert by_metric["ebitda_reported"]["proof_state"] == "verified"
    assert by_metric["ebitda_reported"]["materiality_outcome"] == "auto_verified_minor_variance"
    assert by_metric["ebitda_reported"]["primary_action"]["id"] == "view_source_evidence"
    assert all(
        item["display_group"] != "verified_changes"
        for item in delta_payload["items"]
        if item["case_certainty"] in {"review_signal", "candidate_only", "conflict_detected", "missing_source"}
    )


def test_seed_canonical_demo_deals_is_idempotent_and_preserves_unrelated_data(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "internal_api.sqlite3"
    events_log = tmp_path / "runtime" / "agent_events.jsonl"
    docs_dir = tmp_path / "runtime" / "seed_docs"

    store = InternalStore(db_path)
    store.ensure_deal("deal_customer_real", "Customer Real")
    store.assign_deal_workspace("deal_customer_real", "ws_default")

    first = seed_canonical_demo_deals(
        db_path=db_path,
        events_log=events_log,
        workspace_id="ws_default",
        docs_dir=docs_dir,
    )
    second = seed_canonical_demo_deals(
        db_path=db_path,
        events_log=events_log,
        workspace_id="ws_default",
        docs_dir=docs_dir,
    )

    assert first["intake_demo"]["current_package_id"] == second["intake_demo"]["current_package_id"]
    assert first["delta_demo"]["baseline_package_id"] == second["delta_demo"]["baseline_package_id"]
    assert first["delta_demo"]["current_package_id"] == second["delta_demo"]["current_package_id"]

    packages = [
        record
        for record in store.list_packages()
        if record.deal_id in {"deal_alderon", "deal_northstar"}
    ]
    package_ids = sorted(record.package_id for record in packages)
    assert package_ids == [
        "deal_alderon_period_2025_09_30",
        "deal_northstar_period_2025_06_30",
        "deal_northstar_period_2025_09_30",
    ]

    intake_trace = f"tr_{first['intake_demo']['current_package_id']}_total_debt"
    delta_trace = f"tr_{first['delta_demo']['current_package_id']}_total_debt"
    assert len(store.list_trace_resolutions(intake_trace)) == 1
    assert len(store.list_trace_resolutions(delta_trace)) == 1

    customer = store.get_deal_meta("deal_customer_real")
    assert customer is not None
    assert customer["display_name"] == "Customer Real"


def test_seed_northstar_followup_adds_post_sep_period_and_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "internal_api.sqlite3"
    events_log = tmp_path / "runtime" / "agent_events.jsonl"
    docs_dir = tmp_path / "runtime" / "seed_docs"
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    seed_canonical_demo_deals(
        db_path=db_path,
        events_log=events_log,
        workspace_id="ws_default",
        docs_dir=docs_dir,
    )
    first = seed_northstar_followup_package(
        db_path=db_path,
        events_log=events_log,
        workspace_id="ws_default",
        docs_dir=docs_dir,
    )
    second = seed_northstar_followup_package(
        db_path=db_path,
        events_log=events_log,
        workspace_id="ws_default",
        docs_dir=docs_dir,
    )
    assert first["current_package_id"] == "deal_northstar_period_2025_12_31"
    assert second["current_package_id"] == first["current_package_id"]

    store = InternalStore(db_path)
    northstar_packages = sorted(
        pkg.package_id for pkg in store.list_packages() if pkg.deal_id == "deal_northstar"
    )
    assert northstar_packages == [
        "deal_northstar_period_2025_06_30",
        "deal_northstar_period_2025_09_30",
        "deal_northstar_period_2025_12_31",
    ]

    followup_trace = "tr_deal_northstar_period_2025_12_31_total_debt"
    assert len(store.list_trace_resolutions(followup_trace)) == 1

    app = create_app(
        db_path=db_path,
        labels_dir=labels_dir,
        events_log_path=events_log,
    )
    client = TestClient(app)
    queue = client.get(
        "/internal/v1/deals/deal_northstar/periods/deal_northstar_period_2025_12_31/review_queue"
    )
    assert queue.status_code == 200
    payload = queue.json()
    assert payload["product_mode"] == "delta_review"
    assert payload["summary"]["blockers"] >= 1
    assert payload["summary"]["review_signals"] >= 1
    by_metric = {item["metric_key"]: item for item in payload["items"]}
    assert by_metric["net_income"]["case_mode"] == "investigation_missing_required_reporting"
    assert by_metric["net_income"]["primary_action"]["id"] == "request_borrower_update"
    assert by_metric["revenue_total"]["review_reason_code"] == "source_conflict_across_rows"
    assert by_metric["revenue_total"]["primary_action"]["id"] == "confirm_source_of_record"
    revenue_anchors = by_metric["revenue_total"]["competing_anchors"]
    assert len(revenue_anchors) == 2
    assert revenue_anchors[0]["value_display"] == "12,180,000"
    assert revenue_anchors[1]["value_display"] == "11,940,000"
    assert revenue_anchors[0]["preview_url"] != revenue_anchors[1]["preview_url"]
    assert "/evidence-preview" in revenue_anchors[0]["preview_url"]
    assert "/evidence-preview" in revenue_anchors[1]["preview_url"]
    assert by_metric["ebitda_adjusted"]["review_reason_code"] == "exact_row_header_missing"
    assert by_metric["ebitda_adjusted"]["primary_action"]["id"] == "review_possible_requirement"
    assert by_metric["ebitda_reported"]["proof_state"] == "verified"
    assert by_metric["ebitda_reported"]["display_group"] == "review_signals"
