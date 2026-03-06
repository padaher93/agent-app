from __future__ import annotations

import json
from pathlib import Path

from agent_app_dataset.agent_workflow import WorkflowConfig, check_log_integrity, run_workflow


def test_agent_workflow_enforces_retry_cap_and_appends_log(tmp_path: Path) -> None:
    events_log = tmp_path / "events" / "agent_events.jsonl"

    package_predictions = [
        {
            "package_id": "pkg_9999",
            "deal_id": "deal_test",
            "period_end_date": "2025-12-31",
            "rows": [
                {
                    "concept_id": "revenue_total",
                    "status": "candidate_flagged",
                    "normalized_value": 100.0,
                    "unit_currency": "USD",
                    "confidence": 0.84,
                    "hard_blockers": [],
                    "trace_id": "tr_pkg_9999_revenue_total",
                    "evidence": {
                        "doc_id": "file_9999_01",
                        "locator_type": "cell",
                        "locator_value": "B10",
                    },
                }
            ],
        }
    ]

    payload, summary = run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=2),
    )

    row = payload["packages"][0]["rows"][0]
    assert row["retry_count"] <= 2
    assert row["final_resolver"] == "agent_4"
    assert row["verification"]["final_status"] in {"verified", "candidate_flagged", "unresolved"}
    assert isinstance(row["verification"]["attempts"], list)
    assert summary.events > 0
    assert events_log.exists()

    lines_first_run = events_log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines_first_run) == summary.events
    assert not check_log_integrity(events_log)

    # Append-only check.
    _, summary2 = run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=2),
    )
    lines_second_run = events_log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines_second_run) == summary.events + summary2.events

    first = json.loads(lines_second_run[0])
    second = json.loads(lines_second_run[1])
    assert first["sequence_id"] == 1
    assert second["sequence_id"] == 2
    assert second["previous_hash"] == first["event_hash"]


def test_missing_evidence_forces_unresolved(tmp_path: Path) -> None:
    events_log = tmp_path / "events" / "missing_evidence.jsonl"

    package_predictions = [
        {
            "package_id": "pkg_1000",
            "deal_id": "deal_test",
            "period_end_date": "2025-12-31",
            "rows": [
                {
                    "concept_id": "revenue_total",
                    "status": "verified",
                    "normalized_value": 100.0,
                    "unit_currency": "USD",
                    "confidence": 0.99,
                    "hard_blockers": [],
                    "trace_id": "tr_pkg_1000_revenue_total",
                    "evidence": {
                        "doc_id": "",
                        "locator_type": "cell",
                        "locator_value": "",
                    },
                }
            ],
        }
    ]

    payload, _ = run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=2),
    )

    row = payload["packages"][0]["rows"][0]
    assert row["status"] == "unresolved"
    assert "missing_evidence_location" in row["objection_reasons"]


def test_retry_exhaustion_can_end_as_candidate_flagged(tmp_path: Path) -> None:
    events_log = tmp_path / "events" / "retry_exhaustion.jsonl"

    package_predictions = [
        {
            "package_id": "pkg_1001",
            "deal_id": "deal_test",
            "period_end_date": "2025-12-31",
            "rows": [
                {
                    "concept_id": "revenue_total",
                    "status": "candidate_flagged",
                    "normalized_value": 100.0,
                    "unit_currency": "USD",
                    "confidence": 0.80,
                    "hard_blockers": [],
                    "trace_id": "tr_pkg_1001_revenue_total",
                    "evidence": {
                        "doc_id": "file_1001_01",
                        "locator_type": "cell",
                        "locator_value": "B10",
                    },
                }
            ],
        }
    ]

    payload, _ = run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=2),
    )

    row = payload["packages"][0]["rows"][0]
    assert row["retry_count"] == 2
    assert row["status"] == "candidate_flagged"
    assert row["verification"]["final_status"] == "candidate_flagged"


def test_log_integrity_detects_tamper(tmp_path: Path) -> None:
    events_log = tmp_path / "events" / "tamper.jsonl"

    package_predictions = [
        {
            "package_id": "pkg_1002",
            "deal_id": "deal_test",
            "period_end_date": "2025-12-31",
            "rows": [
                {
                    "concept_id": "revenue_total",
                    "status": "verified",
                    "normalized_value": 100.0,
                    "unit_currency": "USD",
                    "confidence": 0.99,
                    "hard_blockers": [],
                    "trace_id": "tr_pkg_1002_revenue_total",
                    "evidence": {
                        "doc_id": "file_1002_01",
                        "locator_type": "cell",
                        "locator_value": "B10",
                    },
                }
            ],
        }
    ]

    run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=2),
    )

    lines = events_log.read_text(encoding="utf-8").splitlines()
    tampered = json.loads(lines[0])
    tampered["payload"]["tampered"] = True
    lines[0] = json.dumps(tampered, sort_keys=True)
    events_log.write_text("\n".join(lines) + "\n", encoding="utf-8")

    issues = check_log_integrity(events_log)
    assert issues
    assert any("event_hash mismatch" in issue for issue in issues)


def test_invalid_initial_status_is_forced_to_terminal(tmp_path: Path) -> None:
    events_log = tmp_path / "events" / "invalid_status.jsonl"

    package_predictions = [
        {
            "package_id": "pkg_1003",
            "deal_id": "deal_test",
            "period_end_date": "2025-12-31",
            "rows": [
                {
                    "concept_id": "revenue_total",
                    "status": "draft",
                    "normalized_value": 100.0,
                    "unit_currency": "USD",
                    "confidence": 0.92,
                    "hard_blockers": [],
                    "trace_id": "tr_pkg_1003_revenue_total",
                    "evidence": {
                        "doc_id": "file_1003_01",
                        "locator_type": "cell",
                        "locator_value": "B10",
                    },
                }
            ],
        }
    ]

    payload, _ = run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=2),
    )

    row = payload["packages"][0]["rows"][0]
    assert row["status"] in {"verified", "candidate_flagged", "unresolved"}
    assert "invalid_initial_status" in row["hard_blockers"]
