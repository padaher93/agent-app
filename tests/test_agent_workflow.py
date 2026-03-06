from __future__ import annotations

from pathlib import Path

from agent_app_dataset.agent_workflow import WorkflowConfig, run_workflow


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
    assert summary.events > 0
    assert events_log.exists()

    lines_first_run = events_log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines_first_run) == summary.events

    # Append-only check.
    payload2, summary2 = run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=2),
    )
    _ = payload2
    lines_second_run = events_log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines_second_run) == summary.events + summary2.events
