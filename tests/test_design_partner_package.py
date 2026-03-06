from __future__ import annotations

from pathlib import Path

from agent_app_dataset.design_partner_package import (
    build_design_partner_package,
    build_metric_snapshot,
    build_readiness_summary,
    select_representative_traces,
    summarize_failure_taxonomy,
)
from agent_app_dataset.io_utils import read_json


def _sample_eval_report() -> dict:
    return {
        "dataset_version": "dataset_v1.0",
        "pipeline_version": "pipeline_123",
        "generated_at": "2026-03-06T12:00:00+00:00",
        "metrics": {
            "verified_precision": 0.985,
            "evidence_link_accuracy": 0.992,
            "false_verified_rate": 0.005,
            "unresolved_rate": 0.11,
            "package_completion_rate": 0.97,
        },
        "gate_pass": True,
        "failures": [],
        "row_counts": {
            "total_rows": 130,
            "verified_rows": 110,
            "evidence_rows": 130,
            "packages": 10,
        },
        "failure_taxonomy": [
            {"category": "extraction_errors", "count": 8},
            {"category": "normalization_errors", "count": 3},
        ],
    }


def _sample_traces() -> list[dict]:
    return [
        {
            "trace_id": "tr_002",
            "package_id": "pkg_2",
            "concept_id": "ebitda_adjusted",
            "status": "candidate_flagged",
            "confidence": 0.83,
            "doc_id": "file_2",
            "locator_type": "cell",
            "locator_value": "C12",
            "source_snippet": "EBITDA adjusted 100",
        },
        {
            "trace_id": "tr_001",
            "package_id": "pkg_2",
            "concept_id": "interest_expense",
            "status": "unresolved",
            "confidence": 0.61,
            "doc_id": "file_2",
            "locator_type": "paragraph",
            "locator_value": "p4:l2",
            "source_snippet": "missing data",
        },
        {
            "trace_id": "tr_003",
            "package_id": "pkg_1",
            "concept_id": "revenue_total",
            "status": "verified",
            "confidence": 0.99,
            "doc_id": "file_1",
            "locator_type": "cell",
            "locator_value": "B2",
            "source_snippet": "Revenue total 500",
        },
    ]


def test_metric_snapshot_and_taxonomy_summary() -> None:
    report = _sample_eval_report()
    snapshot = build_metric_snapshot(report)

    assert snapshot["gate_pass"] is True
    assert len(snapshot["metrics"]) == 5
    assert all(entry["pass"] for entry in snapshot["metrics"])

    taxonomy = summarize_failure_taxonomy(report)
    assert taxonomy["total_failures"] == 11
    assert taxonomy["top_categories"][0]["category"] == "extraction_errors"

    readiness = build_readiness_summary(report, streak=3, required_streak=3)
    assert readiness["release_ready"] is True


def test_build_design_partner_package_writes_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "readiness"

    outputs = build_design_partner_package(
        output_dir=output_dir,
        eval_report=_sample_eval_report(),
        traces=_sample_traces(),
        streak=2,
        required_streak=3,
    )

    for file_path in outputs.values():
        assert file_path.exists()

    readiness = read_json(outputs["readiness_summary"])
    assert readiness["release_ready"] is False

    traces = read_json(outputs["representative_traces"])["traces"]
    selected = select_representative_traces(_sample_traces())
    assert traces == selected
    assert traces[0]["status"] == "unresolved"

    artifact = outputs["trust_artifact"].read_text(encoding="utf-8")
    assert "Design Partner Trust Artifact" in artifact
