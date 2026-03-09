from __future__ import annotations

from pathlib import Path

from agent_app_dataset.eval_metrics import evaluate
from agent_app_dataset.io_utils import write_json


def _write_ground_truth(path: Path) -> None:
    payload = {
        "schema_version": "1.0",
        "package_id": "pkg_9999",
        "deal_id": "deal_eval",
        "period_end_date": "2026-01-31",
        "dictionary_version": "v1.0",
        "labeling": {
            "primary_labeler": "qa",
            "reviewer": "qa",
            "adjudication_required": False,
        },
        "rows": [
            {
                "trace_id": "tr_pkg_9999_revenue_total",
                "concept_id": "revenue_total",
                "period_end_date": "2026-01-31",
                "raw_value_text": "$100",
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
                    "doc_id": "file_1",
                    "doc_name": "x.xlsx",
                    "page_or_sheet": "Sheet1",
                    "locator_type": "cell",
                    "locator_value": "B2",
                    "source_snippet": "Revenue total 100",
                },
            }
        ],
    }
    write_json(path, payload)


def _write_predictions(path: Path) -> None:
    payload = {
        "schema_version": "1.0",
        "generator": "test",
        "packages": [
            {
                "package_id": "pkg_9999",
                "deal_id": "deal_eval",
                "period_end_date": "2026-01-31",
                "rows": [
                    {
                        "concept_id": "revenue_total",
                        "status": "verified",
                        "normalized_value": 80.0,
                        "confidence": 0.95,
                        "evidence": {
                            "doc_id": "file_1",
                            "locator_type": "cell",
                            "locator_value": "B9",
                        },
                    }
                ],
            }
        ],
    }
    write_json(path, payload)


def test_critical_concept_floor_and_regression_rules(tmp_path: Path) -> None:
    gt_dir = tmp_path / "gt"
    gt_dir.mkdir(parents=True, exist_ok=True)
    pred_file = tmp_path / "predictions.json"

    _write_ground_truth(gt_dir / "pkg_9999.ground_truth.json")
    _write_predictions(pred_file)

    previous_report = {
        "metrics": {
            "verified_precision": 1.0,
            "evidence_link_accuracy": 1.0,
        }
    }

    result = evaluate(
        ground_truth_dir=gt_dir,
        predictions_file=pred_file,
        previous_report=previous_report,
    )

    assert result.gate_pass is False
    assert "critical_concept_precision:revenue_total" in result.failures
    assert "verified_precision_regression" in result.failures
    assert "evidence_link_accuracy_regression" in result.failures
    assert result.regressions["blocked"] is True


def test_blocking_security_incident_forces_gate_fail(tmp_path: Path) -> None:
    gt_dir = tmp_path / "gt_incident"
    gt_dir.mkdir(parents=True, exist_ok=True)
    pred_file = tmp_path / "predictions_incident.json"

    _write_ground_truth(gt_dir / "pkg_9999.ground_truth.json")
    _write_predictions(pred_file)

    result = evaluate(
        ground_truth_dir=gt_dir,
        predictions_file=pred_file,
        blocking_incident=True,
    )

    assert result.gate_pass is False
    assert "security_data_integrity_incident" in result.failures
