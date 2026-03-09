from __future__ import annotations

from pathlib import Path

from agent_app_dataset.constants import STARTER_CONCEPT_IDS
from agent_app_dataset.io_utils import write_json
from agent_app_dataset.shadow_eval import run_shadow_eval


def _write_package(packages_dir: Path, package_id: str) -> None:
    payload = {
        "schema_version": "1.0",
        "package_id": package_id,
        "workspace_id": "ws_default",
        "deal_id": "deal_shadow",
        "period_end_date": "2026-01-31",
        "source_email_id": f"email_{package_id}",
        "received_at": "2026-03-06T12:00:00+00:00",
        "files": [
            {
                "file_id": "file_shadow_01",
                "source_id": "src_shadow_01",
                "doc_type": "PDF",
                "filename": "borrower_update.pdf",
                "storage_uri": "s3://shadow/borrower_update.pdf",
                "checksum": "shadow_checksum_01",
                "pages_or_sheets": 1,
            }
        ],
        "source_ids": ["src_shadow_01"],
        "variant_tags": [],
        "quality_flags": [],
        "labeling_workflow": {
            "primary_labeler_status": "completed",
            "reviewer_status": "completed",
            "adjudication_status": "not_required",
        },
        "notes": "shadow package",
    }
    write_json(packages_dir / f"{package_id}.json", payload)


def _write_label(labels_dir: Path, package_id: str) -> None:
    rows = []
    for concept_id in STARTER_CONCEPT_IDS:
        rows.append(
            {
                "trace_id": f"tr_{package_id}_{concept_id}",
                "concept_id": concept_id,
                "period_end_date": "2026-01-31",
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
                    "doc_id": "file_shadow_01",
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
        "deal_id": "deal_shadow",
        "period_end_date": "2026-01-31",
        "dictionary_version": "v1.0",
        "labeling": {
            "primary_labeler": "qa",
            "reviewer": "qa_reviewer",
            "adjudication_required": False,
        },
        "rows": rows,
    }
    write_json(labels_dir / f"{package_id}.ground_truth.json", payload)


def test_shadow_eval_passes_when_minimum_sample_met(tmp_path: Path) -> None:
    packages_dir = tmp_path / "packages"
    labels_dir = tmp_path / "labels"
    packages_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    _write_package(packages_dir, "pkg_shadow_001")
    _write_label(labels_dir, "pkg_shadow_001")

    summary = run_shadow_eval(
        packages_dir=packages_dir,
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        extraction_mode="eval",
        max_retries=2,
        predictions_output_path=tmp_path / "runtime" / "predictions.json",
        report_output_path=tmp_path / "runtime" / "report.json",
        history_dir=tmp_path / "history",
        dataset_version="real_shadow_test",
        pipeline_version="unit_test",
        required_streak=1,
        min_packages=1,
        failure_taxonomy=[],
        blocking_incident=False,
        incident_summary="none",
    )

    assert summary.package_count == 1
    assert summary.gate_pass is True
    assert summary.release_ready is True


def test_shadow_eval_fails_when_minimum_sample_not_met(tmp_path: Path) -> None:
    packages_dir = tmp_path / "packages"
    labels_dir = tmp_path / "labels"
    packages_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    _write_package(packages_dir, "pkg_shadow_002")
    _write_label(labels_dir, "pkg_shadow_002")

    summary = run_shadow_eval(
        packages_dir=packages_dir,
        labels_dir=labels_dir,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        extraction_mode="eval",
        max_retries=2,
        predictions_output_path=tmp_path / "runtime" / "predictions.json",
        report_output_path=tmp_path / "runtime" / "report.json",
        history_dir=tmp_path / "history",
        dataset_version="real_shadow_test",
        pipeline_version="unit_test",
        required_streak=1,
        min_packages=2,
        failure_taxonomy=[],
        blocking_incident=False,
        incident_summary="none",
    )

    assert summary.package_count == 1
    assert summary.gate_pass is False
    assert summary.release_ready is False

