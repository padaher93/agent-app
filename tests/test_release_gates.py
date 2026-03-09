from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from agent_app_dataset.constants import STARTER_CONCEPT_IDS
from agent_app_dataset.io_utils import read_json, write_json
from agent_app_dataset.release_gates import (
    check_strict_config,
    run_pre_partner_readiness,
    run_llm_smoke,
    validate_shadow_partition,
)


def _write_package(packages_dir: Path, package_id: str, storage_uri: str) -> None:
    numeric = package_id.split("_")[1]
    write_json(
        packages_dir / f"{package_id}.json",
        {
            "schema_version": "1.0",
            "package_id": package_id,
            "deal_id": "deal_release",
            "period_end_date": "2026-01-31",
            "source_email_id": f"email_{numeric}",
            "received_at": "2026-03-06T12:00:00+00:00",
            "files": [
                {
                    "file_id": f"file_{numeric}_01",
                    "source_id": f"src_{numeric}",
                    "doc_type": "PDF",
                    "filename": "borrower_update.pdf",
                    "storage_uri": storage_uri,
                    "checksum": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "pages_or_sheets": 1,
                }
            ],
            "source_ids": [f"src_{numeric}"],
            "variant_tags": [],
            "quality_flags": [],
            "labeling_workflow": {
                "primary_labeler_status": "completed",
                "reviewer_status": "completed",
                "adjudication_status": "not_required",
            },
            "notes": "release gate package",
        },
    )


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
                    "doc_id": "file_release_01",
                    "doc_name": "borrower_update.pdf",
                    "page_or_sheet": "Page 1",
                    "locator_type": "paragraph",
                    "locator_value": "p1:l1",
                    "source_snippet": "Revenue total: 100.00",
                },
            }
        )

    write_json(
        labels_dir / f"{package_id}.ground_truth.json",
        {
            "schema_version": "1.0",
            "package_id": package_id,
            "deal_id": "deal_release",
            "period_end_date": "2026-01-31",
            "dictionary_version": "v1.0",
            "labeling": {
                "primary_labeler": "qa",
                "reviewer": "qa_reviewer",
                "adjudication_required": False,
            },
            "rows": rows,
        },
    )


def test_validate_shadow_partition_flags_missing_label(tmp_path: Path) -> None:
    packages_dir = tmp_path / "packages"
    labels_dir = tmp_path / "labels"
    packages_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    sample = tmp_path / "sample.pdf"
    sample.write_bytes(b"%PDF-1.7\nsample")
    _write_package(packages_dir, "pkg_0001", "s3://unit-tests/pkg_0001/borrower_update.pdf")

    result = validate_shadow_partition(
        packages_dir=packages_dir,
        labels_dir=labels_dir,
        min_packages=1,
        min_deals=1,
        min_periods_per_deal=1,
        require_supported_storage=False,
    )
    assert result.passed is False
    assert any("missing_label_for_package" in issue for issue in result.issues)


def test_validate_shadow_partition_passes_minimum_requirements(tmp_path: Path) -> None:
    packages_dir = tmp_path / "packages"
    labels_dir = tmp_path / "labels"
    packages_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    sample = tmp_path / "sample.pdf"
    sample.write_bytes(b"%PDF-1.7\nsample")
    _write_package(packages_dir, "pkg_0002", "s3://unit-tests/pkg_0002/borrower_update.pdf")
    _write_package(packages_dir, "pkg_0003", "s3://unit-tests/pkg_0003/borrower_update.pdf")
    _write_label(labels_dir, "pkg_0002")
    _write_label(labels_dir, "pkg_0003")

    # ensure at least two periods in the same deal
    first = (packages_dir / "pkg_0002.json").read_text(encoding="utf-8")
    second = (packages_dir / "pkg_0003.json").read_text(encoding="utf-8")
    assert "deal_release" in first and "deal_release" in second

    result = validate_shadow_partition(
        packages_dir=packages_dir,
        labels_dir=labels_dir,
        min_packages=2,
        min_deals=1,
        min_periods_per_deal=1,
        require_supported_storage=False,
    )
    assert result.passed is True


def test_run_llm_smoke_fails_on_unresolved_hard_blocker(tmp_path: Path) -> None:
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir(parents=True, exist_ok=True)
    sample = tmp_path / "sample.pdf"
    sample.write_bytes(b"%PDF-1.7\nsample")
    _write_package(packages_dir, "pkg_0004", str(sample))

    def _fake_process_fn(**kwargs):
        package_manifest = kwargs["package_manifest"]
        return (
            {
                "packages": [
                    {
                        "package_id": package_manifest["package_id"],
                        "rows": [
                            {
                                "concept_id": "revenue_total",
                                "status": "unresolved",
                                "hard_blockers": ["missing_evidence_location"],
                                "evidence": {
                                    "doc_id": "file_release_01",
                                    "locator_type": "paragraph",
                                    "locator_value": "p1:l1",
                                },
                            }
                        ],
                    }
                ]
            },
            {"status": "needs_review"},
        )

    result = run_llm_smoke(
        package_manifest_paths=[packages_dir / "pkg_0004.json"],
        labels_dir=None,
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        max_retries=1,
        fail_on_unresolved_hard_blocker=True,
        max_candidate_flagged=None,
        extraction_mode="llm",
        process_fn=_fake_process_fn,
    )
    assert result.passed is False
    assert result.unresolved_hard_blockers == 1


def test_check_strict_config_accepts_dev_profile_without_secrets() -> None:
    result = check_strict_config(
        runtime_profile="dev",
        internal_token=None,
        require_https=False,
        openai_api_key=None,
        internal_api_token=None,
        internal_api_require_https=False,
        postmark_server_token=None,
        outbound_email_mode="none",
        outbound_postmark_server_token=None,
        mailgun_signing_key=None,
        sendgrid_inbound_token=None,
        attachment_storage_mode="local",
        attachment_storage_s3_bucket=None,
    )
    assert result.passed is True
    assert result.issues == []


def test_run_pre_partner_readiness_passes_and_blocks_production_launch(tmp_path: Path) -> None:
    packages_dir = tmp_path / "packages"
    labels_dir = tmp_path / "labels"
    runtime_dir = tmp_path / "runtime"
    packages_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    _write_package(packages_dir, "pkg_0005", "s3://unit-tests/pkg_0005/borrower_update.pdf")
    _write_label(labels_dir, "pkg_0005")

    result = run_pre_partner_readiness(
        runtime_profile="dev",
        strict_config={
            "internal_token": None,
            "require_https": False,
            "openai_api_key": None,
            "internal_api_token": None,
            "internal_api_require_https": False,
            "postmark_server_token": None,
            "outbound_email_mode": "none",
            "outbound_postmark_server_token": None,
            "mailgun_signing_key": None,
            "sendgrid_inbound_token": None,
            "attachment_storage_mode": "local",
            "attachment_storage_s3_bucket": None,
        },
        smoke_package_manifest_paths=[packages_dir / "pkg_0005.json"],
        smoke_events_log_path=runtime_dir / "smoke_events.jsonl",
        smoke_labels_dir=labels_dir,
        smoke_max_retries=1,
        smoke_fail_on_unresolved_hard_blocker=False,
        smoke_max_candidate_flagged=None,
        smoke_extraction_mode="eval",
        proxy_packages_dir=packages_dir,
        proxy_labels_dir=labels_dir,
        proxy_min_packages=1,
        proxy_min_deals=1,
        proxy_min_periods_per_deal=1,
        proxy_require_supported_storage=False,
        proxy_eval_kwargs={
            "events_log_path": runtime_dir / "proxy_eval_events.jsonl",
            "extraction_mode": "eval",
            "max_retries": 1,
            "predictions_output_path": runtime_dir / "proxy_predictions.json",
            "report_output_path": runtime_dir / "proxy_report.json",
            "history_dir": runtime_dir / "history",
            "dataset_version": "proxy_test",
            "pipeline_version": "unit",
            "required_streak": 1,
            "min_packages": 1,
            "failure_taxonomy": [],
            "blocking_incident": False,
            "incident_summary": "",
        },
        summary_output_path=runtime_dir / "pre_partner_summary.json",
        shadow_eval_fn=lambda **_: SimpleNamespace(release_ready=True),
    )

    summary = read_json(runtime_dir / "pre_partner_summary.json")
    assert result.passed is True
    assert result.production_launch_ready is False
    assert summary["blocked_by"] == ["real_shadow_partner_gate_pending"]
