from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .eval_metrics import consecutive_passes, evaluate, write_eval_report
from .internal_processing import process_package_manifest
from .io_utils import read_json, write_json
from .schemas import validate_with_schema


@dataclass(frozen=True)
class ShadowEvalSummary:
    package_count: int
    report_path: Path
    history_path: Path
    gate_pass: bool
    consecutive_pass_streak: int
    release_ready: bool


def run_shadow_eval(
    *,
    packages_dir: Path,
    labels_dir: Path,
    events_log_path: Path,
    extraction_mode: str,
    max_retries: int,
    predictions_output_path: Path,
    report_output_path: Path,
    history_dir: Path,
    dataset_version: str,
    pipeline_version: str,
    required_streak: int,
    min_packages: int,
    failure_taxonomy: list[dict[str, Any]] | None = None,
    blocking_incident: bool = False,
    incident_summary: str = "",
) -> ShadowEvalSummary:
    history_dir.mkdir(parents=True, exist_ok=True)
    predictions_output_path.parent.mkdir(parents=True, exist_ok=True)
    report_output_path.parent.mkdir(parents=True, exist_ok=True)
    events_log_path.parent.mkdir(parents=True, exist_ok=True)

    previous_report = None
    history_reports = sorted(history_dir.glob("*.json"))
    if history_reports:
        previous_report = read_json(history_reports[-1])

    package_files = sorted(packages_dir.glob("*.json"))
    predictions_payload: dict[str, Any] = {
        "schema_version": "1.0",
        "generator": f"shadow_eval_{extraction_mode}_v1",
        "packages": [],
    }

    for package_file in package_files:
        package_manifest = read_json(package_file)
        workflow_payload, _summary = process_package_manifest(
            package_manifest=package_manifest,
            labels_dir=labels_dir,
            events_log_path=events_log_path,
            max_retries=max_retries,
            extraction_mode=extraction_mode,
        )
        predictions_payload["packages"].append(workflow_payload["packages"][0])

    write_json(predictions_output_path, predictions_payload)

    result = evaluate(
        ground_truth_dir=labels_dir,
        predictions_file=predictions_output_path,
        previous_report=previous_report,
        blocking_incident=blocking_incident,
    )

    report = write_eval_report(
        output_report=report_output_path,
        dataset_version=dataset_version,
        pipeline_version=pipeline_version,
        result=result,
        failure_taxonomy=failure_taxonomy or [],
    )
    report["incident_status"] = {
        "blocking": blocking_incident,
        "summary": incident_summary,
    }
    if len(package_files) < min_packages:
        report["gate_pass"] = False
        report["failures"] = sorted(set([*report.get("failures", []), "min_package_count_not_met"]))

    schema_errors = validate_with_schema("eval_report", report)
    if schema_errors:
        messages = "; ".join(schema_errors)
        raise ValueError(f"eval_report_schema_validation_failed:{messages}")

    write_json(report_output_path, report)

    history_file = history_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    write_json(history_file, report)

    streak = consecutive_passes(history_dir)
    release_ready = bool(report.get("gate_pass", False)) and streak >= int(required_streak)

    return ShadowEvalSummary(
        package_count=len(package_files),
        report_path=report_output_path,
        history_path=history_file,
        gate_pass=bool(report.get("gate_pass", False)),
        consecutive_pass_streak=streak,
        release_ready=release_ready,
    )
