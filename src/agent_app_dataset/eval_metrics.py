from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .constants import QUALITY_THRESHOLDS
from .io_utils import read_json, write_json


@dataclass
class EvalResult:
    metrics: dict[str, float]
    gate_pass: bool
    failures: list[str]
    row_counts: dict[str, int]
    per_concept_verified_precision: dict[str, float]
    regressions: dict[str, float | bool]
    incident_blocking: bool


def _index_ground_truth(ground_truth_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for file in sorted(ground_truth_dir.glob("*.ground_truth.json")):
        payload = read_json(file)
        package_id = payload["package_id"]
        for row in payload["rows"]:
            index[(package_id, row["concept_id"])] = row
    return index


def _index_predictions(predictions_file: Path) -> dict[tuple[str, str], dict[str, Any]]:
    payload = read_json(predictions_file)
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for package in payload["packages"]:
        package_id = package["package_id"]
        for row in package["rows"]:
            index[(package_id, row["concept_id"])] = row
    return index


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def evaluate(
    ground_truth_dir: Path,
    predictions_file: Path,
    thresholds: dict[str, float] | None = None,
    previous_report: dict[str, Any] | None = None,
    blocking_incident: bool = False,
) -> EvalResult:
    limits = thresholds or QUALITY_THRESHOLDS

    gt = _index_ground_truth(ground_truth_dir)
    pred = _index_predictions(predictions_file)

    total_rows = len(gt)
    verified_rows = 0
    verified_correct = 0
    false_verified = 0
    unresolved_rows = 0

    evidence_total = 0
    evidence_correct = 0

    package_expected: dict[str, int] = {}
    package_present: dict[str, set[str]] = {}
    concept_verified_rows: dict[str, int] = {}
    concept_verified_correct: dict[str, int] = {}

    for (package_id, concept_id), gt_row in gt.items():
        package_expected[package_id] = package_expected.get(package_id, 0) + 1
        package_present.setdefault(package_id, set())

        prediction = pred.get((package_id, concept_id))
        if prediction is None:
            continue

        package_present[package_id].add(concept_id)

        status = prediction["status"]
        if status == "unresolved":
            unresolved_rows += 1

        predicted_evidence = prediction.get("evidence", {})
        gt_evidence = gt_row.get("evidence", {})

        if predicted_evidence:
            evidence_total += 1
            same_doc = predicted_evidence.get("doc_id") == gt_evidence.get("doc_id")
            same_locator = (
                predicted_evidence.get("locator_type") == gt_evidence.get("locator_type")
                and predicted_evidence.get("locator_value") == gt_evidence.get("locator_value")
            )
            if same_doc and same_locator:
                evidence_correct += 1

        if status == "verified":
            verified_rows += 1
            concept_verified_rows[concept_id] = concept_verified_rows.get(concept_id, 0) + 1
            value_match = prediction.get("normalized_value") == gt_row.get("normalized_value")
            if value_match:
                verified_correct += 1
                concept_verified_correct[concept_id] = concept_verified_correct.get(concept_id, 0) + 1
            else:
                false_verified += 1

    completed_packages = 0
    for package_id, expected_count in package_expected.items():
        if len(package_present.get(package_id, set())) == expected_count:
            completed_packages += 1

    metrics = {
        "verified_precision": _safe_ratio(verified_correct, verified_rows),
        "evidence_link_accuracy": _safe_ratio(evidence_correct, evidence_total),
        "false_verified_rate": _safe_ratio(false_verified, verified_rows),
        "unresolved_rate": _safe_ratio(unresolved_rows, total_rows),
        "package_completion_rate": _safe_ratio(completed_packages, len(package_expected)),
    }

    failures: list[str] = []
    if metrics["verified_precision"] < limits["verified_precision_min"]:
        failures.append("verified_precision")
    if metrics["evidence_link_accuracy"] < limits["evidence_link_accuracy_min"]:
        failures.append("evidence_link_accuracy")
    if metrics["false_verified_rate"] >= limits["false_verified_rate_max"]:
        failures.append("false_verified_rate")
    if metrics["unresolved_rate"] > limits["unresolved_rate_max"]:
        failures.append("unresolved_rate")
    if metrics["package_completion_rate"] < limits["package_completion_rate_min"]:
        failures.append("package_completion_rate")

    per_concept_verified_precision: dict[str, float] = {}
    for concept_id, rows_count in concept_verified_rows.items():
        precision = _safe_ratio(concept_verified_correct.get(concept_id, 0), rows_count)
        per_concept_verified_precision[concept_id] = precision
        if precision < 0.95:
            failures.append(f"critical_concept_precision:{concept_id}")

    regressions: dict[str, float | bool] = {
        "verified_precision_delta": 0.0,
        "evidence_link_accuracy_delta": 0.0,
        "blocked": False,
    }
    if previous_report:
        prev_metrics = previous_report.get("metrics", {})
        prev_verified = float(prev_metrics.get("verified_precision", metrics["verified_precision"]))
        prev_evidence = float(prev_metrics.get("evidence_link_accuracy", metrics["evidence_link_accuracy"]))

        vp_delta = metrics["verified_precision"] - prev_verified
        ea_delta = metrics["evidence_link_accuracy"] - prev_evidence
        regressions["verified_precision_delta"] = round(vp_delta, 6)
        regressions["evidence_link_accuracy_delta"] = round(ea_delta, 6)

        if vp_delta < -0.01:
            failures.append("verified_precision_regression")
            regressions["blocked"] = True
        if ea_delta < -0.01:
            failures.append("evidence_link_accuracy_regression")
            regressions["blocked"] = True

    if blocking_incident:
        failures.append("security_data_integrity_incident")

    return EvalResult(
        metrics=metrics,
        gate_pass=(len(failures) == 0),
        failures=failures,
        row_counts={
            "total_rows": total_rows,
            "verified_rows": verified_rows,
            "evidence_rows": evidence_total,
            "packages": len(package_expected),
        },
        per_concept_verified_precision=per_concept_verified_precision,
        regressions=regressions,
        incident_blocking=blocking_incident,
    )


def write_eval_report(
    output_report: Path,
    dataset_version: str,
    pipeline_version: str,
    result: EvalResult,
    failure_taxonomy: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    report = {
        "dataset_version": dataset_version,
        "pipeline_version": pipeline_version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": result.metrics,
        "gate_pass": result.gate_pass,
        "failures": result.failures,
        "row_counts": result.row_counts,
        "failure_taxonomy": failure_taxonomy or [],
        "per_concept_verified_precision": result.per_concept_verified_precision,
        "regressions": result.regressions,
        "incident_status": {
            "blocking": result.incident_blocking,
        },
    }
    write_json(output_report, report)
    return report


def consecutive_passes(history_dir: Path) -> int:
    if not history_dir.exists():
        return 0

    reports = []
    for file in sorted(history_dir.glob("*.json")):
        payload = read_json(file)
        reports.append(payload)

    streak = 0
    for report in reversed(reports):
        if report.get("gate_pass"):
            streak += 1
        else:
            break

    return streak
