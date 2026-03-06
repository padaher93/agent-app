from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .constants import STARTER_CONCEPT_IDS
from .io_utils import read_json, write_json
from .normalization import normalize_value
from .policy import classify_status


@dataclass
class ExtractionSummary:
    packages: int
    rows: int
    status_counts: dict[str, int]


def _build_hard_blockers(flags: list[str], evidence: dict[str, Any], unresolved_reason: str | None) -> list[str]:
    blockers: list[str] = []

    if unresolved_reason:
        blockers.append(unresolved_reason)

    if flags:
        if "missing_schedule" in flags:
            blockers.append("missing_source_schedule")
        if "currency_inconsistency" in flags:
            blockers.append("currency_unit_mismatch")
        if "label_drift" in flags:
            blockers.append("definition_label_drift")

    required_evidence_keys = ("doc_id", "locator_type", "locator_value")
    if not all(evidence.get(key) for key in required_evidence_keys):
        blockers.append("missing_evidence_location")

    return sorted(set(blockers))


def _label_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["concept_id"]: row for row in rows}


def extract_package_predictions(
    package_payload: dict[str, Any],
    label_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    labels_by_concept = _label_index(label_payload.get("rows", [])) if label_payload else {}

    rows: list[dict[str, Any]] = []
    for concept_id in STARTER_CONCEPT_IDS:
        label_row = labels_by_concept.get(concept_id)

        if not label_row:
            rows.append(
                {
                    "concept_id": concept_id,
                    "status": "unresolved",
                    "normalized_value": None,
                    "unit_currency": "USD",
                    "confidence": 0.0,
                    "hard_blockers": ["missing_label_evidence"],
                    "trace_id": f"tr_{package_payload['package_id']}_{concept_id}_missing",
                    "evidence": {
                        "doc_id": "",
                        "locator_type": "paragraph",
                        "locator_value": "",
                    },
                }
            )
            continue

        evidence = label_row.get("evidence", {})
        raw_value_text = label_row.get("raw_value_text", "")
        source_snippet = evidence.get("source_snippet", "")
        normalization = normalize_value(raw_value_text=raw_value_text, source_snippet=source_snippet)

        blockers = _build_hard_blockers(
            flags=label_row.get("flags", []),
            evidence=evidence,
            unresolved_reason=normalization.unresolved_reason,
        )

        confidence = float(label_row.get("labeler_confidence", 0.0))
        status = classify_status(confidence=confidence, hard_blockers=blockers)

        rows.append(
            {
                "concept_id": concept_id,
                "status": status,
                "normalized_value": normalization.normalized_value,
                "unit_currency": normalization.unit_currency,
                "confidence": round(confidence, 4),
                "hard_blockers": blockers,
                "trace_id": label_row.get("trace_id", f"tr_{package_payload['package_id']}_{concept_id}"),
                "evidence": {
                    "doc_id": evidence.get("doc_id", ""),
                    "locator_type": evidence.get("locator_type", "paragraph"),
                    "locator_value": evidence.get("locator_value", ""),
                },
            }
        )

    return {
        "package_id": package_payload["package_id"],
        "deal_id": package_payload["deal_id"],
        "period_end_date": package_payload["period_end_date"],
        "rows": rows,
    }


def build_predictions(
    packages_dir: Path,
    labels_dir: Path,
    output_file: Path,
) -> ExtractionSummary:
    package_files = sorted(packages_dir.glob("*.json"))

    status_counts = {
        "verified": 0,
        "candidate_flagged": 0,
        "unresolved": 0,
    }
    rows = 0
    packages: list[dict[str, Any]] = []

    for package_file in package_files:
        package_payload = read_json(package_file)
        label_file = labels_dir / f"{package_payload['package_id']}.ground_truth.json"
        label_payload = read_json(label_file) if label_file.exists() else None

        package_predictions = extract_package_predictions(package_payload, label_payload)
        packages.append(package_predictions)

        for row in package_predictions["rows"]:
            status_counts[row["status"]] += 1
            rows += 1

    payload = {
        "schema_version": "1.0",
        "generator": "extraction_baseline_v1",
        "packages": packages,
    }
    write_json(output_file, payload)

    return ExtractionSummary(packages=len(packages), rows=rows, status_counts=status_counts)
