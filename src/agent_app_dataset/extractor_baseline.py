from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .constants import CONCEPT_LABELS, STARTER_CONCEPT_IDS
from .io_utils import read_json, write_json
from .normalization import normalize_value
from .policy import classify_status
from .source_grounding import unique_trustworthy_anchors


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


def _source_anchors_for_label_row(
    *,
    package_id: str,
    concept_id: str,
    concept_label: str,
    trace_id: str,
    label_row: dict[str, Any],
    normalized_value: float | None,
    unit_currency: str,
) -> list[dict[str, Any]]:
    explicit = label_row.get("source_anchors")
    if not isinstance(explicit, list):
        explicit = label_row.get("candidate_anchors")
    anchors: list[dict[str, Any]] = []
    if isinstance(explicit, list):
        for index, anchor in enumerate(explicit, start=1):
            if not isinstance(anchor, dict):
                continue
            anchors.append(
                {
                    "anchor_id": str(anchor.get("anchor_id", "")).strip() or f"{trace_id}:cand:{index}",
                    "doc_id": str(anchor.get("doc_id", "")).strip(),
                    "doc_name": str(anchor.get("doc_name", "")).strip(),
                    "page_or_sheet": str(anchor.get("page_or_sheet", "")).strip(),
                    "locator_type": str(anchor.get("locator_type", "")).strip(),
                    "locator_value": str(anchor.get("locator_value", "")).strip(),
                    "source_snippet": str(anchor.get("source_snippet", "")).strip(),
                    "raw_value_text": str(anchor.get("raw_value_text", "")).strip(),
                    "normalized_value": anchor.get("normalized_value"),
                    "unit_currency": str(anchor.get("unit_currency", "")).strip() or unit_currency,
                    "concept_id": concept_id,
                    "concept_label": concept_label,
                    "period_id": package_id,
                    "trace_id": trace_id,
                    "source_role": str(anchor.get("source_role", "")).strip() or "submitted_source",
                    "confidence": anchor.get("confidence"),
                }
            )

    evidence = label_row.get("evidence", {}) if isinstance(label_row.get("evidence"), dict) else {}
    anchors.append(
        {
            "anchor_id": f"{trace_id}:primary",
            "doc_id": str(evidence.get("doc_id", "")).strip(),
            "doc_name": str(evidence.get("doc_name", "")).strip(),
            "page_or_sheet": str(evidence.get("page_or_sheet", "")).strip(),
            "locator_type": str(evidence.get("locator_type", "")).strip(),
            "locator_value": str(evidence.get("locator_value", "")).strip(),
            "source_snippet": str(evidence.get("source_snippet", "")).strip(),
            "raw_value_text": str(label_row.get("raw_value_text", "")).strip(),
            "normalized_value": normalized_value,
            "unit_currency": unit_currency,
            "concept_id": concept_id,
            "concept_label": concept_label,
            "period_id": package_id,
            "trace_id": trace_id,
            "source_role": "submitted_source",
            "confidence": label_row.get("labeler_confidence"),
        }
    )

    return unique_trustworthy_anchors(anchors, max_items=8)


def _requirement_anchor_for_label_row(label_row: dict[str, Any]) -> dict[str, Any] | None:
    raw = label_row.get("requirement_anchor")
    if not isinstance(raw, dict):
        raw = label_row.get("reporting_requirement_anchor")
    if not isinstance(raw, dict):
        return None

    return {
        "doc_id": str(raw.get("doc_id", "")).strip(),
        "doc_name": str(raw.get("doc_name", "")).strip(),
        "locator_type": str(raw.get("locator_type", "")).strip(),
        "locator_value": str(raw.get("locator_value", "")).strip(),
        "page_or_sheet": str(raw.get("page_or_sheet", "")).strip(),
        "source_snippet": str(raw.get("source_snippet", "")).strip(),
        "required_concept_id": str(raw.get("required_concept_id", "")).strip(),
        "required_concept_label": str(raw.get("required_concept_label", "")).strip(),
        "obligation_type": str(raw.get("obligation_type", "")).strip() or "reporting_requirement",
        "source_role": str(raw.get("source_role", "")).strip() or "deal_reporting_document",
        "trace_id": str(raw.get("trace_id", "")).strip(),
    }


def _extraction_reason_for_label_row(label_row: dict[str, Any]) -> dict[str, Any]:
    candidate_count_raw = label_row.get("candidate_count")
    try:
        candidate_count = int(candidate_count_raw)
    except Exception:
        candidate_count = 0

    return {
        "extraction_reason_code": str(label_row.get("extraction_reason_code", "")).strip() or None,
        "extraction_reason_label": str(label_row.get("extraction_reason_label", "")).strip() or None,
        "extraction_reason_detail": str(label_row.get("extraction_reason_detail", "")).strip() or None,
        "uncertainty_source": str(label_row.get("uncertainty_source", "")).strip() or None,
        "match_basis": str(label_row.get("match_basis", "")).strip() or None,
        "source_modality": str(label_row.get("source_modality", "")).strip() or None,
        "candidate_count": max(candidate_count, 0),
        "expected_section_state": str(label_row.get("expected_section_state", "")).strip() or None,
    }


def extract_package_predictions(
    package_payload: dict[str, Any],
    label_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    labels_by_concept = _label_index(label_payload.get("rows", [])) if label_payload else {}
    dictionary_version = (
        str(label_payload.get("dictionary_version", "v1.0"))
        if label_payload is not None
        else "v1.0"
    )
    extracted_at = datetime.now(timezone.utc).isoformat()
    files = package_payload.get("files", [])
    first_file = files[0] if files else {}
    fallback_doc_id = str(first_file.get("file_id", ""))
    fallback_doc_name = str(first_file.get("filename", ""))
    fallback_doc_type = str(first_file.get("doc_type", "")).upper()
    fallback_page = "Page 1"
    if fallback_doc_type == "XLSX":
        fallback_page = "Sheet: unknown"
    if not fallback_doc_id:
        fallback_page = "Package Context"

    rows: list[dict[str, Any]] = []
    for concept_id in STARTER_CONCEPT_IDS:
        label_row = labels_by_concept.get(concept_id)

        if not label_row:
            rows.append(
                {
                    "concept_id": concept_id,
                    "label": CONCEPT_LABELS[concept_id],
                    "status": "unresolved",
                    "dictionary_version": dictionary_version,
                    "raw_value_text": "",
                    "normalized_value": None,
                    "current_value": None,
                    "unit_currency": "USD",
                    "confidence": 0.0,
                    "hard_blockers": ["missing_label_evidence"],
                    "trace_id": f"tr_{package_payload['package_id']}_{concept_id}_missing",
                    "source_anchors": [],
                    "evidence_link": {
                        "doc_id": fallback_doc_id,
                        "doc_name": fallback_doc_name,
                        "page_or_sheet": fallback_page,
                        "locator_type": "paragraph",
                        "locator_value": "unresolved:missing_label_row",
                    },
                    "evidence": {
                        "doc_id": fallback_doc_id,
                        "doc_name": fallback_doc_name,
                        "page_or_sheet": fallback_page,
                        "locator_type": "paragraph",
                        "locator_value": "unresolved:missing_label_row",
                        "source_snippet": "No label row available for this concept in eval labeling payload.",
                        "raw_value_text": "",
                        "normalized_value": None,
                        "unit_currency": "USD",
                        "extractor_agent_id": "agent_3",
                        "verifier_agent_id": "agent_4",
                        "trace_id": f"tr_{package_payload['package_id']}_{concept_id}_missing",
                        "extracted_at": extracted_at,
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
        trace_id = label_row.get("trace_id", f"tr_{package_payload['package_id']}_{concept_id}")
        source_anchors = _source_anchors_for_label_row(
            package_id=str(package_payload["package_id"]),
            concept_id=concept_id,
            concept_label=CONCEPT_LABELS[concept_id],
            trace_id=str(trace_id),
            label_row=label_row,
            normalized_value=normalization.normalized_value,
            unit_currency=normalization.unit_currency,
        )
        requirement_anchor = _requirement_anchor_for_label_row(label_row)
        extraction_reason = _extraction_reason_for_label_row(label_row)

        rows.append(
            {
                "concept_id": concept_id,
                "label": CONCEPT_LABELS[concept_id],
                "status": status,
                "dictionary_version": dictionary_version,
                "raw_value_text": raw_value_text,
                "normalized_value": normalization.normalized_value,
                "current_value": normalization.normalized_value,
                "unit_currency": normalization.unit_currency,
                "confidence": round(confidence, 4),
                "hard_blockers": blockers,
                "trace_id": trace_id,
                **extraction_reason,
                "source_anchors": source_anchors,
                "requirement_anchor": requirement_anchor,
                "evidence_link": {
                    "doc_id": evidence.get("doc_id", ""),
                    "doc_name": evidence.get("doc_name", ""),
                    "page_or_sheet": evidence.get("page_or_sheet", ""),
                    "locator_type": evidence.get("locator_type", "paragraph"),
                    "locator_value": evidence.get("locator_value", ""),
                },
                "evidence": {
                    "doc_id": evidence.get("doc_id", ""),
                    "doc_name": evidence.get("doc_name", ""),
                    "page_or_sheet": evidence.get("page_or_sheet", ""),
                    "locator_type": evidence.get("locator_type", "paragraph"),
                    "locator_value": evidence.get("locator_value", ""),
                    "source_snippet": evidence.get("source_snippet", ""),
                    "raw_value_text": raw_value_text,
                    "normalized_value": normalization.normalized_value,
                    "unit_currency": normalization.unit_currency,
                    "extractor_agent_id": "agent_3",
                    "verifier_agent_id": "agent_4",
                    "trace_id": trace_id,
                    "extracted_at": extracted_at,
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
