from __future__ import annotations

from pathlib import Path
from typing import Any

from .agent_workflow import WorkflowConfig, run_workflow
from .constants import CONCEPT_LABELS, STARTER_CONCEPT_IDS
from .extractor_baseline import extract_package_predictions
from .io_utils import read_json
from .runtime_extractor import runtime_extract_package_predictions


def _build_fallback_prediction(package_manifest: dict[str, Any]) -> dict[str, Any]:
    files = package_manifest.get("files", [])
    first_file = files[0] if files else {}

    rows: list[dict[str, Any]] = []
    for concept_id in STARTER_CONCEPT_IDS:
        rows.append(
            {
                "concept_id": concept_id,
                "label": CONCEPT_LABELS[concept_id],
                "status": "unresolved",
                "dictionary_version": "v1.0",
                "raw_value_text": "",
                "normalized_value": None,
                "current_value": None,
                "unit_currency": "USD",
                "confidence": 0.0,
                "hard_blockers": ["missing_label_evidence"],
                "trace_id": f"tr_{package_manifest['package_id']}_{concept_id}_fallback",
                "evidence_link": {
                    "doc_id": first_file.get("file_id", ""),
                    "doc_name": first_file.get("filename", ""),
                    "page_or_sheet": "",
                    "locator_type": "paragraph",
                    "locator_value": "",
                },
                "evidence": {
                    "doc_id": first_file.get("file_id", ""),
                    "doc_name": first_file.get("filename", ""),
                    "page_or_sheet": "",
                    "locator_type": "paragraph",
                    "locator_value": "",
                    "source_snippet": "",
                    "raw_value_text": "",
                    "normalized_value": None,
                    "unit_currency": "USD",
                    "extractor_agent_id": "agent_3",
                    "verifier_agent_id": "agent_4",
                    "trace_id": f"tr_{package_manifest['package_id']}_{concept_id}_fallback",
                    "extracted_at": package_manifest.get("received_at", ""),
                },
            }
        )

    return {
        "package_id": package_manifest["package_id"],
        "deal_id": package_manifest["deal_id"],
        "period_end_date": package_manifest["period_end_date"],
        "rows": rows,
    }


def process_package_manifest(
    package_manifest: dict[str, Any],
    labels_dir: Path | None,
    events_log_path: Path,
    max_retries: int = 2,
    extraction_mode: str = "runtime",
) -> tuple[dict[str, Any], dict[str, Any]]:
    if extraction_mode not in {"runtime", "eval"}:
        raise ValueError(f"Unsupported extraction_mode: {extraction_mode}")

    base_prediction: dict[str, Any]
    if extraction_mode == "runtime":
        base_prediction = runtime_extract_package_predictions(package_manifest)
    else:
        label_file = (labels_dir / f"{package_manifest['package_id']}.ground_truth.json") if labels_dir else None
        if label_file is not None and label_file.exists():
            label_payload = read_json(label_file)
            base_prediction = extract_package_predictions(package_manifest, label_payload)
        else:
            base_prediction = _build_fallback_prediction(package_manifest)

    workflow_payload, summary = run_workflow(
        package_predictions=[base_prediction],
        events_log_path=events_log_path,
        config=WorkflowConfig(max_retries=max_retries),
    )

    rows = workflow_payload["packages"][0]["rows"]
    statuses = {row.get("status") for row in rows}
    if "unresolved" in statuses or "candidate_flagged" in statuses:
        lifecycle_status = "needs_review"
    else:
        lifecycle_status = "completed"

    result_summary = {
        "packages": summary.packages,
        "rows": summary.rows,
        "retries": summary.retries,
        "events": summary.events,
        "status": lifecycle_status,
    }

    return workflow_payload, result_summary
