from __future__ import annotations

from pathlib import Path
from typing import Any

from .agent_workflow import WorkflowConfig, run_workflow
from .constants import STARTER_CONCEPT_IDS
from .extractor_baseline import extract_package_predictions
from .io_utils import read_json


def _build_fallback_prediction(package_manifest: dict[str, Any]) -> dict[str, Any]:
    files = package_manifest.get("files", [])
    first_file = files[0] if files else {}

    rows: list[dict[str, Any]] = []
    for concept_id in STARTER_CONCEPT_IDS:
        rows.append(
            {
                "concept_id": concept_id,
                "status": "unresolved",
                "normalized_value": None,
                "unit_currency": "USD",
                "confidence": 0.0,
                "hard_blockers": ["missing_label_evidence"],
                "trace_id": f"tr_{package_manifest['package_id']}_{concept_id}_fallback",
                "evidence": {
                    "doc_id": first_file.get("file_id", ""),
                    "locator_type": "paragraph",
                    "locator_value": "",
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
    labels_dir: Path,
    events_log_path: Path,
    max_retries: int = 2,
) -> tuple[dict[str, Any], dict[str, Any]]:
    label_file = labels_dir / f"{package_manifest['package_id']}.ground_truth.json"

    if label_file.exists():
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
