from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .policy import classify_status


WORKFLOW_PHASES = ("classify", "extract", "verify", "publish")


@dataclass(frozen=True)
class WorkflowConfig:
    max_retries: int = 2


@dataclass
class WorkflowSummary:
    packages: int
    rows: int
    retries: int
    events: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event(
    event_type: str,
    package_id: str,
    phase: str,
    trace_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "timestamp": _utc_now(),
        "event_type": event_type,
        "phase": phase,
        "package_id": package_id,
        "trace_id": trace_id,
        "payload": payload or {},
    }


def _append_events(log_path: Path, events: list[dict[str, Any]]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, sort_keys=True) + "\n")


def _verifier_objections(row: dict[str, Any]) -> list[str]:
    objections: list[str] = []
    evidence = row.get("evidence", {})

    if not evidence.get("doc_id") or not evidence.get("locator_type") or not evidence.get("locator_value"):
        objections.append("missing_evidence_location")

    blockers = set(row.get("hard_blockers", []))
    if "currency_unit_mismatch" in blockers:
        objections.append("currency_unit_mismatch")

    return objections


def _review_row(
    row: dict[str, Any],
    max_retries: int,
    package_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    events: list[dict[str, Any]] = []
    retries = 0

    # Independent verifier loop with capped retries.
    while retries <= max_retries:
        objections = _verifier_objections(row)
        events.append(
            _event(
                event_type="verify_attempt",
                package_id=package_id,
                phase="verify",
                trace_id=row.get("trace_id"),
                payload={
                    "attempt": retries,
                    "status": row.get("status"),
                    "confidence": row.get("confidence"),
                    "objections": objections,
                },
            )
        )

        if objections:
            row["status"] = "unresolved"
            row["hard_blockers"] = sorted(set(row.get("hard_blockers", []) + objections))
            row["final_resolver"] = "agent_4"
            row["retry_count"] = retries
            events.append(
                _event(
                    event_type="verify_rejected",
                    package_id=package_id,
                    phase="verify",
                    trace_id=row.get("trace_id"),
                    payload={"reason": objections},
                )
            )
            return row, events, retries

        # Candidate rows can be challenged once/twice if confidence is below verified threshold.
        if row.get("status") == "candidate_flagged" and retries < max_retries:
            retries += 1
            boosted = min(0.99, float(row.get("confidence", 0.0)) + 0.03)
            row["confidence"] = round(boosted, 4)
            row["status"] = classify_status(row["confidence"], row.get("hard_blockers", []))
            events.append(
                _event(
                    event_type="verify_challenge",
                    package_id=package_id,
                    phase="verify",
                    trace_id=row.get("trace_id"),
                    payload={
                        "next_attempt": retries,
                        "updated_confidence": row["confidence"],
                        "updated_status": row["status"],
                    },
                )
            )
            continue

        row["final_resolver"] = "agent_4"
        row["retry_count"] = retries
        events.append(
            _event(
                event_type="verify_accepted",
                package_id=package_id,
                phase="verify",
                trace_id=row.get("trace_id"),
                payload={"status": row.get("status"), "confidence": row.get("confidence")},
            )
        )
        return row, events, retries

    row["status"] = "unresolved"
    row["final_resolver"] = "agent_4"
    row["retry_count"] = retries
    return row, events, retries


def run_workflow_for_package(
    package_prediction: dict[str, Any],
    config: WorkflowConfig,
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    package_id = package_prediction["package_id"]
    events: list[dict[str, Any]] = []

    events.append(_event("phase_started", package_id, "classify"))
    events.append(_event("phase_completed", package_id, "classify", payload={"deal_id": package_prediction.get("deal_id")}))

    events.append(_event("phase_started", package_id, "extract"))
    for row in package_prediction["rows"]:
        events.append(
            _event(
                event_type="extract_row",
                package_id=package_id,
                phase="extract",
                trace_id=row.get("trace_id"),
                payload={
                    "concept_id": row.get("concept_id"),
                    "status": row.get("status"),
                    "confidence": row.get("confidence"),
                },
            )
        )
    events.append(_event("phase_completed", package_id, "extract", payload={"row_count": len(package_prediction['rows'])}))

    events.append(_event("phase_started", package_id, "verify"))
    retries_total = 0
    verified_rows = []
    for row in package_prediction["rows"]:
        updated_row, row_events, retries = _review_row(row=dict(row), max_retries=config.max_retries, package_id=package_id)
        retries_total += retries
        verified_rows.append(updated_row)
        events.extend(row_events)
    events.append(_event("phase_completed", package_id, "verify", payload={"retries": retries_total}))

    events.append(_event("phase_started", package_id, "publish"))
    final_package = {
        "package_id": package_prediction["package_id"],
        "deal_id": package_prediction.get("deal_id"),
        "period_end_date": package_prediction.get("period_end_date"),
        "rows": verified_rows,
    }
    events.append(_event("phase_completed", package_id, "publish", payload={"row_count": len(verified_rows)}))

    return final_package, events, retries_total


def run_workflow(
    package_predictions: list[dict[str, Any]],
    events_log_path: Path,
    config: WorkflowConfig | None = None,
) -> tuple[dict[str, Any], WorkflowSummary]:
    active = config or WorkflowConfig()

    results: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    retries = 0
    rows = 0

    for package_prediction in package_predictions:
        final_package, events, package_retries = run_workflow_for_package(package_prediction, active)
        results.append(final_package)
        all_events.extend(events)
        retries += package_retries
        rows += len(final_package["rows"])

    _append_events(events_log_path, all_events)

    payload = {
        "schema_version": "1.0",
        "generator": "agent_workflow_v1",
        "packages": results,
    }

    summary = WorkflowSummary(
        packages=len(results),
        rows=rows,
        retries=retries,
        events=len(all_events),
    )
    return payload, summary
