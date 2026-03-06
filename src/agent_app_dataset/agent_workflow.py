from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from .policy import classify_status


WORKFLOW_PHASES = ("classify", "extract", "verify", "publish")
TERMINAL_STATUSES = ("verified", "candidate_flagged", "unresolved")
ALLOWED_TRANSITIONS = {
    "verified": {"verified", "unresolved"},
    "candidate_flagged": {"candidate_flagged", "verified", "unresolved"},
    "unresolved": {"unresolved"},
}


@dataclass(frozen=True)
class WorkflowConfig:
    max_retries: int = 2


@dataclass
class WorkflowSummary:
    packages: int
    rows: int
    retries: int
    events: int


class WorkflowTransitionError(ValueError):
    pass


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


def _compute_event_hash(record: dict[str, Any]) -> str:
    hash_payload = dict(record)
    hash_payload.pop("event_hash", None)
    canonical = json.dumps(hash_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def check_log_integrity(log_path: Path) -> list[str]:
    if not log_path.exists() or log_path.stat().st_size == 0:
        return []

    issues: list[str] = []
    prev_seq = 0
    prev_hash = "GENESIS"

    with log_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue

            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                issues.append(f"line {line_no}: invalid JSON ({exc})")
                continue

            required = ("sequence_id", "previous_hash", "event_hash")
            missing = [key for key in required if key not in record]
            if missing:
                issues.append(f"line {line_no}: missing integrity fields {missing}")
                continue

            sequence_id = record["sequence_id"]
            if not isinstance(sequence_id, int):
                issues.append(f"line {line_no}: sequence_id must be int")
                continue

            expected_seq = prev_seq + 1
            if sequence_id != expected_seq:
                issues.append(
                    f"line {line_no}: sequence_id {sequence_id} does not match expected {expected_seq}"
                )

            if record["previous_hash"] != prev_hash:
                issues.append(f"line {line_no}: previous_hash mismatch")

            expected_hash = _compute_event_hash(record)
            if record["event_hash"] != expected_hash:
                issues.append(f"line {line_no}: event_hash mismatch")

            prev_seq = sequence_id
            prev_hash = record["event_hash"]

    return issues


def _append_events(log_path: Path, events: list[dict[str, Any]]) -> None:
    issues = check_log_integrity(log_path)
    if issues:
        raise ValueError("Event log integrity check failed before append: " + "; ".join(issues))

    last_seq = 0
    last_hash = "GENESIS"
    if log_path.exists() and log_path.stat().st_size > 0:
        with log_path.open("r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                record = json.loads(raw)
                last_seq = int(record["sequence_id"])
                last_hash = str(record["event_hash"])

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        for event in events:
            last_seq += 1
            record = dict(event)
            record["sequence_id"] = last_seq
            record["previous_hash"] = last_hash
            record["event_hash"] = _compute_event_hash(record)
            f.write(json.dumps(record, sort_keys=True) + "\n")
            last_hash = record["event_hash"]


def append_events(log_path: Path, events: list[dict[str, Any]]) -> None:
    """Public append-only event writer with integrity chaining."""
    _append_events(log_path, events)


def _verifier_objections(row: dict[str, Any]) -> list[str]:
    objections: list[str] = []
    evidence = row.get("evidence", {})

    if not evidence.get("doc_id") or not evidence.get("locator_type") or not evidence.get("locator_value"):
        objections.append("missing_evidence_location")

    blockers = set(row.get("hard_blockers", []))
    if "currency_unit_mismatch" in blockers:
        objections.append("currency_unit_mismatch")

    return objections


def _transition_status(current_status: str, next_status: str) -> str:
    if current_status not in TERMINAL_STATUSES:
        raise WorkflowTransitionError(f"invalid current status: {current_status}")
    if next_status not in TERMINAL_STATUSES:
        raise WorkflowTransitionError(f"invalid next status: {next_status}")
    if next_status not in ALLOWED_TRANSITIONS[current_status]:
        raise WorkflowTransitionError(f"illegal status transition: {current_status} -> {next_status}")
    return next_status


def _review_row(
    row: dict[str, Any],
    max_retries: int,
    package_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    events: list[dict[str, Any]] = []
    retries = 0
    objection_reasons: list[str] = []
    attempt_history: list[dict[str, Any]] = []

    current_status = str(row.get("status", "unresolved"))
    if current_status not in TERMINAL_STATUSES:
        row["hard_blockers"] = sorted(set(row.get("hard_blockers", []) + ["invalid_initial_status"]))
        current_status = "unresolved"
    row["status"] = current_status

    # Independent verifier loop with capped retries.
    while retries <= max_retries:
        attempt_status = str(row.get("status"))
        attempt_confidence = row.get("confidence")
        objections = _verifier_objections(row)
        objection_reasons.extend(objections)
        attempt_record = {
            "attempt": retries,
            "status_before": attempt_status,
            "confidence_before": attempt_confidence,
            "objections": objections,
        }

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
            row["status"] = _transition_status(str(row.get("status")), "unresolved")
            row["hard_blockers"] = sorted(set(row.get("hard_blockers", []) + objections))
            row["final_resolver"] = "agent_4"
            row["retry_count"] = retries
            attempt_record["decision"] = "reject"
            attempt_record["status_after"] = row["status"]
            attempt_history.append(attempt_record)
            events.append(
                _event(
                    event_type="verify_rejected",
                    package_id=package_id,
                    phase="verify",
                    trace_id=row.get("trace_id"),
                    payload={"reason": objections},
                )
            )
            break

        # Candidate rows can be challenged once/twice if confidence is below verified threshold.
        if row.get("status") == "candidate_flagged" and retries < max_retries:
            retries += 1
            boosted = min(0.99, float(row.get("confidence", 0.0)) + 0.03)
            row["confidence"] = round(boosted, 4)
            next_status = classify_status(row["confidence"], row.get("hard_blockers", []))
            row["status"] = _transition_status(str(row.get("status")), next_status)
            attempt_record["decision"] = "challenge"
            attempt_record["status_after"] = row["status"]
            attempt_history.append(attempt_record)
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
        attempt_record["decision"] = "accept"
        attempt_record["status_after"] = row.get("status")
        attempt_history.append(attempt_record)
        events.append(
            _event(
                event_type="verify_accepted",
                package_id=package_id,
                phase="verify",
                trace_id=row.get("trace_id"),
                payload={"status": row.get("status"), "confidence": row.get("confidence")},
            )
        )
        break

    row["status"] = str(row.get("status", "unresolved"))
    if row["status"] not in TERMINAL_STATUSES:
        row["status"] = "unresolved"
        objection_reasons.append("invalid_terminal_status")

    row["final_resolver"] = "agent_4"
    row["retry_count"] = retries
    row["max_retries"] = max_retries
    row["objection_reasons"] = sorted(set(objection_reasons))
    row["verification"] = {
        "attempts": attempt_history,
        "retry_count": retries,
        "max_retries": max_retries,
        "final_status": row["status"],
        "objections": row["objection_reasons"],
        "resolver": "agent_4",
    }

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
