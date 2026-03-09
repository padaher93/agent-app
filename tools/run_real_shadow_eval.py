#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.io_utils import read_json
from agent_app_dataset.shadow_eval import run_shadow_eval


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run shadow-mode evaluation on real/redacted dataset partition")
    parser.add_argument("--packages-dir", default="dataset/real_shadow_test/packages")
    parser.add_argument("--labels-dir", default="dataset/real_shadow_test/labels")
    parser.add_argument("--events-log", default="runtime/real_shadow_events.jsonl")
    parser.add_argument("--extraction-mode", default="llm", choices=["llm", "runtime", "eval"])
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--predictions-output", default="runtime/real_shadow_predictions.json")
    parser.add_argument("--report-output", default="runtime/real_shadow_eval_report.json")
    parser.add_argument("--history-dir", default="dataset/eval/history/real_shadow_test")
    parser.add_argument("--dataset-version", default="real_shadow_test")
    parser.add_argument("--pipeline-version", default="local")
    parser.add_argument("--required-streak", type=int, default=3)
    parser.add_argument("--min-packages", type=int, default=20)
    parser.add_argument("--failure-taxonomy-file")
    parser.add_argument("--incident-file")
    return parser.parse_args()


def _load_failure_taxonomy(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    payload = read_json(Path(path))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("failure_taxonomy"), list):
        return payload["failure_taxonomy"]
    raise ValueError("Failure taxonomy payload must be a list or include 'failure_taxonomy'")


def _load_incident_status(path: str | None) -> tuple[bool, str]:
    if not path:
        return False, "No incident file provided"

    payload = read_json(Path(path))
    if isinstance(payload, dict):
        if "blocking" in payload:
            return bool(payload.get("blocking")), str(payload.get("summary", "Explicit incident blocking flag"))

        incidents = payload.get("incidents")
        if isinstance(incidents, list):
            blocking_count = 0
            for incident in incidents:
                if not isinstance(incident, dict):
                    continue
                status = str(incident.get("status", "")).strip().lower()
                severity = str(incident.get("severity", "")).strip().lower()
                category = str(incident.get("category", "")).strip().lower()
                is_open = status in {"open", "active", "unresolved"}
                is_blocking_severity = severity in {"critical", "high"}
                is_blocking_category = category in {"security", "data_integrity", "integrity"}
                if is_open and is_blocking_severity and is_blocking_category:
                    blocking_count += 1

            return (blocking_count > 0), f"Blocking incidents: {blocking_count}"

    return False, "Incident file parsed; no blocking incidents detected"


def main() -> int:
    args = parse_args()
    blocking_incident, incident_summary = _load_incident_status(args.incident_file)

    summary = run_shadow_eval(
        packages_dir=Path(args.packages_dir),
        labels_dir=Path(args.labels_dir),
        events_log_path=Path(args.events_log),
        extraction_mode=args.extraction_mode,
        max_retries=args.max_retries,
        predictions_output_path=Path(args.predictions_output),
        report_output_path=Path(args.report_output),
        history_dir=Path(args.history_dir),
        dataset_version=args.dataset_version,
        pipeline_version=args.pipeline_version,
        required_streak=args.required_streak,
        min_packages=args.min_packages,
        failure_taxonomy=_load_failure_taxonomy(args.failure_taxonomy_file),
        blocking_incident=blocking_incident,
        incident_summary=incident_summary,
    )

    print("Shadow eval complete")
    print(f"- package_count: {summary.package_count}")
    print(f"- report_path: {summary.report_path}")
    print(f"- history_path: {summary.history_path}")
    print(f"- gate_pass: {summary.gate_pass}")
    print(f"- consecutive_pass_streak: {summary.consecutive_pass_streak}")
    print(f"- release_ready: {summary.release_ready}")

    return 0 if summary.release_ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
