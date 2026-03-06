#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.agent_workflow import WorkflowConfig, check_log_integrity, run_workflow
from agent_app_dataset.extractor_baseline import extract_package_predictions
from agent_app_dataset.io_utils import read_json, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic Agent 1-4 workflow over packages")
    parser.add_argument("--packages-dir", required=True)
    parser.add_argument("--labels-dir", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--events-log", required=True)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--truncate-log", action="store_true")
    parser.add_argument("--skip-integrity-check", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    package_predictions = []
    for package_file in sorted(Path(args.packages_dir).glob("*.json")):
        package_payload = read_json(package_file)
        label_file = Path(args.labels_dir) / f"{package_payload['package_id']}.ground_truth.json"
        label_payload = read_json(label_file) if label_file.exists() else None
        package_predictions.append(extract_package_predictions(package_payload, label_payload))

    events_log = Path(args.events_log)
    if args.truncate_log and events_log.exists():
        events_log.unlink()

    payload, summary = run_workflow(
        package_predictions=package_predictions,
        events_log_path=events_log,
        config=WorkflowConfig(max_retries=args.max_retries),
    )
    if not args.skip_integrity_check:
        issues = check_log_integrity(events_log)
        if issues:
            print("Event log integrity failed after run")
            for issue in issues:
                print(issue)
            return 1

    write_json(Path(args.output_file), payload)

    print("Agent workflow run complete")
    print(f"packages={summary.packages}")
    print(f"rows={summary.rows}")
    print(f"retries={summary.retries}")
    print(f"events={summary.events}")
    print(f"events_log={events_log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
