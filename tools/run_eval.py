#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.eval_metrics import consecutive_passes, evaluate, write_eval_report
from agent_app_dataset.io_utils import read_json, write_json
from agent_app_dataset.schemas import validate_with_schema


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dataset evaluation and gate checks")
    parser.add_argument("--ground-truth-dir", required=True)
    parser.add_argument("--predictions-file", required=True)
    parser.add_argument("--output-report", required=True)
    parser.add_argument("--history-dir", required=True)
    parser.add_argument("--dataset-version", default="dataset_pilot_v0.1")
    parser.add_argument("--pipeline-version", default="local")
    parser.add_argument("--failure-taxonomy-file")
    parser.add_argument("--required-streak", type=int, default=3)
    return parser.parse_args()


def _load_failure_taxonomy(path: str | None) -> list[dict]:
    if not path:
        return []
    payload = read_json(Path(path))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("failure_taxonomy"), list):
        return payload["failure_taxonomy"]
    raise ValueError("Failure taxonomy payload must be a list or include 'failure_taxonomy'")


def main() -> int:
    args = parse_args()
    history_dir = Path(args.history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    result = evaluate(
        ground_truth_dir=Path(args.ground_truth_dir),
        predictions_file=Path(args.predictions_file),
    )

    report = write_eval_report(
        output_report=Path(args.output_report),
        dataset_version=args.dataset_version,
        pipeline_version=args.pipeline_version,
        result=result,
        failure_taxonomy=_load_failure_taxonomy(args.failure_taxonomy_file),
    )

    schema_errors = validate_with_schema("eval_report", report)
    if schema_errors:
        print("Eval report schema validation failed")
        for error in schema_errors:
            print(error)
        return 1

    history_file = history_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    write_json(history_file, report)

    streak = consecutive_passes(history_dir)
    release_ready = report["gate_pass"] and streak >= args.required_streak

    print("Evaluation complete")
    print(f"Report: {args.output_report}")
    print(f"History: {history_file}")
    print(f"Metrics: {report['metrics']}")
    print(f"Gate pass this run: {report['gate_pass']}")
    print(f"Consecutive pass streak: {streak}")
    print(f"Release ready (>={args.required_streak} consecutive): {release_ready}")

    return 0 if release_ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
