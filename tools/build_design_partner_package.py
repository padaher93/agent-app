#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.design_partner_package import build_design_partner_package
from agent_app_dataset.eval_metrics import consecutive_passes
from agent_app_dataset.io_utils import read_json
from agent_app_dataset.trust_artifact import load_traces


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build design-partner readiness package")
    parser.add_argument("--eval-report", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--traces-file")
    parser.add_argument("--history-dir")
    parser.add_argument("--required-streak", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    eval_report = read_json(Path(args.eval_report))
    traces = load_traces(Path(args.traces_file)) if args.traces_file else []

    streak = 0
    if args.history_dir:
        streak = consecutive_passes(Path(args.history_dir))
    elif eval_report.get("gate_pass"):
        streak = 1

    outputs = build_design_partner_package(
        output_dir=Path(args.output_dir),
        eval_report=eval_report,
        traces=traces,
        streak=streak,
        required_streak=args.required_streak,
    )

    print("Design-partner package generated")
    for name, path in outputs.items():
        print(f"- {name}: {path}")

    readiness_summary = read_json(outputs["readiness_summary"])
    print(f"Release ready: {readiness_summary['release_ready']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
