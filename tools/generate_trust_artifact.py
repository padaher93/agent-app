#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.io_utils import read_json
from agent_app_dataset.trust_artifact import build_trust_artifact_markdown, load_traces


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate design-partner trust artifact markdown")
    parser.add_argument("--eval-report", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--traces-file")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    eval_report = read_json(Path(args.eval_report))
    traces = load_traces(Path(args.traces_file)) if args.traces_file else []
    markdown = build_trust_artifact_markdown(eval_report, traces)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markdown, encoding="utf-8")

    print(f"Trust artifact generated at {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
