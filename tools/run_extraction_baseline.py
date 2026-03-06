#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.extractor_baseline import build_predictions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run extraction baseline and write prediction payload")
    parser.add_argument("--packages-dir", required=True)
    parser.add_argument("--labels-dir", required=True)
    parser.add_argument("--output-file", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    summary = build_predictions(
        packages_dir=Path(args.packages_dir),
        labels_dir=Path(args.labels_dir),
        output_file=Path(args.output_file),
    )

    print("Extraction baseline complete")
    print(f"packages={summary.packages}")
    print(f"rows={summary.rows}")
    print(f"status_counts={summary.status_counts}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
