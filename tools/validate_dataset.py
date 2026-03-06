#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.validation import format_issues, validate_dataset_layout


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate dataset contracts and cross references")
    parser.add_argument(
        "--source-registry-file",
        default="dataset/source_registry/source_registry.v1.json",
        help="Path to source registry JSON",
    )
    parser.add_argument(
        "--packages-dir",
        default="dataset/packages/pilot",
        help="Directory containing package manifests",
    )
    parser.add_argument(
        "--labels-dir",
        default="dataset/labels/pilot",
        help="Directory containing ground truth files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    issues = validate_dataset_layout(
        source_registry_file=Path(args.source_registry_file),
        packages_dir=Path(args.packages_dir),
        labels_dir=Path(args.labels_dir),
    )

    if issues:
        print("Validation failed:")
        print(format_issues(issues))
        return 1

    print("Validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
