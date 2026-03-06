#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check two-pass labeling completion before freeze")
    parser.add_argument("--packages-dir", required=True)
    parser.add_argument("--labels-dir", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    issues = []

    for package_file in sorted(Path(args.packages_dir).glob("*.json")):
        package = json.load(package_file.open("r", encoding="utf-8"))
        wf = package.get("labeling_workflow", {})
        package_id = package.get("package_id", package_file.name)

        if wf.get("primary_labeler_status") != "completed":
            issues.append(f"{package_id}: primary labeler not completed")
        if wf.get("reviewer_status") != "completed":
            issues.append(f"{package_id}: reviewer not completed")

        label_file = Path(args.labels_dir) / f"{package_id}.ground_truth.json"
        if not label_file.exists():
            issues.append(f"{package_id}: missing label file")
            continue

        labels = json.load(label_file.open("r", encoding="utf-8"))
        labeling = labels.get("labeling", {})
        if not labeling.get("primary_labeler"):
            issues.append(f"{package_id}: missing primary_labeler identity")
        if not labeling.get("reviewer"):
            issues.append(f"{package_id}: missing reviewer identity")

    if issues:
        print("Labeling quality check failed")
        for issue in issues:
            print(issue)
        return 1

    print("Labeling quality check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
