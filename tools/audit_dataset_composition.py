#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit dataset package/deal/noise composition")
    parser.add_argument("--packages-dir", required=True)
    parser.add_argument("--labels-dir", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    package_files = sorted(Path(args.packages_dir).glob("*.json"))
    label_files = sorted(Path(args.labels_dir).glob("*.ground_truth.json"))

    total_packages = len(package_files)
    deals = set()
    noise_packages = 0

    for file in package_files:
        payload = json.load(file.open("r", encoding="utf-8"))
        deals.add(payload["deal_id"])

    for file in label_files:
        payload = json.load(file.open("r", encoding="utf-8"))
        has_noise = False
        for row in payload.get("rows", []):
            if row.get("expected_status") != "verified" or row.get("flags"):
                has_noise = True
                break
        if has_noise:
            noise_packages += 1

    ratio = (noise_packages / total_packages) if total_packages else 0.0

    print("Dataset composition")
    print(f"packages={total_packages}")
    print(f"deals={len(deals)}")
    print(f"noise_packages={noise_packages}")
    print(f"noise_ratio={ratio:.4f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
