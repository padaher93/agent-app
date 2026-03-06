#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import random
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.io_utils import read_json, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sample predictions from ground truth")
    parser.add_argument("--ground-truth-dir", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--error-rate", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=20260306)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    packages = []
    for gt_file in sorted(Path(args.ground_truth_dir).glob("*.ground_truth.json")):
        payload = read_json(gt_file)
        rows = []
        for row in payload["rows"]:
            predicted_value = row["normalized_value"]
            status = row["expected_status"]
            evidence = row["evidence"]

            if rng.random() < args.error_rate and predicted_value is not None:
                predicted_value = round(predicted_value * 1.02, 2)
                status = "verified"

            rows.append(
                {
                    "concept_id": row["concept_id"],
                    "status": status,
                    "normalized_value": predicted_value,
                    "confidence": row["labeler_confidence"],
                    "trace_id": row["trace_id"],
                    "evidence": {
                        "doc_id": evidence["doc_id"],
                        "locator_type": evidence["locator_type"],
                        "locator_value": evidence["locator_value"],
                    },
                }
            )

        packages.append({"package_id": payload["package_id"], "rows": rows})

    out = {
        "schema_version": "1.0",
        "packages": packages,
    }
    write_json(Path(args.output_file), out)
    print(f"Generated predictions at {args.output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
