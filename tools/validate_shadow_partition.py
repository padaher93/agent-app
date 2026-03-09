#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.release_gates import validate_shadow_partition


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate shadow dataset partition readiness before eval runs")
    parser.add_argument("--packages-dir", required=True)
    parser.add_argument("--labels-dir", required=True)
    parser.add_argument("--min-packages", type=int, default=20)
    parser.add_argument("--min-deals", type=int, default=3)
    parser.add_argument("--min-periods-per-deal", type=int, default=2)
    parser.add_argument("--skip-storage-check", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = validate_shadow_partition(
        packages_dir=Path(args.packages_dir),
        labels_dir=Path(args.labels_dir),
        min_packages=args.min_packages,
        min_deals=args.min_deals,
        min_periods_per_deal=args.min_periods_per_deal,
        require_supported_storage=not args.skip_storage_check,
    )

    print("Shadow partition readiness")
    print(f"- passed: {result.passed}")
    print(f"- package_count: {result.package_count}")
    print(f"- label_count: {result.label_count}")
    print(f"- deal_count: {result.deal_count}")
    print(f"- min_periods_seen: {result.min_periods_seen}")
    if result.issues:
        print("- issues:")
        for issue in result.issues:
            print(f"  - {issue}")

    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
