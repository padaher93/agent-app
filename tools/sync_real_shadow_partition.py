#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.shadow_partition import sync_real_shadow_partition


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync redacted real packages into isolated real_shadow_test partition")
    parser.add_argument("--source-packages-dir", required=True)
    parser.add_argument("--source-labels-dir", required=True)
    parser.add_argument("--target-root", default="dataset/real_shadow_test")
    parser.add_argument("--apply", action="store_true", help="Apply copy; default is dry-run")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    summary = sync_real_shadow_partition(
        source_packages_dir=Path(args.source_packages_dir),
        source_labels_dir=Path(args.source_labels_dir),
        target_root=Path(args.target_root),
        dry_run=not args.apply,
    )

    print("Real shadow partition sync summary")
    print(f"- dry_run: {not args.apply}")
    print(f"- packages_copied: {summary.packages_copied}")
    print(f"- labels_copied: {summary.labels_copied}")
    print(f"- target_packages_dir: {summary.target_packages_dir}")
    print(f"- target_labels_dir: {summary.target_labels_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
