#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.freeze import SplitConfig, build_freeze


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create immutable dataset freeze manifests")
    parser.add_argument("--packages-dir", required=True)
    parser.add_argument("--labels-dir", required=True)
    parser.add_argument("--freeze-dir", required=True)
    parser.add_argument("--dataset-version", default="dataset_v1.0")
    parser.add_argument("--train-ratio", type=float, default=0.70)
    parser.add_argument("--validation-ratio", type=float, default=0.10)
    parser.add_argument("--test-ratio", type=float, default=0.20)
    parser.add_argument("--seed", type=int, default=20260306)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = SplitConfig(
        train_ratio=args.train_ratio,
        validation_ratio=args.validation_ratio,
        test_ratio=args.test_ratio,
        seed=args.seed,
    )

    manifest = build_freeze(
        packages_dir=Path(args.packages_dir),
        labels_dir=Path(args.labels_dir),
        freeze_dir=Path(args.freeze_dir),
        dataset_version=args.dataset_version,
        split_config=config,
    )

    print("Dataset freeze created")
    print(f"Version: {manifest['dataset_version']}")
    print(f"Packages: {manifest['counts']['packages']}")
    print(f"Freeze directory: {args.freeze_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
