#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.scale_dataset import scale_dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scale proxy dataset to target package/deal counts")
    parser.add_argument(
        "--source-registry-file",
        default="dataset/source_registry/source_registry.v1.json",
    )
    parser.add_argument(
        "--packages-dir",
        default="dataset/packages/proxy_v1",
        help="Directory where scaled package manifests will be written",
    )
    parser.add_argument(
        "--labels-dir",
        default="dataset/labels/proxy_v1",
        help="Directory where scaled labels will be written",
    )
    parser.add_argument("--target-packages", type=int, default=50)
    parser.add_argument("--target-deals", type=int, default=15)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    result = scale_dataset(
        source_registry_file=Path(args.source_registry_file),
        packages_dir=Path(args.packages_dir),
        labels_dir=Path(args.labels_dir),
        target_packages=args.target_packages,
        target_deals=args.target_deals,
    )

    print("Proxy dataset scaled")
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
