#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.stress_variants import generate_variants


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic stress variants")
    parser.add_argument("--packages-dir", required=True)
    parser.add_argument("--labels-dir", required=True)
    parser.add_argument("--output-packages-dir", required=True)
    parser.add_argument("--output-labels-dir", required=True)
    parser.add_argument("--variant-ratio", type=float, default=0.30)
    parser.add_argument("--seed", type=int, default=20260306)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    result = generate_variants(
        packages_dir=Path(args.packages_dir),
        labels_dir=Path(args.labels_dir),
        output_packages_dir=Path(args.output_packages_dir),
        output_labels_dir=Path(args.output_labels_dir),
        variant_ratio=args.variant_ratio,
        seed=args.seed,
    )

    print("Stress variants generated")
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
