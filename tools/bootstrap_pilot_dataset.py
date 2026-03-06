#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.bootstrap import (
    build_pilot_packages_and_labels,
    build_source_registry,
)
from agent_app_dataset.freeze import SplitConfig, build_freeze


def main() -> int:
    root = ROOT

    source_registry_file = root / "dataset" / "source_registry" / "source_registry.v1.json"
    packages_dir = root / "dataset" / "packages" / "pilot"
    labels_dir = root / "dataset" / "labels" / "pilot"
    freeze_dir = root / "dataset" / "freezes" / "pilot_v0.1"

    source_registry = build_source_registry(source_registry_file)
    counts = build_pilot_packages_and_labels(source_registry, packages_dir, labels_dir)

    build_freeze(
        packages_dir=packages_dir,
        labels_dir=labels_dir,
        freeze_dir=freeze_dir,
        dataset_version="dataset_pilot_v0.1",
        split_config=SplitConfig(train_ratio=0.7, validation_ratio=0.1, test_ratio=0.2),
    )

    print("Pilot dataset bootstrapped")
    print(counts)
    print(f"Source registry: {source_registry_file}")
    print(f"Packages: {packages_dir}")
    print(f"Labels: {labels_dir}")
    print(f"Freeze: {freeze_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
