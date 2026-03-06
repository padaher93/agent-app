from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil


@dataclass
class ShadowSyncSummary:
    packages_copied: int
    labels_copied: int
    target_packages_dir: Path
    target_labels_dir: Path


def sync_real_shadow_partition(
    source_packages_dir: Path,
    source_labels_dir: Path,
    target_root: Path,
    dry_run: bool = True,
) -> ShadowSyncSummary:
    target_packages_dir = target_root / "packages"
    target_labels_dir = target_root / "labels"

    package_files = sorted(source_packages_dir.glob("*.json"))
    label_files = sorted(source_labels_dir.glob("*.ground_truth.json"))

    if not dry_run:
        target_packages_dir.mkdir(parents=True, exist_ok=True)
        target_labels_dir.mkdir(parents=True, exist_ok=True)

        for package in package_files:
            shutil.copy2(package, target_packages_dir / package.name)

        for label in label_files:
            shutil.copy2(label, target_labels_dir / label.name)

    return ShadowSyncSummary(
        packages_copied=len(package_files),
        labels_copied=len(label_files),
        target_packages_dir=target_packages_dir,
        target_labels_dir=target_labels_dir,
    )
