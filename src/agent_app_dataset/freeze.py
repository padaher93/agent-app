from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import random

from .io_utils import file_sha256, read_json, write_json


@dataclass
class SplitConfig:
    train_ratio: float = 0.70
    validation_ratio: float = 0.10
    test_ratio: float = 0.20
    seed: int = 20260306


def _compute_split_sizes(total: int, cfg: SplitConfig) -> tuple[int, int, int]:
    train = int(total * cfg.train_ratio)
    validation = int(total * cfg.validation_ratio)
    test = total - train - validation
    return train, validation, test


def build_freeze(
    packages_dir: Path,
    labels_dir: Path,
    freeze_dir: Path,
    dataset_version: str,
    split_config: SplitConfig | None = None,
) -> dict:
    cfg = split_config or SplitConfig()

    package_files = sorted(packages_dir.glob("*.json"))
    package_ids = [read_json(p)["package_id"] for p in package_files]

    rng = random.Random(cfg.seed)
    rng.shuffle(package_ids)

    train_n, val_n, _ = _compute_split_sizes(len(package_ids), cfg)
    split = {
        "train_dev": sorted(package_ids[:train_n]),
        "validation": sorted(package_ids[train_n : train_n + val_n]),
        "test": sorted(package_ids[train_n + val_n :]),
    }

    freeze_dir.mkdir(parents=True, exist_ok=True)

    checksums: list[dict] = []
    for path in sorted(packages_dir.glob("*.json")):
        checksums.append(
            {
                "path": str(path.as_posix()),
                "sha256": file_sha256(path),
                "kind": "package_manifest",
            }
        )

    for path in sorted(labels_dir.glob("*.ground_truth.json")):
        checksums.append(
            {
                "path": str(path.as_posix()),
                "sha256": file_sha256(path),
                "kind": "ground_truth",
            }
        )

    dataset_manifest = {
        "dataset_version": dataset_version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "split_policy": {
            "train_dev": cfg.train_ratio,
            "validation": cfg.validation_ratio,
            "test": cfg.test_ratio,
            "seed": cfg.seed,
        },
        "counts": {
            "packages": len(package_ids),
            "labels": len(list(labels_dir.glob("*.ground_truth.json"))),
        },
        "splits": split,
        "checksums": checksums,
        "immutable": True,
    }

    write_json(freeze_dir / "splits.json", split)
    write_json(freeze_dir / "dataset_manifest.json", dataset_manifest)

    return dataset_manifest
