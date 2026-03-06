from __future__ import annotations

from pathlib import Path

from agent_app_dataset.io_utils import read_json


def test_dataset_freeze_manifest_exists_and_matches_counts() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest_file = root / "dataset" / "freezes" / "dataset_v1.0" / "dataset_manifest.json"
    manifest = read_json(manifest_file)

    assert manifest["dataset_version"] == "dataset_v1.0"
    assert manifest["counts"]["packages"] == 60
    assert len(manifest["splits"]["train_dev"]) + len(manifest["splits"]["validation"]) + len(manifest["splits"]["test"]) == 60
