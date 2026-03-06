from __future__ import annotations

from pathlib import Path

from agent_app_dataset.validation import validate_dataset_layout


def test_proxy_dataset_contracts_pass() -> None:
    root = Path(__file__).resolve().parents[1]
    issues = validate_dataset_layout(
        source_registry_file=root / "dataset" / "source_registry" / "source_registry.v1.json",
        packages_dir=root / "dataset" / "packages" / "proxy_v1_full",
        labels_dir=root / "dataset" / "labels" / "proxy_v1_full",
    )

    assert not issues
