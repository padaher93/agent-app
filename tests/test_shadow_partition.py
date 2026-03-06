from __future__ import annotations

from pathlib import Path

from agent_app_dataset.shadow_partition import sync_real_shadow_partition


def test_shadow_partition_sync(tmp_path: Path) -> None:
    src_packages = tmp_path / "src_packages"
    src_labels = tmp_path / "src_labels"
    src_packages.mkdir(parents=True, exist_ok=True)
    src_labels.mkdir(parents=True, exist_ok=True)

    (src_packages / "pkg_a.json").write_text("{}", encoding="utf-8")
    (src_packages / "pkg_b.json").write_text("{}", encoding="utf-8")
    (src_labels / "pkg_a.ground_truth.json").write_text("{}", encoding="utf-8")

    target = tmp_path / "real_shadow_test"

    dry_summary = sync_real_shadow_partition(
        source_packages_dir=src_packages,
        source_labels_dir=src_labels,
        target_root=target,
        dry_run=True,
    )
    assert dry_summary.packages_copied == 2
    assert dry_summary.labels_copied == 1
    assert not (target / "packages").exists()

    apply_summary = sync_real_shadow_partition(
        source_packages_dir=src_packages,
        source_labels_dir=src_labels,
        target_root=target,
        dry_run=False,
    )
    assert apply_summary.packages_copied == 2
    assert apply_summary.labels_copied == 1
    assert (target / "packages" / "pkg_a.json").exists()
    assert (target / "packages" / "pkg_b.json").exists()
    assert (target / "labels" / "pkg_a.ground_truth.json").exists()
