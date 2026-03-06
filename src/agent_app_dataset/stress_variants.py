from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import random

from .io_utils import read_json, write_json

VARIANT_LIBRARY = [
    "label_drift",
    "missing_schedule",
    "currency_inconsistency",
    "ocr_degraded_pdf",
]


def _apply_variant_to_manifest(payload: dict, variant: str) -> dict:
    updated = deepcopy(payload)
    updated["package_id"] = f"{payload['package_id']}_var_{variant}"
    tags = set(updated.get("variant_tags", []))
    tags.add(variant)
    tags.add("synthetic_stress")
    updated["variant_tags"] = sorted(tags)

    if variant == "missing_schedule" and updated["files"]:
        # Remove one non-primary file to simulate missing schedule package quality.
        if len(updated["files"]) > 1:
            updated["files"] = updated["files"][:-1]
            updated["source_ids"] = sorted({f["source_id"] for f in updated["files"]})

    if variant == "ocr_degraded_pdf":
        for file in updated["files"]:
            if file["doc_type"] == "PDF":
                file["ocr_quality"] = "degraded"

    if variant == "currency_inconsistency":
        flags = set(updated.get("quality_flags", []))
        flags.add("currency_inconsistency")
        updated["quality_flags"] = sorted(flags)

    if variant == "label_drift":
        flags = set(updated.get("quality_flags", []))
        flags.add("label_drift")
        updated["quality_flags"] = sorted(flags)

    return updated


def _apply_variant_to_labels(payload: dict, variant: str, package_id: str) -> dict:
    updated = deepcopy(payload)
    updated["package_id"] = package_id
    for row in updated.get("rows", []):
        flags = set(row.get("flags", []))
        if variant in {"label_drift", "currency_inconsistency"}:
            flags.add(variant)
        if variant == "missing_schedule" and row["concept_id"] in {
            "inventory_total",
            "accounts_receivable_total",
        }:
            row["expected_status"] = "unresolved"
            flags.add("missing_schedule")
        if variant == "ocr_degraded_pdf":
            flags.add("ocr_degraded_pdf")
            row["labeler_confidence"] = min(row.get("labeler_confidence", 1.0), 0.85)
        row["flags"] = sorted(flags)
    return updated


def generate_variants(
    packages_dir: Path,
    labels_dir: Path,
    output_packages_dir: Path,
    output_labels_dir: Path,
    variant_ratio: float = 0.3,
    seed: int = 20260306,
) -> dict[str, int]:
    output_packages_dir.mkdir(parents=True, exist_ok=True)
    output_labels_dir.mkdir(parents=True, exist_ok=True)

    package_files = sorted(packages_dir.glob("*.json"))
    if not package_files:
        return {"generated_variants": 0}

    rng = random.Random(seed)
    count = max(1, int(len(package_files) * variant_ratio))
    selected = rng.sample(package_files, k=count)

    generated = 0
    for i, package_file in enumerate(selected):
        package_payload = read_json(package_file)
        base_package_id = package_payload["package_id"]

        label_file = labels_dir / f"{base_package_id}.ground_truth.json"
        if not label_file.exists():
            continue

        label_payload = read_json(label_file)
        variant = VARIANT_LIBRARY[i % len(VARIANT_LIBRARY)]

        updated_manifest = _apply_variant_to_manifest(package_payload, variant)
        updated_labels = _apply_variant_to_labels(label_payload, variant, updated_manifest["package_id"])

        write_json(output_packages_dir / f"{updated_manifest['package_id']}.json", updated_manifest)
        write_json(
            output_labels_dir / f"{updated_manifest['package_id']}.ground_truth.json",
            updated_labels,
        )
        generated += 1

    return {"generated_variants": generated, "selected_packages": len(selected)}
