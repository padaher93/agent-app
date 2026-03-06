from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .constants import STARTER_CONCEPT_IDS
from .io_utils import read_json
from .schemas import list_json_files, validate_with_schema


@dataclass
class ValidationIssue:
    path: Path
    message: str


def _cross_validate_package_manifest(
    payload: dict,
    source_ids: set[str],
) -> list[str]:
    issues: list[str] = []
    file_source_ids = {f["source_id"] for f in payload["files"]}
    listed_source_ids = set(payload["source_ids"])

    if file_source_ids != listed_source_ids:
        issues.append(
            "source_ids mismatch between files[] and source_ids[] "
            f"(files={sorted(file_source_ids)}, source_ids={sorted(listed_source_ids)})"
        )

    unknown = listed_source_ids - source_ids
    if unknown:
        issues.append(f"unknown source_ids referenced: {sorted(unknown)}")

    return issues


def _cross_validate_ground_truth(payload: dict) -> list[str]:
    issues: list[str] = []
    concept_ids = {row["concept_id"] for row in payload["rows"]}

    missing = [c for c in STARTER_CONCEPT_IDS if c not in concept_ids]
    if missing:
        issues.append(f"missing starter concepts: {missing}")

    duplicate_candidates = []
    seen: set[tuple[str, str]] = set()
    for row in payload["rows"]:
        key = (row["concept_id"], row["period_end_date"])
        if key in seen:
            duplicate_candidates.append(key)
        seen.add(key)
    if duplicate_candidates:
        issues.append(f"duplicate concept rows for same period: {duplicate_candidates}")

    return issues


def _validate_file(path: Path, schema_name: str) -> list[ValidationIssue]:
    payload = read_json(path)
    errors = validate_with_schema(schema_name, payload)
    return [ValidationIssue(path=path, message=err) for err in errors]


def validate_dataset_layout(
    source_registry_file: Path,
    packages_dir: Path,
    labels_dir: Path,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    source_payload = read_json(source_registry_file)
    issues.extend(_validate_file(source_registry_file, "source_registry"))

    source_ids = {source["source_id"] for source in source_payload.get("sources", [])}

    package_files = list_json_files(packages_dir)
    label_files = list_json_files(labels_dir)

    label_ids = {
        lf.stem.replace(".ground_truth", "")
        for lf in label_files
        if lf.name.endswith(".ground_truth.json")
    }

    for package_file in package_files:
        payload = read_json(package_file)
        issues.extend(_validate_file(package_file, "package_manifest"))
        if issues and any(i.path == package_file for i in issues):
            continue

        for message in _cross_validate_package_manifest(payload, source_ids):
            issues.append(ValidationIssue(path=package_file, message=message))

        package_id = payload["package_id"]
        if package_id not in label_ids:
            issues.append(
                ValidationIssue(
                    path=package_file,
                    message=f"missing ground truth file for package_id={package_id}",
                )
            )

    for label_file in label_files:
        payload = read_json(label_file)
        issues.extend(_validate_file(label_file, "ground_truth_file"))
        if any(i.path == label_file for i in issues):
            continue

        for message in _cross_validate_ground_truth(payload):
            issues.append(ValidationIssue(path=label_file, message=message))

    return issues


def format_issues(issues: Iterable[ValidationIssue]) -> str:
    lines = []
    for issue in issues:
        lines.append(f"{issue.path}: {issue.message}")
    return "\n".join(lines)
