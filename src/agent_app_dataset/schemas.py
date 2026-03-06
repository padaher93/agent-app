from __future__ import annotations

from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .constants import SCHEMA_DIR
from .io_utils import read_json

_SCHEMA_MAP = {
    "source_registry": "source_registry.schema.json",
    "package_manifest": "package_manifest.schema.json",
    "ground_truth_file": "ground_truth_file.schema.json",
    "eval_report": "eval_report.schema.json",
}


def load_schema(name: str) -> dict[str, Any]:
    if name not in _SCHEMA_MAP:
        raise ValueError(f"Unknown schema name: {name}")
    path = SCHEMA_DIR / _SCHEMA_MAP[name]
    return read_json(path)


def validate_with_schema(name: str, payload: dict[str, Any]) -> list[str]:
    schema = load_schema(name)
    validator = Draft202012Validator(schema)
    errors: list[str] = []
    for error in sorted(validator.iter_errors(payload), key=lambda e: e.path):
        location = "/".join(str(x) for x in error.path) or "<root>"
        errors.append(f"{location}: {error.message}")
    return errors


def list_json_files(path: Path) -> list[Path]:
    return sorted(p for p in path.glob("*.json") if p.is_file())
