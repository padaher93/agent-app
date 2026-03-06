from __future__ import annotations

from pathlib import Path

from agent_app_dataset.constants import STARTER_CONCEPT_IDS
from agent_app_dataset.extractor_baseline import build_predictions
from agent_app_dataset.io_utils import read_json


def test_extraction_baseline_outputs_expected_shape(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_file = tmp_path / "predictions.json"

    summary = build_predictions(
        packages_dir=root / "dataset" / "packages" / "proxy_v1_full",
        labels_dir=root / "dataset" / "labels" / "proxy_v1_full",
        output_file=output_file,
    )

    payload = read_json(output_file)
    assert payload["schema_version"] == "1.0"
    assert summary.packages == 60
    assert summary.rows == 60 * len(STARTER_CONCEPT_IDS)

    first_package = payload["packages"][0]
    assert len(first_package["rows"]) == len(STARTER_CONCEPT_IDS)
    assert set(summary.status_counts).issuperset({"verified", "candidate_flagged", "unresolved"})
