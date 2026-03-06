from __future__ import annotations

from pathlib import Path

from agent_app_dataset.eval_metrics import evaluate


def test_eval_metrics_gate_pass_on_sample_predictions() -> None:
    root = Path(__file__).resolve().parents[1]
    result = evaluate(
        ground_truth_dir=root / "dataset" / "labels" / "proxy_v1_full",
        predictions_file=root / "dataset" / "predictions" / "sample_predictions.json",
    )

    assert result.gate_pass is True
    assert result.metrics["verified_precision"] >= 0.98
    assert result.metrics["evidence_link_accuracy"] >= 0.99
    assert result.metrics["false_verified_rate"] < 0.01
