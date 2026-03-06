from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .constants import QUALITY_THRESHOLDS
from .io_utils import write_json
from .trust_artifact import build_trust_artifact_markdown


METRIC_SPECS = (
    {
        "metric": "verified_precision",
        "threshold_key": "verified_precision_min",
        "comparator": ">=",
        "label": "Verified Precision",
    },
    {
        "metric": "evidence_link_accuracy",
        "threshold_key": "evidence_link_accuracy_min",
        "comparator": ">=",
        "label": "Evidence-Link Accuracy",
    },
    {
        "metric": "false_verified_rate",
        "threshold_key": "false_verified_rate_max",
        "comparator": "<",
        "label": "False-Verified Rate",
    },
    {
        "metric": "unresolved_rate",
        "threshold_key": "unresolved_rate_max",
        "comparator": "<=",
        "label": "Unresolved Rate",
    },
    {
        "metric": "package_completion_rate",
        "threshold_key": "package_completion_rate_min",
        "comparator": ">=",
        "label": "Package Completion Rate",
    },
)


def _evaluate_metric(value: float, target: float, comparator: str) -> tuple[bool, float]:
    if comparator == ">=":
        margin = value - target
        return value >= target, margin
    if comparator == "<=":
        margin = target - value
        return value <= target, margin
    if comparator == "<":
        margin = target - value
        return value < target, margin
    raise ValueError(f"Unsupported comparator: {comparator}")


def build_metric_snapshot(eval_report: dict[str, Any]) -> dict[str, Any]:
    metrics = eval_report.get("metrics", {})

    entries: list[dict[str, Any]] = []
    for spec in METRIC_SPECS:
        metric_name = spec["metric"]
        target_key = spec["threshold_key"]
        comparator = spec["comparator"]

        value = float(metrics.get(metric_name, 0.0))
        target = float(QUALITY_THRESHOLDS[target_key])
        is_pass, margin = _evaluate_metric(value=value, target=target, comparator=comparator)

        entries.append(
            {
                "metric": metric_name,
                "label": spec["label"],
                "value": value,
                "target": target,
                "comparator": comparator,
                "pass": is_pass,
                "margin": margin,
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_version": eval_report.get("dataset_version", "unknown"),
        "pipeline_version": eval_report.get("pipeline_version", "unknown"),
        "gate_pass": bool(eval_report.get("gate_pass", False)),
        "metrics": entries,
    }


def summarize_failure_taxonomy(eval_report: dict[str, Any], top_n: int = 10) -> dict[str, Any]:
    taxonomy = eval_report.get("failure_taxonomy", [])

    normalized: list[dict[str, Any]] = []
    total = 0
    for entry in taxonomy:
        count = int(entry.get("count", 0))
        total += count
        normalized.append(
            {
                "category": str(entry.get("category", "unknown")),
                "count": count,
                "examples": list(entry.get("examples", [])),
            }
        )

    ranked = sorted(normalized, key=lambda item: item["count"], reverse=True)
    top = ranked[:top_n]
    for item in top:
        item["share"] = (item["count"] / total) if total > 0 else 0.0

    return {
        "total_failures": total,
        "top_categories": top,
    }


def select_representative_traces(traces: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    status_rank = {
        "unresolved": 0,
        "candidate_flagged": 1,
        "verified": 2,
    }

    def sort_key(item: dict[str, Any]) -> tuple[int, float, str]:
        status = str(item.get("status", "verified"))
        confidence = float(item.get("confidence", 1.0))
        trace_id = str(item.get("trace_id", ""))
        return status_rank.get(status, 3), confidence, trace_id

    ranked = sorted(traces, key=sort_key)
    return ranked[:limit]


def build_readiness_summary(
    eval_report: dict[str, Any],
    streak: int,
    required_streak: int,
) -> dict[str, Any]:
    gate_pass = bool(eval_report.get("gate_pass", False))
    release_ready = gate_pass and streak >= required_streak
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_version": eval_report.get("dataset_version", "unknown"),
        "pipeline_version": eval_report.get("pipeline_version", "unknown"),
        "gate_pass": gate_pass,
        "consecutive_pass_streak": streak,
        "required_streak": required_streak,
        "release_ready": release_ready,
    }


def build_design_partner_package(
    output_dir: Path,
    eval_report: dict[str, Any],
    traces: list[dict[str, Any]],
    streak: int,
    required_streak: int,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    selected_traces = select_representative_traces(traces=traces, limit=5)
    metric_snapshot = build_metric_snapshot(eval_report)
    taxonomy_summary = summarize_failure_taxonomy(eval_report)
    readiness_summary = build_readiness_summary(
        eval_report=eval_report,
        streak=streak,
        required_streak=required_streak,
    )

    metric_file = output_dir / "metric_snapshot.json"
    taxonomy_file = output_dir / "error_taxonomy_summary.json"
    traces_file = output_dir / "representative_traces.json"
    readiness_file = output_dir / "readiness_summary.json"
    trust_markdown_file = output_dir / "trust_artifact.md"

    write_json(metric_file, metric_snapshot)
    write_json(taxonomy_file, taxonomy_summary)
    write_json(traces_file, {"traces": selected_traces})
    write_json(readiness_file, readiness_summary)

    trust_markdown = build_trust_artifact_markdown(
        eval_report=eval_report,
        evidence_traces=selected_traces,
    )
    trust_markdown_file.write_text(trust_markdown, encoding="utf-8")

    return {
        "metric_snapshot": metric_file,
        "error_taxonomy_summary": taxonomy_file,
        "representative_traces": traces_file,
        "readiness_summary": readiness_file,
        "trust_artifact": trust_markdown_file,
    }
