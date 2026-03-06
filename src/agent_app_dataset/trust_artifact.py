from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .io_utils import read_json


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def build_trust_artifact_markdown(
    eval_report: dict[str, Any],
    evidence_traces: list[dict[str, Any]],
) -> str:
    metrics = eval_report["metrics"]
    generated_at = datetime.now(timezone.utc).isoformat()

    lines: list[str] = []
    lines.append("# Design Partner Trust Artifact")
    lines.append("")
    lines.append(f"Generated at: {generated_at}")
    lines.append(f"Dataset version: `{eval_report['dataset_version']}`")
    lines.append(f"Pipeline version: `{eval_report['pipeline_version']}`")
    lines.append("")
    lines.append("## Metric snapshot")
    lines.append("")
    lines.append(f"- Verified precision: {_fmt_pct(metrics['verified_precision'])}")
    lines.append(f"- Evidence-link accuracy: {_fmt_pct(metrics['evidence_link_accuracy'])}")
    lines.append(f"- False-verified rate: {_fmt_pct(metrics['false_verified_rate'])}")
    lines.append(f"- Unresolved rate: {_fmt_pct(metrics['unresolved_rate'])}")
    lines.append(f"- Package completion rate: {_fmt_pct(metrics['package_completion_rate'])}")
    lines.append(f"- Gate pass: `{eval_report['gate_pass']}`")
    lines.append("")
    lines.append("## Error taxonomy summary")
    lines.append("")

    taxonomy = eval_report.get("failure_taxonomy", [])
    if taxonomy:
        for entry in taxonomy:
            lines.append(f"- {entry.get('category', 'unknown')}: {entry.get('count', 0)}")
    else:
        lines.append("- No taxonomy entries provided.")

    lines.append("")
    lines.append("## Representative evidence traces")
    lines.append("")

    if evidence_traces:
        for i, trace in enumerate(evidence_traces[:5], start=1):
            lines.append(
                f"{i}. `{trace.get('trace_id', 'n/a')}` | package `{trace.get('package_id', 'n/a')}` "
                f"| concept `{trace.get('concept_id', 'n/a')}`"
            )
            lines.append(
                f"   - Value: `{trace.get('normalized_value', 'n/a')}` | confidence: `{trace.get('confidence', 'n/a')}`"
            )
            lines.append(
                f"   - Evidence: {trace.get('doc_id', 'n/a')} @ {trace.get('locator_type', 'n/a')}={trace.get('locator_value', 'n/a')}"
            )
            lines.append(f"   - Snippet: {trace.get('source_snippet', 'n/a')}")
    else:
        lines.append("- No evidence traces provided.")

    lines.append("")
    lines.append("## Notes")
    lines.append("- This artifact is generated from proxy/phase data unless otherwise stated.")
    lines.append("- Real borrower data should remain in shadow partitions until stability criteria are met.")

    return "\n".join(lines)


def load_traces(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    payload = read_json(path)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "traces" in payload and isinstance(payload["traces"], list):
        return payload["traces"]
    raise ValueError("Trace payload must be a list or object with a 'traces' list")
