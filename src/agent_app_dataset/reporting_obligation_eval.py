from __future__ import annotations

from dataclasses import dataclass
import csv
import json
from pathlib import Path
from typing import Any

from .llm_runtime import LLMClient
from .reporting_obligation_candidates import discover_reporting_obligation_candidates
from .reporting_obligations import extract_reporting_obligations

try:  # pragma: no cover - optional dependency path
    from openpyxl import Workbook
except Exception:  # pragma: no cover - optional dependency path
    Workbook = None


_ALLOWED_EXPECTED_OUTCOMES = {"grounded", "ambiguous", "unsupported"}


@dataclass(frozen=True)
class CorpusExample:
    example_id: str
    source_kind: str
    text: str
    expected_outcome: str
    expectation_bucket: str
    expected_grounded_concepts: tuple[str, ...]
    expected_candidate_concepts: tuple[str, ...]
    notes: str
    reviewer_label: str | None = None
    reviewer_comment: str | None = None
    llm_probe_concepts: tuple[str, ...] = ()


class _ProbeLLMClient:
    def __init__(self, candidates: list[dict[str, Any]]) -> None:
        self._candidates = candidates
        self.model_name = "reporting-obligation-probe"

    def run_json(self, *, agent_id: str, system_prompt: str, user_payload: dict[str, Any]) -> dict[str, Any]:
        if agent_id != "agent_reporting_obligation_candidates":
            raise RuntimeError(f"unexpected_agent_id:{agent_id}")
        return {"candidates": self._candidates}


def _normalize_concepts(values: Any) -> tuple[str, ...]:
    if not isinstance(values, list):
        return ()
    concepts: list[str] = []
    for value in values:
        normalized = str(value).strip().lower()
        if normalized and normalized not in concepts:
            concepts.append(normalized)
    return tuple(concepts)


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _has_text(value: Any) -> bool:
    if value is None:
        return False
    return bool(str(value).strip())


def load_real_reporting_obligation_corpus(path: Path) -> list[CorpusExample]:
    rows: list[CorpusExample] = []
    if not path.exists():
        raise FileNotFoundError(f"corpus_not_found:{path}")

    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid_jsonl_line:{line_number}") from exc
            if not isinstance(payload, dict):
                raise ValueError(f"invalid_jsonl_object:{line_number}")

            example_id = str(payload.get("example_id", "")).strip()
            source_kind = str(payload.get("source_kind", "")).strip()
            text = str(payload.get("text", "")).strip()
            expected_outcome = str(payload.get("expected_outcome", "")).strip().lower()
            expectation_bucket = str(payload.get("expectation_bucket", "")).strip().lower() or "unbucketed"
            if not example_id:
                raise ValueError(f"missing_example_id:{line_number}")
            if not source_kind:
                raise ValueError(f"missing_source_kind:{line_number}")
            if not text:
                raise ValueError(f"missing_text:{line_number}")
            if expected_outcome not in _ALLOWED_EXPECTED_OUTCOMES:
                raise ValueError(f"invalid_expected_outcome:{line_number}")

            rows.append(
                CorpusExample(
                    example_id=example_id,
                    source_kind=source_kind,
                    text=text,
                    expected_outcome=expected_outcome,
                    expectation_bucket=expectation_bucket,
                    expected_grounded_concepts=_normalize_concepts(payload.get("expected_grounded_concepts")),
                    expected_candidate_concepts=_normalize_concepts(payload.get("expected_candidate_concepts")),
                    notes=str(payload.get("notes", "")).strip(),
                    reviewer_label=_normalize_optional_text(payload.get("reviewer_label")),
                    reviewer_comment=_normalize_optional_text(payload.get("reviewer_comment")),
                    llm_probe_concepts=_normalize_concepts(payload.get("llm_probe_concepts")),
                )
            )

    if not rows:
        raise ValueError("corpus_empty")
    return rows


def _write_requirement_doc(path: Path, text: str) -> None:
    if Workbook is None:  # pragma: no cover - dependency guard
        raise RuntimeError("openpyxl_required_for_reporting_obligation_eval")

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Requirements"
    lines = str(text).splitlines() or [str(text)]
    for index, line in enumerate(lines, start=1):
        sheet[f"A{index}"] = line
    workbook.save(path)


def _probe_candidates(example: CorpusExample, *, doc_id: str) -> list[dict[str, Any]]:
    concepts: list[str] = []
    for concept in list(example.expected_candidate_concepts) + list(example.llm_probe_concepts):
        if concept and concept not in concepts:
            concepts.append(concept)

    # Allow explicit stress-testing for negatives where we still want a model-like candidate.
    if not concepts and example.expected_outcome in {"ambiguous", "unsupported"}:
        if "net_income" in (example.notes or "").lower() or "net income" in example.text.lower():
            concepts = ["net_income"]

    return [
        {
            "doc_id": doc_id,
            "locator_type": "cell",
            "locator_value": "A1",
            "source_snippet": example.text,
            "candidate_obligation_type": "reporting_requirement",
            "candidate_concept_id": concept_id,
            "reason": f"eval_probe:{example.source_kind}:{example.expectation_bucket}",
            "certainty_bucket": "high" if example.expected_outcome == "grounded" else "medium",
        }
        for concept_id in concepts
    ]


def _run_llm_candidates(
    *,
    deal_id: str,
    docs: list[dict[str, Any]],
    example: CorpusExample,
    llm_mode: str,
    llm_client: LLMClient | None,
) -> dict[str, Any]:
    mode = str(llm_mode).strip().lower()
    if mode == "off":
        return {
            "mode": "off",
            "model_name": "",
            "status": "skipped",
            "candidates": [],
            "promoted_obligations": [],
            "error": "",
        }
    if mode == "probe":
        probe = _ProbeLLMClient(
            _probe_candidates(example, doc_id=str(docs[0].get("doc_id", "")))
        )
        result = discover_reporting_obligation_candidates(
            deal_id=deal_id,
            docs=docs,
            llm_client=probe,
        )
        return {
            "mode": "probe",
            "model_name": str(result.get("model_name", "")).strip() or probe.model_name,
            "status": "completed",
            "candidates": [
                row for row in result.get("candidates", [])
                if isinstance(row, dict)
            ],
            "promoted_obligations": [
                row for row in result.get("promoted_obligations", [])
                if isinstance(row, dict)
            ],
            "error": "",
        }

    try:
        result = discover_reporting_obligation_candidates(
            deal_id=deal_id,
            docs=docs,
            llm_client=llm_client,
        )
        return {
            "mode": mode,
            "model_name": str(result.get("model_name", "")).strip(),
            "status": "completed",
            "candidates": [
                row for row in result.get("candidates", [])
                if isinstance(row, dict)
            ],
            "promoted_obligations": [
                row for row in result.get("promoted_obligations", [])
                if isinstance(row, dict)
            ],
            "error": "",
        }
    except Exception as exc:
        return {
            "mode": mode,
            "model_name": "",
            "status": "failed",
            "candidates": [],
            "promoted_obligations": [],
            "error": str(exc),
        }


def _actual_outcome(
    *,
    grounded_concepts: set[str],
    deterministic_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
) -> str:
    if grounded_concepts:
        return "grounded"
    has_ambiguous = any(
        str(row.get("grounding_state", "")).strip().lower() == "ambiguous"
        for row in deterministic_rows + candidate_rows
    )
    if has_ambiguous:
        return "ambiguous"
    return "unsupported"


def evaluate_real_reporting_obligation_corpus(
    *,
    corpus_path: Path,
    run_dir: Path,
    llm_mode: str = "probe",
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    examples = load_real_reporting_obligation_corpus(corpus_path)
    docs_dir = run_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    true_promotions = 0
    false_promotions = 0
    grounded_expected = 0
    grounded_actual = 0
    grounded_misses = 0
    ambiguous_blocked = 0
    ambiguous_total = 0
    unsupported_blocked = 0
    unsupported_total = 0

    for example in examples:
        doc_path = docs_dir / f"{example.example_id}.xlsx"
        _write_requirement_doc(doc_path, example.text)
        doc_id = f"req_{example.example_id}"
        docs = [
            {
                "doc_id": doc_id,
                "doc_type": "XLSX",
                "filename": doc_path.name,
                "storage_uri": str(doc_path),
                "checksum": f"checksum_{example.example_id}",
                "pages_or_sheets": 1,
            }
        ]

        deterministic_rows = extract_reporting_obligations(
            deal_id=f"deal_eval_{example.example_id}",
            docs=docs,
        )
        llm_result = _run_llm_candidates(
            deal_id=f"deal_eval_{example.example_id}",
            docs=docs,
            example=example,
            llm_mode=llm_mode,
            llm_client=llm_client,
        )
        candidate_rows = list(llm_result.get("candidates", []))
        promoted_from_candidates = list(llm_result.get("promoted_obligations", []))

        obligations_by_id: dict[str, dict[str, Any]] = {}
        for obligation in deterministic_rows:
            if not isinstance(obligation, dict):
                continue
            obligation_id = str(obligation.get("obligation_id", "")).strip()
            if not obligation_id:
                continue
            obligations_by_id[obligation_id] = obligation
        for obligation in promoted_from_candidates:
            if not isinstance(obligation, dict):
                continue
            obligation_id = str(obligation.get("obligation_id", "")).strip()
            if not obligation_id:
                continue
            obligations_by_id.setdefault(obligation_id, obligation)

        grounded_concepts = {
            str(row.get("required_concept_id", "")).strip().lower()
            for row in obligations_by_id.values()
            if str(row.get("grounding_state", "")).strip().lower() == "grounded"
            and str(row.get("required_concept_id", "")).strip()
        }
        candidate_concepts = {
            str(row.get("candidate_concept_id", "")).strip().lower()
            for row in candidate_rows
            if str(row.get("candidate_concept_id", "")).strip()
        }
        expected_grounded = set(example.expected_grounded_concepts)
        expected_candidates = set(example.expected_candidate_concepts)

        actual_outcome = _actual_outcome(
            grounded_concepts=grounded_concepts,
            deterministic_rows=deterministic_rows,
            candidate_rows=candidate_rows,
        )

        false_promotion_concepts = sorted(grounded_concepts - expected_grounded)
        miss_concepts = sorted(expected_grounded - grounded_concepts)
        false_promotion = bool(false_promotion_concepts)
        miss = bool(miss_concepts)

        if expected_grounded:
            grounded_expected += len(expected_grounded)
        if grounded_concepts:
            grounded_actual += len(grounded_concepts)
        true_promotions += len(grounded_concepts & expected_grounded)
        false_promotions += len(false_promotion_concepts)
        grounded_misses += len(miss_concepts)

        if example.expected_outcome == "ambiguous":
            ambiguous_total += 1
            if actual_outcome in {"ambiguous", "unsupported"}:
                ambiguous_blocked += 1
        if example.expected_outcome == "unsupported":
            unsupported_total += 1
            if actual_outcome == "unsupported":
                unsupported_blocked += 1

        promotions = [
            {
                "required_concept_id": str(row.get("required_concept_id", "")).strip().lower(),
                "doc_name": str(row.get("doc_name", "")).strip(),
                "locator_type": str(row.get("locator_type", "")).strip(),
                "locator_value": str(row.get("locator_value", "")).strip(),
                "source_snippet": str(row.get("source_snippet", "")).strip(),
                "grounding_state": str(row.get("grounding_state", "")).strip().lower(),
            }
            for row in obligations_by_id.values()
            if str(row.get("grounding_state", "")).strip().lower() == "grounded"
        ]
        blocked_candidates = [
            {
                "candidate_concept_id": str(row.get("candidate_concept_id", "")).strip().lower(),
                "grounding_state": str(row.get("grounding_state", "")).strip().lower(),
                "reason": str(row.get("reason", "")).strip(),
                "locator_type": str(row.get("locator_type", "")).strip(),
                "locator_value": str(row.get("locator_value", "")).strip(),
            }
            for row in candidate_rows
            if not _has_text(row.get("promoted_obligation_id"))
        ]

        verdict = "pass"
        if false_promotion and miss:
            verdict = "false_promotion_and_miss"
        elif false_promotion:
            verdict = "false_promotion"
        elif miss:
            verdict = "miss"
        elif example.expected_outcome != actual_outcome:
            verdict = "outcome_mismatch"

        rows.append(
            {
                "example_id": example.example_id,
                "source_kind": example.source_kind,
                "expectation_bucket": example.expectation_bucket,
                "text": example.text,
                "expected_outcome": example.expected_outcome,
                "actual_outcome": actual_outcome,
                "expected_candidate_concepts": sorted(expected_candidates),
                "actual_candidate_concepts": sorted(candidate_concepts),
                "expected_grounded_concepts": sorted(expected_grounded),
                "actual_grounded_concepts": sorted(grounded_concepts),
                "false_promotion": false_promotion,
                "false_promotion_concepts": false_promotion_concepts,
                "miss": miss,
                "miss_concepts": miss_concepts,
                "verdict": verdict,
                "pass": verdict == "pass",
                "candidate_count": len(candidate_rows),
                "promotion_count": len(promotions),
                "blocked_candidate_count": len(blocked_candidates),
                "promotions": promotions,
                "blocked_candidates": blocked_candidates,
                "llm_mode": str(llm_result.get("mode", "")),
                "llm_status": str(llm_result.get("status", "")),
                "llm_model_name": str(llm_result.get("model_name", "")),
                "llm_error": str(llm_result.get("error", "")),
                "notes": example.notes,
                "reviewer_label": example.reviewer_label,
                "reviewer_comment": example.reviewer_comment,
            }
        )

    precision = (
        true_promotions / (true_promotions + false_promotions)
        if (true_promotions + false_promotions)
        else 1.0
    )
    recall_grounded = (true_promotions / grounded_expected) if grounded_expected else 1.0

    summary = {
        "total_examples": len(rows),
        "grounded_expected_count": grounded_expected,
        "grounded_actual_count": grounded_actual,
        "false_promotions": false_promotions,
        "misses_on_grounded_examples": grounded_misses,
        "precision": round(precision, 4),
        "recall_on_grounded_examples": round(recall_grounded, 4),
        "ambiguous_correctly_blocked_count": ambiguous_blocked,
        "ambiguous_total_count": ambiguous_total,
        "unsupported_correctly_blocked_count": unsupported_blocked,
        "unsupported_total_count": unsupported_total,
        "pass_count": sum(1 for row in rows if bool(row.get("pass"))),
        "fail_count": sum(1 for row in rows if not bool(row.get("pass"))),
        "false_promotion_examples": [
            row["example_id"] for row in rows if bool(row.get("false_promotion"))
        ],
        "miss_examples": [
            row["example_id"] for row in rows if bool(row.get("miss"))
        ],
        "bucket_counts": {},
        "bucket_false_promotions": {},
        "bucket_misses": {},
    }
    bucket_counts: dict[str, int] = {}
    bucket_false_promotions: dict[str, int] = {}
    bucket_misses: dict[str, int] = {}
    for row in rows:
        bucket = str(row.get("expectation_bucket", "")).strip().lower() or "unbucketed"
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        if bool(row.get("false_promotion")):
            bucket_false_promotions[bucket] = bucket_false_promotions.get(bucket, 0) + 1
        if bool(row.get("miss")):
            bucket_misses[bucket] = bucket_misses.get(bucket, 0) + 1
    summary["bucket_counts"] = bucket_counts
    summary["bucket_false_promotions"] = bucket_false_promotions
    summary["bucket_misses"] = bucket_misses

    return {
        "corpus_path": str(corpus_path),
        "run_dir": str(run_dir),
        "llm_mode": llm_mode,
        "summary": summary,
        "rows": rows,
    }


def write_reporting_obligation_eval_report(
    *,
    result: dict[str, Any],
    output_dir: Path,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_path = output_dir / "summary.json"
    rows_jsonl_path = output_dir / "rows.jsonl"
    rows_csv_path = output_dir / "rows.csv"
    markdown_path = output_dir / "report.md"

    summary = result.get("summary", {})
    rows = result.get("rows", [])

    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    with rows_jsonl_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")

    csv_fields = [
        "example_id",
        "source_kind",
        "expectation_bucket",
        "expected_outcome",
        "actual_outcome",
        "pass",
        "verdict",
        "false_promotion",
        "miss",
        "expected_grounded_concepts",
        "actual_grounded_concepts",
        "expected_candidate_concepts",
        "actual_candidate_concepts",
        "candidate_count",
        "promotion_count",
        "blocked_candidate_count",
    ]
    with rows_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_fields)
        writer.writeheader()
        for row in rows:
            record = {
                key: row.get(key)
                for key in csv_fields
            }
            for list_key in (
                "expected_grounded_concepts",
                "actual_grounded_concepts",
                "expected_candidate_concepts",
                "actual_candidate_concepts",
            ):
                value = record.get(list_key)
                if isinstance(value, list):
                    record[list_key] = ",".join(str(item) for item in value)
            writer.writerow(record)

    lines: list[str] = []
    lines.append("# Reporting Obligation Real-Document Evaluation")
    lines.append("")
    lines.append(f"- Corpus: `{result.get('corpus_path', '')}`")
    lines.append(f"- LLM mode: `{result.get('llm_mode', '')}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total examples: `{summary.get('total_examples', 0)}`")
    lines.append(f"- Grounded expected count: `{summary.get('grounded_expected_count', 0)}`")
    lines.append(f"- Grounded actual count: `{summary.get('grounded_actual_count', 0)}`")
    lines.append(f"- False promotions: `{summary.get('false_promotions', 0)}`")
    lines.append(f"- Misses on grounded examples: `{summary.get('misses_on_grounded_examples', 0)}`")
    lines.append(f"- Precision: `{summary.get('precision', 0)}`")
    lines.append(f"- Recall on grounded examples: `{summary.get('recall_on_grounded_examples', 0)}`")
    lines.append(f"- Buckets: `{summary.get('bucket_counts', {})}`")
    lines.append("")
    lines.append("## Example Outcomes")
    lines.append("")
    lines.append("| example_id | bucket | expected | actual | verdict | promotions | false_promotion | miss |")
    lines.append("|---|---|---|---|---|---:|---|---|")
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("example_id", "")),
                    str(row.get("expectation_bucket", "")),
                    str(row.get("expected_outcome", "")),
                    str(row.get("actual_outcome", "")),
                    str(row.get("verdict", "")),
                    str(row.get("promotion_count", 0)),
                    str(bool(row.get("false_promotion"))),
                    str(bool(row.get("miss"))),
                ]
            )
            + " |"
        )

    false_examples = [row for row in rows if bool(row.get("false_promotion"))]
    miss_examples = [row for row in rows if bool(row.get("miss"))]
    lines.append("")
    lines.append("## False Promotions")
    lines.append("")
    if not false_examples:
        lines.append("- None")
    else:
        for row in false_examples:
            lines.append(f"- `{row.get('example_id', '')}`: grounded `{', '.join(row.get('actual_grounded_concepts', []))}` unexpectedly")

    lines.append("")
    lines.append("## Misses")
    lines.append("")
    if not miss_examples:
        lines.append("- None")
    else:
        for row in miss_examples:
            lines.append(f"- `{row.get('example_id', '')}`: missing expected `{', '.join(row.get('miss_concepts', []))}`")

    lines.append("")
    lines.append("## Promotion Review")
    lines.append("")
    for row in rows:
        lines.append(f"### {row.get('example_id', '')}")
        lines.append(f"- Expected outcome: `{row.get('expected_outcome', '')}`")
        lines.append(f"- Actual outcome: `{row.get('actual_outcome', '')}`")
        lines.append(f"- Bucket: `{row.get('expectation_bucket', '')}`")
        lines.append(f"- Verdict: `{row.get('verdict', '')}`")
        lines.append(f"- Text: {row.get('text', '')}")
        promotions = row.get("promotions", [])
        if isinstance(promotions, list) and promotions:
            for promotion in promotions:
                lines.append(
                    "- Promoted:"
                    + f" concept `{promotion.get('required_concept_id', '')}`"
                    + f" | file `{promotion.get('doc_name', '')}`"
                    + f" | locator `{promotion.get('locator_type', '')}:{promotion.get('locator_value', '')}`"
                )
        else:
            lines.append("- Promoted: none")
        blocked_candidates = row.get("blocked_candidates", [])
        if isinstance(blocked_candidates, list) and blocked_candidates:
            for blocked in blocked_candidates:
                lines.append(
                    "- Blocked candidate:"
                    + f" concept `{blocked.get('candidate_concept_id', '')}`"
                    + f" | state `{blocked.get('grounding_state', '')}`"
                    + f" | reason `{blocked.get('reason', '')}`"
                )
        lines.append("")

    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "summary_json": str(summary_path),
        "rows_jsonl": str(rows_jsonl_path),
        "rows_csv": str(rows_csv_path),
        "report_markdown": str(markdown_path),
    }
