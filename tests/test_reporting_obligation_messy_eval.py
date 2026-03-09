from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

from agent_app_dataset.reporting_obligation_eval import (
    evaluate_real_reporting_obligation_corpus,
    load_real_reporting_obligation_corpus,
    write_reporting_obligation_eval_report,
)


def _fixture_path() -> Path:
    return Path(__file__).parent / "fixtures" / "messy_reporting_obligation_corpus.jsonl"


def _row_by_id(result: dict, example_id: str) -> dict:
    for row in result.get("rows", []):
        if row.get("example_id") == example_id:
            return row
    raise AssertionError(f"missing_example_row:{example_id}")


def test_messy_reporting_obligation_corpus_fixture_loads() -> None:
    corpus = load_real_reporting_obligation_corpus(_fixture_path())
    assert 30 <= len(corpus) <= 40

    bucket_counts = Counter(example.expectation_bucket for example in corpus)
    required_buckets = {
        "explicit_positive",
        "vague_negative",
        "descriptive_negative",
        "adversarial_negative",
        "defined_term_trap",
        "cross_reference_trap",
        "mixed",
        "schedule",
        "ebitda_eval",
        "cash_eval",
        "near_miss_negative",
    }
    assert required_buckets.issubset(set(bucket_counts.keys()))


def test_messy_eval_runs_with_precision_first_boundary(tmp_path: Path) -> None:
    result = evaluate_real_reporting_obligation_corpus(
        corpus_path=_fixture_path(),
        run_dir=tmp_path / "run",
        llm_mode="probe",
    )

    summary = result.get("summary", {})
    rows = list(result.get("rows", []))
    assert rows
    assert summary.get("total_examples") == len(rows)
    assert summary.get("false_promotions") == 0

    # Explicit positives and schedule positives should still ground net_income.
    for example_id in (
        "messy_explicit_net_income_shall_quarterly",
        "messy_explicit_net_income_must_monthly",
        "messy_explicit_net_earnings_certificate",
        "messy_explicit_net_profit_furnish",
        "messy_explicit_required_deliverable",
        "messy_explicit_package_line_item",
        "messy_schedule_explicit_net_income",
        "messy_schedule_table_required_net_income",
    ):
        row = _row_by_id(result, example_id)
        assert row["actual_outcome"] == "grounded"
        assert "net_income" in row["actual_grounded_concepts"]

    # Risk buckets must not falsely promote.
    blocked_buckets = {
        "vague_negative",
        "descriptive_negative",
        "adversarial_negative",
        "defined_term_trap",
        "cross_reference_trap",
        "ebitda_eval",
        "cash_eval",
        "near_miss_negative",
    }
    for row in rows:
        bucket = str(row.get("expectation_bucket", "")).strip().lower()
        if bucket in blocked_buckets:
            assert row["actual_grounded_concepts"] == []

    # Unsupported concepts remain candidate-only.
    ebitda_rows = [row for row in rows if row.get("expectation_bucket") == "ebitda_eval"]
    cash_rows = [row for row in rows if row.get("expectation_bucket") == "cash_eval"]
    assert ebitda_rows and cash_rows
    assert any("ebitda_reported" in row["actual_candidate_concepts"] for row in ebitda_rows)
    assert all("ebitda_reported" not in row["actual_grounded_concepts"] for row in ebitda_rows)
    assert any("cash_and_equivalents" in row["actual_candidate_concepts"] for row in cash_rows)
    assert all("cash_and_equivalents" not in row["actual_grounded_concepts"] for row in cash_rows)

    # Trust boundary: only net_income may become grounded product truth.
    grounded_concepts = {
        concept
        for row in rows
        for concept in row.get("actual_grounded_concepts", [])
    }
    assert grounded_concepts <= {"net_income"}

    # Bucket-level metrics should be emitted for report analysis.
    assert summary.get("bucket_counts", {})
    assert "explicit_positive" in summary.get("bucket_counts", {})


def test_messy_eval_generates_report_artifacts(tmp_path: Path) -> None:
    result = evaluate_real_reporting_obligation_corpus(
        corpus_path=_fixture_path(),
        run_dir=tmp_path / "run",
        llm_mode="probe",
    )
    artifacts = write_reporting_obligation_eval_report(
        result=result,
        output_dir=tmp_path / "report",
    )
    for key in ("summary_json", "rows_jsonl", "rows_csv", "report_markdown"):
        path = Path(str(artifacts[key]))
        assert path.exists()
        assert path.stat().st_size > 0


def test_messy_eval_distinguishes_false_promotion_vs_miss(tmp_path: Path) -> None:
    corpus_path = tmp_path / "messy_eval_flags.jsonl"
    entries = [
        {
            "example_id": "messy_flag_false_promotion",
            "source_kind": "agreement_clause",
            "text": "Borrower shall provide Net Income with each quarterly reporting package.",
            "expected_outcome": "unsupported",
            "expectation_bucket": "adversarial_negative",
            "expected_grounded_concepts": [],
            "expected_candidate_concepts": ["net_income"],
            "notes": "Intentional mismatch to force false promotion flag.",
        },
        {
            "example_id": "messy_flag_miss",
            "source_kind": "agreement_clause",
            "text": "Borrower shall provide EBITDA with each quarterly reporting package.",
            "expected_outcome": "grounded",
            "expectation_bucket": "ebitda_eval",
            "expected_grounded_concepts": ["ebitda_reported"],
            "expected_candidate_concepts": ["ebitda_reported"],
            "notes": "Intentional mismatch to force miss flag.",
        },
    ]
    with corpus_path.open("w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry))
            handle.write("\n")

    result = evaluate_real_reporting_obligation_corpus(
        corpus_path=corpus_path,
        run_dir=tmp_path / "run",
        llm_mode="probe",
    )
    false_row = _row_by_id(result, "messy_flag_false_promotion")
    miss_row = _row_by_id(result, "messy_flag_miss")

    assert false_row["false_promotion"] is True
    assert false_row["miss"] is False
    assert false_row["verdict"] in {"false_promotion", "false_promotion_and_miss"}

    assert miss_row["false_promotion"] is False
    assert miss_row["miss"] is True
    assert miss_row["verdict"] in {"miss", "false_promotion_and_miss"}
