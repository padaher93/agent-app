from __future__ import annotations

import json
from pathlib import Path

from agent_app_dataset.reporting_obligation_eval import (
    evaluate_real_reporting_obligation_corpus,
    load_real_reporting_obligation_corpus,
    write_reporting_obligation_eval_report,
)


def _fixture_path() -> Path:
    return Path(__file__).parent / "fixtures" / "real_reporting_obligation_corpus.jsonl"


def _row_by_id(result: dict, example_id: str) -> dict:
    for row in result.get("rows", []):
        if row.get("example_id") == example_id:
            return row
    raise AssertionError(f"missing_example_row:{example_id}")


def test_real_reporting_obligation_corpus_fixture_loads() -> None:
    corpus = load_real_reporting_obligation_corpus(_fixture_path())
    assert corpus
    assert all(example.example_id for example in corpus)
    assert all(example.source_kind for example in corpus)
    assert all(example.text for example in corpus)
    assert all(example.expected_outcome in {"grounded", "ambiguous", "unsupported"} for example in corpus)


def test_real_eval_generates_review_report_artifacts(tmp_path: Path) -> None:
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


def test_real_eval_explicit_net_income_clause_is_grounded(tmp_path: Path) -> None:
    result = evaluate_real_reporting_obligation_corpus(
        corpus_path=_fixture_path(),
        run_dir=tmp_path / "run",
        llm_mode="probe",
    )
    row = _row_by_id(result, "real_clause_net_income_quarterly")
    assert row["actual_outcome"] == "grounded"
    assert "net_income" in row["actual_grounded_concepts"]
    assert row["false_promotion"] is False
    assert row["miss"] is False


def test_real_eval_vague_clause_is_blocked_not_grounded(tmp_path: Path) -> None:
    result = evaluate_real_reporting_obligation_corpus(
        corpus_path=_fixture_path(),
        run_dir=tmp_path / "run",
        llm_mode="probe",
    )
    row = _row_by_id(result, "real_clause_overview_only")
    assert row["actual_outcome"] in {"ambiguous", "unsupported"}
    assert row["actual_grounded_concepts"] == []


def test_real_eval_unsupported_concepts_do_not_become_grounded(tmp_path: Path) -> None:
    result = evaluate_real_reporting_obligation_corpus(
        corpus_path=_fixture_path(),
        run_dir=tmp_path / "run",
        llm_mode="probe",
    )
    ebitda_row = _row_by_id(result, "real_clause_ebitda_requirement_eval_only")
    cash_row = _row_by_id(result, "real_clause_cash_requirement_eval_only")

    assert "ebitda_reported" in ebitda_row["actual_candidate_concepts"]
    assert "ebitda_reported" not in ebitda_row["actual_grounded_concepts"]
    assert "cash_and_equivalents" in cash_row["actual_candidate_concepts"]
    assert "cash_and_equivalents" not in cash_row["actual_grounded_concepts"]


def test_real_eval_rows_distinguish_false_promotion_vs_miss(tmp_path: Path) -> None:
    corpus_path = tmp_path / "real_eval_flags.jsonl"
    entries = [
        {
            "example_id": "flag_false_promotion",
            "source_kind": "agreement_clause",
            "text": "Borrower shall provide Net Income with each quarterly reporting package.",
            "expected_outcome": "unsupported",
            "expected_grounded_concepts": [],
            "expected_candidate_concepts": ["net_income"],
            "notes": "Intentional mismatch to force false promotion flag.",
        },
        {
            "example_id": "flag_miss",
            "source_kind": "agreement_clause",
            "text": "Borrower shall provide EBITDA with each quarterly reporting package.",
            "expected_outcome": "grounded",
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
    false_row = _row_by_id(result, "flag_false_promotion")
    miss_row = _row_by_id(result, "flag_miss")

    assert false_row["false_promotion"] is True
    assert false_row["miss"] is False
    assert false_row["verdict"] in {"false_promotion", "false_promotion_and_miss"}

    assert miss_row["false_promotion"] is False
    assert miss_row["miss"] is True
    assert miss_row["verdict"] in {"miss", "false_promotion_and_miss"}

