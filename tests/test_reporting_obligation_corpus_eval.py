from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

from openpyxl import Workbook

from agent_app_dataset.internal_store import InternalStore
from agent_app_dataset.reporting_obligation_candidates import discover_reporting_obligation_candidates


class _ProbeLLMClient:
    def __init__(self, candidates: list[dict]) -> None:
        self._candidates = candidates
        self.model_name = "corpus-probe-llm"

    def run_json(self, *, agent_id: str, system_prompt: str, user_payload: dict) -> dict:
        assert agent_id == "agent_reporting_obligation_candidates"
        return {"candidates": self._candidates}


def _write_requirement_sheet(path: Path, text: str) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Requirements"
    for index, line in enumerate(str(text).splitlines() or [str(text)], start=1):
        sheet[f"A{index}"] = line
    workbook.save(path)


def _load_corpus() -> list[dict]:
    corpus_path = Path(__file__).parent / "fixtures" / "reporting_obligation_corpus.json"
    rows = json.loads(corpus_path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise AssertionError("reporting obligation corpus must be a list")
    return [row for row in rows if isinstance(row, dict)]


def _build_probe_candidates(example: dict, *, doc_id: str) -> list[dict]:
    text = str(example.get("text", "")).strip()
    expected = [str(item).strip().lower() for item in example.get("expected_concept_candidates", []) if str(item).strip()]
    probe_only = [str(item).strip().lower() for item in example.get("llm_probe_concepts", []) if str(item).strip()]
    concept_ids: list[str] = []
    for concept in expected + probe_only:
        if concept and concept not in concept_ids:
            concept_ids.append(concept)

    bucket = str(example.get("expectation_bucket", "")).strip().lower()
    if not concept_ids and bucket in {"adversarial_negative", "vague_negative"}:
        # Deliberately stress-test verifier against hallucination risk.
        concept_ids = ["net_income"]

    certainty_bucket = "high" if bucket in {"explicit_positive", "schedule", "mixed"} else "medium"
    return [
        {
            "doc_id": doc_id,
            "locator_type": "cell",
            "locator_value": "A1",
            "source_snippet": text,
            "candidate_obligation_type": "reporting_requirement",
            "candidate_concept_id": concept_id,
            "reason": f"corpus_probe:{bucket}",
            "certainty_bucket": certainty_bucket,
        }
        for concept_id in concept_ids
    ]


def _evaluate_example(tmp_path: Path, example: dict) -> dict:
    example_id = str(example.get("example_id", "")).strip()
    if not example_id:
        raise AssertionError("example_id is required")

    case_dir = tmp_path / example_id
    case_dir.mkdir(parents=True, exist_ok=True)
    doc_path = case_dir / f"{example_id}.xlsx"
    _write_requirement_sheet(doc_path, str(example.get("text", "")))

    doc_id = f"req_{example_id}"
    docs = [
        {
            "doc_id": doc_id,
            "doc_type": "XLSX",
            "filename": doc_path.name,
            "storage_uri": str(doc_path),
            "checksum": f"checksum_{example_id}",
            "pages_or_sheets": 1,
        }
    ]
    probe_candidates = _build_probe_candidates(example, doc_id=doc_id)

    result = discover_reporting_obligation_candidates(
        deal_id=f"deal_{example_id}",
        docs=docs,
        llm_client=_ProbeLLMClient(probe_candidates),
    )

    store = InternalStore(case_dir / "eval.sqlite3")
    deal_id = f"deal_{example_id}"
    store.ensure_deal(deal_id)
    store.upsert_reporting_obligation_candidates(
        deal_id=deal_id,
        candidates=result.get("candidates", []),
        clear_doc_ids=[doc_id],
    )
    store.upsert_reporting_obligations(
        deal_id=deal_id,
        obligations=result.get("promoted_obligations", []),
        clear_doc_ids=[doc_id],
    )

    candidate_rows = store.list_reporting_obligation_candidates(deal_id=deal_id)
    grounded_rows = store.list_reporting_obligations(deal_id=deal_id, grounding_state="grounded")

    expected_candidate = {
        str(item).strip().lower()
        for item in example.get("expected_concept_candidates", [])
        if str(item).strip()
    }
    expected_grounded = {
        str(item).strip().lower()
        for item in example.get("expected_grounded_concepts", [])
        if str(item).strip()
    }
    actual_candidate = {
        str(row.get("candidate_concept_id", "")).strip().lower()
        for row in candidate_rows
        if str(row.get("candidate_concept_id", "")).strip()
    }
    actual_grounded = {
        str(row.get("required_concept_id", "")).strip().lower()
        for row in grounded_rows
        if str(row.get("required_concept_id", "")).strip()
    }

    return {
        "example_id": example_id,
        "bucket": str(example.get("expectation_bucket", "")).strip().lower(),
        "expected_candidate": expected_candidate,
        "actual_candidate": actual_candidate,
        "expected_grounded": expected_grounded,
        "actual_grounded": actual_grounded,
        "candidate_count": len(candidate_rows),
        "grounded_count": len(grounded_rows),
        "promoted_count": len(result.get("promoted_obligations", [])),
    }


def test_reporting_obligation_corpus_precision_eval(tmp_path: Path) -> None:
    corpus = _load_corpus()
    assert 20 <= len(corpus) <= 30

    bucket_counts = Counter(str(row.get("expectation_bucket", "")).strip().lower() for row in corpus)
    assert bucket_counts["explicit_positive"] >= 4
    assert bucket_counts["vague_negative"] >= 3
    assert bucket_counts["descriptive_negative"] >= 3
    assert bucket_counts["adversarial_negative"] >= 3
    assert bucket_counts["mixed"] >= 2
    assert bucket_counts["schedule"] >= 2
    assert bucket_counts["ebitda_eval"] >= 1
    assert bucket_counts["cash_eval"] >= 1

    evaluated = [_evaluate_example(tmp_path, example) for example in corpus]

    false_promotions = 0
    true_promotions = 0
    explicit_expected = 0
    explicit_hits = 0
    ambiguous_blocked = 0
    blocked_total = 0

    for row in evaluated:
        unexpected_grounded = row["actual_grounded"] - row["expected_grounded"]
        missing_expected_grounded = row["expected_grounded"] - row["actual_grounded"]
        false_promotions += len(unexpected_grounded)
        true_promotions += len(row["actual_grounded"] & row["expected_grounded"])

        if row["bucket"] in {"explicit_positive", "schedule"}:
            explicit_expected += len(row["expected_grounded"])
            explicit_hits += len(row["actual_grounded"] & row["expected_grounded"])

        if row["bucket"] in {
            "vague_negative",
            "descriptive_negative",
            "adversarial_negative",
            "ebitda_eval",
            "cash_eval",
        }:
            blocked_total += 1
            if not row["actual_grounded"]:
                ambiguous_blocked += 1

        # Expected candidate concepts should be represented when probes are provided.
        assert row["expected_candidate"].issubset(row["actual_candidate"])
        # Grounded concepts must never exceed expected grounded truth for each line.
        assert not unexpected_grounded
        # Explicit positives should only miss if we intentionally tighten aggressively.
        if row["bucket"] in {"explicit_positive", "schedule"}:
            assert not missing_expected_grounded

    precision = true_promotions / (true_promotions + false_promotions) if (true_promotions + false_promotions) else 1.0
    recall_explicit = explicit_hits / explicit_expected if explicit_expected else 1.0
    blocked_rate = ambiguous_blocked / blocked_total if blocked_total else 1.0

    assert false_promotions == 0
    assert precision == 1.0
    assert recall_explicit >= 0.95
    assert blocked_rate >= 0.95

    # Promotion boundary remains conservative: only net_income can be grounded truth.
    promoted_concepts = {
        concept
        for row in evaluated
        for concept in row["actual_grounded"]
    }
    assert promoted_concepts <= {"net_income"}

