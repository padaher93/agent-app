from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from openpyxl import Workbook

from agent_app_dataset.internal_api import create_app


class _CaseLLMClient:
    def __init__(self, candidates: list[dict]) -> None:
        self._candidates = candidates
        self.model_name = "robustness-llm"

    def run_json(self, *, agent_id: str, system_prompt: str, user_payload: dict) -> dict:
        assert agent_id == "agent_reporting_obligation_candidates"
        return {"candidates": self._candidates}


def _write_requirement_sheet(path: Path, lines: list[str]) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Requirements"
    for idx, line in enumerate(lines, start=1):
        sheet[f"A{idx}"] = line
    workbook.save(path)


def _write_borrower_sheet(path: Path, rows: list[tuple[str, float]]) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Coverage"
    for idx, (label, value) in enumerate(rows, start=1):
        sheet[f"A{idx}"] = label
        sheet[f"B{idx}"] = value
    workbook.save(path)


def _build_client(tmp_path: Path, candidates: list[dict]) -> TestClient:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        reporting_obligation_llm_client=_CaseLLMClient(candidates),
    )
    return TestClient(app)


def _create_deal(client: TestClient, deal_id: str) -> None:
    response = client.post(
        "/internal/v1/deals",
        json={"display_name": deal_id, "deal_id": deal_id},
    )
    assert response.status_code == 200


def _ingest_requirement_doc(
    client: TestClient,
    *,
    deal_id: str,
    doc_id: str,
    path: Path,
) -> dict:
    response = client.post(
        f"/internal/v1/deals/{deal_id}/reporting-obligations:ingest",
        json={
            "docs": [
                {
                    "doc_id": doc_id,
                    "doc_type": "XLSX",
                    "filename": path.name,
                    "storage_uri": str(path),
                    "checksum": f"checksum_{doc_id}",
                    "pages_or_sheets": 1,
                }
            ],
            "clear_existing_for_docs": True,
            "llm_discovery": "on",
        },
    )
    assert response.status_code == 200
    return response.json()


def _list_candidates(client: TestClient, deal_id: str) -> list[dict]:
    response = client.get(f"/internal/v1/deals/{deal_id}/reporting-obligation-candidates")
    assert response.status_code == 200
    return response.json()["candidates"]


def _list_grounded_obligations(client: TestClient, deal_id: str) -> list[dict]:
    response = client.get(
        f"/internal/v1/deals/{deal_id}/reporting-obligations",
        params={"grounding_state": "grounded"},
    )
    assert response.status_code == 200
    return response.json()["obligations"]


def _ingest_package(
    client: TestClient,
    *,
    deal_id: str,
    source_email_id: str,
    period_end_date: str,
    received_at: str,
    file_id: str,
    source_id: str,
    path: Path,
) -> str:
    response = client.post(
        "/internal/v1/packages:ingest",
        json={
            "sender_email": "ops@borrower.com",
            "source_email_id": source_email_id,
            "deal_id": deal_id,
            "period_end_date": period_end_date,
            "received_at": received_at,
            "files": [
                {
                    "file_id": file_id,
                    "source_id": source_id,
                    "doc_type": "XLSX",
                    "filename": path.name,
                    "storage_uri": str(path),
                    "checksum": f"checksum_{source_id}",
                    "pages_or_sheets": 1,
                }
            ],
            "variant_tags": ["runtime_test"],
            "quality_flags": [],
        },
    )
    assert response.status_code == 200
    return response.json()["package_id"]


def _process_runtime(client: TestClient, package_id: str) -> None:
    response = client.post(
        f"/internal/v1/packages/{package_id}:process",
        json={"async_mode": False, "max_retries": 1, "extraction_mode": "runtime"},
    )
    assert response.status_code == 200
    assert response.json()["status"] in {"completed", "needs_review"}


def _run_case(
    tmp_path: Path,
    *,
    case_id: str,
    requirement_line: str,
    llm_candidate: dict,
) -> tuple[dict, list[dict], list[dict]]:
    client = _build_client(tmp_path / case_id, [llm_candidate])
    deal_id = f"deal_{case_id}"
    _create_deal(client, deal_id)

    requirement_path = (tmp_path / case_id) / "requirements.xlsx"
    requirement_path.parent.mkdir(parents=True, exist_ok=True)
    _write_requirement_sheet(requirement_path, [requirement_line])

    ingest_payload = _ingest_requirement_doc(
        client,
        deal_id=deal_id,
        doc_id=f"req_{case_id}",
        path=requirement_path,
    )
    candidates = _list_candidates(client, deal_id)
    obligations = _list_grounded_obligations(client, deal_id)
    return ingest_payload, candidates, obligations


def _summarize_cases(results: list[dict]) -> dict[str, int]:
    summary = {
        "total_cases": 0,
        "expected_promotions": 0,
        "actual_promotions": 0,
        "false_promotions": 0,
        "explicit_misses": 0,
    }
    for result in results:
        summary["total_cases"] += 1
        if result["expected_promoted"]:
            summary["expected_promotions"] += 1
            if not result["promoted"]:
                summary["explicit_misses"] += 1
        if result["promoted"]:
            summary["actual_promotions"] += 1
            if not result["expected_promoted"]:
                summary["false_promotions"] += 1
    return summary


def test_explicit_examples_promote_when_source_linkage_is_real(tmp_path: Path) -> None:
    explicit_cases = [
        (
            "explicit_shall",
            "Borrower shall provide net profit with each quarterly reporting package.",
            {
                "doc_id": "req_explicit_shall",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": "Borrower shall provide net profit with each quarterly reporting package.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net profit",
                "reason": "explicit requirement",
                "certainty_bucket": "high",
            },
        ),
        (
            "explicit_must",
            "Borrower must include net profit in each quarterly reporting package.",
            {
                "doc_id": "req_explicit_must",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": "Borrower must include net profit in each quarterly reporting package.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "explicit requirement",
                "certainty_bucket": "high",
            },
        ),
    ]

    for case_id, line, candidate in explicit_cases:
        ingest_payload, candidates, obligations = _run_case(
            tmp_path,
            case_id=case_id,
            requirement_line=line,
            llm_candidate=candidate,
        )
        assert ingest_payload["candidate_discovery"]["status"] == "completed"
        assert ingest_payload["candidate_discovery"]["summary"]["promoted"] >= 1
        assert obligations
        assert any(row["required_concept_id"] == "net_income" for row in obligations)
        promoted_rows = [row for row in candidates if row["promoted"]]
        assert promoted_rows
        promoted = promoted_rows[0]
        assert promoted["grounding_state"] == "grounded"
        assert promoted["candidate_concept_id"] == "net_income"
        assert promoted["source_snippet"]
        assert promoted["locator_type"]
        assert promoted["locator_value"]


def test_vague_or_descriptive_language_does_not_promote(tmp_path: Path) -> None:
    negative_lines = [
        "Quarterly reporting package overview.",
        "Net Income appears in the package index.",
        "Financial information should be discussed with lender.",
        "Net profit was positive for the quarter.",
        "The reporting package includes several metrics such as revenue and net profit.",
        "Management may provide operating results from time to time.",
    ]

    for idx, line in enumerate(negative_lines, start=1):
        case_id = f"negative_{idx}"
        ingest_payload, candidates, obligations = _run_case(
            tmp_path,
            case_id=case_id,
            requirement_line=line,
            llm_candidate={
                "doc_id": f"req_{case_id}",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": line,
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "possible requirement",
                "certainty_bucket": "medium",
            },
        )
        assert ingest_payload["candidate_discovery"]["status"] == "completed"
        assert ingest_payload["candidate_discovery"]["summary"]["promoted"] == 0
        assert not obligations
        assert candidates
        assert all(row["promoted"] is False for row in candidates)
        assert all(row["grounding_state"] in {"ambiguous", "unsupported"} for row in candidates)


def test_hallucination_risk_is_blocked_when_concept_not_explicit(tmp_path: Path) -> None:
    ingest_payload, candidates, obligations = _run_case(
        tmp_path,
        case_id="hallucination_concept_not_explicit",
        requirement_line="Borrower shall deliver quarterly financial statements to lender.",
        llm_candidate={
            "doc_id": "req_hallucination_concept_not_explicit",
            "locator_type": "cell",
            "locator_value": "A1",
            "source_snippet": "Borrower shall deliver quarterly financial statements to lender.",
            "candidate_obligation_type": "reporting_requirement",
            "candidate_concept_id": "net_income",
            "reason": "model inferred net income from financial statements",
            "certainty_bucket": "high",
        },
    )
    assert ingest_payload["candidate_discovery"]["summary"]["promoted"] == 0
    assert not obligations
    assert candidates
    candidate = candidates[0]
    assert candidate["grounding_state"] == "unsupported"
    assert candidate["promoted"] is False
    assert "concept_not_explicit_in_snippet" in candidate["reason"]


def test_source_linkage_integrity_blocks_promotion_on_locator_or_snippet_mismatch(tmp_path: Path) -> None:
    mismatch_cases = [
        (
            "locator_mismatch",
            {
                "doc_id": "req_locator_mismatch",
                "locator_type": "cell",
                "locator_value": "A99",
                "source_snippet": "Borrower shall provide net profit with each quarterly reporting package.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "wrong locator",
                "certainty_bucket": "high",
            },
            "locator_mismatch",
        ),
        (
            "snippet_mismatch",
            {
                "doc_id": "req_snippet_mismatch",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": "Borrower must include net income monthly in covenant package.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "wrong snippet",
                "certainty_bucket": "high",
            },
            "snippet_mismatch",
        ),
    ]

    for case_id, candidate, expected_reason in mismatch_cases:
        ingest_payload, candidates, obligations = _run_case(
            tmp_path,
            case_id=case_id,
            requirement_line="Borrower shall provide net profit with each quarterly reporting package.",
            llm_candidate=candidate,
        )
        assert ingest_payload["candidate_discovery"]["summary"]["promoted"] == 0
        assert not obligations
        assert candidates
        row = candidates[0]
        assert row["grounding_state"] == "unsupported"
        assert row["promoted"] is False
        assert expected_reason in row["reason"]


def test_review_queue_consumes_only_grounded_obligations_not_raw_candidates(tmp_path: Path) -> None:
    client = _build_client(
        tmp_path / "queue_boundary",
        [
            {
                "doc_id": "req_queue_boundary",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": "Borrower shall deliver quarterly financial statements to lender.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "inferred from statements",
                "certainty_bucket": "high",
            }
        ],
    )
    deal_id = "deal_queue_boundary"
    _create_deal(client, deal_id)

    requirement_path = (tmp_path / "queue_boundary") / "requirements.xlsx"
    requirement_path.parent.mkdir(parents=True, exist_ok=True)
    _write_requirement_sheet(
        requirement_path,
        ["Borrower shall deliver quarterly financial statements to lender."],
    )
    ingest_payload = _ingest_requirement_doc(
        client,
        deal_id=deal_id,
        doc_id="req_queue_boundary",
        path=requirement_path,
    )
    assert ingest_payload["candidate_discovery"]["summary"]["promoted"] == 0
    candidates = _list_candidates(client, deal_id)
    assert candidates
    assert all(row["promoted"] is False for row in candidates)

    baseline_path = (tmp_path / "queue_boundary") / "baseline.xlsx"
    current_path = (tmp_path / "queue_boundary") / "current.xlsx"
    _write_borrower_sheet(
        baseline_path,
        [
            ("Revenue total", 12_500_000),
            ("Net Income", 980_000),
            ("EBITDA adjusted", 2_610_000),
        ],
    )
    _write_borrower_sheet(
        current_path,
        [
            ("Revenue total", 12_300_000),
            ("EBITDA adjusted", 2_450_000),
            ("Interest expense", 460_000),
        ],
    )
    baseline_pkg = _ingest_package(
        client,
        deal_id=deal_id,
        source_email_id=f"{deal_id}_baseline",
        period_end_date="2025-06-30",
        received_at="2025-07-05T12:00:00+00:00",
        file_id=f"{deal_id}_file_baseline",
        source_id=f"{deal_id}_src_baseline",
        path=baseline_path,
    )
    current_pkg = _ingest_package(
        client,
        deal_id=deal_id,
        source_email_id=f"{deal_id}_current",
        period_end_date="2025-09-30",
        received_at="2025-10-05T12:00:00+00:00",
        file_id=f"{deal_id}_file_current",
        source_id=f"{deal_id}_src_current",
        path=current_path,
    )
    _process_runtime(client, baseline_pkg)
    _process_runtime(client, current_pkg)

    queue = client.get(f"/internal/v1/deals/{deal_id}/periods/{current_pkg}/review_queue")
    assert queue.status_code == 200
    net_income = next(item for item in queue.json()["items"] if item["metric_key"] == "net_income")
    assert net_income["case_mode"] in {"investigation_missing_source", "investigation_candidate_only"}
    assert net_income["case_mode"] != "investigation_missing_required_reporting"
    assert net_income["requirement_anchor"] is None
    assert net_income["obligation_grounding_state"] == "not_grounded"


def test_robustness_corpus_summary_has_no_false_promotions(tmp_path: Path) -> None:
    corpus = [
        {
            "case_id": "summary_explicit",
            "line": "Borrower shall provide net profit with each quarterly reporting package.",
            "candidate": {
                "doc_id": "req_summary_explicit",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": "Borrower shall provide net profit with each quarterly reporting package.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "explicit requirement",
                "certainty_bucket": "high",
            },
            "expected_promoted": True,
        },
        {
            "case_id": "summary_vague",
            "line": "Quarterly reporting package overview.",
            "candidate": {
                "doc_id": "req_summary_vague",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": "Quarterly reporting package overview.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "weak signal",
                "certainty_bucket": "medium",
            },
            "expected_promoted": False,
        },
        {
            "case_id": "summary_hallucination",
            "line": "Borrower shall deliver quarterly financial statements to lender.",
            "candidate": {
                "doc_id": "req_summary_hallucination",
                "locator_type": "cell",
                "locator_value": "A1",
                "source_snippet": "Borrower shall deliver quarterly financial statements to lender.",
                "candidate_obligation_type": "reporting_requirement",
                "candidate_concept_id": "net_income",
                "reason": "inferred concept",
                "certainty_bucket": "high",
            },
            "expected_promoted": False,
        },
    ]

    results: list[dict] = []
    for case in corpus:
        ingest_payload, candidates, obligations = _run_case(
            tmp_path,
            case_id=str(case["case_id"]),
            requirement_line=str(case["line"]),
            llm_candidate=dict(case["candidate"]),
        )
        promoted = bool(obligations)
        results.append(
            {
                "case_id": case["case_id"],
                "expected_promoted": bool(case["expected_promoted"]),
                "promoted": promoted,
                "candidate_count": len(candidates),
                "grounded_count": ingest_payload["candidate_discovery"]["summary"]["grounded"],
            }
        )

    summary = _summarize_cases(results)
    assert summary["explicit_misses"] == 0
    assert summary["false_promotions"] == 0
