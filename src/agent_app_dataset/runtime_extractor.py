from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

from .constants import CONCEPT_DEFINITIONS, CONCEPT_LABELS, STARTER_CONCEPT_IDS
from .normalization import normalize_value
from .policy import classify_status

try:  # pragma: no cover - optional dependency path
    from openpyxl import load_workbook
except Exception:  # pragma: no cover - optional dependency path
    load_workbook = None

try:  # pragma: no cover - optional dependency path
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency path
    PdfReader = None


@dataclass(frozen=True)
class ConceptCandidate:
    concept_id: str
    raw_value_text: str
    source_snippet: str
    doc_id: str
    doc_name: str
    page_or_sheet: str
    locator_type: str
    locator_value: str
    confidence: float


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_storage_uri(uri: str) -> Path | None:
    normalized = str(uri).strip()
    if not normalized:
        return None

    if normalized.startswith("file://"):
        candidate = Path(normalized[len("file://") :])
    elif normalized.startswith("s3://"):
        return None
    else:
        candidate = Path(normalized)

    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()

    return candidate if candidate.exists() else None


def _extract_numeric_text(value: Any) -> str | None:
    if isinstance(value, (int, float)):
        return str(value)
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    m = re.search(r"-?[0-9][0-9,]*(?:\.[0-9]+)?", text)
    if not m:
        return None
    return m.group(0)


def _keyword_score(line: str, keywords: tuple[str, ...]) -> float:
    lowered = line.lower()
    if not lowered:
        return 0.0

    score = 0.0
    for keyword in keywords:
        kw = keyword.lower()
        if kw == lowered.strip():
            score = max(score, 1.0)
        elif kw in lowered:
            score = max(score, 0.92)
    return score


def _candidate_from_text(
    concept_id: str,
    line: str,
    doc_id: str,
    doc_name: str,
    page_or_sheet: str,
    locator_type: str,
    locator_value: str,
) -> ConceptCandidate | None:
    keywords = tuple(CONCEPT_DEFINITIONS[concept_id]["keywords"])
    keyword_score = _keyword_score(line, keywords)
    if keyword_score <= 0:
        return None

    numeric_text = _extract_numeric_text(line)
    if numeric_text:
        confidence = min(0.97, keyword_score + 0.02)
        raw_value_text = numeric_text
    else:
        confidence = 0.81
        raw_value_text = ""

    return ConceptCandidate(
        concept_id=concept_id,
        raw_value_text=raw_value_text,
        source_snippet=line[:500],
        doc_id=doc_id,
        doc_name=doc_name,
        page_or_sheet=page_or_sheet,
        locator_type=locator_type,
        locator_value=locator_value,
        confidence=round(confidence, 4),
    )


def _read_xlsx_candidates(path: Path, file_meta: dict[str, Any]) -> list[ConceptCandidate]:
    if load_workbook is None:
        return []

    candidates: list[ConceptCandidate] = []
    workbook = load_workbook(filename=path, data_only=True, read_only=True)

    try:
        for sheet in workbook.worksheets:
            for row in sheet.iter_rows(min_row=1, max_row=500):
                row_values: list[str] = []
                numeric_by_col: dict[int, str] = {}
                for idx, cell in enumerate(row):
                    value = cell.value
                    if value is None:
                        continue
                    if isinstance(value, str):
                        row_values.append(value)
                    elif isinstance(value, (int, float)):
                        row_values.append(str(value))
                    numeric_text = _extract_numeric_text(value)
                    if numeric_text:
                        numeric_by_col[idx] = numeric_text

                if not row_values:
                    continue

                combined = " | ".join(row_values)
                for concept_id in STARTER_CONCEPT_IDS:
                    first_cell = row[0] if row else None
                    locator_value = getattr(first_cell, "coordinate", "") if first_cell is not None else ""
                    base = _candidate_from_text(
                        concept_id=concept_id,
                        line=combined,
                        doc_id=str(file_meta.get("file_id", "")),
                        doc_name=str(file_meta.get("filename", path.name)),
                        page_or_sheet=f"Sheet: {sheet.title}",
                        locator_type="cell",
                        locator_value=locator_value,
                    )
                    if base is None:
                        continue

                    if not base.raw_value_text and numeric_by_col:
                        first_numeric = next(iter(numeric_by_col.values()))
                        base = ConceptCandidate(
                            concept_id=base.concept_id,
                            raw_value_text=first_numeric,
                            source_snippet=base.source_snippet,
                            doc_id=base.doc_id,
                            doc_name=base.doc_name,
                            page_or_sheet=base.page_or_sheet,
                            locator_type=base.locator_type,
                            locator_value=base.locator_value,
                            confidence=0.85,
                        )
                    candidates.append(base)
    finally:
        workbook.close()

    return candidates


def _read_pdf_candidates(path: Path, file_meta: dict[str, Any]) -> list[ConceptCandidate]:
    if PdfReader is None:
        return []

    candidates: list[ConceptCandidate] = []
    reader = PdfReader(str(path))

    for page_num, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        if not text.strip():
            continue

        for line_num, line in enumerate(text.splitlines(), start=1):
            clean = line.strip()
            if not clean:
                continue

            for concept_id in STARTER_CONCEPT_IDS:
                candidate = _candidate_from_text(
                    concept_id=concept_id,
                    line=clean,
                    doc_id=str(file_meta.get("file_id", "")),
                    doc_name=str(file_meta.get("filename", path.name)),
                    page_or_sheet=f"Page {page_num}",
                    locator_type="paragraph",
                    locator_value=f"p{page_num}:l{line_num}",
                )
                if candidate is not None:
                    candidates.append(candidate)

    return candidates


def _collect_candidates(package_manifest: dict[str, Any]) -> tuple[dict[str, list[ConceptCandidate]], bool]:
    by_concept: dict[str, list[ConceptCandidate]] = {concept_id: [] for concept_id in STARTER_CONCEPT_IDS}
    missing_sources = False

    for file_meta in package_manifest.get("files", []):
        doc_type = str(file_meta.get("doc_type", "")).upper()
        path = _resolve_storage_uri(str(file_meta.get("storage_uri", "")))
        if path is None:
            missing_sources = True
            continue

        if doc_type == "XLSX":
            extracted = _read_xlsx_candidates(path=path, file_meta=file_meta)
        elif doc_type == "PDF":
            extracted = _read_pdf_candidates(path=path, file_meta=file_meta)
        else:
            extracted = []

        for item in extracted:
            by_concept[item.concept_id].append(item)

    return by_concept, missing_sources


def _best_candidate(candidates: list[ConceptCandidate]) -> ConceptCandidate | None:
    if not candidates:
        return None
    ranked = sorted(
        candidates,
        key=lambda item: (item.confidence, bool(item.raw_value_text), item.doc_id),
        reverse=True,
    )
    return ranked[0]


def runtime_extract_package_predictions(
    package_manifest: dict[str, Any],
    deal_currency: str = "USD",
) -> dict[str, Any]:
    package_id = str(package_manifest["package_id"])
    concept_candidates, missing_sources = _collect_candidates(package_manifest)

    rows: list[dict[str, Any]] = []
    extracted_at = _now_utc()

    for concept_id in STARTER_CONCEPT_IDS:
        trace_id = f"tr_{package_id}_{concept_id}"
        selected = _best_candidate(concept_candidates.get(concept_id, []))

        if selected is None:
            blockers = ["missing_evidence_location", "missing_numeric_value"]
            if missing_sources:
                blockers.append("missing_source_document")

            row = {
                "concept_id": concept_id,
                "label": CONCEPT_LABELS[concept_id],
                "status": "unresolved",
                "dictionary_version": "v1.0",
                "raw_value_text": "",
                "normalized_value": None,
                "current_value": None,
                "unit_currency": deal_currency,
                "confidence": 0.0,
                "hard_blockers": blockers,
                "trace_id": trace_id,
                "evidence_link": {
                    "doc_id": "",
                    "doc_name": "",
                    "page_or_sheet": "",
                    "locator_type": "paragraph",
                    "locator_value": "",
                },
                "evidence": {
                    "doc_id": "",
                    "doc_name": "",
                    "page_or_sheet": "",
                    "locator_type": "paragraph",
                    "locator_value": "",
                    "source_snippet": "",
                    "raw_value_text": "",
                    "normalized_value": None,
                    "unit_currency": deal_currency,
                    "extractor_agent_id": "agent_3",
                    "verifier_agent_id": "agent_4",
                    "trace_id": trace_id,
                    "extracted_at": extracted_at,
                },
            }
            rows.append(row)
            continue

        normalization = normalize_value(
            raw_value_text=selected.raw_value_text,
            source_snippet=selected.source_snippet,
            deal_currency=deal_currency,
        )

        blockers: list[str] = []
        if normalization.unresolved_reason:
            blockers.append(normalization.unresolved_reason)
        if not selected.doc_id or not selected.locator_value:
            blockers.append("missing_evidence_location")

        status = classify_status(confidence=selected.confidence, hard_blockers=blockers)
        normalized_value = normalization.normalized_value
        evidence = {
            "doc_id": selected.doc_id,
            "doc_name": selected.doc_name,
            "page_or_sheet": selected.page_or_sheet,
            "locator_type": selected.locator_type,
            "locator_value": selected.locator_value,
            "source_snippet": selected.source_snippet,
            "raw_value_text": selected.raw_value_text,
            "normalized_value": normalized_value,
            "unit_currency": normalization.unit_currency,
            "extractor_agent_id": "agent_3",
            "verifier_agent_id": "agent_4",
            "trace_id": trace_id,
            "extracted_at": extracted_at,
        }

        rows.append(
            {
                "concept_id": concept_id,
                "label": CONCEPT_LABELS[concept_id],
                "status": status,
                "dictionary_version": "v1.0",
                "raw_value_text": selected.raw_value_text,
                "normalized_value": normalized_value,
                "current_value": normalized_value,
                "unit_currency": normalization.unit_currency,
                "confidence": round(float(selected.confidence), 4),
                "hard_blockers": sorted(set(blockers)),
                "trace_id": trace_id,
                "evidence_link": {
                    "doc_id": selected.doc_id,
                    "doc_name": selected.doc_name,
                    "page_or_sheet": selected.page_or_sheet,
                    "locator_type": selected.locator_type,
                    "locator_value": selected.locator_value,
                },
                "evidence": evidence,
            }
        )

    return {
        "package_id": package_id,
        "deal_id": package_manifest["deal_id"],
        "period_end_date": package_manifest["period_end_date"],
        "rows": rows,
    }
