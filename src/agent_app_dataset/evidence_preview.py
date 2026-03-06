from __future__ import annotations

from pathlib import Path
import re
from typing import Any

try:  # pragma: no cover - optional dependency path
    from openpyxl import load_workbook
    from openpyxl.utils.cell import get_column_letter
except Exception:  # pragma: no cover - optional dependency path
    load_workbook = None
    get_column_letter = None

try:  # pragma: no cover - optional dependency path
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency path
    PdfReader = None


def resolve_storage_uri(uri: str) -> Path | None:
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


def _column_index(col: str) -> int:
    value = 0
    for char in col.upper():
        value = value * 26 + (ord(char) - 64)
    return max(value, 1)


def _parse_cell_locator(locator: str) -> tuple[int, int] | None:
    m = re.match(r"^([A-Za-z]+)([0-9]+)$", str(locator).strip())
    if not m:
        return None
    col = _column_index(m.group(1))
    row = int(m.group(2))
    return row, col


def _xlsx_grid_preview(path: Path, sheet_hint: str, locator: str) -> dict[str, Any]:
    if load_workbook is None:
        return {"kind": "none", "reason": "openpyxl_unavailable"}

    workbook = load_workbook(filename=path, data_only=True, read_only=True)
    try:
        sheet_name = sheet_hint.replace("Sheet: ", "").strip() if sheet_hint else ""
        sheet = workbook[sheet_name] if sheet_name in workbook.sheetnames else workbook.worksheets[0]

        target = _parse_cell_locator(locator) or (1, 1)
        row_idx, col_idx = target

        start_row = max(1, row_idx - 2)
        end_row = row_idx + 2
        start_col = max(1, col_idx - 2)
        end_col = col_idx + 2

        rows: list[list[dict[str, Any]]] = []
        for row_offset, row in enumerate(
            sheet.iter_rows(
                min_row=start_row,
                max_row=end_row,
                min_col=start_col,
                max_col=end_col,
            ),
            start=0,
        ):
            line: list[dict[str, Any]] = []
            for col_offset, cell in enumerate(row, start=0):
                abs_row = start_row + row_offset
                abs_col = start_col + col_offset
                if get_column_letter:
                    coordinate = f"{get_column_letter(abs_col)}{abs_row}"
                else:
                    coordinate = f"R{abs_row}C{abs_col}"
                value = getattr(cell, "value", None)
                line.append(
                    {
                        "coordinate": coordinate,
                        "value": value,
                        "highlight": abs_row == row_idx and abs_col == col_idx,
                    }
                )
            rows.append(line)

        return {
            "kind": "xlsx_grid",
            "sheet": sheet.title,
            "target": locator,
            "rows": rows,
        }
    finally:
        workbook.close()


def _pdf_text_preview(path: Path, locator: str) -> dict[str, Any]:
    if PdfReader is None:
        return {"kind": "none", "reason": "pypdf_unavailable"}

    reader = PdfReader(str(path))
    page_no = 1
    m = re.match(r"p([0-9]+):", locator)
    if m:
        page_no = max(1, int(m.group(1)))

    page_no = min(page_no, len(reader.pages))
    text = (reader.pages[page_no - 1].extract_text() or "").strip()
    return {
        "kind": "pdf_text",
        "page": page_no,
        "text": text[:4000],
    }


def build_evidence_preview(
    trace_row: dict[str, Any],
    package_manifest: dict[str, Any],
) -> dict[str, Any]:
    evidence = trace_row.get("evidence", {})
    doc_id = str(evidence.get("doc_id", ""))
    locator_type = str(evidence.get("locator_type", "paragraph"))
    locator_value = str(evidence.get("locator_value", ""))

    file_meta = None
    for file in package_manifest.get("files", []):
        if str(file.get("file_id", "")) == doc_id:
            file_meta = file
            break

    if file_meta is None:
        return {
            "available": False,
            "reason": "document_not_in_manifest",
            "doc_id": doc_id,
            "locator_type": locator_type,
            "locator_value": locator_value,
        }

    storage_uri = str(file_meta.get("storage_uri", ""))
    resolved_path = resolve_storage_uri(storage_uri)
    if resolved_path is None:
        return {
            "available": False,
            "reason": "document_unavailable",
            "doc_id": doc_id,
            "doc_type": str(file_meta.get("doc_type", "")).upper(),
            "filename": str(file_meta.get("filename", "")),
            "storage_uri": storage_uri,
            "locator_type": locator_type,
            "locator_value": locator_value,
            "source_snippet": evidence.get("source_snippet", ""),
        }

    doc_type = str(file_meta.get("doc_type", "")).upper()
    if doc_type == "XLSX":
        preview = _xlsx_grid_preview(
            path=resolved_path,
            sheet_hint=str(evidence.get("page_or_sheet", "")),
            locator=locator_value,
        )
    elif doc_type == "PDF":
        preview = _pdf_text_preview(path=resolved_path, locator=locator_value)
    else:
        preview = {"kind": "none", "reason": "unsupported_doc_type"}

    return {
        "available": True,
        "doc_id": doc_id,
        "doc_type": doc_type,
        "filename": str(file_meta.get("filename", "")),
        "storage_uri": storage_uri,
        "locator_type": locator_type,
        "locator_value": locator_value,
        "source_snippet": evidence.get("source_snippet", ""),
        "preview": preview,
    }
