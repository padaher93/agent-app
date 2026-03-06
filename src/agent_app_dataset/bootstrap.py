from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path

from .constants import STARTER_CONCEPT_IDS
from .io_utils import write_json


@dataclass
class DealPeriod:
    deal_id: str
    package_id: str
    period_end_date: str
    baseline: bool


DEAL_PERIODS = [
    DealPeriod("deal_alderon", "pkg_0001", "2025-09-30", True),
    DealPeriod("deal_alderon", "pkg_0002", "2025-12-31", False),
    DealPeriod("deal_bracken", "pkg_0003", "2025-09-30", True),
    DealPeriod("deal_bracken", "pkg_0004", "2025-12-31", False),
    DealPeriod("deal_coriant", "pkg_0005", "2025-08-31", True),
    DealPeriod("deal_coriant", "pkg_0006", "2025-11-30", False),
    DealPeriod("deal_dunlin", "pkg_0007", "2025-10-31", True),
    DealPeriod("deal_dunlin", "pkg_0008", "2026-01-31", False),
    DealPeriod("deal_ember", "pkg_0009", "2025-09-30", True),
    DealPeriod("deal_ember", "pkg_0010", "2025-12-31", False),
]


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _source_url(index: int, doc_type: str) -> str:
    if doc_type == "PDF":
        # Representative public SEC/EDGAR-style PDF endpoints.
        return f"https://www.sec.gov/Archives/edgar/data/1007019/00014931522404117{index % 10}/formars.pdf"
    return f"https://www.sec.gov/Archives/edgar/data/1722926/0001193125212145{index:02d}/d127351dex101.htm"


def build_source_registry(output_file: Path) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    sources = []
    for i in range(1, 41):
        doc_type = "PDF" if i % 2 == 0 else "XLSX"
        source_id = f"src_{i:04d}"
        url = _source_url(i, doc_type)
        checksum = _hash_text(f"{source_id}:{url}")
        sources.append(
            {
                "source_id": source_id,
                "url": url,
                "retrieved_at": now,
                "checksum": checksum,
                "doc_type": doc_type,
                "license_note": "Public filing metadata; raw file stored in object storage.",
                "storage_uri": f"s3://patricius-proxy-sources/{source_id}.{doc_type.lower()}",
            }
        )

    payload = {
        "schema_version": "1.0",
        "generated_at": now,
        "sources": sources,
    }
    write_json(output_file, payload)
    return payload


def _concept_base_value(concept_id: str) -> float:
    base_values = {
        "revenue_total": 12500000.0,
        "ebitda_reported": 2450000.0,
        "ebitda_adjusted": 2620000.0,
        "operating_income_ebit": 1980000.0,
        "interest_expense": 460000.0,
        "net_income": 1210000.0,
        "cash_and_equivalents": 1850000.0,
        "accounts_receivable_total": 2730000.0,
        "inventory_total": 1490000.0,
        "accounts_payable_total": 2140000.0,
        "total_debt": 11200000.0,
        "total_assets": 28600000.0,
        "total_liabilities": 16700000.0,
    }
    return base_values[concept_id]


def _value_for(deal_idx: int, is_baseline: bool, concept_id: str) -> float:
    base = _concept_base_value(concept_id)
    deal_multiplier = 1.0 + (deal_idx * 0.08)
    period_multiplier = 1.0 if is_baseline else 1.06
    return round(base * deal_multiplier * period_multiplier, 2)


def _file_entries_for_package(package_idx: int, source_ids: list[str]) -> list[dict]:
    file_count = 2 + (package_idx % 3)
    files = []
    for i in range(file_count):
        source_id = source_ids[(package_idx * 3 + i) % len(source_ids)]
        doc_type = "PDF" if int(source_id.split("_")[1]) % 2 == 0 else "XLSX"
        file_id = f"file_{package_idx + 1:04d}_{i + 1:02d}"
        files.append(
            {
                "file_id": file_id,
                "source_id": source_id,
                "doc_type": doc_type,
                "filename": f"{file_id}.{doc_type.lower()}",
                "storage_uri": f"s3://patricius-proxy-sources/{source_id}.{doc_type.lower()}",
                "checksum": _hash_text(file_id + source_id),
                "pages_or_sheets": 12 if doc_type == "PDF" else 5,
            }
        )
    return files


def build_pilot_packages_and_labels(
    source_registry: dict,
    packages_dir: Path,
    labels_dir: Path,
) -> dict[str, int]:
    source_ids = [s["source_id"] for s in source_registry["sources"]]
    now = datetime.now(timezone.utc)

    packages_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    for idx, period in enumerate(DEAL_PERIODS):
        files = _file_entries_for_package(idx, source_ids)
        package_payload = {
            "schema_version": "1.0",
            "package_id": period.package_id,
            "deal_id": period.deal_id,
            "period_end_date": period.period_end_date,
            "source_email_id": f"email_{idx + 1:04d}",
            "received_at": (now + timedelta(minutes=idx)).isoformat(),
            "files": files,
            "source_ids": sorted({file["source_id"] for file in files}),
            "variant_tags": ["pilot", "baseline" if period.baseline else "follow_up"],
            "quality_flags": [],
            "labeling_workflow": {
                "primary_labeler_status": "completed",
                "reviewer_status": "completed",
                "adjudication_status": "not_required",
            },
            "notes": "Proxy package built from public-source metadata.",
        }
        write_json(packages_dir / f"{period.package_id}.json", package_payload)

        rows = []
        for concept_idx, concept_id in enumerate(STARTER_CONCEPT_IDS):
            normalized_value = _value_for(idx // 2, period.baseline, concept_id)
            expected_status = "verified"
            flags: list[str] = []

            # Intentionally include small unresolved pockets for realism.
            if period.package_id in {"pkg_0004", "pkg_0008"} and concept_id in {
                "inventory_total",
                "accounts_receivable_total",
            }:
                expected_status = "candidate_flagged"
                flags = ["label_variation"]

            if period.package_id == "pkg_0010" and concept_id == "interest_expense":
                expected_status = "unresolved"
                flags = ["missing_schedule"]

            evidence_file = files[(concept_idx + idx) % len(files)]
            locator_type = "cell" if evidence_file["doc_type"] == "XLSX" else "paragraph"
            locator_value = (
                f"{['B', 'C', 'D'][idx % 3]}{10 + (idx % 5)}"
                if locator_type == "cell"
                else f"p{2 + (idx % 6)}:l{3 + (idx % 4)}"
            )

            rows.append(
                {
                    "trace_id": f"tr_{period.package_id}_{concept_id}",
                    "concept_id": concept_id,
                    "period_end_date": period.period_end_date,
                    "raw_value_text": f"${normalized_value:,.2f}",
                    "normalized_value": normalized_value,
                    "unit_currency": "USD",
                    "expected_status": expected_status,
                    "labeler_confidence": 0.99 if expected_status == "verified" else 0.84,
                    "flags": flags,
                    "normalization": {
                        "raw_scale": "absolute",
                        "normalized_scale": "absolute",
                        "currency_conversion_applied": False,
                    },
                    "evidence": {
                        "doc_id": evidence_file["file_id"],
                        "doc_name": evidence_file["filename"],
                        "page_or_sheet": "Summary" if locator_type == "cell" else f"Page {2 + (idx % 6)}",
                        "locator_type": locator_type,
                        "locator_value": locator_value,
                        "source_snippet": (
                            f"{concept_id.replace('_', ' ').title()}: {normalized_value:,.2f}"
                        ),
                    },
                }
            )

        label_payload = {
            "schema_version": "1.0",
            "package_id": period.package_id,
            "deal_id": period.deal_id,
            "period_end_date": period.period_end_date,
            "dictionary_version": "v1.0",
            "labeling": {
                "primary_labeler": "analyst_a",
                "reviewer": "reviewer_a",
                "adjudication_required": False,
            },
            "rows": rows,
        }
        write_json(labels_dir / f"{period.package_id}.ground_truth.json", label_payload)

    return {
        "packages": len(DEAL_PERIODS),
        "labels": len(DEAL_PERIODS),
        "deals": len({p.deal_id for p in DEAL_PERIODS}),
    }
