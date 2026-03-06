from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import re

from .constants import STARTER_CONCEPT_IDS
from .io_utils import read_json, write_json


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _next_package_number(packages_dir: Path) -> int:
    max_num = 0
    for file in packages_dir.glob("pkg_*.json"):
        match = re.match(r"pkg_(\d{4})", file.stem)
        if match:
            max_num = max(max_num, int(match.group(1)))
    return max_num + 1


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


def _value_for(deal_idx: int, period_idx: int, concept_id: str) -> float:
    base = _concept_base_value(concept_id)
    deal_multiplier = 1.0 + deal_idx * 0.04
    period_multiplier = 1.0 + period_idx * 0.03
    return round(base * deal_multiplier * period_multiplier, 2)


def _existing_deals(packages_dir: Path) -> set[str]:
    deals = set()
    for file in packages_dir.glob("pkg_*.json"):
        payload = read_json(file)
        deals.add(payload["deal_id"])
    return deals


def _make_files(package_num: int, source_ids: list[str], period_idx: int) -> list[dict]:
    file_count = 2 + (package_num % 3)
    files: list[dict] = []
    for i in range(file_count):
        source_id = source_ids[(package_num + i) % len(source_ids)]
        doc_type = "PDF" if int(source_id.split("_")[1]) % 2 == 0 else "XLSX"
        file_id = f"file_{package_num:04d}_{i + 1:02d}"
        files.append(
            {
                "file_id": file_id,
                "source_id": source_id,
                "doc_type": doc_type,
                "filename": f"{file_id}.{doc_type.lower()}",
                "storage_uri": f"s3://patricius-proxy-sources/{source_id}.{doc_type.lower()}",
                "checksum": _hash_text(file_id + source_id),
                "pages_or_sheets": 8 + (period_idx % 6),
            }
        )
    return files


def _build_package_payload(
    package_num: int,
    deal_id: str,
    period_end_date: str,
    source_ids: list[str],
    now: datetime,
    period_idx: int,
    is_baseline: bool,
) -> dict:
    files = _make_files(package_num, source_ids, period_idx)
    package_id = f"pkg_{package_num:04d}"
    return {
        "schema_version": "1.0",
        "package_id": package_id,
        "deal_id": deal_id,
        "period_end_date": period_end_date,
        "source_email_id": f"email_{package_num:04d}",
        "received_at": (now + timedelta(minutes=package_num)).isoformat(),
        "files": files,
        "source_ids": sorted({file["source_id"] for file in files}),
        "variant_tags": ["scaled", "baseline" if is_baseline else "follow_up"],
        "quality_flags": [],
        "labeling_workflow": {
            "primary_labeler_status": "completed",
            "reviewer_status": "completed",
            "adjudication_status": "not_required",
        },
        "notes": "Scaled proxy package metadata.",
    }


def _build_label_payload(
    package_payload: dict,
    deal_idx: int,
    period_idx: int,
    uncertain_toggle: bool,
) -> dict:
    package_id = package_payload["package_id"]
    rows = []
    for concept_idx, concept_id in enumerate(STARTER_CONCEPT_IDS):
        value = _value_for(deal_idx, period_idx, concept_id)
        expected_status = "verified"
        confidence = 0.98
        flags: list[str] = []

        if uncertain_toggle and concept_id in {"inventory_total", "accounts_receivable_total"}:
            expected_status = "candidate_flagged"
            confidence = 0.84
            flags.append("label_variation")

        if uncertain_toggle and concept_id == "interest_expense" and period_idx % 4 == 3:
            expected_status = "unresolved"
            confidence = 0.72
            flags.append("missing_schedule")
            value = None

        evidence_file = package_payload["files"][(concept_idx + period_idx) % len(package_payload["files"])]
        locator_type = "cell" if evidence_file["doc_type"] == "XLSX" else "paragraph"
        locator_value = (
            f"{['B', 'C', 'D'][period_idx % 3]}{15 + (concept_idx % 10)}"
            if locator_type == "cell"
            else f"p{2 + (concept_idx % 8)}:l{3 + (period_idx % 5)}"
        )

        rows.append(
            {
                "trace_id": f"tr_{package_id}_{concept_id}",
                "concept_id": concept_id,
                "period_end_date": package_payload["period_end_date"],
                "raw_value_text": "N/A" if value is None else f"${value:,.2f}",
                "normalized_value": value,
                "unit_currency": "USD",
                "expected_status": expected_status,
                "labeler_confidence": confidence,
                "flags": flags,
                "normalization": {
                    "raw_scale": "absolute",
                    "normalized_scale": "absolute",
                    "currency_conversion_applied": False,
                },
                "evidence": {
                    "doc_id": evidence_file["file_id"],
                    "doc_name": evidence_file["filename"],
                    "page_or_sheet": "Summary" if locator_type == "cell" else f"Page {2 + (concept_idx % 8)}",
                    "locator_type": locator_type,
                    "locator_value": locator_value,
                    "source_snippet": f"{concept_id.replace('_', ' ').title()}: {value if value is not None else 'N/A'}",
                },
            }
        )

    return {
        "schema_version": "1.0",
        "package_id": package_id,
        "deal_id": package_payload["deal_id"],
        "period_end_date": package_payload["period_end_date"],
        "dictionary_version": "v1.0",
        "labeling": {
            "primary_labeler": "analyst_scaled",
            "reviewer": "reviewer_scaled",
            "adjudication_required": False,
        },
        "rows": rows,
    }


def scale_dataset(
    source_registry_file: Path,
    packages_dir: Path,
    labels_dir: Path,
    target_packages: int = 50,
    target_deals: int = 15,
) -> dict[str, int]:
    packages_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    source_registry = read_json(source_registry_file)
    source_ids = [s["source_id"] for s in source_registry["sources"]]

    existing_deals = sorted(_existing_deals(packages_dir))
    package_num = _next_package_number(packages_dir)

    now = datetime.now(timezone.utc)

    generated = 0

    deal_cursor = len(existing_deals) + 1
    while len(existing_deals) < target_deals and len(list(packages_dir.glob("pkg_*.json"))) < target_packages:
        deal_id = f"deal_scale_{deal_cursor:03d}"
        existing_deals.append(deal_id)
        deal_cursor += 1

        # Add at least 2 periods for each new deal.
        for period_idx, period_end in enumerate(["2025-09-30", "2025-12-31"]):
            if len(list(packages_dir.glob("pkg_*.json"))) >= target_packages:
                break
            package_payload = _build_package_payload(
                package_num=package_num,
                deal_id=deal_id,
                period_end_date=period_end,
                source_ids=source_ids,
                now=now,
                period_idx=period_idx,
                is_baseline=(period_idx == 0),
            )
            uncertain_toggle = package_num % 5 == 0
            label_payload = _build_label_payload(
                package_payload=package_payload,
                deal_idx=deal_cursor,
                period_idx=period_idx,
                uncertain_toggle=uncertain_toggle,
            )
            write_json(packages_dir / f"{package_payload['package_id']}.json", package_payload)
            write_json(labels_dir / f"{package_payload['package_id']}.ground_truth.json", label_payload)
            package_num += 1
            generated += 1

    # If package target not reached, add extra follow-up periods across existing deals.
    deal_index = 0
    while len(list(packages_dir.glob("pkg_*.json"))) < target_packages:
        deal_id = existing_deals[deal_index % len(existing_deals)]
        quarter_offset = deal_index // len(existing_deals)
        month = 3 + ((quarter_offset % 4) * 3)
        year = 2026 + (quarter_offset // 4)
        period_end = f"{year}-{month:02d}-30"

        package_payload = _build_package_payload(
            package_num=package_num,
            deal_id=deal_id,
            period_end_date=period_end,
            source_ids=source_ids,
            now=now,
            period_idx=quarter_offset + 2,
            is_baseline=False,
        )
        uncertain_toggle = package_num % 5 == 0
        label_payload = _build_label_payload(
            package_payload=package_payload,
            deal_idx=(deal_index % len(existing_deals)) + 1,
            period_idx=quarter_offset + 2,
            uncertain_toggle=uncertain_toggle,
        )
        write_json(packages_dir / f"{package_payload['package_id']}.json", package_payload)
        write_json(labels_dir / f"{package_payload['package_id']}.ground_truth.json", label_payload)

        package_num += 1
        deal_index += 1
        generated += 1

    return {
        "generated_packages": generated,
        "total_packages": len(list(packages_dir.glob("pkg_*.json"))),
        "total_deals": len(_existing_deals(packages_dir)),
    }
