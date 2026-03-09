from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Any
from urllib.parse import urlencode

from .concept_maturity import (
    authority_level_for_maturity,
    concept_maturity,
    concept_maturity_payload,
    review_required_for_case,
    trust_tier_for_maturity,
    visible_maturity,
)
from .constants import CONCEPT_LABELS
from .internal_store import InternalStore, PackageRecord
from .source_grounding import (
    anchors_for_conflict,
    is_structured_locator,
    select_conflict_pair,
    unique_trustworthy_anchors,
)


GROUP_ORDER = {
    "blockers": 0,
    "review_signals": 1,
    "near_trigger": 2,
    "material_changes": 3,
    "verified_changes": 4,
    "confirmed_findings": 5,
}

SEVERITY_ORDER = {
    "blocker": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}

TRUST_TIER_ORDER = {
    "grounded": 0,
    "review": 1,
    "hidden": 9,
}

CONCEPT_RELEVANCE = {
    "cash_and_equivalents": {"liquidity": 1.0, "covenant": 0.6},
    "net_income": {"liquidity": 0.6, "covenant": 0.9},
    "ebitda_adjusted": {"liquidity": 0.6, "covenant": 1.0},
    "ebitda_reported": {"liquidity": 0.5, "covenant": 1.0},
    "interest_expense": {"liquidity": 0.5, "covenant": 0.9},
    "total_debt": {"liquidity": 0.4, "covenant": 0.9},
    "revenue_total": {"liquidity": 0.4, "covenant": 0.6},
    "operating_income_ebit": {"liquidity": 0.4, "covenant": 0.8},
}

MATERIALITY_DEFAULT_POLICY: dict[str, dict[str, float]] = {
    "net_income": {"pct_minor_variance_max": 1.5, "abs_minor_variance_max": 25_000.0},
    "ebitda_adjusted": {"pct_minor_variance_max": 1.0, "abs_minor_variance_max": 35_000.0},
    "ebitda_reported": {"pct_minor_variance_max": 1.25, "abs_minor_variance_max": 50_000.0},
    "cash_and_equivalents": {"pct_minor_variance_max": 2.5, "abs_minor_variance_max": 60_000.0},
    "revenue_total": {"pct_minor_variance_max": 0.75, "abs_minor_variance_max": 150_000.0},
}

# Optional deal-level override path for demo/partner tuning without frontend-owned logic.
MATERIALITY_DEAL_POLICY_OVERRIDES: dict[str, dict[str, dict[str, float]]] = {
    # "deal_northstar": {"revenue_total": {"pct_minor_variance_max": 0.6}},
}

MATERIALITY_FALLBACK_POLICY: dict[str, float] = {
    "pct_minor_variance_max": 0.75,
    "abs_minor_variance_max": 50_000.0,
}

MISSING_SOURCE_MARKERS = (
    "missing_source",
    "missing_label_evidence",
    "missing_evidence_location",
    "document_unavailable",
    "unresolved:missing",
    "unresolved:not_found",
)


@dataclass(frozen=True)
class BaselineChoice:
    package: PackageRecord | None
    basis: str


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    if text.lower() in {"n/a", "na", "none", "null", "unresolved"}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _format_date_label(iso_date: str) -> str:
    text = str(iso_date or "").strip()
    if not text:
        return "Unknown period"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return text
    return dt.strftime("%b %Y")


def _humanize_identifier(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = text.replace("-", " ").replace("_", " ").strip()
    if not normalized:
        return text
    if normalized.islower() or normalized.isupper():
        normalized = " ".join(part.capitalize() for part in normalized.split())
    return normalized


def _period_label(pkg: PackageRecord) -> str:
    base = _format_date_label(pkg.period_end_date)
    if int(pkg.period_revision) > 1:
        return f"{base} r{pkg.period_revision}"
    return base


def _format_value_display(value: Any) -> str:
    parsed = _as_float(value)
    if parsed is None:
        return "unresolved"
    if math.isclose(parsed, round(parsed), abs_tol=1e-9):
        return f"{int(round(parsed)):,}"
    return f"{parsed:,.2f}"


def _format_delta_display(previous_value: Any, current_value: Any) -> str:
    prev_num = _as_float(previous_value)
    curr_num = _as_float(current_value)
    if prev_num is None or curr_num is None:
        return "N/A"
    absolute = curr_num - prev_num
    if math.isclose(prev_num, 0.0, abs_tol=1e-12):
        pct = None
    else:
        pct = (absolute / abs(prev_num)) * 100.0

    signed_abs = f"{absolute:+,.2f}" if not math.isclose(absolute, round(absolute), abs_tol=1e-9) else f"{int(round(absolute)):+,}"
    if pct is None:
        return signed_abs
    return f"{signed_abs} ({pct:+.1f}%)"


def _pct_delta(previous_value: Any, current_value: Any) -> float | None:
    prev_num = _as_float(previous_value)
    curr_num = _as_float(current_value)
    if prev_num is None or curr_num is None:
        return None
    if math.isclose(prev_num, 0.0, abs_tol=1e-12):
        return None
    return ((curr_num - prev_num) / abs(prev_num)) * 100.0


def _resolution_status(row: dict[str, Any]) -> str:
    if row.get("resolved_by_user") is True or row.get("user_resolution") or row.get("resolution"):
        return "resolved"
    status = str(row.get("status", "")).strip().lower()
    if status == "verified":
        return "verified"
    return "unresolved"


def _hard_blockers(row: dict[str, Any]) -> list[str]:
    blockers = row.get("hard_blockers", [])
    if not isinstance(blockers, list):
        return []
    return [str(item).strip().lower() for item in blockers if str(item).strip()]


def _proof_state(row: dict[str, Any], deterministic_conflict: dict[str, Any] | None = None) -> str:
    blockers = _hard_blockers(row)
    evidence = row.get("evidence", {}) or {}
    locator_value = str(evidence.get("locator_value", "")).strip().lower()
    status = str(row.get("status", "unresolved")).strip().lower()

    if deterministic_conflict is not None:
        return "conflict_detected"
    if any(any(marker in blocker for marker in MISSING_SOURCE_MARKERS) for blocker in blockers):
        return "missing_source"
    if any(marker in locator_value for marker in ("unresolved:not_found", "missing")):
        return "missing_source"
    if not evidence.get("doc_id") or not evidence.get("locator_value"):
        return "missing_source"
    if status == "verified":
        return "verified"
    return "needs_confirmation"


def _concept_relevance(concept_id: str) -> tuple[float, float]:
    entry = CONCEPT_RELEVANCE.get(concept_id, {})
    return float(entry.get("liquidity", 0.25)), float(entry.get("covenant", 0.25))


def _materiality_score(row: dict[str, Any]) -> float:
    concept_id = str(row.get("concept_id", "")).strip().lower()
    liquidity_rel, covenant_rel = _concept_relevance(concept_id)

    previous_value = row.get("prior_value")
    current_value = row.get("current_value", row.get("normalized_value"))
    explicit_abs = _as_float(row.get("abs_delta"))
    explicit_pct = _as_float(row.get("pct_delta"))
    prev_num = _as_float(previous_value)
    curr_num = _as_float(current_value)

    abs_delta = explicit_abs
    if abs_delta is None and prev_num is not None and curr_num is not None:
        abs_delta = abs(curr_num - prev_num)
    if abs_delta is None:
        abs_delta = abs(curr_num or 0.0)

    pct_delta = explicit_pct
    if pct_delta is None:
        pct_delta = _pct_delta(previous_value, current_value) or 0.0

    confidence = _as_float(row.get("confidence"))
    confidence_norm = max(0.0, min(1.0, confidence or 0.0))
    abs_norm = min(1.0, math.log10(abs(abs_delta) + 1.0) / 6.0)
    pct_norm = min(1.0, abs(pct_delta) / 100.0)
    relevance_norm = min(1.0, (liquidity_rel * 0.5) + (covenant_rel * 0.5))

    return round((abs_norm * 0.45) + (pct_norm * 0.30) + (relevance_norm * 0.20) + (confidence_norm * 0.05), 6)


def _is_near_trigger(row: dict[str, Any]) -> bool:
    status = str(row.get("status", "")).strip().lower()
    if status != "verified":
        return False
    concept_id = str(row.get("concept_id", "")).strip().lower()
    pct = _pct_delta(row.get("prior_value"), row.get("current_value", row.get("normalized_value")))
    if pct is None:
        return False

    if concept_id in {"cash_and_equivalents", "net_income", "ebitda_adjusted", "ebitda_reported"}:
        return pct <= -12.0
    if concept_id in {"total_debt", "interest_expense", "total_liabilities"}:
        return pct >= 10.0
    return False


def _materiality_policy_for_metric(
    *,
    deal_id: str,
    concept_id: str,
) -> dict[str, float]:
    base = dict(MATERIALITY_FALLBACK_POLICY)
    base.update(MATERIALITY_DEFAULT_POLICY.get(concept_id, {}))
    deal_override = MATERIALITY_DEAL_POLICY_OVERRIDES.get(deal_id, {}).get(concept_id, {})
    base.update(deal_override)
    return base


def _materiality_decision_for_row(
    *,
    screen_mode: str,
    case_mode: str,
    status: str,
    proof_state: str,
    previous_value: Any,
    current_value: Any,
    policy: dict[str, float] | None,
) -> str | None:
    if screen_mode == "first_package_intake":
        return None
    if case_mode != "review_possible_material_change":
        return None
    if status != "verified" or proof_state != "verified":
        return None
    if not isinstance(policy, dict):
        return "review_signal"

    prev_num = _as_float(previous_value)
    curr_num = _as_float(current_value)
    if prev_num is None or curr_num is None:
        return "review_signal"

    abs_delta = abs(curr_num - prev_num)
    pct_delta = _pct_delta(previous_value, current_value)
    abs_threshold = max(0.0, float(policy.get("abs_minor_variance_max", MATERIALITY_FALLBACK_POLICY["abs_minor_variance_max"])))
    pct_threshold = max(0.0, float(policy.get("pct_minor_variance_max", MATERIALITY_FALLBACK_POLICY["pct_minor_variance_max"])))

    abs_minor = abs_delta <= abs_threshold
    pct_minor = True if pct_delta is None else abs(pct_delta) <= pct_threshold
    if abs_minor and pct_minor:
        return "auto_verified_minor_variance"
    return "review_signal"


def _group_for_row(
    row: dict[str, Any],
    proof_state: str,
    materiality: float,
    *,
    screen_mode: str,
) -> str:
    status = str(row.get("status", "unresolved")).strip().lower()
    if proof_state in {"missing_source", "conflict_detected"}:
        return "blockers"
    if status in {"unresolved", "candidate_flagged"}:
        return "blockers"
    if screen_mode == "first_package_intake":
        if status == "verified":
            return "verified_changes"
        return "near_trigger"
    if _is_near_trigger(row):
        return "near_trigger"
    if materiality >= 0.45:
        return "material_changes"
    return "verified_changes"


def _severity_for_row(group: str, proof_state: str, materiality: float) -> str:
    if group == "blockers":
        if proof_state in {"missing_source", "conflict_detected"}:
            return "blocker"
        return "high"
    if group == "near_trigger":
        return "high"
    if group == "material_changes":
        if materiality >= 0.75:
            return "high"
        return "medium"
    if materiality >= 0.35:
        return "medium"
    return "low"


def _metric_label(row: dict[str, Any]) -> str:
    concept_id = str(row.get("concept_id", "")).strip().lower()
    label = str(row.get("label", "")).strip()
    if label:
        return label
    return CONCEPT_LABELS.get(concept_id, concept_id.replace("_", " ").title() or "Metric")


def _headline(
    group: str,
    proof_state: str,
    row: dict[str, Any],
    pct_delta: float | None,
    *,
    screen_mode: str,
) -> str:
    concept_id = str(row.get("concept_id", "")).strip().lower()
    label = _metric_label(row)
    status = str(row.get("status", "unresolved")).strip().lower()

    if screen_mode == "first_package_intake":
        if proof_state == "missing_source":
            if concept_id == "net_income":
                return "Net Income missing from current package"
            return f"{label} missing from current package"
        if proof_state == "conflict_detected":
            return f"{label} conflicts across current submitted sources"
        if status == "candidate_flagged":
            return f"{label} candidate found, not confirmed"
        if status == "verified":
            return f"{label} extracted from current package"
        return f"{label} needs intake review"

    if group == "blockers":
        if proof_state == "missing_source":
            if concept_id == "cash_and_equivalents":
                return "Liquidity cannot be confirmed"
            if concept_id == "net_income":
                return "Net Income missing from current package"
            return f"{label} cannot be confirmed"
        if proof_state == "conflict_detected":
            if concept_id == "cash_and_equivalents":
                return "Cash balance conflicts across sources"
            return f"{label} conflicts across sources"
        if status == "candidate_flagged":
            return f"{label} needs confirmation before use"
        return f"{label} is unresolved"

    if group == "near_trigger":
        if concept_id in {"total_debt", "interest_expense", "total_liabilities"}:
            return f"{label} now above watch level"
        return f"{label} now near watch level"

    if pct_delta is not None:
        direction = "down" if pct_delta < 0 else "up"
        pct_text = f"{abs(pct_delta):.1f} percent"
        if concept_id in {"ebitda_adjusted", "ebitda_reported"}:
            tail = "headroom tighter" if pct_delta < 0 else "headroom wider"
            return f"EBITDA {direction} {pct_text}, {tail}"
        if concept_id == "cash_and_equivalents":
            tail = "liquidity thinner" if pct_delta < 0 else "liquidity stronger"
            return f"Cash {direction} {pct_text}, {tail}"
        return f"{label} {direction} {pct_text} versus prior period"

    return f"{label} changed versus prior period"


def _subline(
    row: dict[str, Any],
    previous_display: str,
    current_display: str,
    delta_display: str,
    *,
    screen_mode: str,
    current_search_state: str,
) -> str:
    label = _metric_label(row)
    if screen_mode == "first_package_intake":
        if current_search_state == "candidate_only":
            return f"{label}: candidate only ({current_display})"
        if current_search_state in {"missing", "candidate_unanchored"}:
            return f"{label}: not provided in current package"
        return f"{label}: current package value {current_display}"

    movement = f"{previous_display} \u2192 {current_display}"
    if delta_display != "N/A":
        movement = f"{movement} \u2022 \u0394 {delta_display}"
    return f"{label}: {movement}"


def _why_it_matters(
    group: str,
    proof_state: str,
    row: dict[str, Any],
    pct_delta: float | None,
    *,
    screen_mode: str,
) -> str:
    concept_id = str(row.get("concept_id", "")).strip().lower()
    status = str(row.get("status", "unresolved")).strip().lower()

    if screen_mode == "first_package_intake":
        if proof_state == "missing_source":
            return "Current package review is blocked until support is provided."
        if proof_state == "conflict_detected":
            return "Cannot rely on this current package value until the conflict is resolved."
        if status == "candidate_flagged":
            return "Needs analyst confirmation before use."
        if status == "verified":
            return "Current package extraction is supported by source evidence."
        return "Needs intake review."

    if proof_state == "missing_source":
        if concept_id == "cash_and_equivalents":
            return "Liquidity view is blocked until support is provided."
        return "Review cannot be completed until support is received."
    if proof_state == "conflict_detected":
        return "Cannot rely on this value until the conflict is resolved."
    if group == "near_trigger":
        return "Near watch threshold and needs immediate review."
    if group == "material_changes":
        if pct_delta is not None and pct_delta < 0:
            return "Tightens headroom versus prior quarter."
        return "Verified shift versus prior quarter."
    return "Change recorded."


def _implication(group: str, proof_state: str, pct_delta: float | None) -> str:
    if proof_state in {"missing_source", "conflict_detected"}:
        return "investigation_required"
    if group == "near_trigger":
        return "watch_level"
    if pct_delta is not None and pct_delta < 0:
        return "deterioration"
    return "change_recorded"


def _file_name_from_manifest(pkg: PackageRecord | None, file_id: str) -> str:
    if pkg is None:
        return ""
    for file_meta in pkg.package_manifest.get("files", []):
        if str(file_meta.get("file_id", "")).strip() == file_id:
            return str(file_meta.get("filename", ""))
    return ""


def _document_evidence_preview_url(
    *,
    package_id: str,
    doc_id: str,
    locator_type: str,
    locator_value: str,
    page_or_sheet: str,
) -> str:
    package_key = str(package_id or "").strip()
    doc_key = str(doc_id or "").strip()
    locator_kind = str(locator_type or "").strip()
    locator = str(locator_value or "").strip()
    if not package_key or not doc_key or not locator_kind or not locator:
        return ""
    query = urlencode(
        {
            "locator_type": locator_kind,
            "locator_value": locator,
            "page_or_sheet": str(page_or_sheet or "").strip(),
        }
    )
    return (
        f"/internal/v1/packages/{package_key}/files/{doc_key}/evidence-preview"
        f"?{query}"
    )


def _evidence_side(row: dict[str, Any], pkg: PackageRecord | None) -> dict[str, Any]:
    evidence = (row.get("evidence") or row.get("evidence_link") or {}) if isinstance(row, dict) else {}
    file_id = str(evidence.get("doc_id", "")).strip()
    locator_type = str(evidence.get("locator_type", "")).strip() or "locator"
    locator_value = str(evidence.get("locator_value", "")).strip()
    page_or_sheet = str(evidence.get("page_or_sheet", "")).strip()
    locator = f"{page_or_sheet} \u2022 {locator_type}={locator_value}".strip(" \u2022")
    trace_id = str(row.get("trace_id", "")).strip() if isinstance(row, dict) else ""
    confidence = _as_float(row.get("confidence")) if isinstance(row, dict) else None

    download_url = ""
    if pkg is not None and file_id:
        download_url = f"/internal/v1/packages/{pkg.package_id}/files/{file_id}:download"

    preview_url = _document_evidence_preview_url(
        package_id=pkg.package_id if pkg is not None else "",
        doc_id=file_id,
        locator_type=locator_type,
        locator_value=locator_value,
        page_or_sheet=page_or_sheet,
    )
    if not preview_url and trace_id:
        preview_url = f"/internal/v1/traces/{trace_id}/evidence"
    return {
        "file_id": file_id,
        "file_name": _file_name_from_manifest(pkg, file_id) or str(evidence.get("doc_name", "")).strip(),
        "locator": locator,
        "excerpt": str(evidence.get("source_snippet", "")).strip(),
        "confidence": round(confidence, 4) if confidence is not None else None,
        "preview_url": preview_url,
        "download_url": download_url,
    }


def _empty_evidence_side() -> dict[str, Any]:
    return {
        "file_id": "",
        "file_name": "",
        "locator": "",
        "excerpt": "",
        "confidence": None,
        "preview_url": "",
        "download_url": "",
    }


def _download_url_for_file(pkg: PackageRecord | None, file_id: str) -> str:
    if pkg is None or not file_id:
        return ""
    return f"/internal/v1/packages/{pkg.package_id}/files/{file_id}:download"


def _canonical_anchor(
    *,
    anchor: dict[str, Any],
    row: dict[str, Any],
    package: PackageRecord | None,
    index: int,
) -> dict[str, Any]:
    concept_id = str(row.get("concept_id", "")).strip().lower()
    concept_label = _metric_label(row)
    trace_id = str(anchor.get("trace_id") or row.get("trace_id") or "").strip()
    period_id = str(anchor.get("period_id") or (package.package_id if package is not None else "")).strip()
    doc_id = str(anchor.get("doc_id", "")).strip()
    locator_type = str(anchor.get("locator_type", "")).strip()
    locator_value = str(anchor.get("locator_value", "")).strip()
    normalized_value = _as_float(anchor.get("normalized_value"))
    confidence = _as_float(anchor.get("confidence"))
    source_role = str(anchor.get("source_role", "")).strip() or "submitted_source"
    anchor_id = str(anchor.get("anchor_id", "")).strip() or f"{trace_id}:{index}"
    page_or_sheet = str(anchor.get("page_or_sheet", "")).strip()
    locator_display = f"{page_or_sheet} • {locator_type}={locator_value}".strip(" •")
    if not locator_display:
        locator_display = f"{locator_type}={locator_value}".strip("=")

    preview_url = _document_evidence_preview_url(
        package_id=period_id,
        doc_id=doc_id,
        locator_type=locator_type,
        locator_value=locator_value,
        page_or_sheet=page_or_sheet,
    )
    if not preview_url and trace_id:
        preview_url = f"/internal/v1/traces/{trace_id}/evidence"

    return {
        "anchor_id": anchor_id,
        "doc_id": doc_id,
        "doc_name": str(anchor.get("doc_name", "")).strip() or _file_name_from_manifest(package, doc_id),
        "locator_type": locator_type,
        "locator_value": locator_value,
        "page_or_sheet": page_or_sheet,
        "locator_display": locator_display,
        "source_snippet": str(anchor.get("source_snippet", "")).strip(),
        "raw_value_text": str(anchor.get("raw_value_text", "")).strip(),
        "normalized_value": normalized_value,
        "value_display": _format_value_display(normalized_value),
        "concept_id": concept_id,
        "concept_label": concept_label,
        "period_id": period_id,
        "trace_id": trace_id,
        "source_role": source_role,
        "confidence": round(confidence, 4) if confidence is not None else None,
        "preview_url": preview_url,
        "download_url": _download_url_for_file(package, doc_id),
    }


def _anchors_from_row(row: dict[str, Any], package: PackageRecord | None) -> list[dict[str, Any]]:
    raw_candidates = row.get("source_anchors")
    if not isinstance(raw_candidates, list):
        raw_candidates = []

    evidence = row.get("evidence", {}) if isinstance(row.get("evidence"), dict) else {}
    primary_anchor = {
        "anchor_id": f"{row.get('trace_id', '')}:primary",
        "doc_id": evidence.get("doc_id", ""),
        "doc_name": evidence.get("doc_name", ""),
        "locator_type": evidence.get("locator_type", ""),
        "locator_value": evidence.get("locator_value", ""),
        "page_or_sheet": evidence.get("page_or_sheet", ""),
        "source_snippet": evidence.get("source_snippet", ""),
        "raw_value_text": evidence.get("raw_value_text", row.get("raw_value_text", "")),
        "normalized_value": evidence.get("normalized_value", row.get("normalized_value")),
        "concept_id": row.get("concept_id", ""),
        "concept_label": _metric_label(row),
        "period_id": package.package_id if package is not None else "",
        "trace_id": row.get("trace_id", ""),
        "source_role": evidence.get("source_role", "submitted_source"),
        "confidence": row.get("confidence"),
    }

    combined = [candidate for candidate in raw_candidates if isinstance(candidate, dict)]
    combined.append(primary_anchor)
    trusted = unique_trustworthy_anchors(combined, max_items=12)
    return [
        _canonical_anchor(anchor=anchor, row=row, package=package, index=index)
        for index, anchor in enumerate(trusted, start=1)
    ]


def _deterministic_conflict_from_row(row: dict[str, Any], package: PackageRecord | None) -> dict[str, Any] | None:
    anchors = _anchors_from_row(row, package)
    conflict = select_conflict_pair(anchors)
    if conflict is None:
        return None

    selected = anchors_for_conflict(anchors, conflict)
    if len(selected) < 2:
        return None

    anchor_by_id = {
        str(anchor.get("anchor_id", "")).strip(): anchor
        for anchor in anchors
        if str(anchor.get("anchor_id", "")).strip()
    }
    selected_enriched: list[dict[str, Any]] = []
    for anchor in selected:
        anchor_id = str(anchor.get("anchor_id", "")).strip()
        if anchor_id and anchor_id in anchor_by_id:
            selected_enriched.append(anchor_by_id[anchor_id])
            continue
        selected_enriched.append(
            _canonical_anchor(
                anchor=anchor,
                row=row,
                package=package,
                index=len(selected_enriched) + 1,
            )
        )
    selected = selected_enriched

    # Deterministic first slice: require precise structured anchors and distinct docs.
    if any(str(anchor.get("locator_type", "")).strip().lower() != "cell" for anchor in selected):
        return None
    if str(selected[0].get("doc_id", "")) == str(selected[1].get("doc_id", "")):
        return None

    return {
        "kind": str(conflict.get("type", "value_mismatch")),
        "value_delta": _as_float(conflict.get("value_delta")) or 0.0,
        "anchors": selected[:2],
    }


def _find_download_url_for_doc(file_id: str, *packages: PackageRecord | None) -> str:
    normalized = str(file_id or "").strip()
    if not normalized:
        return ""
    for pkg in packages:
        if pkg is None:
            continue
        for file_meta in pkg.package_manifest.get("files", []):
            if str(file_meta.get("file_id", "")).strip() != normalized:
                continue
            return f"/internal/v1/packages/{pkg.package_id}/files/{normalized}:download"
    return ""


def _first_trustworthy_anchor(row: dict[str, Any] | None, package: PackageRecord | None) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    anchors = _anchors_from_row(row, package)
    if not anchors:
        return None
    return anchors[0]


def _requirement_raw_from_catalog(obligation: dict[str, Any], deal_id: str) -> dict[str, Any]:
    obligation_id = str(obligation.get("obligation_id", "")).strip()
    preview_url = (
        f"/internal/v1/deals/{deal_id}/reporting-obligations/{obligation_id}/preview"
        if obligation_id and deal_id
        else ""
    )
    return {
        "doc_id": str(obligation.get("doc_id", "")).strip(),
        "doc_name": str(obligation.get("doc_name", "")).strip(),
        "locator_type": str(obligation.get("locator_type", "")).strip(),
        "locator_value": str(obligation.get("locator_value", "")).strip(),
        "page_or_sheet": str(obligation.get("page_or_sheet", "")).strip(),
        "source_snippet": str(obligation.get("source_snippet", "")).strip(),
        "required_concept_id": str(obligation.get("required_concept_id", "")).strip(),
        "required_concept_label": str(obligation.get("required_concept_label", "")).strip(),
        "obligation_type": str(obligation.get("obligation_type", "")).strip() or "reporting_requirement",
        "source_role": str(obligation.get("source_role", "")).strip() or "deal_reporting_document",
        "trace_id": str(obligation.get("obligation_id", "")).strip(),
        "obligation_id": obligation_id,
        "cadence": str(obligation.get("cadence", "")).strip(),
        "grounding_state": str(obligation.get("grounding_state", "")).strip() or "not_grounded",
        "preview_url": preview_url,
        "download_url": (
            f"/internal/v1/deals/{deal_id}/reporting-obligations/{obligation_id}/document:download"
            if obligation_id and deal_id
            else ""
        ),
    }


def _requirement_raw_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "doc_id": str(candidate.get("doc_id", "")).strip(),
        "doc_name": str(candidate.get("doc_name", "")).strip(),
        "locator_type": str(candidate.get("locator_type", "")).strip(),
        "locator_value": str(candidate.get("locator_value", "")).strip(),
        "page_or_sheet": str(candidate.get("page_or_sheet", "")).strip(),
        "source_snippet": str(candidate.get("source_snippet", "")).strip(),
        "required_concept_id": str(candidate.get("candidate_concept_id", "")).strip(),
        "required_concept_label": str(candidate.get("candidate_concept_label", "")).strip(),
        "obligation_type": str(candidate.get("candidate_obligation_type", "")).strip() or "reporting_requirement_candidate",
        "source_role": "deal_reporting_document",
        "trace_id": str(candidate.get("candidate_id", "")).strip(),
        "obligation_id": str(candidate.get("promoted_obligation_id", "")).strip(),
        "cadence": "",
        "grounding_state": str(candidate.get("grounding_state", "")).strip() or "candidate",
        "preview_url": "",
        "download_url": "",
    }


def _requirement_anchor(
    *,
    row: dict[str, Any],
    package: PackageRecord | None,
    baseline_package: PackageRecord | None,
    deal_id: str,
    catalog_obligation: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    raw = row.get("requirement_anchor")
    if not isinstance(raw, dict):
        raw = row.get("reporting_requirement_anchor")
    if not isinstance(raw, dict) and isinstance(catalog_obligation, dict):
        raw = _requirement_raw_from_catalog(catalog_obligation, deal_id=deal_id)
    if not isinstance(raw, dict):
        return None

    concept_id = str(row.get("concept_id", "")).strip().lower()
    concept_label = _metric_label(row)
    doc_id = str(raw.get("doc_id", "")).strip()
    locator_type = str(raw.get("locator_type", "")).strip()
    locator_value = str(raw.get("locator_value", "")).strip()
    page_or_sheet = str(raw.get("page_or_sheet", "")).strip()
    source_snippet = str(raw.get("source_snippet", "")).strip()
    locator_display = f"{page_or_sheet} • {locator_type}={locator_value}".strip(" •")
    if not locator_display:
        locator_display = f"{locator_type}={locator_value}".strip("=")

    grounding_state_raw = str(raw.get("grounding_state", "")).strip().lower()
    has_structured_evidence = bool(doc_id and is_structured_locator(locator_type, locator_value) and source_snippet)
    if grounding_state_raw:
        grounded = bool(grounding_state_raw == "grounded" and has_structured_evidence)
    else:
        # Backward-compatibility for legacy/eval rows that carried anchors without explicit state.
        grounded = has_structured_evidence
    requirement = {
        "obligation_id": str(raw.get("obligation_id", "")).strip(),
        "doc_id": doc_id,
        "doc_name": str(raw.get("doc_name", "")).strip() or _file_name_from_manifest(package, doc_id) or _file_name_from_manifest(baseline_package, doc_id),
        "locator_type": locator_type,
        "locator_value": locator_value,
        "page_or_sheet": page_or_sheet,
        "locator_display": locator_display,
        "source_snippet": source_snippet,
        "required_concept_id": str(raw.get("required_concept_id", "")).strip() or concept_id,
        "required_concept_label": str(raw.get("required_concept_label", "")).strip() or concept_label,
        "obligation_type": str(raw.get("obligation_type", "")).strip() or "reporting_requirement",
        "cadence": str(raw.get("cadence", "")).strip(),
        "source_role": str(raw.get("source_role", "")).strip() or "deal_reporting_document",
        "trace_id": str(raw.get("trace_id", "")).strip() or str(row.get("trace_id", "")).strip(),
        "grounded": grounded,
        "grounding_state": grounding_state_raw or ("grounded" if grounded else "not_grounded"),
        "preview_url": str(raw.get("preview_url", "")).strip(),
        "download_url": str(raw.get("download_url", "")).strip() or _find_download_url_for_doc(doc_id, package, baseline_package),
    }
    return requirement


def _current_search_state(
    *,
    status: str,
    proof_state: str,
    current_candidate_anchor: dict[str, Any] | None,
) -> str:
    if proof_state == "conflict_detected":
        return "conflict"
    if status == "verified" and proof_state == "verified":
        return "found_verified"
    if status == "candidate_flagged":
        return "candidate_only" if current_candidate_anchor is not None else "candidate_unanchored"
    if proof_state == "missing_source":
        return "missing"
    return "unresolved"


def _qualifies_missing_required_reporting(
    *,
    requirement_anchor: dict[str, Any] | None,
    status: str,
    proof_state: str,
    baseline_anchor: dict[str, Any] | None,
    current_candidate_anchor: dict[str, Any] | None,
) -> bool:
    if not requirement_anchor or not bool(requirement_anchor.get("grounded")):
        return False
    if status not in {"candidate_flagged", "unresolved"} and proof_state != "missing_source":
        return False
    # Deterministic qualification: current package is missing or candidate-only and
    # we have requirement grounding plus either prior support or a concrete candidate context.
    if baseline_anchor is not None:
        return True
    if proof_state == "missing_source":
        return True
    return current_candidate_anchor is not None


def _case_mode_for_row(
    *,
    proof_state: str,
    status: str,
    requirement_anchor: dict[str, Any] | None = None,
    baseline_anchor: dict[str, Any] | None = None,
    current_candidate_anchor: dict[str, Any] | None = None,
) -> str:
    if proof_state == "conflict_detected":
        return "investigation_conflict"
    if _qualifies_missing_required_reporting(
        requirement_anchor=requirement_anchor,
        status=status,
        proof_state=proof_state,
        baseline_anchor=baseline_anchor,
        current_candidate_anchor=current_candidate_anchor,
    ):
        return "investigation_missing_required_reporting"
    if proof_state == "missing_source":
        return "investigation_missing_source"
    if status == "unresolved":
        # Guardrail: unresolved rows must never collapse into verified_review copy paths.
        return "investigation_candidate_only"
    if status == "candidate_flagged":
        return "investigation_candidate_only"
    return "verified_review"


def _case_mode_for_maturity(
    *,
    base_case_mode: str,
    maturity: str,
) -> str:
    if maturity != "review":
        return base_case_mode
    if base_case_mode == "investigation_conflict":
        return "review_possible_source_conflict"
    if base_case_mode in {"investigation_missing_required_reporting", "investigation_missing_source"}:
        return "review_possible_missing_reporting_item"
    if base_case_mode == "investigation_candidate_only":
        return "review_possible_requirement"
    return "review_possible_material_change"


def _proof_compare_mode_for_case(
    *,
    case_mode: str,
    current_search_state: str,
    has_requirement_anchor: bool,
    screen_mode: str,
) -> str:
    if case_mode in {"investigation_conflict", "review_possible_source_conflict"}:
        return "source_vs_source"
    if screen_mode == "first_package_intake":
        if case_mode in {"investigation_missing_required_reporting", "review_possible_missing_reporting_item"}:
            if has_requirement_anchor:
                return "current_plus_requirement"
            return "current_vs_candidate" if current_search_state == "candidate_only" else "current_only"
        if case_mode in {"investigation_candidate_only", "review_possible_requirement"}:
            return "current_vs_candidate" if current_search_state == "candidate_only" else "current_only"
        if case_mode == "investigation_missing_source":
            return "current_only"
        return "current_only"
    if case_mode in {"investigation_missing_required_reporting", "review_possible_missing_reporting_item"}:
        if has_requirement_anchor:
            return "baseline_current_plus_requirement"
        return "baseline_vs_current_candidate" if current_search_state == "candidate_only" else "baseline_vs_current_missing"
    if case_mode in {"investigation_candidate_only", "review_possible_requirement"}:
        return "baseline_vs_current_candidate"
    if case_mode == "investigation_missing_source":
        return "baseline_vs_current_missing"
    return "baseline_vs_current"


def _obligation_grounding_state(requirement_anchor: dict[str, Any] | None) -> str:
    if requirement_anchor and bool(requirement_anchor.get("grounded")):
        return "grounded"
    return "not_grounded"


def _display_group(
    *,
    group: str,
    screen_mode: str,
    case_certainty: str,
) -> str:
    certainty = str(case_certainty or "").strip().lower()
    if certainty in {"missing_required_support", "missing_source", "candidate_only", "conflict_detected"}:
        return "blockers"
    if screen_mode == "first_package_intake":
        if certainty in {"confirmed_current_extraction", "grounded_fact"}:
            return "confirmed_findings"
        return "review_signals"
    if certainty in {"grounded_fact", "confirmed_current_extraction"}:
        return "verified_changes"
    if certainty == "review_signal":
        return "review_signals"
    if group == "blockers":
        return "blockers"
    return "review_signals"


def _delta_event_type(
    *,
    group: str,
    proof_state: str,
    case_mode: str,
) -> str:
    if proof_state == "conflict_detected" or case_mode in {"investigation_conflict", "review_possible_source_conflict"}:
        return "source_conflict"
    if proof_state == "missing_source" or case_mode in {
        "investigation_missing_required_reporting",
        "investigation_missing_source",
        "review_possible_missing_reporting_item",
        "review_possible_requirement",
    }:
        return "missing_support"
    if case_mode in {"investigation_candidate_only"}:
        return "candidate_gap"
    if group == "near_trigger":
        return "near_trigger"
    if group == "material_changes":
        return "material_change"
    if group == "verified_changes":
        return "verified_change"
    return "review_signal"


def _screen_taxonomy(*, screen_mode: str) -> dict[str, Any]:
    if screen_mode == "first_package_intake":
        return {
            "summary_keys": ["blockers", "review_signals", "confirmed_findings"],
            "section_order": ["blockers", "review_signals", "confirmed_findings"],
            "section_labels": {
                "blockers": "Blockers",
                "review_signals": "Review Signals",
                "confirmed_findings": "Confirmed Findings",
            },
        }
    return {
        "summary_keys": ["blockers", "review_signals", "verified_changes"],
        "section_order": ["blockers", "review_signals", "verified_changes"],
        "section_labels": {
            "blockers": "Blockers",
            "review_signals": "Review Signals",
            "verified_changes": "Verified Changes",
        },
    }


def _case_certainty(
    *,
    case_mode: str,
    proof_state: str,
    status: str,
    concept_maturity_value: str,
    screen_mode: str,
) -> str:
    if case_mode in {"investigation_conflict", "review_possible_source_conflict"}:
        return "conflict_detected"
    if case_mode == "investigation_missing_required_reporting":
        return "missing_required_support"
    if case_mode == "investigation_missing_source":
        return "missing_source"
    if case_mode in {"investigation_candidate_only", "review_possible_requirement"}:
        return "candidate_only"
    if case_mode in {"review_possible_missing_reporting_item", "review_possible_material_change"}:
        return "review_signal"
    if proof_state == "verified" and status == "verified":
        if screen_mode == "first_package_intake":
            return "confirmed_current_extraction"
        if concept_maturity_value == "grounded":
            return "grounded_fact"
        return "confirmed_current_extraction"
    return "review_signal"


def _case_certainty_label(case_certainty: str) -> str:
    normalized = str(case_certainty or "").strip().lower()
    if normalized == "grounded_fact":
        return "Grounded fact"
    if normalized == "missing_required_support":
        return "Missing required support"
    if normalized == "confirmed_current_extraction":
        return "Confirmed extraction"
    if normalized == "candidate_only":
        return "Candidate only"
    if normalized == "conflict_detected":
        return "Conflict detected"
    if normalized == "missing_source":
        return "Missing source"
    return "Review signal"


EXTRACTION_REVIEW_REASON_DEFAULTS: dict[str, tuple[str, str]] = {
    "exact_row_header_missing": (
        "Exact row header missing",
        "Candidate value was found without a precise structured row locator.",
    ),
    "label_variant_match": (
        "Label variant match",
        "Match used a label variant instead of an exact header match.",
    ),
    "multiple_matching_rows": (
        "Multiple matching rows",
        "More than one matching anchor was found for this metric.",
    ),
    "candidate_from_pdf_text_only": (
        "Extracted from PDF text only",
        "Candidate was found in PDF text, not in a structured table row.",
    ),
    "candidate_from_narrative_text": (
        "Candidate extracted from narrative text",
        "Candidate came from narrative text, not an exact table cell.",
    ),
    "found_outside_expected_section": (
        "Found outside expected section",
        "Candidate was found outside the expected reporting section.",
    ),
    "source_conflict_across_rows": (
        "Source conflict across rows",
        "Two submitted anchors disagree for this metric.",
    ),
    "current_package_missing_exact_support": (
        "Current package missing exact support",
        "No exact source anchor was found in the current package.",
    ),
}

EXTRACTION_REVIEW_REASON_ALIASES = {
    "candidate_from_text_only": "candidate_from_narrative_text",
}


def _extraction_review_reason(row: dict[str, Any]) -> dict[str, str] | None:
    raw_code = str(row.get("extraction_reason_code", "")).strip().lower()
    if not raw_code:
        return None

    code = EXTRACTION_REVIEW_REASON_ALIASES.get(raw_code, raw_code)
    defaults = EXTRACTION_REVIEW_REASON_DEFAULTS.get(code)
    if defaults is None:
        return None

    label = str(row.get("extraction_reason_label", "")).strip() or defaults[0]
    detail = str(row.get("extraction_reason_detail", "")).strip() or defaults[1]
    label_lower = label.lower()
    detail_lower = detail.lower()
    if "confirmation required" in label_lower or "review lane" in label_lower:
        label = defaults[0]
    if "confirmation required" in detail_lower or "review lane" in detail_lower:
        detail = defaults[1]

    return {"code": code, "label": label, "detail": detail}


def _review_reason_for_item(
    *,
    row: dict[str, Any],
    case_mode: str,
    case_certainty: str,
    current_search_state: str,
    requirement_anchor: dict[str, Any] | None,
    current_candidate_anchor: dict[str, Any] | None,
) -> dict[str, str] | None:
    certainty = str(case_certainty or "").strip().lower()
    if certainty not in {"candidate_only", "review_signal", "conflict_detected", "missing_source"}:
        return None

    def _reason(code: str, label: str, detail: str) -> dict[str, str]:
        return {"code": code, "label": label, "detail": detail}

    if case_mode in {"investigation_conflict", "review_possible_source_conflict"}:
        return _reason(
            "source_conflict_across_rows",
            "Source conflict across rows",
            "Two submitted anchors disagree for this metric.",
        )

    extraction_reason = _extraction_review_reason(row)
    if extraction_reason is not None:
        return extraction_reason

    if case_mode in {"review_possible_requirement", "review_possible_missing_reporting_item"} and not (
        requirement_anchor and bool(requirement_anchor.get("grounded"))
    ):
        return _reason(
            "requirement_grounding_unavailable",
            "Requirement grounding unavailable",
            "Deal-document requirement anchor is not grounded for this concept.",
        )

    if current_search_state == "candidate_unanchored":
        return _reason(
            "exact_row_header_missing",
            "Exact row header missing",
            "Candidate value found, but no exact structured anchor was captured.",
        )

    if current_search_state == "missing" or case_mode == "investigation_missing_source":
        return _reason(
            "current_package_missing_exact_support",
            "Current package missing exact support",
            "No exact source anchor was found in the current package.",
        )

    if current_search_state == "candidate_only":
        locator_type = str((current_candidate_anchor or {}).get("locator_type", "")).strip().lower()
        doc_name = str((current_candidate_anchor or {}).get("doc_name", "")).strip().lower()
        if locator_type in {"paragraph", "line"} and doc_name.endswith(".pdf"):
            return _reason(
                "candidate_from_pdf_text_only",
                "Candidate extracted from PDF text only",
                "Candidate was found in PDF text, not an exact table row.",
            )

    return None


def _conflict_movement_subline(label: str, left_value: str, right_value: str) -> str:
    return f"{label}: {left_value} ↔ {right_value} across submitted sources"


def _missing_movement_subline(
    *,
    label: str,
    baseline_value: str,
    current_search_state: str,
    current_value: str,
    screen_mode: str,
) -> str:
    if screen_mode == "first_package_intake":
        if current_search_state == "candidate_only":
            return f"{label}: candidate only ({current_value})"
        if current_search_state in {"missing", "candidate_unanchored"}:
            return f"{label}: not provided in current package"
        return f"{label}: current package value {current_value}"
    if current_search_state == "candidate_only":
        return f"{label}: {baseline_value} → candidate only ({current_value})"
    if current_search_state == "missing":
        return f"{label}: {baseline_value} → not provided"
    return f"{label}: {baseline_value} → {current_value}"


def _review_headline_for_case(
    *,
    case_mode: str,
    metric_label: str,
    current_search_state: str,
    pct_delta: float | None,
    screen_mode: str,
    requirement_anchor: dict[str, Any] | None = None,
) -> str:
    if case_mode == "review_possible_source_conflict":
        if screen_mode == "first_package_intake":
            return f"Possible {metric_label} source conflict in current package"
        return f"Possible {metric_label} source conflict"
    if case_mode == "review_possible_missing_reporting_item":
        if current_search_state == "candidate_only":
            return f"Possible {metric_label} support missing"
        return f"Possible {metric_label} reporting gap"
    if case_mode == "review_possible_requirement":
        if requirement_anchor and bool(requirement_anchor.get("grounded")):
            return f"Possible {metric_label} requirement gap"
        if current_search_state == "candidate_only":
            return f"Possible {metric_label} support gap"
        return f"Possible {metric_label} reporting gap"
    if screen_mode == "first_package_intake":
        return f"Possible {metric_label} extraction needs review"
    if pct_delta is not None:
        return f"Possible {metric_label} change needs review"
    return f"Possible {metric_label} review signal"


def _review_item_has_useful_evidence(
    *,
    case_mode: str,
    baseline_anchor: dict[str, Any] | None,
    current_candidate_anchor: dict[str, Any] | None,
    requirement_anchor: dict[str, Any] | None,
    competing_anchors: list[dict[str, Any]],
    pct_delta: float | None,
) -> bool:
    if case_mode == "review_possible_source_conflict":
        return len(competing_anchors) >= 2
    if case_mode in {"review_possible_missing_reporting_item", "review_possible_requirement"}:
        if baseline_anchor is not None:
            return True
        if current_candidate_anchor is not None:
            return True
        if isinstance(requirement_anchor, dict):
            snippet = str(requirement_anchor.get("source_snippet", "")).strip()
            locator_type = str(requirement_anchor.get("locator_type", "")).strip()
            locator_value = str(requirement_anchor.get("locator_value", "")).strip()
            return bool(snippet and is_structured_locator(locator_type, locator_value))
        return False
    if pct_delta is not None:
        return True
    return current_candidate_anchor is not None or baseline_anchor is not None


def _grounded_implication_for_case(
    *,
    case_mode: str,
    metric_label: str,
    requirement_anchor: dict[str, Any] | None = None,
    screen_mode: str = "delta_review",
) -> str:
    if case_mode == "investigation_conflict":
        return f"Conflicting submitted values for {metric_label}; confirm source of record before use."
    if case_mode == "investigation_missing_required_reporting":
        return f"Required support for {metric_label} is missing from the current package."
    if case_mode == "investigation_missing_source":
        return f"No trustworthy source anchor for {metric_label} in the current package."
    if case_mode == "investigation_candidate_only":
        return f"{metric_label} is candidate-only and cannot be relied on yet."
    if screen_mode == "first_package_intake":
        return f"Current package evidence is sufficient for {metric_label} intake review."
    return "Evidence is sufficient for period review."


def _review_implication_for_case(
    *,
    case_mode: str,
    metric_label: str,
    requirement_anchor: dict[str, Any] | None = None,
    screen_mode: str = "delta_review",
) -> str:
    if case_mode == "review_possible_source_conflict":
        if screen_mode == "first_package_intake":
            return "Current package sources conflict. Confirm source evidence before relying on this item."
        return "Conflicting sources detected. Confirm source evidence before relying on this item."
    if case_mode == "review_possible_missing_reporting_item":
        if requirement_anchor and str(requirement_anchor.get("source_snippet", "")).strip():
            if screen_mode == "first_package_intake":
                return "Current package suggests a reporting gap that still needs analyst confirmation."
            return "Possible reporting gap. Confirm source evidence before relying on this item."
        if screen_mode == "first_package_intake":
            return "Current package may have a reporting gap. Confirm evidence before relying on this item."
        return "Possible reporting gap. Confirm source evidence before relying on this item."
    if case_mode == "review_possible_requirement":
        if screen_mode == "first_package_intake":
            return "Review package evidence before treating this as a required reporting gap."
        return "Review source evidence before treating this as a requirement gap."
    if screen_mode == "first_package_intake":
        return f"Current package suggests a {metric_label} extraction that still needs confirmation."
    return "Confirm source evidence before relying on this item."


def _draft_conflict_borrower_query(
    *,
    deal_name: str,
    current_period_label: str,
    metric_label: str,
    anchor_a: dict[str, Any],
    anchor_b: dict[str, Any],
) -> dict[str, str]:
    a_file = str(anchor_a.get("doc_name", "")).strip() or "Source A file"
    b_file = str(anchor_b.get("doc_name", "")).strip() or "Source B file"
    a_locator = str(anchor_a.get("locator_display", "")).strip() or "locator unavailable"
    b_locator = str(anchor_b.get("locator_display", "")).strip() or "locator unavailable"
    a_value = str(anchor_a.get("value_display", "unresolved"))
    b_value = str(anchor_b.get("value_display", "unresolved"))

    subject = f"Source of record confirmation requested — {metric_label} ({current_period_label})"
    body = (
        f"Hello,\n\n"
        f"During period review for {deal_name} ({current_period_label}), we found conflicting submitted values for {metric_label}.\n\n"
        f"Source A: {a_file} | {a_locator} | value {a_value}\n"
        f"Source B: {b_file} | {b_locator} | value {b_value}\n\n"
        f"Please confirm the authoritative source of record for {metric_label} and resend corrected support if needed.\n\n"
        f"Thank you."
    )
    return {
        "subject": subject,
        "body": body,
        "text": f"Subject: {subject}\n\n{body}",
    }


def _draft_missing_reporting_query(
    *,
    deal_name: str,
    current_period_label: str,
    metric_label: str,
    baseline_anchor: dict[str, Any] | None,
    current_anchor: dict[str, Any] | None,
    requirement_anchor: dict[str, Any] | None,
) -> dict[str, str]:
    baseline_line = "Prior package anchor: not available"
    if baseline_anchor is not None:
        baseline_line = (
            f"Prior package anchor: {baseline_anchor.get('doc_name', 'source')} | "
            f"{baseline_anchor.get('locator_display', 'locator unavailable')} | "
            f"value {baseline_anchor.get('value_display', 'unresolved')}"
        )

    current_line = "Current package status: no trustworthy anchor found"
    if current_anchor is not None:
        current_line = (
            f"Current package candidate: {current_anchor.get('doc_name', 'source')} | "
            f"{current_anchor.get('locator_display', 'locator unavailable')} | "
            f"value {current_anchor.get('value_display', 'unresolved')} (candidate only)"
        )

    requirement_line = ""
    if requirement_anchor and requirement_anchor.get("grounded"):
        requirement_line = (
            f"Requirement reference: {requirement_anchor.get('doc_name', 'requirement document')} | "
            f"{requirement_anchor.get('locator_display', 'locator unavailable')}\n"
        )

    subject = f"Reporting support request — {metric_label} ({current_period_label})"
    body = (
        f"Hello,\n\n"
        f"During period review for {deal_name} ({current_period_label}), {metric_label} is missing from the submitted reporting package.\n\n"
        f"{baseline_line}\n"
        f"{current_line}\n"
        f"{requirement_line}"
        f"\nPlease provide the authoritative support for {metric_label} in the current period package "
        f"or resend corrected files.\n\n"
        f"Thank you."
    )
    return {
        "subject": subject,
        "body": body,
        "text": f"Subject: {subject}\n\n{body}",
    }


def _draft_analyst_note(
    *,
    deal_name: str,
    current_period_label: str,
    metric_label: str,
    case_mode: str,
    subline: str,
    implication: str,
) -> dict[str, str]:
    subject = f"Analyst note — {metric_label} ({current_period_label})"
    body = (
        f"Deal: {deal_name}\n"
        f"Period: {current_period_label}\n"
        f"Case mode: {case_mode}\n"
        f"Observation: {subline}\n"
        f"Implication: {implication}\n"
        "Next: Confirm supporting source evidence before finalizing period conclusions."
    )
    return {
        "subject": subject,
        "body": body,
        "text": f"Subject: {subject}\n\n{body}",
    }


def _recommended_actions_for_case(
    case_mode: str,
    concept_maturity: str,
    *,
    screen_mode: str,
    case_certainty: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if concept_maturity == "review":
        if case_mode == "verified_review":
            actions = [
                {"id": "view_source_evidence", "label": "View source evidence"},
                {"id": "view_review_history", "label": "View review history"},
                {"id": "mark_expected_noise", "label": "Mark as expected noise"},
            ]
            return actions[0], actions
        if case_mode == "review_possible_source_conflict":
            actions = [
                {"id": "confirm_source_of_record", "label": "Resolve conflict"},
                {"id": "prepare_borrower_follow_up", "label": "Prepare borrower follow-up"},
                {"id": "draft_analyst_note", "label": "Draft analyst note"},
                {"id": "mark_expected_noise", "label": "Mark as expected noise"},
                {"id": "dismiss_after_review", "label": "Dismiss after analyst review"},
                {"id": "view_source_evidence", "label": "View source evidence"},
                {"id": "view_review_history", "label": "View review history"},
            ]
            return actions[0], actions
        if case_mode in {"review_possible_missing_reporting_item", "review_possible_requirement"}:
            actions = [
                {"id": "review_possible_requirement", "label": "Review possible requirement"},
                {"id": "confirm_source_of_record", "label": "Confirm source of record"},
                {"id": "prepare_borrower_follow_up", "label": "Prepare borrower follow-up"},
                {"id": "draft_analyst_note", "label": "Draft analyst note"},
                {"id": "mark_expected_noise", "label": "Mark as expected noise"},
                {"id": "dismiss_after_review", "label": "Dismiss after analyst review"},
                {"id": "view_source_evidence", "label": "View source evidence"},
                {"id": "view_review_history", "label": "View review history"},
            ]
            return actions[0], actions

        actions = [
            {"id": "confirm_source_of_record", "label": "Confirm source evidence"},
            {"id": "draft_analyst_note", "label": "Draft analyst note"},
            {"id": "prepare_borrower_follow_up", "label": "Prepare borrower follow-up"},
            {"id": "mark_expected_noise", "label": "Mark as expected noise"},
            {"id": "dismiss_after_review", "label": "Dismiss after analyst review"},
            {"id": "view_source_evidence", "label": "View source evidence"},
            {"id": "view_review_history", "label": "View review history"},
        ]
        return actions[0], actions

    if case_mode == "investigation_conflict":
        actions = [
            {"id": "confirm_source_of_record", "label": "Resolve conflict"},
            {"id": "draft_borrower_query", "label": "Draft borrower query"},
            {"id": "view_source_evidence", "label": "View source evidence"},
            {"id": "view_review_history", "label": "View review history"},
        ]
        return actions[0], actions

    if case_mode == "investigation_missing_required_reporting":
        actions = [
            {"id": "request_borrower_update", "label": "Request borrower update"},
            {"id": "confirm_alternate_source", "label": "Confirm alternate source"},
            {"id": "mark_item_received", "label": "Mark item received"},
            {"id": "view_reporting_requirement", "label": "View reporting requirement"},
            {"id": "copy_borrower_draft", "label": "Copy borrower draft"},
            {"id": "view_review_history", "label": "View review history"},
        ]
        return actions[0], actions

    if case_mode in {"investigation_missing_source", "investigation_candidate_only"}:
        if case_mode == "investigation_candidate_only" or screen_mode == "first_package_intake":
            actions = [
                {"id": "confirm_alternate_source", "label": "Confirm alternate source"},
                {"id": "view_source_evidence", "label": "View source evidence"},
                {"id": "request_borrower_update", "label": "Request borrower update"},
                {"id": "mark_item_received", "label": "Mark item received"},
                {"id": "copy_borrower_draft", "label": "Copy borrower draft"},
                {"id": "view_review_history", "label": "View review history"},
            ]
            return actions[0], actions
        actions = [
            {"id": "request_borrower_update", "label": "Request borrower update"},
            {"id": "confirm_alternate_source", "label": "Confirm alternate source"},
            {"id": "mark_item_received", "label": "Mark item received"},
            {"id": "view_source_evidence", "label": "View source evidence"},
            {"id": "copy_borrower_draft", "label": "Copy borrower draft"},
            {"id": "view_review_history", "label": "View review history"},
        ]
        return actions[0], actions

    actions = [
        {"id": "view_source_evidence", "label": "View source evidence"},
        {"id": "view_review_history", "label": "View review history"},
    ]
    return actions[0], actions


def _conflict_evidence_from_anchor(anchor: dict[str, Any]) -> dict[str, Any]:
    return {
        "file_id": str(anchor.get("doc_id", "")).strip(),
        "file_name": str(anchor.get("doc_name", "")).strip(),
        "locator": str(anchor.get("locator_display", "")).strip(),
        "excerpt": str(anchor.get("source_snippet", "")).strip(),
        "confidence": anchor.get("confidence"),
        "preview_url": str(anchor.get("preview_url", "")).strip(),
        "download_url": str(anchor.get("download_url", "")).strip(),
        "source_label": str(anchor.get("source_role", "")).strip() or "submitted_source",
        "anchor_value_display": str(anchor.get("value_display", "")).strip(),
        "locator_type": str(anchor.get("locator_type", "")).strip(),
        "locator_value": str(anchor.get("locator_value", "")).strip(),
    }


def _build_period_options(
    deal_packages: list[PackageRecord],
    current_period_id: str,
    baseline_period_id: str | None,
) -> list[dict[str, Any]]:
    ordered = sorted(
        deal_packages,
        key=lambda item: (item.period_end_date, item.period_revision, item.received_at),
        reverse=True,
    )
    options: list[dict[str, Any]] = []
    for pkg in ordered:
        options.append(
            {
                "id": pkg.package_id,
                "label": _period_label(pkg),
                "end_date": pkg.period_end_date,
                "status": pkg.status,
                "is_current": pkg.package_id == current_period_id,
                "is_baseline": pkg.package_id == baseline_period_id,
            }
        )
    return options


def _baseline_for_period(
    *,
    store: InternalStore,
    deal_packages: list[PackageRecord],
    current_period_id: str,
    baseline_period_id: str | None,
) -> BaselineChoice:
    ordered = sorted(
        deal_packages,
        key=lambda item: (item.period_end_date, item.period_revision, item.received_at),
    )
    by_id = {item.package_id: item for item in ordered}
    current = by_id.get(current_period_id)
    if current is None:
        raise ValueError("period_not_found")

    if baseline_period_id:
        if baseline_period_id == current_period_id:
            raise ValueError("baseline_period_cannot_match_current")
        chosen = by_id.get(baseline_period_id)
        if chosen is None:
            raise ValueError("baseline_period_not_found")
        return BaselineChoice(package=chosen, basis="explicit_baseline_period")

    prior = [pkg for pkg in ordered if (pkg.period_end_date, pkg.period_revision, pkg.received_at) < (current.period_end_date, current.period_revision, current.received_at)]

    for pkg in reversed(prior):
        effective = store.compute_effective_package_status(pkg.package_id)
        if effective == "completed":
            return BaselineChoice(package=pkg, basis="prior_verified_period")

    for pkg in reversed(prior):
        if pkg.processed_payload is not None:
            return BaselineChoice(package=pkg, basis="prior_processed_period")

    return BaselineChoice(package=None, basis="none")


def _safe_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not payload:
        return []
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _screen_mode_for_baseline(baseline_package: PackageRecord | None) -> str:
    return "delta_review" if baseline_package is not None else "first_package_intake"


def _workspace_mode_for_item(*, trust_tier: str, screen_mode: str) -> str:
    if trust_tier == "review":
        return "investigation_mode"
    return screen_mode


def build_review_queue_payload(
    *,
    store: InternalStore,
    deal_id: str,
    period_id: str,
    deal_packages: list[PackageRecord],
    baseline_period_id: str | None = None,
    include_resolved: bool = False,
) -> dict[str, Any]:
    current_package = next((pkg for pkg in deal_packages if pkg.package_id == period_id), None)
    if current_package is None:
        raise ValueError("period_not_found")
    if current_package.processed_payload is None:
        raise ValueError("period_not_processed")

    baseline_choice = _baseline_for_period(
        store=store,
        deal_packages=deal_packages,
        current_period_id=period_id,
        baseline_period_id=baseline_period_id,
    )
    baseline_package = baseline_choice.package
    screen_mode = _screen_mode_for_baseline(baseline_package)

    current_delta = store.get_delta(deal_id=deal_id, period_id=period_id)
    if current_delta is None:
        raise ValueError("period_not_found")
    baseline_delta = (
        store.get_delta(deal_id=deal_id, period_id=baseline_package.package_id)
        if baseline_package is not None
        else None
    )

    baseline_rows_by_concept: dict[str, dict[str, Any]] = {}
    for row in _safe_rows(baseline_delta):
        concept_id = str(row.get("concept_id", "")).strip().lower()
        if concept_id:
            baseline_rows_by_concept[concept_id] = row

    deal_meta = store.get_deal_meta(deal_id) or {}
    deal_display_name = str(deal_meta.get("display_name", "")).strip()
    if not deal_display_name or deal_display_name == deal_id:
        deal_name = _humanize_identifier(deal_id)
    else:
        deal_name = deal_display_name
    grounded_obligations = store.list_reporting_obligations(
        deal_id=deal_id,
        grounding_state="grounded",
    )
    obligation_by_concept: dict[str, dict[str, Any]] = {}
    for obligation in grounded_obligations:
        concept_key = str(obligation.get("required_concept_id", "")).strip().lower()
        if not concept_key or concept_key in obligation_by_concept:
            continue
        obligation_by_concept[concept_key] = obligation
    candidate_obligations = store.list_reporting_obligation_candidates(
        deal_id=deal_id,
    )
    candidate_obligation_by_concept: dict[str, dict[str, Any]] = {}
    for candidate in candidate_obligations:
        concept_key = str(candidate.get("candidate_concept_id", "")).strip().lower()
        if not concept_key:
            continue
        if concept_key in candidate_obligation_by_concept:
            continue
        candidate_obligation_by_concept[concept_key] = candidate
    items_all: list[dict[str, Any]] = []

    for row in _safe_rows(current_delta):
        concept_id = str(row.get("concept_id", "")).strip().lower()
        if not concept_id:
            continue
        maturity = concept_maturity(concept_id)
        if not visible_maturity(maturity):
            continue
        trust_tier = trust_tier_for_maturity(maturity)
        authority_level = authority_level_for_maturity(maturity)

        baseline_row = baseline_rows_by_concept.get(concept_id)
        previous_value = row.get("prior_value")
        if baseline_row is not None:
            previous_value = baseline_row.get("current_value", baseline_row.get("normalized_value"))
        if screen_mode == "first_package_intake":
            previous_value = None
        current_value = row.get("current_value", row.get("normalized_value"))

        materiality = _materiality_score(row)
        deterministic_conflict = _deterministic_conflict_from_row(row, current_package)
        proof_state = _proof_state(row, deterministic_conflict=deterministic_conflict)
        status = str(row.get("status", "unresolved")).strip().lower()
        baseline_anchor = _first_trustworthy_anchor(baseline_row, baseline_package)
        current_candidate_anchor = _first_trustworthy_anchor(row, current_package)
        catalog_obligation = (
            obligation_by_concept.get(concept_id)
            if status in {"candidate_flagged", "unresolved"} or proof_state == "missing_source"
            else None
        )
        candidate_obligation = (
            candidate_obligation_by_concept.get(concept_id)
            if catalog_obligation is None
            else None
        )
        if catalog_obligation is None and candidate_obligation is not None:
            candidate_grounding_state = str(candidate_obligation.get("grounding_state", "")).strip().lower()
            if candidate_grounding_state not in {"candidate", "ambiguous"}:
                candidate_obligation = None
        row_with_requirement = row
        if catalog_obligation is None and candidate_obligation is not None:
            row_with_requirement = dict(row)
            row_with_requirement["reporting_requirement_anchor"] = _requirement_raw_from_candidate(candidate_obligation)
        requirement_anchor = _requirement_anchor(
            row=row_with_requirement,
            package=current_package,
            baseline_package=baseline_package,
            deal_id=deal_id,
            catalog_obligation=catalog_obligation,
        )
        current_search_state = _current_search_state(
            status=status,
            proof_state=proof_state,
            current_candidate_anchor=current_candidate_anchor,
        )
        base_case_mode = _case_mode_for_row(
            proof_state=proof_state,
            status=status,
            requirement_anchor=requirement_anchor,
            baseline_anchor=baseline_anchor,
            current_candidate_anchor=current_candidate_anchor,
        )
        case_mode = _case_mode_for_maturity(
            base_case_mode=base_case_mode,
            maturity=maturity,
        )
        materiality_policy = _materiality_policy_for_metric(
            deal_id=deal_id,
            concept_id=concept_id,
        )
        materiality_decision = _materiality_decision_for_row(
            screen_mode=screen_mode,
            case_mode=case_mode,
            status=status,
            proof_state=proof_state,
            previous_value=previous_value,
            current_value=current_value,
            policy=materiality_policy,
        )
        if materiality_decision == "auto_verified_minor_variance":
            case_mode = "verified_review"
        obligation_grounding_state = _obligation_grounding_state(requirement_anchor)

        previous_display = _format_value_display(previous_value)
        current_display = _format_value_display(current_value)
        delta_display = _format_delta_display(previous_value, current_value)
        pct_delta = _pct_delta(previous_value, current_value)
        competing_anchors: list[dict[str, Any]] = []
        if deterministic_conflict is not None:
            anchors = deterministic_conflict.get("anchors", [])
            competing_anchors = [anchor for anchor in anchors if isinstance(anchor, dict)][:2]
            if len(competing_anchors) == 2:
                previous_display = str(competing_anchors[0].get("value_display", previous_display))
                current_display = str(competing_anchors[1].get("value_display", current_display))
                delta_display = "source_conflict"
                pct_delta = None
        proof_compare_mode = _proof_compare_mode_for_case(
            case_mode=case_mode,
            current_search_state=current_search_state,
            has_requirement_anchor=bool(requirement_anchor),
            screen_mode=screen_mode,
        )
        case_certainty = _case_certainty(
            case_mode=case_mode,
            proof_state=proof_state,
            status=status,
            concept_maturity_value=maturity,
            screen_mode=screen_mode,
        )
        review_reason = _review_reason_for_item(
            row=row,
            case_mode=case_mode,
            case_certainty=case_certainty,
            current_search_state=current_search_state,
            requirement_anchor=requirement_anchor,
            current_candidate_anchor=current_candidate_anchor,
        )

        group = _group_for_row(
            row,
            proof_state,
            materiality,
            screen_mode=screen_mode,
        )
        severity = _severity_for_row(group, proof_state, materiality)
        label = _metric_label(row)
        headline = _headline(
            group,
            proof_state,
            row,
            pct_delta,
            screen_mode=screen_mode,
        )
        subline = _subline(
            row,
            previous_display,
            current_display,
            delta_display,
            screen_mode=screen_mode,
            current_search_state=current_search_state,
        )
        why_it_matters = _why_it_matters(
            group,
            proof_state,
            row,
            pct_delta,
            screen_mode=screen_mode,
        )
        if case_mode in {"investigation_conflict", "review_possible_source_conflict"}:
            if len(competing_anchors) == 2:
                headline = f"{label} conflicts across submitted sources"
            subline = _conflict_movement_subline(
                label=label,
                left_value=previous_display,
                right_value=current_display,
            )
            if maturity == "grounded":
                why_it_matters = _grounded_implication_for_case(
                    case_mode=case_mode,
                    metric_label=label,
                    screen_mode=screen_mode,
                )
            else:
                headline = _review_headline_for_case(
                    case_mode=case_mode,
                    metric_label=label,
                    current_search_state=current_search_state,
                    pct_delta=pct_delta,
                    screen_mode=screen_mode,
                    requirement_anchor=requirement_anchor,
                )
                why_it_matters = _review_implication_for_case(
                    case_mode=case_mode,
                    metric_label=label,
                    screen_mode=screen_mode,
                )
        elif case_mode in {
            "investigation_missing_required_reporting",
            "investigation_missing_source",
            "investigation_candidate_only",
            "review_possible_missing_reporting_item",
            "review_possible_requirement",
        }:
            if case_mode == "investigation_missing_required_reporting":
                if current_search_state == "candidate_only":
                    headline = f"{label} support is unconfirmed in current package"
                else:
                    headline = f"{label} missing from current package"
            elif case_mode in {"review_possible_missing_reporting_item", "review_possible_requirement"}:
                headline = _review_headline_for_case(
                    case_mode=case_mode,
                    metric_label=label,
                    current_search_state=current_search_state,
                    pct_delta=pct_delta,
                    screen_mode=screen_mode,
                    requirement_anchor=requirement_anchor,
                )
            subline = _missing_movement_subline(
                label=label,
                baseline_value=previous_display,
                current_search_state=current_search_state,
                current_value=current_display,
                screen_mode=screen_mode,
            )
            if maturity == "grounded":
                why_it_matters = _grounded_implication_for_case(
                    case_mode=case_mode,
                    metric_label=label,
                    requirement_anchor=requirement_anchor,
                    screen_mode=screen_mode,
                )
            else:
                why_it_matters = _review_implication_for_case(
                    case_mode=case_mode,
                    metric_label=label,
                    requirement_anchor=requirement_anchor,
                    screen_mode=screen_mode,
                )
        elif maturity == "review" and case_mode != "verified_review":
            headline = _review_headline_for_case(
                case_mode=case_mode,
                metric_label=label,
                current_search_state=current_search_state,
                pct_delta=pct_delta,
                screen_mode=screen_mode,
                requirement_anchor=requirement_anchor,
            )
            why_it_matters = _review_implication_for_case(
                case_mode=case_mode,
                metric_label=label,
                requirement_anchor=requirement_anchor,
                screen_mode=screen_mode,
            )
        if case_mode == "verified_review" and status == "verified" and proof_state == "verified":
            if materiality_decision == "auto_verified_minor_variance":
                if pct_delta is None or abs(pct_delta) < 0.05:
                    headline = f"{label} in line with prior period"
                else:
                    headline = f"{label} variance within materiality policy"
                why_it_matters = "Minor variance is within policy and does not require investigation."

        if maturity == "grounded":
            grounded_implication = _grounded_implication_for_case(
                case_mode=case_mode,
                metric_label=label,
                requirement_anchor=requirement_anchor,
                screen_mode=screen_mode,
            )
        else:
            if case_mode == "verified_review" and status == "verified" and proof_state == "verified":
                if materiality_decision == "auto_verified_minor_variance":
                    grounded_implication = f"Minor {label} variance is within policy and auto-verified."
                else:
                    grounded_implication = "Evidence is sufficient for period review."
            else:
                grounded_implication = _review_implication_for_case(
                    case_mode=case_mode,
                    metric_label=label,
                    requirement_anchor=requirement_anchor,
                    screen_mode=screen_mode,
                )
        resolution_status = _resolution_status(row)

        current_trace = str(row.get("trace_id", "")).strip()
        baseline_trace = str(baseline_row.get("trace_id", "")).strip() if baseline_row else ""
        trace_ids = [trace for trace in [current_trace, baseline_trace] if trace]
        primary_action, actions = _recommended_actions_for_case(
            case_mode,
            maturity,
            screen_mode=screen_mode,
            case_certainty=case_certainty,
        )

        evidence_baseline = _evidence_side(baseline_row, baseline_package) if baseline_row else _empty_evidence_side()
        evidence_current = _evidence_side(row, current_package)
        draft_query = None
        draft_analyst_note = None
        if case_mode == "investigation_conflict" and len(competing_anchors) == 2:
            evidence_baseline = _conflict_evidence_from_anchor(competing_anchors[0])
            evidence_current = _conflict_evidence_from_anchor(competing_anchors[1])
            draft_query = _draft_conflict_borrower_query(
                deal_name=deal_name,
                current_period_label=_period_label(current_package),
                metric_label=label,
                anchor_a=competing_anchors[0],
                anchor_b=competing_anchors[1],
            )
        elif case_mode == "review_possible_source_conflict" and len(competing_anchors) == 2:
            evidence_baseline = _conflict_evidence_from_anchor(competing_anchors[0])
            evidence_current = _conflict_evidence_from_anchor(competing_anchors[1])
            draft_query = _draft_conflict_borrower_query(
                deal_name=deal_name,
                current_period_label=_period_label(current_package),
                metric_label=label,
                anchor_a=competing_anchors[0],
                anchor_b=competing_anchors[1],
            )
        elif case_mode in {
            "investigation_missing_required_reporting",
            "investigation_missing_source",
            "investigation_candidate_only",
            "review_possible_missing_reporting_item",
            "review_possible_requirement",
        }:
            if baseline_anchor is not None:
                evidence_baseline = _conflict_evidence_from_anchor(baseline_anchor)
            if current_candidate_anchor is not None and current_search_state == "candidate_only":
                evidence_current = _conflict_evidence_from_anchor(current_candidate_anchor)
            draft_query = _draft_missing_reporting_query(
                deal_name=deal_name,
                current_period_label=_period_label(current_package),
                metric_label=label,
                baseline_anchor=baseline_anchor,
                current_anchor=current_candidate_anchor,
                requirement_anchor=requirement_anchor
                if case_mode == "investigation_missing_required_reporting"
                else None,
            )
        if maturity == "review":
            draft_analyst_note = _draft_analyst_note(
                deal_name=deal_name,
                current_period_label=_period_label(current_package),
                metric_label=label,
                case_mode=case_mode,
                subline=subline,
                implication=grounded_implication,
            )
            if not _review_item_has_useful_evidence(
                case_mode=case_mode,
                baseline_anchor=baseline_anchor,
                current_candidate_anchor=current_candidate_anchor,
                requirement_anchor=requirement_anchor,
                competing_anchors=competing_anchors,
                pct_delta=pct_delta,
            ):
                continue

        display_group = _display_group(
            group=group,
            screen_mode=screen_mode,
            case_certainty=case_certainty,
        )
        materiality_outcome = (
            "blocker"
            if display_group == "blockers"
            else "auto_verified_minor_variance"
            if materiality_decision == "auto_verified_minor_variance"
            else "review_signal"
            if display_group == "review_signals"
            else "verified_change"
        )
        delta_event_type = _delta_event_type(
            group=group,
            proof_state=proof_state,
            case_mode=case_mode,
        )
        item = {
            "id": f"rq_{period_id}_{concept_id}",
            "group": group,
            "display_group": display_group,
            "severity": severity,
            "status": status,
            "case_mode": case_mode,
            "screen_mode": screen_mode,
            "concept_maturity": maturity,
            "trust_tier": trust_tier,
            "authority_level": authority_level,
            "review_required": review_required_for_case(maturity=maturity, case_mode=case_mode),
            "workspace_mode": _workspace_mode_for_item(trust_tier=trust_tier, screen_mode=screen_mode),
            "case_certainty": case_certainty,
            "case_certainty_label": _case_certainty_label(case_certainty),
            "review_reason_code": review_reason["code"] if review_reason else None,
            "review_reason_label": review_reason["label"] if review_reason else None,
            "review_reason_detail": review_reason["detail"] if review_reason else None,
            "headline": headline,
            "subline": subline,
            "why_it_matters": why_it_matters,
            "proof_state": proof_state,
            "primary_action": primary_action,
            "available_actions": actions,
            "recommended_actions": actions,
            "metric_key": concept_id,
            "metric_label": label,
            "delta_event_type": delta_event_type,
            "is_material_change": delta_event_type in {"near_trigger", "material_change"},
            "is_verified_change": display_group == "verified_changes",
            "materiality_outcome": materiality_outcome,
            "materiality_policy": (
                {
                    "pct_minor_variance_max": float(materiality_policy.get("pct_minor_variance_max", 0.0)),
                    "abs_minor_variance_max": float(materiality_policy.get("abs_minor_variance_max", 0.0)),
                }
                if screen_mode != "first_package_intake"
                else None
            ),
            "previous_value_display": previous_display,
            "current_value_display": current_display,
            "delta_display": delta_display,
            "implication": _implication(group, proof_state, pct_delta),
            "grounded_implication": grounded_implication,
            "trace_ids": trace_ids,
            "resolution_status": resolution_status,
            "proof_compare_mode": proof_compare_mode,
            "competing_anchors": competing_anchors,
            "draft_borrower_query": draft_query,
            "draft_analyst_note": draft_analyst_note,
            "current_search_state": current_search_state,
            "obligation_grounding_state": obligation_grounding_state,
            "requirement_anchor": requirement_anchor,
            "baseline_anchor": baseline_anchor,
            "current_candidate_anchor": current_candidate_anchor,
            "materiality": materiality,
            "ranking_context": {
                "liquidity_relevance": _concept_relevance(concept_id)[0],
                "covenant_relevance": _concept_relevance(concept_id)[1],
                "pct_delta": pct_delta,
                "trust_tier_order": TRUST_TIER_ORDER.get(trust_tier, 9),
                "evidence_quality": (
                    (1.0 if len(competing_anchors) >= 2 else 0.0)
                    + (0.5 if baseline_anchor is not None else 0.0)
                    + (0.5 if current_candidate_anchor is not None else 0.0)
                    + (0.4 if requirement_anchor is not None else 0.0)
                ),
            },
            "evidence": {
                "baseline": evidence_baseline,
                "current": evidence_current,
            },
        }
        items_all.append(item)

    if include_resolved:
        filtered = items_all
    else:
        filtered = [item for item in items_all if item["resolution_status"] != "resolved"]

    ordered = sorted(
        filtered,
        key=lambda item: (
            int(item["ranking_context"].get("trust_tier_order", 9)),
            GROUP_ORDER.get(str(item.get("display_group", "")), 99),
            SEVERITY_ORDER.get(item["severity"], 99),
            -float(item["ranking_context"].get("evidence_quality", 0.0)),
            -float(item["ranking_context"]["covenant_relevance"]),
            -float(item["ranking_context"]["liquidity_relevance"]),
            -float(item["materiality"]),
            item["metric_label"].lower(),
        ),
    )

    for rank, item in enumerate(ordered, start=1):
        item["rank"] = rank
        item.pop("materiality", None)
        item.pop("ranking_context", None)

    grouped_counts: dict[str, int] = {}
    for item in ordered:
        key = str(item.get("display_group", "")).strip()
        if not key:
            continue
        grouped_counts[key] = grouped_counts.get(key, 0) + 1

    summary_ordered = {
        "blockers": grouped_counts.get("blockers", 0),
        "review_signals": grouped_counts.get("review_signals", 0),
        "confirmed_findings": grouped_counts.get("confirmed_findings", 0),
        "verified_changes": grouped_counts.get("verified_changes", 0),
        "material_changes": sum(1 for item in ordered if item.get("is_material_change") is True),
        "resolved": sum(1 for item in ordered if item.get("resolution_status") == "resolved"),
        "grounded_cases": sum(1 for item in ordered if item.get("trust_tier") == "grounded"),
        "review_cases": sum(1 for item in ordered if item.get("trust_tier") == "review"),
        "total": len(ordered),
    }
    summary_ordered["verified"] = summary_ordered["verified_changes"]

    if screen_mode == "first_package_intake":
        summary = {
            "blockers": summary_ordered["blockers"],
            "review_signals": summary_ordered["review_signals"],
            "confirmed_findings": summary_ordered["confirmed_findings"],
            "resolved": summary_ordered["resolved"],
            "grounded_cases": summary_ordered["grounded_cases"],
            "review_cases": summary_ordered["review_cases"],
            "total": summary_ordered["total"],
        }
    else:
        summary = {
            "blockers": summary_ordered["blockers"],
            "review_signals": summary_ordered["review_signals"],
            "verified_changes": summary_ordered["verified_changes"],
            "material_changes": summary_ordered["material_changes"],
            "verified": summary_ordered["verified"],
            "resolved": summary_ordered["resolved"],
            "grounded_cases": summary_ordered["grounded_cases"],
            "review_cases": summary_ordered["review_cases"],
            "total": summary_ordered["total"],
        }

    baseline_payload = (
        {
            "id": baseline_package.package_id,
            "label": _period_label(baseline_package),
            "end_date": baseline_package.period_end_date,
        }
        if baseline_package is not None
        else None
    )

    return {
        "product_mode": screen_mode,
        "product_state": {
            "screen_mode": screen_mode,
            "has_baseline": baseline_package is not None,
            "comparison_ready": baseline_package is not None,
        },
        "deal": {
            "id": deal_id,
            "name": deal_name,
        },
        "periods": {
            "current": {
                "id": current_package.package_id,
                "label": _period_label(current_package),
                "end_date": current_package.period_end_date,
            },
            "baseline": baseline_payload,
            "comparison_basis": baseline_choice.basis,
        },
        "period_options": _build_period_options(
            deal_packages=deal_packages,
            current_period_id=current_package.package_id,
            baseline_period_id=baseline_package.package_id if baseline_package is not None else None,
        ),
        "summary": summary,
        "screen_taxonomy": _screen_taxonomy(screen_mode=screen_mode),
        "concept_maturity": concept_maturity_payload(),
        "items": ordered,
    }
