from __future__ import annotations

from typing import Any


CONCEPT_MATURITY_REGISTRY: dict[str, str] = {
    # Grounded truth lane
    "net_income": "grounded",
    # Review lane (useful but analyst-confirmed)
    "ebitda_reported": "review",
    "ebitda_adjusted": "review",
    "cash_and_equivalents": "review",
    "revenue_total": "review",
}

_ALLOWED_MATURITY = {"grounded", "review", "hidden"}


def concept_maturity(concept_id: str) -> str:
    normalized = str(concept_id or "").strip().lower()
    maturity = CONCEPT_MATURITY_REGISTRY.get(normalized, "hidden")
    if maturity not in _ALLOWED_MATURITY:
        return "hidden"
    return maturity


def trust_tier_for_maturity(maturity: str) -> str:
    normalized = str(maturity or "").strip().lower()
    if normalized == "grounded":
        return "grounded"
    if normalized == "review":
        return "review"
    return "hidden"


def authority_level_for_maturity(maturity: str) -> str:
    normalized = str(maturity or "").strip().lower()
    if normalized == "grounded":
        return "document_grounded"
    if normalized == "review":
        return "analyst_confirmation_required"
    return "not_surfaceable"


def review_required_for_case(*, maturity: str, case_mode: str) -> bool:
    normalized_maturity = str(maturity or "").strip().lower()
    if normalized_maturity == "review":
        return True
    normalized_mode = str(case_mode or "").strip().lower()
    return normalized_mode != "verified_review"


def visible_maturity(maturity: str) -> bool:
    return str(maturity or "").strip().lower() in {"grounded", "review"}


def concept_maturity_payload() -> dict[str, Any]:
    return {
        "registry": dict(CONCEPT_MATURITY_REGISTRY),
        "tiers": ["grounded", "review", "hidden"],
    }
