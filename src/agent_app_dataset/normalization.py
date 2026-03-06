from __future__ import annotations

from dataclasses import dataclass
import re


_SCALE_MAP = {
    "K": 1_000,
    "M": 1_000_000,
    "B": 1_000_000_000,
}


@dataclass(frozen=True)
class NormalizationResult:
    normalized_value: float | None
    unit_currency: str
    unresolved_reason: str | None
    raw_scale: str


def _extract_currency(raw_value_text: str, source_snippet: str, fallback_currency: str) -> str:
    combined = f"{raw_value_text} {source_snippet}".upper()
    m = re.search(r"\b([A-Z]{3})\b", combined)
    if m:
        return m.group(1)
    if "$" in combined:
        return "USD"
    return fallback_currency


def _extract_scale(raw_value_text: str, source_snippet: str) -> str:
    combined = f"{raw_value_text} {source_snippet}".upper()
    for scale in ("B", "M", "K"):
        if re.search(rf"\b{scale}\b", combined):
            return scale
    return "absolute"


def _extract_numeric(raw_value_text: str, source_snippet: str) -> float | None:
    combined = f"{raw_value_text} {source_snippet}".strip()

    if not combined or "N/A" in combined.upper():
        return None

    parenthetical = re.search(r"\(([0-9,]+(?:\.[0-9]+)?)\)", combined)
    if parenthetical:
        try:
            return -float(parenthetical.group(1).replace(",", ""))
        except ValueError:
            return None

    m = re.search(r"-?[0-9][0-9,]*(?:\.[0-9]+)?", combined)
    if not m:
        return None

    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def normalize_value(
    raw_value_text: str,
    source_snippet: str,
    deal_currency: str = "USD",
) -> NormalizationResult:
    currency = _extract_currency(raw_value_text, source_snippet, deal_currency)
    scale = _extract_scale(raw_value_text, source_snippet)
    numeric = _extract_numeric(raw_value_text, source_snippet)

    if currency != deal_currency:
        return NormalizationResult(
            normalized_value=None,
            unit_currency=currency,
            unresolved_reason=f"currency_mismatch:{currency}_vs_{deal_currency}",
            raw_scale=scale,
        )

    if numeric is None:
        return NormalizationResult(
            normalized_value=None,
            unit_currency=currency,
            unresolved_reason="missing_numeric_value",
            raw_scale=scale,
        )

    factor = _SCALE_MAP.get(scale, 1)
    normalized = round(numeric * factor, 2)
    return NormalizationResult(
        normalized_value=normalized,
        unit_currency=currency,
        unresolved_reason=None,
        raw_scale=scale,
    )
