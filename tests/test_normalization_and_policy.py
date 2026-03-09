from __future__ import annotations

from agent_app_dataset.normalization import normalize_value
from agent_app_dataset.policy import classify_status


def test_normalize_value_handles_scale_and_currency() -> None:
    result = normalize_value(raw_value_text="USD 2.5 M", source_snippet="EBITDA")
    assert result.normalized_value == 2_500_000.0
    assert result.unit_currency == "USD"
    assert result.unresolved_reason is None


def test_normalize_value_handles_missing_numeric() -> None:
    result = normalize_value(raw_value_text="N/A", source_snippet="Interest schedule missing")
    assert result.normalized_value is None
    assert result.unresolved_reason == "missing_numeric_value"


def test_normalize_value_ignores_non_currency_three_letter_words() -> None:
    result = normalize_value(
        raw_value_text="$1,210,000.00",
        source_snippet="Net Income: 1,210,000.00",
        deal_currency="USD",
    )
    assert result.normalized_value == 1_210_000.0
    assert result.unit_currency == "USD"
    assert result.unresolved_reason is None


def test_normalize_value_detects_explicit_currency_mismatch() -> None:
    result = normalize_value(
        raw_value_text="EUR 2.5 M",
        source_snippet="Cash and equivalents",
        deal_currency="USD",
    )
    assert result.normalized_value is None
    assert result.unit_currency == "EUR"
    assert result.unresolved_reason == "currency_mismatch:EUR_vs_USD"


def test_status_policy_thresholds_and_blockers() -> None:
    assert classify_status(0.95, []) == "verified"
    assert classify_status(0.85, []) == "candidate_flagged"
    assert classify_status(0.95, ["missing_evidence_location"]) == "candidate_flagged"
    assert classify_status(0.70, []) == "unresolved"
