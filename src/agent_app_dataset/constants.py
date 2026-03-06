from __future__ import annotations

from pathlib import Path

STARTER_CONCEPT_IDS = [
    "revenue_total",
    "ebitda_reported",
    "ebitda_adjusted",
    "operating_income_ebit",
    "interest_expense",
    "net_income",
    "cash_and_equivalents",
    "accounts_receivable_total",
    "inventory_total",
    "accounts_payable_total",
    "total_debt",
    "total_assets",
    "total_liabilities",
]

QUALITY_THRESHOLDS = {
    "verified_precision_min": 0.98,
    "evidence_link_accuracy_min": 0.99,
    "false_verified_rate_max": 0.01,
    "unresolved_rate_max": 0.15,
    "package_completion_rate_min": 0.95,
}

STATUS_VALUES = ("verified", "candidate_flagged", "unresolved")
LOCATOR_TYPES = ("cell", "bbox", "paragraph")
DOC_TYPES = ("PDF", "XLSX")

ROOT_DIR = Path(__file__).resolve().parents[2]
SCHEMA_DIR = ROOT_DIR / "dataset" / "schemas"
