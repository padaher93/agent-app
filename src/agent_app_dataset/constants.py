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

CONCEPT_DEFINITIONS = {
    "revenue_total": {
        "label": "Revenue (Total)",
        "priority": 13,
        "keywords": ("revenue", "total revenue", "net sales"),
    },
    "ebitda_reported": {
        "label": "EBITDA (Reported)",
        "priority": 12,
        "keywords": ("ebitda reported", "reported ebitda", "ebitda"),
    },
    "ebitda_adjusted": {
        "label": "EBITDA (Adjusted)",
        "priority": 11,
        "keywords": ("adjusted ebitda", "ebitda adjusted"),
    },
    "operating_income_ebit": {
        "label": "Operating Income (EBIT)",
        "priority": 10,
        "keywords": ("operating income", "ebit"),
    },
    "interest_expense": {
        "label": "Interest Expense",
        "priority": 9,
        "keywords": ("interest expense", "interest"),
    },
    "net_income": {
        "label": "Net Income",
        "priority": 8,
        "keywords": ("net income", "net earnings", "profit"),
    },
    "cash_and_equivalents": {
        "label": "Cash and Equivalents",
        "priority": 7,
        "keywords": ("cash and equivalents", "cash"),
    },
    "accounts_receivable_total": {
        "label": "Accounts Receivable (Total)",
        "priority": 6,
        "keywords": ("accounts receivable", "trade receivables"),
    },
    "inventory_total": {
        "label": "Inventory (Total)",
        "priority": 5,
        "keywords": ("inventory", "inventories"),
    },
    "accounts_payable_total": {
        "label": "Accounts Payable (Total)",
        "priority": 4,
        "keywords": ("accounts payable", "trade payables"),
    },
    "total_debt": {
        "label": "Total Debt",
        "priority": 3,
        "keywords": ("total debt", "debt"),
    },
    "total_assets": {
        "label": "Total Assets",
        "priority": 2,
        "keywords": ("total assets", "assets"),
    },
    "total_liabilities": {
        "label": "Total Liabilities",
        "priority": 1,
        "keywords": ("total liabilities", "liabilities"),
    },
}

CONCEPT_LABELS = {
    concept_id: details["label"] for concept_id, details in CONCEPT_DEFINITIONS.items()
}

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
