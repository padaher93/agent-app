# Starter Concept Dictionary V1

Version: 1.0  
Date: 2026-03-06  
Owner: Product

## Purpose
Define the fixed V1 concept schema for delta extraction and ranking.

## Concepts
| concept_id | label | unit_type | statement_type |
|---|---|---|---|
| `revenue_total` | Revenue (Total) | `currency` | `IS` |
| `ebitda_reported` | EBITDA (Reported) | `currency` | `IS` |
| `ebitda_adjusted` | EBITDA (Adjusted) | `currency` | `IS` |
| `operating_income_ebit` | Operating Income (EBIT) | `currency` | `IS` |
| `interest_expense` | Interest Expense | `currency` | `IS` |
| `net_income` | Net Income | `currency` | `IS` |
| `cash_and_equivalents` | Cash and Equivalents | `currency` | `BS` |
| `accounts_receivable_total` | Accounts Receivable (Total) | `currency` | `BS` |
| `inventory_total` | Inventory (Total) | `currency` | `BS` |
| `accounts_payable_total` | Accounts Payable (Total) | `currency` | `BS` |
| `total_debt` | Total Debt | `currency` | `BS` |
| `total_assets` | Total Assets | `currency` | `BS` |
| `total_liabilities` | Total Liabilities | `currency` | `BS` |

## Normalization Rules
1. `concept_id` is immutable and unique.
2. Store both `raw_value_text` and `normalized_value`.
3. Normalize units to absolute numeric values when possible.
4. Preserve original scale metadata (`raw_scale`: `K`, `M`, `B`, `absolute`).
5. Convert to `deal_currency` when conversion context is reliable; otherwise mark row `unresolved`.

## Row Output Fields (Required)
1. `concept_id`
2. `label`
3. `prior_value`
4. `current_value`
5. `abs_delta`
6. `pct_delta`
7. `status` (`verified`, `candidate_flagged`, `unresolved`)
8. `confidence`
9. `evidence_link`

## Evidence Fields (Required Per Row)
1. `doc_id`
2. `doc_name`
3. `page_or_sheet`
4. `locator_type` (`cell`, `bbox`, `paragraph`)
5. `locator_value`
6. `source_snippet`
7. `raw_value_text`
8. `normalized_value`
9. `unit_currency`
10. `extractor_agent_id`
11. `verifier_agent_id`
12. `trace_id`
13. `extracted_at`

## Baseline Rule
For baseline periods, `prior_value`, `abs_delta`, and `pct_delta` are shown as `N/A`.

## Change Control
1. Any concept add/remove/rename requires a new dictionary version.
2. Historical rows are never backfilled to a new dictionary version.
3. All dictionary changes must be logged in `docs/DECISIONS.md`.
