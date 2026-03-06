# Evaluation Dataset Plan

Version: 1.0  
Date: 2026-03-06  
Owner: Product + Applied AI

## Objective
Create a trustworthy evaluation dataset before design-partner borrower packages are available, then expand with real packages as trust grows.

## Strategy
1. Phase A: Public/proxy + synthetic stress dataset (immediate).
2. Phase B: Design-partner real package onboarding (later).
3. Keep the same schema across both phases to preserve comparable metrics.

## Phase A: Proxy Dataset (Start Now)
### Sources
1. Publicly available credit-related financial documents (PDF).
2. Public spreadsheet-style financial tables/schedules (XLSX).
3. Internal synthetic spreadsheets mirroring borrower reporting variability.

### Package Construction
1. Build realistic "email package" bundles:
   - 1 to 8 files per package
   - mixed PDF/XLSX
   - period metadata in package manifest
2. Include baseline and follow-up periods per simulated deal.
3. Simulate messy inputs:
   - inconsistent labels
   - missing schedules/pages
   - amended definitions
   - unit scale variance (`K`, `M`, absolute)
   - optional OCR quality degradation for scanned PDFs

### Ground Truth Schema
For each expected concept row:
1. `concept_id`
2. `period_end_date`
3. `raw_value_text`
4. `normalized_value`
5. `unit_currency`
6. `doc_id`
7. `page_or_sheet`
8. `locator_type` (`cell`, `bbox`, `paragraph`)
9. `locator_value`
10. `source_snippet`
11. `labeler_confidence`

### Dataset Size Targets (V1)
1. Minimum `50` packages.
2. Minimum `15` simulated deals.
3. Minimum `2` periods per deal.
4. At least `20%` of packages must contain deliberate ambiguity/noise.

## Splits and Versioning
1. Freeze split by package:
   - `train/dev`: 70%
   - `validation`: 10%
   - `test`: 20%
2. Version each freeze as `dataset_vX.Y`.
3. Never edit a released test split; create a new version for changes.

## Labeling and QA Process
1. Two-pass labeling:
   - Pass 1: primary labeler creates truth rows.
   - Pass 2: reviewer validates evidence locations and normalization.
2. Disagreements become adjudication tasks.
3. Labeling QA sample check every week.

## Phase B: Real Package Onboarding
1. Start after first design partners agree to participate.
2. Ingest redacted/historical packages in shadow mode first.
3. Add approved real packages to a separate controlled dataset partition.
4. Track metrics separately:
   - `proxy_test`
   - `real_shadow_test`
5. Promote real partition into release gates only after minimum sample size and stability.

## Data Handling
1. Keep proxy and real data physically separated.
2. Apply retention and access controls from the execution charter.
3. Keep immutable audit links from source to ground truth row.

## Deliverables
1. `dataset_manifest.json` per version.
2. Package-level manifests and ground-truth files.
3. Evaluation report template aligned with `docs/V1_QUALITY_GATES.md`.
