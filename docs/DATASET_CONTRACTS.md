# Dataset Contracts

Date: 2026-03-06

Machine-readable schemas are under `dataset/schemas/` and define the public interfaces for the dataset program.

## 1) source_registry

Schema: `dataset/schemas/source_registry.schema.json`

Required fields per source:
- `source_id`
- `url`
- `retrieved_at`
- `checksum`
- `doc_type`
- `license_note`
- `storage_uri`

## 2) package_manifest

Schema: `dataset/schemas/package_manifest.schema.json`

Required top-level fields:
- `package_id`
- `deal_id`
- `period_end_date`
- `source_email_id`
- `files[]`
- `source_ids[]`
- `variant_tags[]`

`files[]` includes source linkage and storage/checksum metadata.

## 3) ground_truth_row

Schema: `dataset/schemas/ground_truth_file.schema.json`

Required row fields align with V1 dictionary and evidence contract:
- `concept_id`
- `period_end_date`
- `raw_value_text`
- `normalized_value`
- `unit_currency`
- `expected_status`
- `evidence` (`doc_id`, `page_or_sheet`, `locator_type`, `locator_value`, `source_snippet`)

## 4) eval_report

Schema: `dataset/schemas/eval_report.schema.json`

Required fields:
- `dataset_version`
- `pipeline_version`
- `generated_at`
- `metrics` (all quality gate metrics)
- `failure_taxonomy`
- `gate_pass`

Optional security gating field:
- `incident_status` (for explicit security/data-integrity release blocks)

## Storage policy

Raw public documents are not stored in git. The repo stores metadata + checksums + `storage_uri` only.
