# Design Partner Trust Artifact

Generated at: 2026-03-06T16:18:30.990582+00:00
Dataset version: `dataset_v1.0`
Pipeline version: `local`

## Metric snapshot

- Verified precision: 100.00%
- Evidence-link accuracy: 100.00%
- False-verified rate: 0.00%
- Unresolved rate: 1.03%
- Package completion rate: 100.00%
- Gate pass: `True`

## Error taxonomy summary

- classification_errors: 2
- extraction_errors: 4
- normalization_errors: 1
- evidence_link_errors: 1
- period_alignment_errors: 0

## Representative evidence traces

1. `tr_pkg_0001_revenue_total` | package `pkg_0001` | concept `revenue_total`
   - Value: `12500000.0` | confidence: `0.99`
   - Evidence: file_0001_01 @ cell=B10
   - Snippet: Revenue Total: 12,500,000.00
2. `tr_pkg_0002_ebitda_reported` | package `pkg_0002` | concept `ebitda_reported`
   - Value: `2597000.0` | confidence: `0.99`
   - Evidence: file_0002_01 @ paragraph=p3:l2
   - Snippet: EBITDA (reported) is 2,597,000
3. `tr_pkg_0010_interest_expense` | package `pkg_0010` | concept `interest_expense`
   - Value: `None` | confidence: `0.72`
   - Evidence: file_0010_02 @ paragraph=p5:l4
   - Snippet: Interest schedule missing in submitted package

## Notes
- This artifact is generated from proxy/phase data unless otherwise stated.
- Real borrower data should remain in shadow partitions until stability criteria are met.