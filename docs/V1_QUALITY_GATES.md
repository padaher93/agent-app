# V1 Quality Gates

Version: 1.0  
Date: 2026-03-06  
Owner: Product + Engineering

## Purpose
Define hard release criteria for V1. If gates fail, release is blocked.

## Core Metrics
1. Verified Row Precision
   - Definition: correct `verified` rows / total `verified` rows.
   - Target: `>= 98.0%`.
2. Evidence-Link Accuracy
   - Definition: rows where linked evidence truly supports the extracted value and concept / total rows with evidence links.
   - Target: `>= 99.0%`.
3. False-Verified Rate
   - Definition: `verified` rows later overturned by audit/review / total `verified` rows.
   - Target: `< 1.0%`.
4. Unresolved Rate
   - Definition: `unresolved` rows / total expected starter-dictionary rows.
   - Target: `<= 15.0%`.
5. Package Completion Rate
   - Definition: packages that finish with published delta page and traceable evidence/logs / total accepted packages.
   - Target: `>= 95.0%`.

## Confidence and Status Policy
1. `verified`: confidence >= `0.90` and no hard blocker.
2. `candidate_flagged`: confidence `0.80` to `0.89` or any hard blocker.
3. `unresolved`: confidence < `0.80`.

Hard blockers:
1. Missing evidence location.
2. Conflicting candidate values.
3. Unresolved currency/unit mismatch.
4. Unresolved period mismatch.

## Release Gate Rules
1. All core metrics must pass for 3 consecutive evaluation runs on the frozen test split.
2. No single critical concept may have verified precision below `95.0%`.
3. Any regression > `1.0` percentage point on Verified Row Precision or Evidence-Link Accuracy blocks release.
4. Any security/data-integrity incident blocks release until closed.
5. Shadow-mode (`real_shadow_test`) must meet the same gate thresholds with minimum sample size `>= 20` packages before production launch.
6. Before design-partner data is available, pre-partner readiness may run on proxy data to validate all non-partner work, but cannot mark production launch as ready.

## Evaluation Cadence
1. Run full regression on every workflow/model/prompt change.
2. Run daily scheduled regression during active build periods.
3. Review error taxonomy weekly:
   - classification errors
   - extraction errors
   - normalization errors
   - evidence-link errors
   - period alignment errors

## Reporting Format
Each evaluation report must include:
1. timestamp + dataset version
2. pipeline commit/version
3. metric table vs thresholds
4. top failure categories
5. blocked/unblocked release status

## Change Control
Any threshold or formula changes must be recorded in `docs/DECISIONS.md` before becoming active.
