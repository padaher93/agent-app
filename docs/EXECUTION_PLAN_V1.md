# V1 Execution Plan

Version: 1.0  
Date: 2026-03-06  
Owner: Product + Engineering  
Status: Active (Phases 1-4 implemented; charter gap-closure hardening implemented)

## Summary
Execute V1 in four build phases after the dataset foundation.

Scope remains locked to:
1. Email-first ingestion.
2. Internal API-first architecture.
3. Fixed starter concept dictionary.
4. Evidence-backed deltas with immutable `trace_id` logs.
5. Release by quality gates only.

## Current State (Completed)
1. Proxy dataset program implemented.
2. Machine-readable schemas frozen.
3. `dataset_v1.0` freeze created.
4. Eval harness and trust artifact generation operational.
5. Validation and tests passing.
6. Phase 1 extraction baseline and CI gate workflow implemented.
7. Phase 2 deterministic agent workflow + append-only integrity-checked logs implemented.
8. Phase 3 internal API + email adapter implemented with async lifecycle and idempotent ingest.
9. Phase 4 desktop tri-panel shell implemented with trace-anchored evidence/log views and user-resolution flow.
10. Design-partner readiness bundle tooling implemented for launch-by-metric operations.
11. Runtime extraction path decoupled from label files (eval mode retained for harness only).
12. Delta API now returns baseline/delta semantics (`prior/current/abs/pct`) and `period_revision`.
13. Role-gated resolution, evidence preview endpoint, retention tooling, inbound gateway, and isolated `real_shadow_test` partition implemented.
14. Trace resolution is append-only with immutable history endpoint and non-destructive correction overlays.
15. Workspace-scoped API access, optional token auth/HTTPS enforcement, and optional at-rest payload encryption mode implemented.
16. Eval gate supports explicit security/data-integrity incident blocking.
17. Desktop shell now pins exact decision events and uses sheet-level XLSX viewport previews.
18. Pre-partner readiness gate implemented for proxy-only progression while preserving production block until real shadow data exists.

## Phase Plan

### Phase 1: Extraction Baseline
Target window: Week 1 to Week 2  
Goal: Produce predictions from package manifests with evidence links and statuses.

Deliver:
1. Extraction runner that outputs eval-compatible predictions.
2. Normalization pipeline (scale + currency handling + unresolved behavior).
3. Evidence linker outputting `doc_id`, `locator_type`, `locator_value`.
4. Confidence-to-status mapper using frozen thresholds.

Exit gates:
1. All schema validations pass.
2. Eval report generated on frozen split.
3. No contract drift from `dataset/schemas/*.json`.

### Phase 2: Agent Workflow + Audit Log
Target window: Week 2 to Week 3  
Goal: Implement Agent 1-4 deterministic orchestration with retry/terminal rules and append-only logs.

Deliver:
1. Orchestrator state machine (`classify -> extract -> verify -> publish`).
2. Agent retry cap enforcement (`max_retries=2` per concept row).
3. Terminal row statuses (`verified`, `candidate_flagged`, `unresolved`).
4. Append-only event log with per-row `trace_id`.

Exit gates:
1. Every row includes evidence pointer and `trace_id`.
2. Retry behavior matches charter rules.
3. Log immutability checks pass.

### Phase 3: Internal API + Email Adapter
Target window: Week 3 to Week 4  
Goal: Expose internal processing interfaces and connect inbound email path.

Deliver:
1. Internal API endpoints for ingest, process, status, delta results, trace lookup.
2. Asynchronous package lifecycle statuses:
   - `received`
   - `processing`
   - `completed`
   - `needs_review`
   - `failed`
3. Idempotency behavior for duplicate submissions.
4. Email adapter from `inbound@patrici.us` to internal ingest API.

Exit gates:
1. End-to-end path works: email metadata -> process -> stored delta + logs.
2. Duplicate package handling behaves deterministically.
3. Package state transitions are auditable.

### Phase 4: Desktop V1 Shell
Target window: Week 4 to Week 6  
Goal: Deliver review UX aligned to locked UI contract.

Deliver:
1. Tri-panel desktop layout (`10vw / 45vw / 45vw`).
2. Middle delta table with materiality ordering.
3. Right panel evidence viewer (PDF + actual XLSX sheet highlighting).
4. Log panel jump-to-exact `trace_id` decision event.
5. Candidate selection flow that resolves row to `verified` with user-resolution event.

Exit gates:
1. Operator can complete full review flow without manual mapping.
2. Evidence/log context always matches selected row.
3. Candidate resolution creates immutable user-resolution event.

## Quality and Release Rules
1. Use `docs/V1_QUALITY_GATES.md` as release source of truth.
2. Block release unless all metrics pass on frozen split for 3 consecutive runs.
3. Run full regression on every model/prompt/workflow change.
4. Weekly error taxonomy review is mandatory.

## Internal Interfaces To Add
1. `POST /internal/v1/packages:ingest`
2. `POST /internal/v1/packages/{package_id}:process`
3. `GET /internal/v1/packages/{package_id}`
4. `GET /internal/v1/deals/{deal_id}/periods/{period_id}/delta`
5. `GET /internal/v1/traces/{trace_id}`

## Ownership and Cadence
1. Product: scope control, decision logging, design-partner readiness.
2. Applied AI: extraction quality, confidence calibration, eval improvements.
3. Platform: API, orchestration runtime, ingestion lifecycle, idempotency.
4. Frontend: desktop tri-panel review app and evidence/log interactions.

Cadence:
1. Daily: build + eval report review.
2. Weekly: error taxonomy review + gate status review.
3. Weekly: decisions update in `docs/DECISIONS.md` when contracts change.

## Assumptions
1. No scope expansion before quality-gate pass.
2. No public API exposure in V1.
3. No covenants or monetization in V1.
4. Real borrower packages are onboarded in shadow mode after design-partner engagement.
