# V1 Execution Charter

Version: 1.0  
Date: 2026-03-06  
Owner: Product

## Mission
Build an AI-first, agent-first service that converts borrower email packages into reliable period deltas with verifiable evidence and immutable audit logs.

## Product Promise (V1)
1. Ingest borrower packages from `inbound@patrici.us` (email-only).
2. Parse and map key concepts from `PDF` and `XLSX` attachments.
3. Publish a Delta Page table for each period.
4. Show exact evidence and agent decision logs per row.
5. Never hide uncertainty: flagged/unresolved rows remain visible.

## Non-Negotiables
1. AI is the primary execution path, not a fallback.
2. No manual mapping UI for core extraction.
3. Every row must be evidence-backed.
4. Logs are append-only and traceable by `trace_id`.
5. Scope is frozen until quality gates pass.

## Scope
### In Scope
1. Email adapter -> internal API -> agent workflow -> delta persistence.
2. One email package maps to one deal in V1.
3. Fixed starter concept dictionary (`docs/STARTER_CONCEPT_DICTIONARY_V1.md`).
4. Baseline period support (delta columns shown as `N/A`).
5. Desktop-first app UI with tri-panel workflow.

### Out of Scope
1. Covenant calculations and covenant breach automation.
2. Public external API.
3. Monetization/paywall.
4. Mobile-first UX.

## System Overview
1. Email Adapter
   - Receives package at `inbound@patrici.us`.
   - Normalizes sender, metadata, and attachments.
   - Calls internal ingestion API asynchronously.
2. Internal Pipeline
   - Classify and understand package context.
   - Extract concept candidates with evidence.
   - Independently verify candidates.
   - Publish period delta + audit trace.
3. App Layer
   - Left panel: deals/packages.
   - Middle panel: relevance-ranked delta tables.
   - Right panel: document evidence + anchored logs.

## Agent Behavior Contract (V1)
1. Agent 1: Orchestrator
   - Runs deterministic workflow phases and acceptance gates.
2. Agent 2: Package Understanding
   - Interprets file intent and period context.
3. Agent 3: Extractor
   - Produces concept candidates with evidence locations.
4. Agent 4: Independent Verifier
   - Validates candidate correctness and evidence fit.

### Retry and Terminal Rules
1. Max retries per concept row: `2`.
2. Row terminal statuses:
   - `verified`
   - `candidate_flagged`
   - `unresolved`
3. User resolution of flagged candidates sets row to `verified` and appends a user-resolution log event.

## Confidence and Materiality (Frozen Defaults)
### Confidence Thresholds
1. `verified`: confidence >= `0.90` and no hard blocker.
2. `candidate_flagged`: `0.80` to `0.89` or any hard blocker.
3. `unresolved`: confidence < `0.80`.

### Hard Blockers
1. Missing evidence location.
2. Conflicting candidate values.
3. Unit/currency mismatch unresolved.
4. Period mismatch unresolved.

### Materiality Score
Used for default row ordering in the delta table:

`materiality = 0.5 * abs_delta_norm + 0.3 * pct_delta_norm + 0.2 * concept_priority - 0.2 * (1 - confidence)`

Tie-breakers:
1. Lower confidence first.
2. Higher absolute delta second.
3. Concept ID alphabetical third.

## Data and Revision Contract
1. Package idempotency key based on normalized sender + received timestamp bucket + attachment content hashes.
2. Exact duplicate package does not create a new revision.
3. Same period with changed content creates a new `period_revision`.
4. Historical outputs are immutable; corrections append new events/revisions.
5. Dictionary version stored per row (`dictionary_version`).

## Access and Governance (V1 Defaults)
1. Tenant model: one workspace per customer account.
2. Roles:
   - `Owner`: full access, can resolve flagged rows.
   - `Operator`: can resolve flagged rows.
   - `Viewer`: read-only.
3. Encryption in transit and at rest.
4. Raw package retention: 24 months.
5. Audit log retention: 7 years, append-only.

## UI Contract (V1)
1. Desktop-first layout: `10vw / 45vw / 45vw`.
2. Dark mode first, blue accent, amber evidence highlight.
3. Middle panel sorted by materiality.
4. Right panel jumps to exact `trace_id` event for selected cell.
5. XLSX evidence renders actual sheet with highlighted cell.

## Quality Gates
Release is blocked until all thresholds in `docs/V1_QUALITY_GATES.md` pass.

## Change Control
1. Any change to this charter must be logged in `docs/DECISIONS.md`.
2. No scope additions before launch-gate metrics are met.
