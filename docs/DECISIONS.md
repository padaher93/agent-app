# Decisions Log

Append-only record of product and execution decisions for this repository.

Rules:
1. Never edit historical rows except typo fixes that do not change meaning.
2. Supersede decisions by adding a new row with `Supersedes: D-XXXX`.
3. All contract changes must reference this file.

| Date | ID | Decision | Status | Owner | Notes |
|---|---|---|---|---|---|
| 2026-03-06 | D-0001 | Product is AI-first and agent-first; AI is not fallback behavior. | Locked | Product | Core thesis |
| 2026-03-06 | D-0002 | V1 value proposition is evidence-backed period deltas, not covenant monitoring. | Locked | Product | Covenants moved to V2 |
| 2026-03-06 | D-0003 | Ingestion channel for V1 is email-only via `inbound@patrici.us`. | Locked | Product | API-first internal architecture |
| 2026-03-06 | D-0004 | Internal API is mandatory now; public API exposure deferred to later versions. | Locked | Product | Externalization V2/V3 |
| 2026-03-06 | D-0005 | V1 supports `PDF` and `XLSX` attachments only. | Locked | Product | Other types rejected/flagged |
| 2026-03-06 | D-0006 | V1 mapping is one email package to one deal. | Locked | Product | Package split deferred |
| 2026-03-06 | D-0007 | Use fixed starter concept dictionary in V1. | Locked | Product | Open-ended extraction deferred |
| 2026-03-06 | D-0008 | Baseline period is published with delta columns shown as `N/A`. | Locked | Product | No prior period exists |
| 2026-03-06 | D-0009 | Delta page is always published; unresolved rows remain visible and flagged. | Locked | Product | Transparency over suppression |
| 2026-03-06 | D-0010 | `candidate_flagged` rows must show candidate value and exact evidence location. | Locked | Product | Trust and auditability |
| 2026-03-06 | D-0011 | Processing target is best-effort latency with accuracy-first priority. | Locked | Product | No hard SLA in V1 |
| 2026-03-06 | D-0012 | Monetization and paywall are out of scope for V1. | Locked | Product | Add later |
| 2026-03-06 | D-0013 | Agent architecture includes Orchestrator, Package Understanding, Extractor, and Independent Verifier. | Locked | Product | 4-agent model |
| 2026-03-06 | D-0014 | Retry cap is 2 per concept row; terminal statuses are `verified`, `candidate_flagged`, `unresolved`. | Locked | Product | Predictable state machine |
| 2026-03-06 | D-0015 | Logs are append-only with a `trace_id` per concept row. | Locked | Product | Immutable audit trail |
| 2026-03-06 | D-0016 | UI is desktop-first for V1. | Locked | Product | Mobile deferred |
| 2026-03-06 | D-0017 | UI theme is dark-first, blue accent, amber evidence highlight. | Locked | Product | Linear-like polish direction |
| 2026-03-06 | D-0018 | Table ordering uses materiality score (smart ordering), not fixed schema order. | Locked | Product | Relevance-first scanning |
| 2026-03-06 | D-0019 | User selection of evidence candidate resolves row immediately to `verified`. | Locked | Product | Adds user-resolution event |
| 2026-03-06 | D-0020 | Right-panel logs jump to exact decision event for selected cell. | Locked | Product | `trace_id` anchored UX |
| 2026-03-06 | D-0021 | Scope freeze rule: no new scope before launch quality gates are met. | Locked | Product | Execution discipline |
| 2026-03-06 | D-0022 | Interim dataset strategy is proxy/public and synthetic first, then design-partner real data. | Locked | Product | Solves chicken-and-egg |
| 2026-03-06 | D-0023 | Internal runtime processing defaults to file-driven extraction; label-assisted extraction is eval-only. | Locked | Product + Applied AI | Removes label dependency from runtime path |
| 2026-03-06 | D-0024 | Same deal + same period with changed content creates incrementing `period_revision`. | Locked | Platform | Immutable correction history |
| 2026-03-06 | D-0025 | Row resolution requires `Owner` or `Operator` role; `Viewer` is read-only. | Locked | Product + Platform | RBAC enforcement for write actions |
| 2026-03-06 | D-0026 | Trace evidence endpoint must return document-backed preview payload (XLSX/PDF) when source is available. | Locked | Product + Frontend | Evidence viewer contract |
| 2026-03-06 | D-0027 | Retention policy is executable tooling: packages 24 months, logs 7 years (with archive on prune). | Locked | Product + Platform | Charter retention operationalized |
| 2026-03-06 | D-0028 | Real borrower shadow data is isolated under dedicated `real_shadow_test` partition and evaluated separately. | Locked | Product + Applied AI | Prevents proxy/real contamination |
| 2026-03-06 | D-0029 | Inbound gateway endpoint is accepted ingress path for provider webhooks into internal ingest/process APIs. | Locked | Platform | Email-only ingestion path hardening |
| 2026-03-06 | D-0030 | Quality gate evaluation enforces critical-concept precision floor and regression blocking rules. | Locked | Product + Applied AI | Completes V1 gate policy implementation |

## Proposed Defaults Pending Explicit Revision
These defaults are active unless superseded by a newer decision row.

1. Confidence thresholds: `>=0.90 verified`, `0.80-0.89 candidate_flagged`, `<0.80 unresolved`.
2. Materiality formula: `0.5*abs_delta_norm + 0.3*pct_delta_norm + 0.2*concept_priority - 0.2*(1-confidence)`.
3. Tenant roles: `Owner`, `Operator`, `Viewer`; only Owner/Operator resolve flagged rows.
4. Retention: raw files 24 months; audit logs 7 years.
