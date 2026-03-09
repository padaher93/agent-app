# Phase 3 Internal API Contract

Date: 2026-03-06

## Purpose
Expose internal processing interfaces for package ingest, async processing, delta retrieval, and trace lookup.

## Endpoints

1. `POST /internal/v1/packages:ingest`
- Input: sender/deal/period/email metadata + `files[]`
- Behavior: computes idempotency key and creates package with status `received`
- Duplicate behavior: same idempotency key returns existing package (`created=false`)

2. `POST /internal/v1/packages/{package_id}:process`
- Input: `async_mode`, `max_retries`, `extraction_mode` (`runtime` default, `eval` optional)
- Behavior: transitions package to `processing`, runs extraction + workflow, stores deltas/traces
- Output lifecycle: `completed` or `needs_review` (or `failed` on exception)

3. `GET /internal/v1/packages/{package_id}`
- Returns package metadata, `period_revision`, and current lifecycle status.
- Supports `include_manifest=true` and `include_processed_payload=true`.

4. `GET /internal/v1/deals/{deal_id}/periods/{period_id}/delta`
- Returns enriched Delta Page rows (`prior/current/abs/pct`) with baseline semantics.

5. `GET /internal/v1/traces/{trace_id}`
- Returns the trace row and associated package/deal context.

6. `GET /internal/v1/traces/{trace_id}/events`
- Returns append-only events filtered by `trace_id`.

7. `GET /internal/v1/traces/{trace_id}/history`
- Returns append-only user resolution history for immutable correction lineage.

8. `GET /internal/v1/traces/{trace_id}/evidence`
- Returns evidence preview payload for document viewer (`xlsx_sheet` or `pdf_text` when available).

9. `POST /internal/v1/traces/{trace_id}:resolve`
- Resolves row to `verified` via append-only trace resolution record (non-destructive correction).
- Requires `X-Role: Owner|Operator` (Viewer is forbidden).

10. `GET /internal/v1/packages/{package_id}/events`
- Returns append-only events filtered by package.

11. `GET /internal/v1/deals`
- Returns deal list with periods.

12. `GET /internal/v1/deals/{deal_id}/periods`
- Returns periods for selected deal.

13. `GET /internal/v1/health`
- Internal health signal for runtime wiring.

## Lifecycle statuses
- `received`
- `processing`
- `completed`
- `needs_review`
- `failed`

## Idempotency policy
Key is derived from:
1. normalized sender email
2. received-at hour bucket (UTC)
3. sorted attachment checksums (or file ids fallback)

## Storage
SQLite runtime store (`runtime/internal_api.sqlite3` by default):
1. `packages` table for ingest/process metadata
2. `traces` table for row-level trace lookup
3. `period_revision` tracked per `(deal_id, period_end_date)` for same-period restatements.
4. `trace_resolutions` table for append-only user correction history.

## Workspace and Access Scope
1. Packages are tagged with `workspace_id` (default `ws_default`).
2. Read/write endpoints are scoped by `X-Workspace-Id`; cross-workspace access returns not-found.
3. Row resolution still requires `X-Role: Owner|Operator`.

## Security Controls
1. Optional shared-token auth for all endpoints: `X-Internal-Token`.
2. Optional HTTPS enforcement for proxied traffic: `X-Forwarded-Proto=https`.
3. Optional at-rest payload encryption mode via Fernet key at API startup.

## Email adapter
Use `tools/email_adapter_ingest.py` to convert normalized email payloads into ingest calls.
Sample payload: `dataset/examples/inbound_email.sample.json`.
