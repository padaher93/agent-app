# Phase 4: Desktop V1 Shell

Version: 1.0  
Date: 2026-03-06  
Owner: Product + Engineering

## Goal
Deliver the desktop tri-panel review shell for evidence-backed delta operations.

## Implemented
1. Static desktop app mounted at `/app` from internal API runtime.
2. Layout contract implemented: `10vw / 45vw / 45vw`.
3. Left panel: deal and period navigator.
4. Middle panel: per-period delta table sorted by materiality score.
5. Right panel:
   - top: evidence context (document grid or selected row evidence focus)
   - bottom: append-only logs stream (package-level or trace-level)
6. Row selection behavior:
   - selecting a row anchors evidence + logs to that `trace_id`
   - logs surface the exact decision event (`verify_accepted`/`verify_rejected`/`user_resolved`) for that selected trace
   - logs support jump-to-trace behavior
   - evidence preview uses `GET /internal/v1/traces/{trace_id}/evidence`
7. User resolution flow:
   - `candidate_flagged` and `unresolved` rows expose candidate evidence options
   - selecting a candidate calls `POST /internal/v1/traces/{trace_id}:resolve`
   - row is moved to `verified` and `user_resolved` event is appended

## Internal API additions used by desktop shell
1. `GET /internal/v1/deals`
2. `GET /internal/v1/deals/{deal_id}/periods`
3. `GET /internal/v1/packages/{package_id}/events`
4. `GET /internal/v1/traces/{trace_id}/events`
5. `POST /internal/v1/traces/{trace_id}:resolve`
6. `GET /internal/v1/packages/{package_id}?include_manifest=true`

## Notes
1. This is desktop-first only for V1.
2. Evidence rendering is trace-anchored and deterministic from stored locators.
3. Materiality ordering uses the locked weighted formula with deterministic tie-breakers.
4. XLSX evidence renders a sheet-level viewport (scrollable) with highlighted target locator.
5. Viewer supports document-backed previews when source files are locally available via `storage_uri`.
