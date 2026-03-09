# Charter Gap Closure (V1)

Version: 1.0  
Date: 2026-03-06  
Owner: Product + Engineering

## Objective
Close previously identified gaps between implementation and locked V1 charter contracts.

## Delivered
1. Runtime extraction is file-driven (`PDF`/`XLSX`) with evidence payloads; eval-label mode is explicit and non-default.
2. Delta API now emits baseline/delta semantics:
   - `prior_value`
   - `current_value`
   - `abs_delta`
   - `pct_delta`
   - `period_revision`
3. Same-period restatements produce incrementing `period_revision`.
4. Resolution endpoint enforces role gating (`Owner`/`Operator` only).
5. Evidence preview endpoint added:
   - `GET /internal/v1/traces/{trace_id}/evidence`
   - returns document-backed preview payload when source file is available.
6. Quality gate engine now enforces:
   - critical concept verified precision floor (`>=95%` per concept)
   - regression blocking (`>1pp` drop in verified precision or evidence-link accuracy)
7. Retention operationalized via tooling:
   - packages: 24 months
   - logs: 7 years with archive on prune
8. Isolated `real_shadow_test` partition and sync tooling added.
9. Inbound gateway service added for provider webhook ingestion path.
10. Trace correction history is append-only and immutable:
    - `POST /internal/v1/traces/{trace_id}:resolve` appends `trace_resolutions`
    - `GET /internal/v1/traces/{trace_id}/history` exposes correction lineage
11. Unresolved rows are now evidence-anchored with explicit fallback locators (no empty evidence pointers).
12. Workspace isolation is enforced with `workspace_id` and `X-Workspace-Id` scoping on read/write APIs.
13. Internal API security controls added:
    - optional shared token auth (`X-Internal-Token`)
    - optional HTTPS enforcement (`X-Forwarded-Proto=https`)
    - optional at-rest payload encryption mode (Fernet key)
14. Eval gate runner accepts incident input and blocks release for open security/data-integrity incidents.
15. Desktop evidence/log fidelity upgraded:
    - sheet-level XLSX viewport preview with highlighted target cell
    - explicit decision-event pinning in logs for selected trace

## Primary tools
1. `tools/run_internal_api.py`
2. `tools/run_inbound_gateway.py`
3. `tools/apply_retention.py`
4. `tools/sync_real_shadow_partition.py`
5. `tools/run_eval.py` (with advanced gate rules)
