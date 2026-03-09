# Agent App Dataset Program

Implementation of the V1 Proxy Dataset Program for Patricius.

## What is included

- Machine-readable contracts (JSON Schemas)
- Source registry and package manifests
- Ground-truth labeling format with evidence locators
- Synthetic stress variant generator
- Dataset freeze/split tooling
- Evaluation harness with release-gate enforcement
- Trust artifact generator for design-partner readiness

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Create the 10-package pilot and validate contracts:

```bash
python tools/bootstrap_pilot_dataset.py
python tools/validate_dataset.py
```

Create a frozen split:

```bash
python tools/freeze_dataset.py \
  --packages-dir dataset/packages/pilot \
  --labels-dir dataset/labels/pilot \
  --freeze-dir dataset/freezes/pilot_v0.1
```

Generate synthetic stress variants:

```bash
python tools/generate_stress_variants.py \
  --packages-dir dataset/packages/pilot \
  --labels-dir dataset/labels/pilot \
  --output-packages-dir dataset/packages/pilot_stress \
  --output-labels-dir dataset/labels/pilot_stress
```

Run evaluation (requires predictions file):

```bash
python tools/run_extraction_baseline.py \
  --packages-dir dataset/packages/proxy_v1_full \
  --labels-dir dataset/labels/proxy_v1_full \
  --output-file dataset/predictions/latest_predictions.json

python tools/run_eval.py \
  --ground-truth-dir dataset/labels/proxy_v1_full \
  --predictions-file dataset/predictions/latest_predictions.json \
  --output-report dataset/eval/reports/latest.json \
  --history-dir dataset/eval/history
```

Run pre-partner readiness gates (proxy-backed, production launch still blocked until real shadow data exists):

```bash
python tools/run_pre_partner_readiness.py \
  --runtime-profile prod \
  --smoke-extraction-mode llm \
  --smoke-max-packages 1 \
  --output-summary runtime/pre_partner_readiness_summary.json
```

Use `--smoke-extraction-mode eval` only as a wiring smoke-check (not as a quality-pass proxy for launch decisions).

Run shadow-mode quality gate on real/redacted partition:

```bash
python tools/run_real_shadow_eval.py \
  --packages-dir dataset/real_shadow_test/packages \
  --labels-dir dataset/real_shadow_test/labels \
  --extraction-mode llm \
  --predictions-output runtime/real_shadow_predictions.json \
  --report-output runtime/real_shadow_eval_report.json \
  --history-dir dataset/eval/history/real_shadow_test \
  --min-packages 20 \
  --required-streak 3
```

Validate shadow partition readiness before eval:

```bash
python tools/validate_shadow_partition.py \
  --packages-dir dataset/real_shadow_test/packages \
  --labels-dir dataset/real_shadow_test/labels \
  --min-packages 20 \
  --min-deals 3 \
  --min-periods-per-deal 2
```

Run strict LLM smoke on selected package manifests:

```bash
python tools/run_strict_llm_smoke.py \
  --package-manifest dataset/real_shadow_test/packages/pkg_0001.json \
  --events-log runtime/strict_llm_smoke_events.jsonl \
  --max-retries 1 \
  --extraction-mode llm
```

Run full release-candidate gate chain with summary artifact:

```bash
python tools/run_release_candidate_gate.py \
  --runtime-profile prod \
  --smoke-packages-dir dataset/real_shadow_test/packages \
  --smoke-labels-dir dataset/real_shadow_test/labels \
  --smoke-max-packages 1 \
  --smoke-extraction-mode llm \
  --shadow-packages-dir dataset/real_shadow_test/packages \
  --shadow-labels-dir dataset/real_shadow_test/labels \
  --shadow-min-packages 20 \
  --shadow-min-deals 3 \
  --shadow-min-periods-per-deal 2 \
  --shadow-required-streak 3 \
  --output-summary runtime/release_candidate_summary.json
```

Run deterministic agent workflow (Phase 2 scaffold):

```bash
python tools/run_agent_workflow.py \
  --packages-dir dataset/packages/proxy_v1_full \
  --labels-dir dataset/labels/proxy_v1_full \
  --output-file dataset/predictions/workflow_predictions.json \
  --events-log dataset/eval/agent_events.jsonl \
  --truncate-log

python tools/check_event_log_integrity.py \
  --events-log dataset/eval/agent_events.jsonl
```

Generate trust artifact:

```bash
python tools/generate_trust_artifact.py \
  --eval-report dataset/eval/reports/latest.json \
  --output reports/design_partner/trust_artifact.md
```

Build design-partner readiness package bundle:

```bash
python tools/build_design_partner_package.py \
  --eval-report dataset/eval/reports/latest.json \
  --history-dir dataset/eval/history \
  --traces-file dataset/traces/sample_traces.json \
  --output-dir reports/design_partner/readiness_bundle
```

Run Phase 3 internal API:

```bash
python tools/run_internal_api.py \
  --host 127.0.0.1 \
  --port 8080 \
  --db-path runtime/internal_api.sqlite3 \
  --labels-dir dataset/labels/proxy_v1_full \
  --events-log runtime/agent_events.jsonl \
  --ui-dir src/agent_app_dataset/ui \
  --internal-token your_internal_token \
  --require-https \
  --encryption-key your_fernet_key \
  --runtime-profile dev
```

`/internal/v1/packages/{package_id}:process` supports:
- `extraction_mode=llm` (default): multi-agent LLM runtime (Agent 2 classify, Agent 3 extract, Agent 4 verify)
- `extraction_mode=runtime`: deterministic file-driven extraction from PDF/XLSX evidence
- `extraction_mode=eval`: ground-truth-assisted mode for harness/regression testing

Runtime profiles:
- `dev`: allows `llm`, `runtime`, and `eval` modes for local development/harness workflows.
- `staging` / `prod`: strict mode, enforces `llm`-only extraction, HTTPS+internal-token guards, and requires `OPENAI_API_KEY`.

Desktop shell URL after startup:

```text
http://127.0.0.1:8080/app/
```

Workspace-scoped shell URL:

```text
http://127.0.0.1:8080/app/?workspace=ws_default
```

Email adapter ingest to internal API:

```bash
python tools/email_adapter_ingest.py \
  --email-json dataset/examples/inbound_email.sample.json \
  --endpoint http://127.0.0.1:8080/internal/v1/packages:ingest
```

Run inbound gateway (provider/webhook-facing):

```bash
python tools/run_inbound_gateway.py \
  --host 127.0.0.1 \
  --port 8090 \
  --internal-api-base http://127.0.0.1:8080 \
  --inbound-token your_shared_secret \
  --internal-api-token your_internal_token \
  --internal-api-require-https \
  --attachments-dir runtime/inbound_attachments \
  --attachment-storage-mode local \
  --postmark-server-token your_postmark_server_token \
  --mailgun-signing-key your_mailgun_signing_key \
  --sendgrid-inbound-token your_sendgrid_inbound_token \
  --outbound-email-mode postmark \
  --outbound-from-email inbound@patrici.us \
  --outbound-postmark-server-token your_outbound_postmark_token \
  --dlq-path runtime/inbound_dlq.jsonl \
  --runtime-profile dev
```

Provider endpoints:
- `/inbound/v1/providers/postmark`
- `/inbound/v1/providers/mailgun`
- `/inbound/v1/providers/sendgrid`

Run ops preflight (runtime/profile + endpoint + DLQ checks):

```bash
python tools/run_ops_preflight.py \
  --runtime-profile prod \
  --inbound-gateway-base https://api.patrici.us \
  --internal-api-base https://api.patrici.us \
  --check-postmark-api \
  --output runtime/ops_preflight_report.json
```

Strict profile behavior (`--runtime-profile staging|prod`):
- only `/inbound/v1/providers/postmark` is enabled.
- `/inbound/v1/email`, `/inbound/v1/providers/mailgun`, and `/inbound/v1/providers/sendgrid` are disabled.
- requires `internal_api_token`, `internal_api_require_https`, `postmark_server_token`, and outbound Postmark mode/token.
- requires shared attachment storage mode: `--attachment-storage-mode s3 --attachment-storage-s3-bucket <bucket>`.

Auth/account onboarding endpoints:
- `POST /auth/v1/onboarding:ensure` (used by inbound flow)
- `POST /auth/v1/magic-link/request`
- `POST /auth/v1/magic-link/consume`
- `POST /auth/v1/login`
- `GET /auth/v1/me`
- `POST /auth/v1/logout`

Replay failed inbound records from DLQ:

```bash
python tools/replay_inbound_dlq.py \
  --dlq-path runtime/inbound_dlq.jsonl \
  --internal-api-base http://127.0.0.1:8080 \
  --internal-token your_internal_token \
  --dry-run
```

Ops runbooks/checklists:
- `docs/OPS_READINESS_CHECKLIST.md`
- `docs/INCIDENT_RUNBOOK_MAILBOX.md`
- `docs/SECRETS_ROTATION_RUNBOOK.md`
- `docs/LAUNCH_GATES_OPS.md`
- `docs/STORAGE_CONTRACT_V1.md`
- `docs/RELEASE_CANDIDATE_GATES.md`
```

Apply retention policy (default dry-run):

```bash
python tools/apply_retention.py \
  --db-path runtime/internal_api.sqlite3 \
  --events-log runtime/agent_events.jsonl

python tools/apply_retention.py \
  --db-path runtime/internal_api.sqlite3 \
  --events-log runtime/agent_events.jsonl \
  --apply
```

Sync isolated `real_shadow_test` partition (default dry-run):

```bash
python tools/sync_real_shadow_partition.py \
  --source-packages-dir /path/to/redacted/packages \
  --source-labels-dir /path/to/redacted/labels
```

## Repo policy for source files

- Raw public documents are not committed to git.
- Only metadata/checksums/storage URIs are committed.
- `storage_uri` points to object storage location for the source file.
