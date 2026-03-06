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
  --ui-dir src/agent_app_dataset/ui
```

`/internal/v1/packages/{package_id}:process` supports:
- `extraction_mode=runtime` (default): file-driven extraction from PDF/XLSX evidence
- `extraction_mode=eval`: ground-truth-assisted mode for harness/regression testing

Desktop shell URL after startup:

```text
http://127.0.0.1:8080/app/
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
  --inbound-token your_shared_secret
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
