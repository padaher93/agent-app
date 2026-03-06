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

Run Phase 3 internal API:

```bash
python tools/run_internal_api.py \
  --host 127.0.0.1 \
  --port 8080 \
  --db-path runtime/internal_api.sqlite3 \
  --labels-dir dataset/labels/proxy_v1_full \
  --events-log runtime/agent_events.jsonl
```

Email adapter ingest to internal API:

```bash
python tools/email_adapter_ingest.py \
  --email-json dataset/examples/inbound_email.sample.json \
  --endpoint http://127.0.0.1:8080/internal/v1/packages:ingest
```

## Repo policy for source files

- Raw public documents are not committed to git.
- Only metadata/checksums/storage URIs are committed.
- `storage_uri` points to object storage location for the source file.
