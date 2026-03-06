# Proxy Dataset Runbook

Date: 2026-03-06

## Goal
Operational runbook to execute the V1 proxy dataset lifecycle end-to-end.

## Contracts
Machine-readable contracts are frozen under `dataset/schemas/`:

1. `source_registry.schema.json`
2. `package_manifest.schema.json`
3. `ground_truth_file.schema.json`
4. `eval_report.schema.json`

## Commands

1. Bootstrap pilot (10 packages):

```bash
python tools/bootstrap_pilot_dataset.py
```

2. Validate contracts:

```bash
python tools/validate_dataset.py --packages-dir dataset/packages/pilot --labels-dir dataset/labels/pilot
```

3. Validate two-pass labeling QA:

```bash
python tools/check_labeling_quality.py --packages-dir dataset/packages/pilot --labels-dir dataset/labels/pilot
```

4. Scale to target (default 50 packages / 15 deals):

```bash
cp dataset/packages/pilot/*.json dataset/packages/proxy_v1/
cp dataset/labels/pilot/*.ground_truth.json dataset/labels/proxy_v1/
python tools/scale_proxy_dataset.py --packages-dir dataset/packages/proxy_v1 --labels-dir dataset/labels/proxy_v1
```

5. Generate stress variants:

```bash
python tools/generate_stress_variants.py \
  --packages-dir dataset/packages/proxy_v1 \
  --labels-dir dataset/labels/proxy_v1 \
  --output-packages-dir dataset/packages/proxy_v1_stress \
  --output-labels-dir dataset/labels/proxy_v1_stress \
  --variant-ratio 0.2
```

6. Build full dataset and freeze:

```bash
cp dataset/packages/proxy_v1/*.json dataset/packages/proxy_v1_full/
cp dataset/packages/proxy_v1_stress/*.json dataset/packages/proxy_v1_full/
cp dataset/labels/proxy_v1/*.ground_truth.json dataset/labels/proxy_v1_full/
cp dataset/labels/proxy_v1_stress/*.ground_truth.json dataset/labels/proxy_v1_full/
python tools/freeze_dataset.py \
  --packages-dir dataset/packages/proxy_v1_full \
  --labels-dir dataset/labels/proxy_v1_full \
  --freeze-dir dataset/freezes/dataset_v1.0 \
  --dataset-version dataset_v1.0
```

7. Run evaluation and gate streak:

```bash
python tools/run_extraction_baseline.py \
  --packages-dir dataset/packages/proxy_v1_full \
  --labels-dir dataset/labels/proxy_v1_full \
  --output-file dataset/predictions/latest_predictions.json

python tools/run_eval.py \
  --ground-truth-dir dataset/labels/proxy_v1_full \
  --predictions-file dataset/predictions/latest_predictions.json \
  --output-report dataset/eval/reports/latest.json \
  --history-dir dataset/eval/history \
  --dataset-version dataset_v1.0 \
  --pipeline-version local \
  --required-streak 3
```

Phase 2 workflow run (append-only `trace_id` event log):

```bash
python tools/run_agent_workflow.py \
  --packages-dir dataset/packages/proxy_v1_full \
  --labels-dir dataset/labels/proxy_v1_full \
  --output-file dataset/predictions/workflow_predictions.json \
  --events-log dataset/eval/agent_events.jsonl \
  --max-retries 2 \
  --truncate-log
```

Verify event-log integrity (hash chain + sequence):

```bash
python tools/check_event_log_integrity.py --events-log dataset/eval/agent_events.jsonl
```

8. Generate design-partner trust artifact:

```bash
python tools/generate_trust_artifact.py \
  --eval-report dataset/eval/reports/latest.json \
  --traces-file dataset/traces/sample_traces.json \
  --output reports/design_partner/trust_artifact.md
```

## Acceptance checks

- `python tools/validate_dataset.py --packages-dir dataset/packages/proxy_v1_full --labels-dir dataset/labels/proxy_v1_full` passes.
- Composition target check:

```bash
python tools/audit_dataset_composition.py --packages-dir dataset/packages/proxy_v1_full --labels-dir dataset/labels/proxy_v1_full
```

Expected baseline today:
- `packages=60`
- `deals=15`
- `noise_ratio>=0.20`

## Policy

- Raw files stay outside git (`storage_uri` only in repo).
- Ground-truth is two-pass (labeler + reviewer).
- Freeze manifests are immutable snapshots.
