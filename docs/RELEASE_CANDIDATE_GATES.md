# Release Candidate Gates

Date: 2026-03-06  
Owner: Product + Engineering

## Objective

Run one deterministic command chain before any deploy decision:

1. strict runtime configuration check
2. strict smoke processing check
3. shadow partition readiness validation
4. shadow eval streak gate check

If design partners are not onboarded yet, run the pre-partner gate to validate all non-partner work against frozen proxy data. This does not unlock production launch.

## Pre-Partner Gate

```bash
python tools/run_pre_partner_readiness.py \
  --runtime-profile prod \
  --smoke-extraction-mode llm \
  --smoke-max-packages 1 \
  --output-summary runtime/pre_partner_readiness_summary.json
```

Pre-partner pass criteria:

1. `strict_config.passed == true`
2. `llm_smoke.passed == true`
3. `proxy_readiness.passed == true`
4. `proxy_eval.release_ready == true`

`eval` extraction mode is acceptable for wiring checks only; readiness/pass decisions should use `llm` mode.

Pre-partner summary will always report:

1. `production_launch_ready == false`
2. `blocked_by` includes `real_shadow_partner_gate_pending`

## Commands

Validate shadow partition:

```bash
python tools/validate_shadow_partition.py \
  --packages-dir dataset/real_shadow_test/packages \
  --labels-dir dataset/real_shadow_test/labels \
  --min-packages 20 \
  --min-deals 3 \
  --min-periods-per-deal 2
```

Run strict LLM smoke:

```bash
python tools/run_strict_llm_smoke.py \
  --package-manifest dataset/real_shadow_test/packages/pkg_0001.json \
  --events-log runtime/strict_llm_smoke_events.jsonl \
  --max-retries 1 \
  --extraction-mode llm
```

Run full RC gate and emit summary JSON:

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

## Pass Criteria

All must be true:

1. `strict_config.passed == true`
2. `llm_smoke.passed == true`
3. `shadow_readiness.passed == true`
4. `shadow_eval.release_ready == true`

If any is false, release is blocked.
