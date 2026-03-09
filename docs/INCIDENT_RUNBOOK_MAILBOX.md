# Incident Runbook: Mailbox and Onboarding Delivery

Applies to inbound gateway + Postmark production path.

## Severity levels

1. `SEV-1`: inbound traffic not processing at all.
2. `SEV-2`: partial processing or elevated delays/failures.
3. `SEV-3`: degraded but recoverable without customer impact.

## Core diagnostics

1. Check gateway health:
```bash
curl -s https://api.patrici.us/inbound/v1/health
```
2. Inspect DLQ growth:
```bash
wc -l runtime/inbound_dlq.jsonl
```
3. Check recent DLQ payloads:
```bash
tail -n 20 runtime/inbound_dlq.jsonl
```

## Incident A: inbound webhook unauthorized / rejected

Symptoms:
- `401 invalid_postmark_token` or repeated provider retries.

Actions:
1. Verify current inbound server token secret in runtime.
2. Verify token configured in Postmark webhook settings.
3. If mismatch, rotate token using standard rotation (see secrets runbook).
4. After fix, replay failed records from DLQ.

## Incident B: downstream internal API failures from gateway

Symptoms:
- `502 ingest_failed:*` or `502 process_failed:*`
- DLQ file increasing rapidly.

Actions:
1. Confirm internal API health:
```bash
curl -s https://api.patrici.us/internal/v1/health
```
2. Check auth and HTTPS expectations (`X-Internal-Token`, `X-Forwarded-Proto`).
3. Once fixed, run replay dry-run:
```bash
python tools/replay_inbound_dlq.py \
  --dlq-path runtime/inbound_dlq.jsonl \
  --internal-api-base https://api.patrici.us \
  --internal-token "$INTERNAL_API_TOKEN" \
  --require-https-header \
  --dry-run
```
4. Execute replay:
```bash
python tools/replay_inbound_dlq.py \
  --dlq-path runtime/inbound_dlq.jsonl \
  --internal-api-base https://api.patrici.us \
  --internal-token "$INTERNAL_API_TOKEN" \
  --require-https-header
```
5. Validate replay report:
- `runtime/inbound_dlq.replay_report.json`
- `runtime/inbound_dlq.replay_failed.jsonl`

## Incident C: onboarding email delivery failures

Symptoms:
- gateway responses include `notification.sent=false`
- user reports no account link email.

Actions:
1. Confirm outbound mode is `postmark` and outbound token exists.
2. Validate sender identity/domain in Postmark.
3. Check provider response codes in logs.
4. Re-trigger onboarding link manually:
```bash
curl -X POST https://api.patrici.us/auth/v1/magic-link/request \
  -H 'Content-Type: application/json' \
  -d '{"email":"user@example.com"}'
```
5. If outage persists, switch to temporary fallback mode (approved SMTP path) and notify on-call.

## Incident D: abnormal DLQ backlog

Symptoms:
- DLQ grows over threshold in short interval.

Actions:
1. Stop traffic amplification (avoid repeated replay loops).
2. Fix root cause first (token, internal API, provider outage).
3. Replay with bounded limit to avoid stampede:
```bash
python tools/replay_inbound_dlq.py \
  --dlq-path runtime/inbound_dlq.jsonl \
  --internal-api-base https://api.patrici.us \
  --internal-token "$INTERNAL_API_TOKEN" \
  --require-https-header \
  --limit 200
```
4. Repeat in batches until stable.

## Post-incident checklist

1. Capture timeline, blast radius, root cause.
2. Record replay counts and data loss confirmation.
3. Add prevention action and owner with due date.
4. Update this runbook if remediation steps changed.
