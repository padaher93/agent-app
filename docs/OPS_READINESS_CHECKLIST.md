# Ops Readiness Checklist (Postmark-First)

Date baseline: March 6, 2026  
Applies to: `agent-app` V1 inbound/extraction service  
Reuse source: `private-agent` Postmark inbound/outbound operating pattern.

## Objective

Operationalize mailbox and onboarding delivery so V1 can run with real inbound traffic at `inbound@patrici.us` and recover from failures safely.

## 1. Provider and routing (Postmark)

1. Use Postmark as the only mailbox provider for V1.
2. Confirm inbound server receives `inbound@patrici.us` traffic.
3. Inbound webhook URL points to:
   - `POST /inbound/v1/providers/postmark`
4. Outbound stream is enabled for onboarding notifications.
5. Separate secrets are configured for:
   - inbound webhook validation (`postmark_server_token`)
   - outbound send API (`outbound_postmark_server_token`)

## 2. DNS and domain controls

1. MX records active for inbound route.
2. SPF, DKIM, DMARC configured and validated.
3. Domain verification green in Postmark for inbound and outbound domains.
4. Anti-spoof and forwarding policy documented in security notes.

## 3. Runtime configuration (gateway)

Run inbound gateway with explicit Postmark modes:

```bash
python tools/run_inbound_gateway.py \
  --host 0.0.0.0 \
  --port 8090 \
  --internal-api-base https://api.patrici.us \
  --internal-api-token "$INTERNAL_API_TOKEN" \
  --internal-api-require-https \
  --postmark-server-token "$POSTMARK_INBOUND_SERVER_TOKEN" \
  --outbound-email-mode postmark \
  --outbound-from-email inbound@patrici.us \
  --outbound-postmark-server-token "$POSTMARK_OUTBOUND_SERVER_TOKEN" \
  --attachment-storage-mode s3 \
  --attachment-storage-s3-bucket "$INBOUND_ATTACHMENTS_S3_BUCKET" \
  --attachment-storage-s3-prefix inbound \
  --attachments-dir runtime/inbound_attachments \
  --dlq-path runtime/inbound_dlq.jsonl \
  --runtime-profile prod
```

## 4. Secrets and rotation

1. Store all secrets in central secret manager only (no plaintext in repo).
2. Required secret set:
   - `INTERNAL_API_TOKEN`
   - `POSTMARK_INBOUND_SERVER_TOKEN`
   - `POSTMARK_OUTBOUND_SERVER_TOKEN`
3. Rotation cadence: every 90 days, with overlap rollout.
4. Follow [SECRETS_ROTATION_RUNBOOK.md](/Users/patriciodaher/Desktop/agent-app/docs/SECRETS_ROTATION_RUNBOOK.md).

## 5. Alerts and SLO observability

1. Alert channels configured for:
   - inbound webhook failures
   - onboarding send failures
   - DLQ growth > threshold
   - sustained process failures from gateway forwarding
2. SLO definitions and thresholds locked in [LAUNCH_GATES_OPS.md](/Users/patriciodaher/Desktop/agent-app/docs/LAUNCH_GATES_OPS.md).

## 6. Replay and backfill readiness

1. DLQ file path configured and writable.
2. Replay dry-run verified:

```bash
python tools/replay_inbound_dlq.py \
  --dlq-path runtime/inbound_dlq.jsonl \
  --internal-api-base https://api.patrici.us \
  --internal-token "$INTERNAL_API_TOKEN" \
  --require-https-header \
  --dry-run
```

3. Real replay verified:

```bash
python tools/replay_inbound_dlq.py \
  --dlq-path runtime/inbound_dlq.jsonl \
  --internal-api-base https://api.patrici.us \
  --internal-token "$INTERNAL_API_TOKEN" \
  --require-https-header
```

4. Incident steps documented in [INCIDENT_RUNBOOK_MAILBOX.md](/Users/patriciodaher/Desktop/agent-app/docs/INCIDENT_RUNBOOK_MAILBOX.md).

## 7. Launch gate sign-off

All must be checked before launch:

- [ ] Provider inbound + outbound validated in production environment.
- [ ] DNS posture (MX/SPF/DKIM/DMARC) validated.
- [ ] Secrets rotation drill completed once.
- [ ] Alerts fire and route to on-call channel.
- [ ] Replay tool tested (dry-run + real replay).
- [ ] 3 consecutive days passing ops gates from [LAUNCH_GATES_OPS.md](/Users/patriciodaher/Desktop/agent-app/docs/LAUNCH_GATES_OPS.md).

## 8. Automated preflight command

Run preflight before deploy and daily during soak:

```bash
python tools/run_ops_preflight.py \
  --runtime-profile prod \
  --inbound-gateway-base https://api.patrici.us \
  --internal-api-base https://api.patrici.us \
  --check-postmark-api \
  --output runtime/ops_preflight_report.json
```

`passed=true` is required for the day to count toward launch streak.
