# Secrets Rotation Runbook

Applies to inbound and onboarding delivery stack.

## Rotation scope

1. `INTERNAL_API_TOKEN`
2. `POSTMARK_INBOUND_SERVER_TOKEN`
3. `POSTMARK_OUTBOUND_SERVER_TOKEN`

## Cadence

1. Standard: every 90 days.
2. Emergency: immediate rotation after suspected leak.

## Rotation model

Use staged cutover with overlap where possible.

## Procedure

1. Generate new secret values.
2. Store new values in secret manager as `next` version.
3. Deploy runtime with `next` values in non-prod and run smoke checks.
4. Promote `next` to production runtime.
5. Update Postmark webhook and outbound server tokens to new values.
6. Validate health and message flow:
   - `/inbound/v1/health`
   - send test inbound package and confirm processing.
   - verify onboarding email delivery success.
7. Keep old secret available for short rollback window (for example 30 minutes).
8. Revoke old secret permanently.

## Validation checklist

- [ ] Inbound webhook accepted with new inbound token.
- [ ] Internal forwarding accepted with new internal token.
- [ ] Outbound onboarding email sends with new outbound token.
- [ ] No DLQ spike after cutover.

## Rollback plan

1. Revert runtime to previous known-good secret versions.
2. Revert provider token settings.
3. Confirm health + test package.
4. Open incident and document failed rotation cause.

## Evidence to capture

1. Rotation timestamp and operator.
2. Secret version IDs (not raw values).
3. Validation command outputs.
4. Any transient errors and resolution.
