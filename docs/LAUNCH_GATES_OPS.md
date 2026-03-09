# Launch Gates: Operations

These gates must pass before production launch.

## Monitoring windows

1. Pre-launch soak: 7 days.
2. Minimum stable streak before launch decision: 3 consecutive days.

## Gate metrics

1. Inbound acceptance rate
- Definition: accepted inbound requests / total inbound requests.
- Gate: `>= 99.0%`.

2. Inbound to process completion rate
- Definition: packages reaching terminal (`completed` or `needs_review`) within SLA.
- Gate: `>= 97.0%` in 24h window.

3. P95 processing latency
- Definition: ingest acceptance to process completion.
- Gate: `<= 10 minutes`.

4. Onboarding notification success rate
- Definition: `notification.sent=true` / onboarding attempts that require password setup.
- Gate: `>= 99.0%`.

5. DLQ backlog health
- Definition: unreplayed failed records older than 24h.
- Gate: `0`.

6. Replay success rate
- Definition: successful replays / attempted replays.
- Gate: `>= 95.0%`.

## Blocking conditions

Any of the following blocks launch:

1. Any SEV-1 unresolved incident in the past 72 hours.
2. DLQ records older than 24 hours not triaged.
3. Secrets rotation not tested at least once in staging.
4. Missing on-call routing for inbound/outbound alert classes.
5. `tools/run_ops_preflight.py` report has `passed=false` on any day of required 3-day streak.

## Required sign-offs

1. Engineering owner
2. Product owner
3. Operations/on-call owner

Each sign-off confirms all gates are met for 3 consecutive days.
