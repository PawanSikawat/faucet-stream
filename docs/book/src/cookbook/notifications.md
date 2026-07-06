# Notifications (Slack / PagerDuty / webhook)

The top-level `notifications:` block fans pipeline **lifecycle** and **health**
events out to Slack, PagerDuty, or a generic signed webhook — so a failure,
SLA breach, or tripped circuit breaker reaches your team without you having to
stand up Prometheus + Alertmanager first.

It is fully opt-in and requires the `notify` build feature
(`cargo install faucet-cli --features notify`, or `--features full`). With no
block, nothing changes.

> **Delivery never fails a run.** Each event is delivered with a short bounded
> retry; a channel outage is logged, counted
> (`faucet_notifications_dropped_total`), and swallowed — the pipeline is never
> blocked or failed by a notification. This is the same log-and-continue
> contract as lineage and SLA monitoring.

## Events

| Event | Fires when | Severity |
|-------|-----------|----------|
| `run_failure` | a run (or its final flush) failed | error |
| `run_success` | a run completed successfully | info |
| `sla_breach` | a post-run SLA check was violated (staleness / min_rows / volume) | warning |
| `circuit_open` | the resilience circuit breaker tripped | critical |
| `contract_abort` | a data-contract breach aborted the run (`on_breach: fail`) | error |
| `dlq_threshold` | a run routed rows to the DLQ at/over the rule's threshold | warning |
| `scheduler_stuck` | `faucet schedule` is exiting on consecutive failures | critical |

Events fire from every runtime — `faucet run`, `faucet schedule`,
`faucet serve`, and `faucet replicate` — because the emit sites live in the
shared executor (plus the scheduler's `scheduler_stuck` signal). They are
scoped to real, whole-pipeline **root** runs: `--dry-run`, `--limit`, sharded,
and cancelled runs do not notify.

## A rule

Each entry in the list is one rule: which events (`on:`), an optional severity
floor, an optional coalesce window, and one delivery `channel:`. The channel
uses the project-wide adjacently-tagged `{ type, config }` shape — the same
shape as connector `auth:`.

```yaml
notifications:
  - name: oncall-pagerduty
    on: [run_failure, circuit_open, contract_abort, scheduler_stuck]
    channel:
      type: pagerduty
      config:
        routing_key: "${env:PAGERDUTY_ROUTING_KEY}"

  - name: slack-alerts
    on: [run_failure, sla_breach, dlq_threshold]
    dedupe_window_secs: 300      # coalesce repeats within 5 minutes
    channel:
      type: slack
      config:
        webhook_url: "${env:SLACK_WEBHOOK_URL}"
        channel: "#data-alerts"

  - name: internal-webhook
    min_severity: warning        # info | warning | error | critical
    # empty `on:` = every event kind
    channel:
      type: webhook
      config:
        url: "https://ops.internal.example.com/hooks/faucet"
        hmac_secret: "${env:FAUCET_WEBHOOK_SECRET}"
```

### Fields

| Field | Meaning |
|-------|---------|
| `name` | Unique rule name (metric label, dedupe key, logs). |
| `on` | Event kinds to fire on. **Empty = all kinds.** |
| `min_severity` | Only deliver events at/above this severity. Default `info`. |
| `dedupe_window_secs` | Leading-edge coalesce: drop an identical event (same rule + pipeline + row) within this window. Absent / `0` = no coalescing. |
| `dlq_threshold` | For `dlq_threshold` only: minimum DLQ rows before firing. Default `1`. |
| `channel` | The delivery channel — `{ type, config }`. |

## Channels

### Slack

```yaml
channel:
  type: slack
  config:
    webhook_url: "${env:SLACK_WEBHOOK_URL}"   # incoming-webhook URL
    channel: "#alerts"                        # optional override
    username: "faucet"                        # optional override
```

### PagerDuty

Uses the Events API v2. A failure-class event **opens** an incident; the next
`run_success` on the same pipeline/row automatically sends a matching
**resolve** (correlated by dedup key), so incidents self-close.

```yaml
channel:
  type: pagerduty
  config:
    routing_key: "${env:PAGERDUTY_ROUTING_KEY}"
    source: "orders-pipeline"     # optional; defaults to the pipeline name
```

### Generic webhook

Posts a stable JSON envelope. If `hmac_secret` is set, the body is signed with
HMAC-SHA256 and the lowercase-hex digest is sent in `signature_header`
(default `X-Faucet-Signature`) so the receiver can verify authenticity.

```yaml
channel:
  type: webhook
  config:
    url: "https://ops.example.com/hooks/faucet"
    method: POST                              # default POST
    headers: { X-Env: prod }                  # optional extra headers
    hmac_secret: "${env:FAUCET_WEBHOOK_SECRET}"
    signature_header: "X-Faucet-Signature"    # default
```

## Secrets

Supply channel credentials via `${env:...}` / `${file:...}` / `${secret:...}`,
which are resolved over the raw config at load time and **registered for log
redaction** — never inline a webhook URL or routing key. (These universal
directives work anywhere in the config; cloud secrets-manager schemes like
`${vault:...}` are resolved for the connector-config surfaces documented under
[Secrets-manager interpolation](./secrets.md).)

## Testing your setup

Fire a synthetic event through a config's rules — no pipeline runs, real
delivery — to confirm a channel is wired correctly:

```bash
faucet notify test pipeline.yaml --event run_failure
```

`--event` accepts any event kind (`run_failure`, `run_success`, `sla_breach`,
`circuit_open`, `contract_abort`, `dlq_threshold`, `scheduler_stuck`).

## Metrics

| Metric | Labels | Meaning |
|--------|--------|---------|
| `faucet_notifications_sent_total` | `channel`, `event`, `outcome` | Deliveries attempted (`outcome` = `ok`/`error`). |
| `faucet_notifications_dropped_total` | `channel`, `reason` | Not delivered (`reason` = `coalesced`/`channel_error`). |
| `faucet_notification_dispatch_duration_seconds` | `channel` | Per-delivery latency. |

## Relationship to Prometheus alerting

This block is a **self-contained** notifier — it needs no external monitoring
stack. It is complementary to shipping Prometheus alert rules against faucet's
metrics: use notifications for immediate, per-run incident routing, and
Prometheus/Alertmanager for threshold- and duration-based alerting across your
fleet.
