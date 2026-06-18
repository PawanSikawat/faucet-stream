# Resilience: retry, circuit breaker, and poison-pill

The top-level `resilience:` block gives a pipeline one declarative place to say
how it should behave under transient and persistent failure. It is **fully
opt-in**: with no `resilience:` block a pipeline behaves exactly as before — no
sink-write retry, and source connectors keep their built-in retry defaults.

```yaml
resilience:
  retry:
    max_attempts: 5            # total tries including the first (1 = no retry)
    backoff: exponential       # none | fixed | exponential
    base_ms: 200
    max_ms: 30000              # per-sleep cap, before jitter
    jitter: true
  retry_on: [http_5xx, rate_limited, connection, timeout]
  circuit_breaker:
    consecutive_failures: 5
    cooldown_secs: 60
  poison:
    max_row_attempts: 3
    action: dlq                # dlq | drop | fail
```

A runnable example lives at `cli/examples/rest_to_jsonl_resilient.yaml`.

## What the policy wraps

The policy is applied at two layers:

- **Sink side (the pipeline loop):** every sink write (`write_batch` /
  `write_batch_partial` / `write_batch_idempotent`), `flush`, and state-store
  `put` is wrapped with retry + the circuit breaker. This covers the default,
  exactly-once, and DLQ write paths.
- **Source side (the connector):** the `retry` policy is injected into the
  connectors that retry their own requests (`rest`, `xml`, `graphql`), replacing
  their ad-hoc retry settings with one shared configuration.

> The pipeline cannot retry a *source page-poll* itself — once a streaming
> source yields an error mid-stream, the page cannot be replayed by re-polling.
> Source-side retry therefore lives inside the connector, governed by the same
> `retry` policy.

## `retry`

| Field | Default | Meaning |
|-------|---------|---------|
| `max_attempts` | `5` | Total attempts including the first. `1` disables retry. |
| `backoff` | `exponential` | `none` (no delay), `fixed` (constant `base_ms`), or `exponential` (`base_ms * 2^attempt`). |
| `base_ms` | `200` | Base delay. |
| `max_ms` | `30000` | Per-sleep cap (before jitter). |
| `jitter` | `true` | Apply `[0.5, 1.5)` decorrelated jitter to each sleep. |

### `retry_on`

The set of transient error classes that are retried. Anything not in the set
(and anything that doesn't classify as transient — auth errors, config errors,
JSON parse errors, 4xx other than 429) fails fast and is **never** retried.

| Class | Matches |
|-------|---------|
| `http_5xx` | HTTP 5xx server errors |
| `rate_limited` | HTTP 429 / rate-limit signals |
| `connection` | connection-level failures (DNS, refused, reset) |
| `timeout` | request timeouts |

Default (omit `retry_on`) = all four. An empty list is rejected at config load.

## `circuit_breaker`

Counts **consecutive fully-failed pages** (a page whose write ultimately failed
after retries). A page with any success resets the counter. When the count
reaches `consecutive_failures`, the run **fails fast** with a `CircuitOpen`
error rather than continuing.

This only changes behavior on the DLQ / poison path — without a DLQ the first
exhausted-retry write already aborts the run. Its real job is to stop a wedged
destination from silently draining the entire source into the dead-letter queue.

`cooldown_secs` is **advisory for the orchestration layer**: when a
[`faucet schedule`](../reference/cli.md) run fails with `CircuitOpen`, the
scheduler waits at least `cooldown_secs` before the next tick. A one-shot
`faucet run` simply exits non-zero; `faucet serve` records the run as failed
(no automatic re-run).

> The cooldown only delays the scheduler's next cron-tick re-entry. An overlap
> run that is **already queued** (`overlap: queue`) starts immediately when the
> active run finishes — it is not delayed by the cooldown.

## `poison`

Per-row handling for the DLQ path. When `write_batch_partial` reports individual
row failures, the still-failing, retriable rows are re-submitted up to
`max_row_attempts` times before the terminal `action` is applied:

| `action` | Effect |
|----------|--------|
| `dlq` | Route the row to the DLQ (the default). **Requires a `dlq:` block** — validated at config load. |
| `drop` | Discard the row (counted; logged once per run). |
| `fail` | Propagate the row error and abort the run. |

## Composition

- **Exactly-once delivery** — retry wraps `write_batch_idempotent`; a retried
  idempotent write is safe because the commit token makes it idempotent.
- **Adaptive batch sizing** — retry wraps each adaptive chunk; the breaker
  counts page-level failures.
- **Cancellation** — a backoff sleep is abandoned immediately on a shutdown /
  timeout cancel, so the policy never wedges a graceful drain.

## REST precedence

The `rest` source predates this unified policy and has its own `max_retries` /
`retry_backoff` config fields. When you leave both at their defaults
(`max_retries: 3`, `retry_backoff: 1s`), the pipeline `resilience.retry` policy
governs the REST source. If you set either field explicitly, the per-connector
value wins — an explicit setting is never silently overridden by a pipeline-wide
default. (Because REST keeps its own 429/`Retry-After`-aware runner, only the
policy's `max_attempts` and `base` apply to REST; `retry_on`/`max`/`jitter` are
honored on the `xml`/`graphql` sources and on every sink-side write.)

## Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `faucet_resilience_retries_total` | counter | `pipeline, row, op, class` |
| `faucet_resilience_retry_sleep_seconds` | histogram | `pipeline, row, op` |
| `faucet_resilience_giveup_total` | counter | `pipeline, row, op` |
| `faucet_resilience_circuit_state` | gauge (0/1) | `pipeline, row` |
| `faucet_resilience_circuit_opened_total` | counter | `pipeline, row` |
| `faucet_resilience_poison_rows_total` | counter | `pipeline, row, action` |

`op` is one of `sink_write`, `flush`, `state_put`. Source-connector retries are
observable through the connector's existing `faucet_source_errors_total` and
tracing output rather than these metrics.

## Inspecting the schema

```bash
faucet schema resilience
```
