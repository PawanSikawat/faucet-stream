# Dead-letter queues

A dead-letter queue (DLQ) keeps a pipeline running when a handful of records
fail to write, instead of aborting the whole run. Failing rows are wrapped in a
fixed-shape envelope and routed to a separate DLQ sink before the page's bookmark
advances.

## When it helps

Sinks whose underlying API reports **per-row** results — BigQuery `insertAll`,
Elasticsearch `_bulk` — can tell exactly which records failed. The DLQ captures
just those, while the good rows commit normally.

## Configure a DLQ

Add a `dlq:` block naming a sink to receive the bad rows and the policy for
sinks that can't report per-row outcomes:

```yaml
pipeline:
  source: { type: rest, config: { /* … */ } }
  sink:   { type: bigquery, config: { /* … */ } }
  dlq:
    on_batch_error: dlq_all      # or `propagate`
    sink:
      type: jsonl
      config:
        path: ./dead-letters.jsonl
```

## The envelope

Each dead-lettered record is wrapped with metadata — the original record, the
reason it failed, and context — so you can inspect, fix, and replay it later.

## `on_batch_error` policy

For a sink that can only succeed or fail a whole batch (no per-row detail):

- `propagate` — a batch failure aborts the run (the default, fail-fast behavior).
- `dlq_all` — route every row in the failed batch to the DLQ and keep going.

Sinks that *do* report per-row results (BigQuery, Elasticsearch, and the HTTP
sink in `Individual` mode) override the partial-write path so only the genuinely
failed rows are dead-lettered — the already-delivered rows are not duplicated
into the DLQ.

## Failure budgets

A DLQ keeps a run going through *occasional* bad rows, but a flood of failures
usually means something is broken upstream. Two optional budgets turn the DLQ
into a circuit breaker:

```yaml
  dlq:
    sink: { type: jsonl, config: { path: ./dead-letters.jsonl } }
    max_failures_per_page: 50    # abort if a single page dead-letters > 50 rows
    max_failures_total: 500      # abort once the run has dead-lettered > 500 rows
```

When a budget trips, the run aborts — but only **after the page that crossed the
threshold is fully committed**: its surviving rows are written to the main sink,
its failed rows are routed to the DLQ, and (if the page carried one) the bookmark
advances. So the committed survivors are *not* re-delivered when you fix the
upstream problem and re-run, and the failed rows are preserved in the DLQ for
replay rather than dropped. The run still stops, so you get alerted.

> The full design is in
> [`docs/superpowers/specs/2026-05-24-dlq-design.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/docs)
> and the `faucet_core::dlq` module on docs.rs.
