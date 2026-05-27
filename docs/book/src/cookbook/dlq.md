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

Sinks that *do* report per-row results (BigQuery, Elasticsearch) override the
partial-write path so only the genuinely failed rows are dead-lettered — they are
not duplicated into the DLQ.

> The full design is in
> [`docs/superpowers/specs/2026-05-24-dlq-design.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/docs)
> and the `faucet_core::dlq` module on docs.rs.
