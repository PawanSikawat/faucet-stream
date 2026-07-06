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

Each dead-lettered record is wrapped in a fixed-shape envelope — the original
record plus the metadata needed to inspect, fix, and replay it:

```json
{
  "error": { "kind": "ContractViolation", "message": "status: value not in enum" },
  "reason": "contract",
  "payload": { "order_id": "A-17", "status": "backordered" },
  "ts_ms": 1751760000000,
  "sink": "jsonl",
  "pipeline": "orders_csv_with_contract",
  "row": "",
  "record_index": 3
}
```

- `payload` — the original record, verbatim. This is what a replay re-feeds.
- `reason` — which stage quarantined the row: `quality`, `contract`,
  `schema_drift`, or `partial` / `dlq_all` for a sink-side row failure. This is
  the value the `--reason` filter matches.
- `error.kind` / `error.message` — the typed failure and its message.
- `record_index` — the row's position within its original page.

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

## Inspecting the DLQ

`faucet dlq inspect` reads a DLQ location back and groups it by reason and error
kind, with a sample — so you can see *why* rows failed before deciding what to do:

```console
$ faucet dlq inspect ./dlq/contract_breaches.jsonl
DLQ inspect: ./dlq/contract_breaches.jsonl
  files read: 1   envelopes: 42   malformed: 0   non-envelope: 0
  by reason:
    contract       42
  by error kind:
    ContractViolation    42
  sample (5 of 42):
    [contract/ContractViolation] status: value not in enum
      {"order_id":"A-17","status":"backordered"}
```

The location may be a single `.jsonl` file, a directory of `*.jsonl` files, or a
glob. Blank, malformed, and non-envelope lines are counted (`malformed` /
`non-envelope`) but never abort the read. Add `--reason contract` to restrict the
breakdown, `--limit N` to size the sample, or `--json` for a machine-readable
summary.

## Replaying

Once you've fixed the root cause — a transform, a contract, the destination
schema — `faucet dlq replay` re-feeds the quarantined **payloads** through a
pipeline config (transforms → quality → contract → sink), exactly as a normal
run:

```console
$ faucet dlq replay orders.yaml --from ./dlq/contract_breaches.jsonl --dry-run
DLQ replay (dry-run): 42 candidate record(s) from ./dlq/contract_breaches.jsonl
would be re-fed; 42 would reach the sink. Failures would go to
./dlq/contract_breaches.replay-failed.jsonl.

$ faucet dlq replay orders.yaml --from ./dlq/contract_breaches.jsonl
DLQ replay: 42 candidate record(s) re-fed; 42 written to the sink. …
```

Rows that fail **again** on replay are quarantined to a *fresh* DLQ — a
`replay-failed.jsonl` sibling of the source by default (override with
`--failed-dlq`) — never back to the source, so a replay can't loop. `--dry-run`
reports what would be replayed without writing; `--reason` replays only matching
envelopes; `--row` picks a specific root when the config has several.

> **Make replay idempotent.** A replay is a fresh run — if some of a page
> originally landed before the failure, replaying can duplicate it on an
> append-only sink. Use [`write_mode: upsert`](./upsert.md) on the target so a
> replayed row overwrites rather than duplicates.

## Discarding

Once envelopes are handled (replayed, or known-bad), `faucet dlq discard` clears
them so the DLQ doesn't grow unbounded:

```console
$ faucet dlq discard ./dlq/contract_breaches.jsonl --reason contract --before 7d
DLQ discard: archived 42 envelope(s) across 1 file(s) → ./dlq/contract_breaches.archived.jsonl
```

By default discarded envelopes are moved to a `<file>.archived.jsonl` sibling;
`--delete` removes them outright. `--reason` and `--before` (an RFC3339 timestamp
or a relative age like `7d` / `24h` / `30m`) select what to discard — everything
else, including non-envelope lines, is left untouched.

> The full design is in
> [`docs/superpowers/specs/2026-05-24-dlq-design.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/docs)
> and the `faucet_core::dlq` module on docs.rs.
