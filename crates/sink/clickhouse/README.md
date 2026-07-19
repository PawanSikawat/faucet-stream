# faucet-sink-clickhouse

ClickHouse **sink** for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Writes each page over the ClickHouse
[HTTP interface](https://clickhouse.com/docs/en/interfaces/http) as a batched
`INSERT … FORMAT JSONEachRow` request (the statement travels in the `query` URL
parameter, the newline-delimited JSON rows in the request body). Records are
type-round-tripped through `JSONEachRow`, which maps cleanly onto ClickHouse
column types.

## Configuration

```yaml
sink:
  kind: clickhouse
  config:
    # Endpoint — either `url` OR `host` (+ optional `http_port` / `tls`).
    url: http://localhost:8123
    # host: localhost
    # http_port: 8123
    # tls: false
    database: default
    user: default          # optional; sent as X-ClickHouse-User
    password: ${env:CH_PASSWORD}   # optional; sent as X-ClickHouse-Key
    table: events          # may be schema-qualified, e.g. analytics.events
    batch_size: 1000       # records per INSERT; 0 = whole page in one request
    async_insert: false    # true → server-side async buffering (async_insert=1)
    wait_for_async_insert: true  # wait for the flush ack (keeps at-least-once durability)
```

The `table` (including a `db.table` qualifier) is identifier-quoted before use,
so it is safe against injection.

### Authentication

Username + password (ClickHouse native HTTP auth), sent as the
`X-ClickHouse-User` / `X-ClickHouse-Key` headers.

### Asynchronous inserts

Set `async_insert: true` to enable ClickHouse
[asynchronous inserts](https://clickhouse.com/docs/en/optimize/asynchronous-inserts),
where the server buffers rows and flushes them in the background — a large
throughput win for many small inserts. Keep `wait_for_async_insert: true` (the
default) so the request is only acknowledged once the batch is durably accepted,
preserving faucet's at-least-once contract (the bookmark advances only after the
write is confirmed).

## Write modes

The sink is **append-only**. ClickHouse upsert semantics are engine-dependent —
use a [`ReplacingMergeTree`](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
(or `CollapsingMergeTree` / `AggregatingMergeTree`) table to deduplicate by key
at merge time. The sink never emulates upsert, so `write_mode` must be `append`.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
