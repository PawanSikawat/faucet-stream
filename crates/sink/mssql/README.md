# faucet-sink-mssql

Microsoft SQL Server sink for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem. Inserts
records via parameterized multi-row `INSERT`s with auto-mapped columns or a
single JSON column. Built on [`tiberius`](https://crates.io/crates/tiberius) +
[`bb8-tiberius`](https://crates.io/crates/bb8-tiberius).

## Config

```yaml
sink:
  type: mssql
  config:
    connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
    # connection_string: "Server=tcp:localhost,1433;Database=sales;User Id=sa;Password=...;"

    table: "dbo.events"

    column_mapping:
      type: auto_columns          # auto_columns | json_column
      on_unknown_field: warn      # warn | drop | error  (auto_columns only)
    # column_mapping: { type: json_column, column: "payload" }

    batch_size: 500               # rows per multi-row INSERT (auto-split at the 2100-param limit)
    max_connections: 5
    transaction_per_batch: true   # wrap each batch in BEGIN/COMMIT TRAN
    isolate_row_failures: true    # retry row-by-row on batch failure so only the bad row is DLQ'd
    statement_timeout_secs: 300   # 0 disables
    create_table: false           # json_column only: create (id IDENTITY, <column> NVARCHAR(MAX)) if absent

    tls:
      type: prefer                # prefer | require | trust_server_certificate | disable
      ca_cert_path: null
```

See [`faucet-mssql-common`](https://crates.io/crates/faucet-mssql-common) for the
full connection / TLS reference.

## Write modes

- **`auto_columns`** — top-level JSON keys map to same-named table columns. The
  column set is the **union** of keys across the batch (a field present only in
  a later record is still written; rows missing a column → SQL `NULL`).
  `IDENTITY` columns are skipped automatically (the server generates them) — do
  **not** put identity values in your records unless you've set
  `SET IDENTITY_INSERT <table> ON` yourself. Keys with no matching column are
  handled by `on_unknown_field` (`warn` / `drop` / `error`).
- **`json_column`** — each record is serialized to JSON and inserted into a
  single `NVARCHAR(MAX)` (or native Azure SQL `JSON`) column. Schema-agnostic.
  `create_table: true` will create the table if absent.

`auto_columns` + `create_table` is rejected — schema inference for MSSQL types is
unsafe, so create the table yourself first.

## Batching, transactions, and MSSQL's statement limits

MSSQL enforces two caps on a multi-row `INSERT`: at most **2100 parameters** per
request (and `tiberius` spends 2 of them on its `sp_executesql` wrapper, so the
usable budget is 2098), and at most **1000 row expressions** in a `VALUES`
clause. The sink auto-splits a batch into multiple `INSERT` statements that stay
within *both* limits — `min(2098 / columns, 1000)` rows each — all within one
transaction when `transaction_per_batch`. `MERGE`/`UPSERT` and bulk-copy (`BCP`)
are out of scope for v1 — this is an append-only sink.

## Partial failures (DLQ)

With `isolate_row_failures: true` (default) a batch that fails is rolled back and
retried one row at a time: the good rows land and only the offending row is
returned as an error for dead-letter routing. Transient errors (deadlock,
lock-timeout, connection drops) are retried with backoff and otherwise
propagated so the pipeline's `on_batch_error` policy decides. Set
`isolate_row_failures: false` to fail the whole batch on the first bad row
(fewer round-trips).

## Security

`tls.type: trust_server_certificate` and `TrustServerCertificate=true` in a
connection string disable certificate-chain validation — they accept **any**
server certificate, which is vulnerable to man-in-the-middle attacks. Use them
only against trusted dev servers. In production use `prefer`/`require` with a
proper certificate, or pin a CA via `tls.ca_cert_path`. Never hard-code
credentials — supply `connection_url` / `connection_string` via secrets-manager
interpolation or environment variables.

## Auth

SQL Server authentication (username + password) only in v1. Windows / Integrated
authentication and Azure AD / Managed Identity are out of scope.

## Testing

Unit tests run with `cargo test -p faucet-sink-mssql --lib`. The integration
tests (`--test integration`) require Docker.

## License

MIT OR Apache-2.0.
