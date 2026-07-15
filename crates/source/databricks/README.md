# faucet-source-databricks

Databricks **SQL query source** for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
ecosystem. Runs a SQL statement against a Databricks **SQL Warehouse** via the
[Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
(plain REST — no JDBC/ODBC driver, no Python) and streams the result rows as
typed JSON objects.

This is the **query-results** read path for Databricks — joins, aggregates,
filtered extracts. For full-table lakehouse scans and the **write** path, use
the Delta Lake connectors ([`faucet-source-delta`](https://crates.io/crates/faucet-source-delta)
/ [`faucet-sink-delta`](https://crates.io/crates/faucet-sink-delta)); a warehouse
`INSERT`/`MERGE` sink is intentionally not provided (slow, INSERT-bound, and
forces billed compute).

## Highlights

- **Async statement lifecycle** — submit → poll until terminal → stream result
  chunks (INLINE + `JSON_ARRAY`), following `next_chunk_internal_link`.
- **Type-aware decode** from the response `manifest` column schema (every
  `JSON_ARRAY` cell is a string; decoded per `type_name` to typed JSON, with
  `DECIMAL`/large `LONG` preserved losslessly as strings).
- **Incremental replication** — a bookmark column + a `${bookmark}` token bound
  as a server-side named parameter, plus a client-side filter backstop.
- **Bearer auth** (PAT / OAuth M2M), inline or via the shared `auth:` catalog.

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `workspace_url` | string | — (required) | `https://<host>.cloud.databricks.com` |
| `warehouse_id` | string | — (required) | target SQL Warehouse id |
| `sql` | string | — (required) | the query; supports `:name` params and a `${bookmark}` token |
| `auth` | `{ type, config }` / `{ ref }` | — (required) | `pat` or `token` bearer, or a shared provider |
| `catalog` / `schema` | string? | — | default Unity Catalog catalog / schema |
| `parameters` | `[{name, value, type?}]` | `[]` | named `:name` SQL parameters |
| `wait_timeout_secs` | int | `50` | server wait before async (`0` or `5`–`50`) |
| `poll_interval_secs` | int | `1` | client poll cadence while running |
| `batch_size` | int | `1000` | rows per emitted page |
| `replication` | `{ type: full \| incremental, column, initial_value }` | `full` | incremental cursor |
| `state_key` | string? | derived | explicit bookmark key |

```yaml
pipeline:
  source:
    type: databricks
    config:
      workspace_url: https://dbc-xxxx.cloud.databricks.com
      warehouse_id: 0123456789abcdef
      sql: SELECT id, ts FROM events WHERE ts > ${bookmark}
      auth: { type: pat, config: { token: "${env:DATABRICKS_TOKEN}" } }
      replication: { type: incremental, column: ts, initial_value: "2026-01-01" }
```

## Out of scope (v1)

`EXTERNAL_LINKS` large-result disposition, `discover()` via `INFORMATION_SCHEMA`,
Unity Catalog Volumes, and a Databricks SQL sink (use the Delta sink).

License: MIT OR Apache-2.0.
