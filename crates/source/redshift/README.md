# faucet-source-redshift

Amazon Redshift query source connector for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Redshift speaks the PostgreSQL wire protocol, so this source connects through
`sqlx`'s Postgres driver, runs a configurable SQL query, and streams the result
rows as JSON objects with `O(batch_size)` memory. It supports **full** and
**incremental** (bookmark-based) replication.

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `host` | yes | Cluster endpoint host. |
| `port` | no | Port (default `5439`). |
| `database` | yes | Database name. |
| `user` | yes | Database user. |
| `credentials` | yes | `{ type: password, config: { password: … } }`. `iam` / `redshift_data_api` are reserved (not yet supported). |
| `tls` | no | Require TLS (default `true`; `false` → `sslmode=prefer`). |
| `query` | yes | SQL query. May contain `${field.path}` context tokens and, for incremental mode, `${bookmark}`. |
| `params` | no | Positional bind values (`$1, $2, …`) applied before context/bookmark values. |
| `max_connections` | no | Pool size (default `10`). |
| `batch_size` | no | Rows per emitted page (default `1000`; `0` = one page). |
| `replication` | no | `{ type: full }` (default) or `{ type: incremental, column, initial_value }`. |
| `state_key` | no | Explicit bookmark key; otherwise derived from host/database/query. |

## Incremental replication

With `replication.type = incremental`, only rows whose `column` is strictly
greater than the stored bookmark (or `initial_value` on the first run) are
emitted. If the query contains the literal `${bookmark}` token it is replaced
with a positional bind so Redshift filters server-side; the source also filters
client-side as a correctness backstop. The new maximum of `column` is persisted
on the final page.

```yaml
host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
database: dev
user: admin
credentials:
  type: password
  config:
    password: ${env:REDSHIFT_PASSWORD}
query: "SELECT * FROM events WHERE updated_at > ${bookmark} ORDER BY updated_at"
replication:
  type: incremental
  column: updated_at
  initial_value: "2026-01-01T00:00:00Z"
```

## Testing

Redshift has no local container image, so live round-trip tests live in
`tests/integration.rs` and are `#[ignore]`d — they run only when `REDSHIFT_*`
environment variables point at a real cluster.

License: MIT OR Apache-2.0
