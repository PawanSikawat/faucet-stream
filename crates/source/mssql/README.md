# faucet-source-mssql

Microsoft SQL Server query source for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem. Runs
parameterized SQL, streams rows as JSON, and supports incremental replication
via a tracking column. Built on [`tiberius`](https://crates.io/crates/tiberius)
+ [`bb8-tiberius`](https://crates.io/crates/bb8-tiberius).

## Config

```yaml
source:
  type: mssql
  config:
    # connection_url OR connection_string (exactly one)
    connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
    # connection_string: "Server=tcp:localhost,1433;Database=sales;User Id=sa;Password=...;"

    query: "SELECT id, email, updated_at FROM dbo.users WHERE updated_at > @bookmark"

    params: []                 # positional @P1, @P2, … bind params
    max_connections: 10
    batch_size: 1000           # rows per emitted page; 0 = whole result set in one page
    statement_timeout_secs: 300  # 0 disables

    tls:
      type: prefer             # prefer | require | trust_server_certificate | disable
      ca_cert_path: null

    replication:
      type: incremental        # full | incremental
      column: updated_at
      initial_value: "1970-01-01T00:00:00Z"
```

URL credentials with special characters must be percent-encoded
(`@`→`%40`, `:`→`%3A`, `/`→`%2F`). See
[`faucet-mssql-common`](https://crates.io/crates/faucet-mssql-common) for the
full connection / TLS reference.

## Incremental replication

Set `replication.type: incremental` with a `column` (the cursor) and an
`initial_value` (the lower bound on the first run). Across runs the source emits
only rows whose `column` is strictly greater than the stored bookmark, and
persists the new maximum on the final page.

- **Server-side pushdown:** put the literal token `@bookmark` in your `WHERE`
  clause (e.g. `WHERE updated_at > @bookmark`). The source binds the cursor
  there so SQL Server does the filtering.
- **Backstop:** whether or not you use `@bookmark`, the source *also* filters
  client-side, so correctness never depends on the query text. (Without
  `@bookmark` the full result set is fetched and filtered in memory — fine for
  small tables, but prefer `@bookmark` for large ones.)

`@bookmark` is a reserved token; don't use it as an identifier.

## Type mapping

| MSSQL | JSON |
|-------|------|
| TINYINT / SMALLINT / INT / BIGINT | number |
| REAL / FLOAT | number |
| BIT | bool |
| DECIMAL / NUMERIC / MONEY | string (precision-preserving) |
| CHAR / VARCHAR / NCHAR / NVARCHAR / TEXT / NTEXT / XML | string |
| DATE | `YYYY-MM-DD` |
| TIME | ISO time |
| DATETIME / DATETIME2 / SMALLDATETIME | ISO 8601 (no offset) |
| DATETIMEOFFSET | RFC 3339 (offset preserved) |
| UNIQUEIDENTIFIER | hyphenated string |
| BINARY / VARBINARY / IMAGE | base64 string |
| SQL NULL | `null` |

## Auth

SQL Server authentication (username + password) only in v1. Windows / Integrated
authentication and Azure AD / Managed Identity are out of scope.

## Testing

Unit tests run with `cargo test -p faucet-source-mssql --lib`. The integration
tests (`--test integration`) require Docker (the `mcr.microsoft.com/mssql/server`
image).

## License

MIT OR Apache-2.0.
