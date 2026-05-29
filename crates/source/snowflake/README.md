# faucet-source-snowflake

Snowflake query source connector for the [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem. Executes a SQL statement against Snowflake via the [SQL REST API](https://docs.snowflake.com/en/developer-guide/sql-api/intro) and streams rows back as `serde_json::Value` records.

## Features

- **JWT key-pair and OAuth bearer authentication** — `SnowflakeAuth` re-exported from [`faucet-snowflake-common`](https://crates.io/crates/faucet-snowflake-common); credentials are masked in `Debug` output.
- **Server-side partitioning** — large result sets are paged via `GET /api/v2/statements/{handle}?partition=N`; one HTTP round-trip per partition, no client-side buffering of unrelated partitions.
- **Configurable batching** — `batch_size` re-frames partitions into pages for `Source::stream_pages`; `batch_size = 0` opts out of re-chunking and emits the full result set as one page.
- **Async / sync handling** — if Snowflake returns `202 Accepted` (statement still running), the source polls the handle until the result is ready, bounded by `poll_timeout` (default 300 s; `0` = poll forever) so a stuck statement fails instead of hanging.
- **Bind parameters** — positional `params` from config are merged with `${parent.path}` tokens resolved from the matrix-row context. Each bind's Snowflake type is inferred from the JSON value (`FIXED` for integers, `REAL` for floats, `BOOLEAN` for bools, `TEXT` otherwise), so numeric/boolean binds compare against typed columns instead of being forced to `TEXT`.
- **Type-aware row conversion** — `FIXED`, `REAL`, `BOOLEAN`, and `VARIANT`/`OBJECT`/`ARRAY` columns are parsed into native JSON shapes; everything else (timestamps, dates, binary) passes through as strings.

## Configuration

```yaml
type: snowflake
config:
  account: xy12345.us-east-1
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema: PUBLIC
  role: ANALYST                     # optional
  auth:
    type: oauth
    config:
      token: ${env:SNOWFLAKE_OAUTH_TOKEN}
  query: |
    SELECT id, name, created_at
    FROM events
    WHERE created_at >= ?
  params:
    - "2026-01-01"
  statement_timeout: 60             # seconds, defaults to 60
  poll_timeout: 300                 # seconds, defaults to 300; 0 = poll forever
  batch_size: 1000                  # default, or 0 to disable re-chunking
```

### Key-pair authentication

```yaml
auth:
  type: key_pair
  config:
    user: SVC_LOAD
    private_key_pem: ${file:/etc/secrets/snowflake.pem}
```

The source generates an RS256 JWT per request (1-hour expiry) signed with the configured PEM key and sends it in the `Authorization: Bearer ...` header along with `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`.

## Library use

```rust
use faucet_core::Source;
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = SnowflakeSourceConfig::new(
    "xy12345.us-east-1",
    "COMPUTE_WH",
    "ANALYTICS",
    "PUBLIC",
    SnowflakeAuth::OAuth { token: std::env::var("SNOWFLAKE_OAUTH_TOKEN")? },
    "SELECT id, name FROM events LIMIT 10",
);
let rows = SnowflakeSource::new(cfg).fetch_all().await?;
println!("got {} rows", rows.len());
# Ok(())
# }
```

## License

MIT OR Apache-2.0
