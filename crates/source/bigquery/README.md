# faucet-source-bigquery

BigQuery query source connector for the [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem. Runs a SQL statement against [BigQuery](https://cloud.google.com/bigquery/docs/reference/rest) via `jobs.query` + `jobs.getQueryResults` and streams the rows back as `serde_json::Value` records.

## Features

- **Three credential modes** — `ApplicationDefault`, `ServiceAccountKeyPath`, or inline `ServiceAccountKey`; the shared `BigQueryCredentials` enum is re-exported from [`faucet-bigquery-common`](https://crates.io/crates/faucet-bigquery-common) so it matches the BigQuery sink byte-for-byte.
- **Sync + async result handling** — when BigQuery returns `jobComplete=false` (statement timeout exceeded), the source polls `getQueryResults` until the job finishes, bounded by `poll_timeout` (default 300 s; `0` = poll forever) so a job that never completes fails instead of hanging.
- **Server-side pagination** — pages via `pageToken` for arbitrarily-large result sets; rows are re-framed into pages of `batch_size` for `Source::stream_pages`.
- **Type-aware row decoding** — `INTEGER`/`INT64` → JSON number, `FLOAT`/`FLOAT64` → number, `BOOLEAN`/`BOOL` → bool, `NUMERIC`/`BIGNUMERIC` → string (full precision), `RECORD`/`STRUCT` → nested object, `REPEATED` → array, `JSON` → parsed value, everything else → string.
- **Positional bind parameters** — `params` from config and `${parent.path}` matrix-context values are sent as `POSITIONAL` parameters whose type is inferred from the JSON value (`INT64` / `FLOAT64` / `BOOL` / `STRING`), so a numeric or boolean bind compares correctly against a typed column.
- **Standard or legacy SQL** — `use_legacy_sql` toggle; defaults to Standard SQL.

## Configuration

```yaml
type: bigquery
config:
  project_id: my-project
  credentials:
    type: service_account_key_path
    config:
      path: /etc/secrets/bigquery-sa.json
  query: |
    SELECT user_id, event_name, occurred_at
    FROM `my-project.analytics.events`
    WHERE occurred_at >= ?
  params:
    - "2026-01-01"
  use_legacy_sql: false            # default
  location: EU                     # optional; matches BigQuery dataset region
  max_results_per_page: 1000       # default
  statement_timeout: 60            # seconds, defaults to 60
  poll_timeout: 300                # seconds, defaults to 300; 0 = poll forever
  batch_size: 1000                 # default, or 0 to disable re-chunking
```

### Other credential variants

```yaml
# Application Default Credentials (workload identity, gcloud auth).
credentials:
  type: application_default
```

```yaml
# Inline service-account JSON — handy with env-var indirection.
credentials:
  type: service_account_key
  config:
    json: ${env:GCP_SERVICE_ACCOUNT_JSON}
```

## Library use

```rust
use faucet_core::Source;
use faucet_source_bigquery::{BigQueryCredentials, BigQuerySource, BigQuerySourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = BigQuerySourceConfig::new(
    "my-project",
    BigQueryCredentials::ApplicationDefault,
    "SELECT COUNT(*) AS n FROM analytics.events",
);
let rows = BigQuerySource::new(cfg).await?.fetch_all().await?;
println!("rows: {rows:?}");
# Ok(())
# }
```

## License

MIT OR Apache-2.0
