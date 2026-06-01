# faucet-sink-bigquery

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-bigquery.svg)](https://crates.io/crates/faucet-sink-bigquery)
[![Docs.rs](https://docs.rs/faucet-sink-bigquery/badge.svg)](https://docs.rs/faucet-sink-bigquery)

Google BigQuery streaming insert sink for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a BigQuery table using the `tabledata.insertAll` streaming API. Records are automatically split into configurable batch sizes to stay within BigQuery API limits. The BigQuery client is authenticated once at construction and reused across all writes.

`write_batch` accepts whatever slice the pipeline hands it. When `batch_size > 0` and the slice is larger than `batch_size`, the sink re-chunks internally and issues one `insertAll` call per chunk; when `batch_size = 0`, the entire slice is sent in a single request — see [Streaming and batching](#streaming-and-batching) for the tradeoffs.

## Installation

```toml
[dependencies]
faucet-sink-bigquery = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "1.0", features = ["sink-bigquery"] }
```

## Quick Start

```rust
use faucet_sink_bigquery::{BigQuerySink, BigQuerySinkConfig, BigQueryCredentials};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BigQuerySinkConfig::new(
        "my-gcp-project",
        "analytics",
        "events",
        BigQueryCredentials::ServiceAccountKeyPath("/path/to/service-account.json".into()),
    )
    .with_batch_size(500);

    let sink = BigQuerySink::new(config).await?;

    let records = vec![
        json!({"user_id": "u123", "event": "page_view", "timestamp": "2026-04-02T10:00:00Z"}),
        json!({"user_id": "u456", "event": "click", "timestamp": "2026-04-02T10:01:00Z"}),
    ];

    let rows_written = sink.write_batch(&records).await?;
    println!("Wrote {rows_written} rows to BigQuery");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_id` | `String` | *(required)* | GCP project ID |
| `dataset_id` | `String` | *(required)* | BigQuery dataset ID |
| `table_id` | `String` | *(required)* | BigQuery table ID |
| `credentials` | `BigQueryCredentials` | *(required)* | Authentication credentials (see below) |
| `batch_size` | `usize` | `1000` | Maximum rows per `insertAll` request. See [Streaming and batching](#streaming-and-batching) below |
| `insert_id_field` | `Option<String>` | `None` | Record field whose value is sent as the per-row streaming `insertId`. See [Retry de-duplication](#retry-de-duplication) below |

### Retry de-duplication

BigQuery streaming inserts are **at-least-once** — a transport retry can
insert the same row twice. Setting `insert_id_field` to a stable per-row
key (e.g. a primary key or event id) makes the sink send that value as the
row's `insertId`, which BigQuery uses for best-effort de-duplication over a
short window. When `insert_id_field` is unset, or a given row lacks the
field, that row is inserted without an `insertId` (no dedup for it).

### Streaming and batching

The BigQuery sink re-chunks each incoming `StreamPage` to keep individual
`tabledata.insertAll` calls under BigQuery's limits.

- **`batch_size > 0`** (default `1000`) — the sink slices the incoming slice
  into `batch_size`-row chunks and issues one `insertAll` HTTP call per
  chunk. **Recommended value is `500`**: that's the documented sweet spot
  for BigQuery streaming inserts (small enough to stay well under the
  ~10MB request limit even for wide rows, large enough to amortise per-call
  overhead). Bump it higher when rows are narrow; drop it when rows are
  wide enough to push individual chunks past 10MB.
- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream
  `StreamPage` is forwarded in a single `insertAll` call. Use this when the
  source already emits page sizes tuned for BigQuery — for example a
  Postgres source configured with `batch_size: 500`. Larger pages risk
  HTTP-413 from BigQuery's ~10MB body limit.

`batch_size` is purely a chunk-size knob — BigQuery's per-row error
reporting and the sink's "first row that failed" error message are
unchanged. Per-call retry is delegated to `gcp_bigquery_client`'s built-in
retry middleware.

### Authentication (`BigQueryCredentials`)

| Variant | Description |
|---------|-------------|
| `ServiceAccountKeyPath(String)` | Path to a service account JSON key file on disk |
| `ServiceAccountKey(String)` | Inline service account JSON key content as a string |
| `ApplicationDefault` | Use application default credentials (workload identity, `gcloud auth application-default login`, etc.) |

The `ServiceAccountKey` variant is useful in environments where the key is injected as an environment variable rather than stored on disk (e.g. Kubernetes secrets, CI/CD).

The `Debug` implementation masks the inline key content with `***` to prevent accidental credential leakage in logs.

### Builder Methods

```rust
use faucet_sink_bigquery::{BigQuerySinkConfig, BigQueryCredentials};

let config = BigQuerySinkConfig::new(
    "my-project",
    "my_dataset",
    "my_table",
    BigQueryCredentials::ApplicationDefault,
)
.with_batch_size(500);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_bigquery::BigQuerySinkConfig;

// From a JSON file
let config: BigQuerySinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: BigQuerySinkConfig = load_env_file(".env", "BIGQUERY")?;
```

### Example JSON config

```json
{
  "project_id": "my-gcp-project",
  "dataset_id": "analytics",
  "table_id": "events",
  "auth": {
    "type": "service_account_key_path",
    "config": { "path": "/etc/secrets/bigquery-sa.json" }
  },
  "batch_size": 1000
}
```

Using application default credentials:

```json
{
  "project_id": "my-gcp-project",
  "dataset_id": "analytics",
  "table_id": "events",
  "auth": {
    "type": "application_default"
  },
  "batch_size": 1000
}
```

### Example .env file

```env
BIGQUERY_PROJECT_ID=my-gcp-project
BIGQUERY_DATASET_ID=analytics
BIGQUERY_TABLE_ID=events
BIGQUERY_CREDENTIALS='{"type":"service_account_key_path","config":{"path":"/etc/secrets/bigquery-sa.json"}}'
BIGQUERY_BATCH_SIZE=1000
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = BigQuerySink::new(config).await?;
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_bigquery::{BigQuerySink, BigQuerySinkConfig, BigQueryCredentials};

let source_config = RestStreamConfig::new("https://api.example.com", "/v1/events");
let source = RestStream::new(source_config);

let sink_config = BigQuerySinkConfig::new(
    "my-project",
    "analytics",
    "events",
    BigQueryCredentials::ApplicationDefault,
);
let sink = BigQuerySink::new(sink_config).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
```

## Examples

### Streaming inserts with a service account key file

```rust
let config = BigQuerySinkConfig::new(
    "production-project",
    "warehouse",
    "user_events",
    BigQueryCredentials::ServiceAccountKeyPath(
        "/etc/secrets/bigquery-writer.json".into()
    ),
)
.with_batch_size(500);

let sink = BigQuerySink::new(config).await?;
let written = sink.write_batch(&records).await?;
```

### Using inline service account JSON

```rust
let sa_json = std::env::var("BIGQUERY_SA_KEY")?;

let config = BigQuerySinkConfig::new(
    "my-project",
    "analytics",
    "events",
    BigQueryCredentials::ServiceAccountKey(sa_json),
);

let sink = BigQuerySink::new(config).await?;
```

### Using application default credentials (local development)

```rust
let config = BigQuerySinkConfig::new(
    "dev-project",
    "scratch",
    "test_table",
    BigQueryCredentials::ApplicationDefault,
)
.with_batch_size(100);

let sink = BigQuerySink::new(config).await?;
```

## How It Works

- The BigQuery client is created and authenticated in `BigQuerySink::new()`. This validates credentials eagerly so failures surface immediately.
- `write_batch()` slices the input into `batch_size`-row chunks (or forwards the whole slice when `batch_size = 0`) and sends each chunk as a separate `insertAll` request.
- Per-row errors in the BigQuery response are detected and reported. If any rows fail, the entire batch returns an error with details about the first failure.
- The client is reused across all `write_batch()` calls -- no re-authentication per request.

## Dead-letter queue support

This sink overrides `Sink::write_batch_partial` to surface per-row
failures from BigQuery `tabledata.insertAll`'s `insertErrors` response.
Configure a DLQ at the pipeline level (see [cli/README.md — dlq:](../../../cli/README.md))
and only the rows BigQuery actually rejected will be routed there —
already-committed rows stay in the main sink with no duplicates.

## License

Licensed under MIT or Apache-2.0.
