# faucet-sink-bigquery

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-bigquery.svg)](https://crates.io/crates/faucet-sink-bigquery)
[![Docs.rs](https://docs.rs/faucet-sink-bigquery/badge.svg)](https://docs.rs/faucet-sink-bigquery)

Google BigQuery streaming insert sink for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a BigQuery table using the `tabledata.insertAll` streaming API. Records are automatically split into configurable batch sizes to stay within BigQuery API limits. The BigQuery client is authenticated once at construction and reused across all writes.

## Installation

```toml
[dependencies]
faucet-sink-bigquery = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-bigquery"] }
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
    .batch_size(500);

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
| `batch_size` | `usize` | `500` | Maximum number of rows per `insertAll` request |

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
.batch_size(1000);
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
  "credentials": {
    "type": "ServiceAccountKeyPath",
    "value": "/etc/secrets/bigquery-sa.json"
  },
  "batch_size": 500
}
```

Using application default credentials:

```json
{
  "project_id": "my-gcp-project",
  "dataset_id": "analytics",
  "table_id": "events",
  "credentials": {
    "type": "ApplicationDefault"
  },
  "batch_size": 1000
}
```

### Example .env file

```env
BIGQUERY_PROJECT_ID=my-gcp-project
BIGQUERY_DATASET_ID=analytics
BIGQUERY_TABLE_ID=events
BIGQUERY_CREDENTIALS='{"type":"ServiceAccountKeyPath","value":"/etc/secrets/bigquery-sa.json"}'
BIGQUERY_BATCH_SIZE=500
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
.batch_size(500);

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
.batch_size(100);

let sink = BigQuerySink::new(config).await?;
```

## How It Works

- The BigQuery client is created and authenticated in `BigQuerySink::new()`. This validates credentials eagerly so failures surface immediately.
- `write_batch()` splits the input records into chunks of `batch_size` and sends each chunk as a separate `insertAll` request.
- Per-row errors in the BigQuery response are detected and reported. If any rows fail, the entire batch returns an error with details about the first failure.
- The client is reused across all `write_batch()` calls -- no re-authentication per request.

## License

Licensed under MIT or Apache-2.0.
