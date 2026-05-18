# faucet-sink-elasticsearch

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-elasticsearch.svg)](https://crates.io/crates/faucet-sink-elasticsearch)
[![Docs.rs](https://docs.rs/faucet-sink-elasticsearch/badge.svg)](https://docs.rs/faucet-sink-elasticsearch)

Elasticsearch sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Indexes JSON records into an Elasticsearch index using the bulk API (`_bulk` endpoint) with NDJSON format. Supports optional document ID extraction from record fields, multiple authentication methods, and configurable batch sizes for bulk requests.

## Installation

```toml
[dependencies]
faucet-sink-elasticsearch = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-elasticsearch"] }
```

## Quick Start

```rust
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ElasticsearchSinkConfig::new(
        "http://localhost:9200",
        "events",
    )
    .batch_size(500);

    let sink = ElasticsearchSink::new(config);

    let records = vec![
        json!({"user_id": "u123", "event": "page_view", "url": "/home"}),
        json!({"user_id": "u456", "event": "click", "url": "/pricing"}),
    ];

    let written = sink.write_batch(&records).await?;
    println!("Indexed {written} documents");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | `String` | *(required)* | Base URL of the Elasticsearch cluster (e.g. `"http://localhost:9200"`). Trailing slashes are stripped automatically. |
| `index` | `String` | *(required)* | Target index name |
| `auth` | `ElasticsearchSinkAuth` | `None` | Authentication method (see below) |
| `batch_size` | `usize` | `500` | Maximum number of documents per `_bulk` request |
| `id_field` | `Option<String>` | `None` | JSON field name to use as the document `_id`. If `None`, Elasticsearch auto-generates IDs. |

### Authentication (`ElasticsearchSinkAuth`)

| Variant | Fields | Description |
|---------|--------|-------------|
| `None` | -- | No authentication |
| `Basic { username, password }` | `String`, `String` | HTTP Basic authentication |
| `Bearer { token }` | `String` | Bearer token in the Authorization header |
| `ApiKey { key }` | `String` | API key sent as `ApiKey <key>` in the Authorization header |

The `Debug` implementation masks passwords, tokens, and API keys with `***` to prevent credential leakage in logs.

### Document ID Extraction

When `id_field` is set, the sink extracts the document `_id` from each record:

- If the field value is a string, it is used directly.
- If the field value is a number or other type, it is converted to its string representation.
- If a record is missing the `id_field`, Elasticsearch auto-generates an ID for that document.

### Builder Methods

```rust
use faucet_sink_elasticsearch::{ElasticsearchSinkConfig, ElasticsearchSinkAuth};

let config = ElasticsearchSinkConfig::new("http://localhost:9200", "events")
    .auth(ElasticsearchSinkAuth::Basic {
        username: "elastic".into(),
        password: "changeme".into(),
    })
    .batch_size(1000)
    .id_field("doc_id");
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_elasticsearch::ElasticsearchSinkConfig;

// From a JSON file
let config: ElasticsearchSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: ElasticsearchSinkConfig = load_env_file(".env", "ES_SINK")?;
```

### Example JSON config

```json
{
  "base_url": "http://localhost:9200",
  "index": "events",
  "auth": {
    "type": "Basic",
    "username": "elastic",
    "password": "changeme"
  },
  "batch_size": 500,
  "id_field": "event_id"
}
```

### Example JSON config (no auth, auto-generated IDs)

```json
{
  "base_url": "http://localhost:9200",
  "index": "logs",
  "auth": {
    "type": "None"
  },
  "batch_size": 1000
}
```

### Example JSON config (API key)

```json
{
  "base_url": "https://my-cluster.es.cloud:9243",
  "index": "metrics",
  "auth": {
    "type": "ApiKey",
    "key": "VnVhQ2ZHY0JDZGJrU..."
  },
  "batch_size": 500
}
```

### Example .env file

```env
ES_SINK_BASE_URL=http://localhost:9200
ES_SINK_INDEX=events
ES_SINK_AUTH='{"type":"Basic","username":"elastic","password":"changeme"}'
ES_SINK_BATCH_SIZE=500
ES_SINK_ID_FIELD=event_id
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = ElasticsearchSink::new(config);
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/logs")
);

let sink = ElasticsearchSink::new(
    ElasticsearchSinkConfig::new("http://localhost:9200", "api_logs")
        .id_field("log_id")
        .batch_size(1000)
);

let result = Pipeline::new(source, sink).run().await?;
println!("Indexed {} documents", result.records_written);
```

## Examples

### Basic indexing with auto-generated IDs

```rust
let config = ElasticsearchSinkConfig::new("http://localhost:9200", "logs");
let sink = ElasticsearchSink::new(config);

let records = vec![
    json!({"level": "info", "message": "Server started", "timestamp": "2026-04-02T10:00:00Z"}),
    json!({"level": "error", "message": "Connection timeout", "timestamp": "2026-04-02T10:01:00Z"}),
];

sink.write_batch(&records).await?;
```

### Custom document IDs from a record field

```rust
let config = ElasticsearchSinkConfig::new("http://localhost:9200", "products")
    .id_field("product_id");

let sink = ElasticsearchSink::new(config);

let records = vec![
    json!({"product_id": "SKU-001", "name": "Widget", "price": 9.99}),
    json!({"product_id": "SKU-002", "name": "Gadget", "price": 24.99}),
];

sink.write_batch(&records).await?;
// Documents are indexed with _id = "SKU-001" and _id = "SKU-002"
```

### Elasticsearch Cloud with API key authentication

```rust
let config = ElasticsearchSinkConfig::new(
    "https://my-deployment.es.us-east-1.aws.found.io:9243",
    "application-events",
)
.auth(ElasticsearchSinkAuth::ApiKey {
    key: std::env::var("ES_API_KEY")?,
})
.batch_size(2000);

let sink = ElasticsearchSink::new(config);
sink.write_batch(&records).await?;
```

## How It Works

- The HTTP client is created in `ElasticsearchSink::new()` and reused across all requests.
- `write_batch()` splits records into chunks of `batch_size`. For each chunk, it builds an NDJSON body with alternating action/data lines:
  ```
  {"index":{"_index":"events","_id":"optional-id"}}
  {"user_id":"u123","event":"page_view"}
  {"index":{"_index":"events"}}
  {"user_id":"u456","event":"click"}
  ```
- The NDJSON body is sent as a POST to `{base_url}/_bulk` with `Content-Type: application/x-ndjson`.
- The bulk response is inspected for per-item errors. If any items report errors, the sink returns a `FaucetError::Sink` with details about the first error.
- Authentication headers are applied to every request based on the configured auth method.

## License

Licensed under MIT or Apache-2.0.
