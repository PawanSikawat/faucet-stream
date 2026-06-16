# faucet-sink-http

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-http.svg)](https://crates.io/crates/faucet-sink-http)
[![Docs.rs](https://docs.rs/faucet-sink-http/badge.svg)](https://docs.rs/faucet-sink-http)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-http.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-http.svg)](https://github.com/PawanSikawat/faucet-stream#license)

HTTP POST sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Sends JSON records to an HTTP endpoint. Supports two batch modes: Individual (one request per record with concurrent execution) and Array (all records in a single request as a JSON array). Includes configurable authentication, retry logic for transient failures, and concurrency control via semaphores.

## Installation

```bash
cargo add faucet-sink-http
cargo add tokio --features full
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features sink-http
```

## Quick Start

```rust
use faucet_sink_http::{HttpSink, HttpSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = HttpSinkConfig::new("https://api.example.com/ingest")
        .max_retries(3)
        .concurrency(10);

    let sink = HttpSink::new(config);

    let records = vec![
        json!({"user_id": "u123", "event": "signup"}),
        json!({"user_id": "u456", "event": "login"}),
    ];

    let written = sink.write_batch(&records).await?;
    println!("Sent {written} records");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | `String` | *(required)* | Target endpoint URL |
| `method` | `reqwest::Method` | `POST` | HTTP method to use |
| `headers` | `HeaderMap` | empty | Additional request headers (not serializable; set via builder only) |
| `auth` | `HttpSinkAuth` | `None` | Authentication method (see below) |
| `batch_mode` | `HttpBatchMode` | `Individual` | How to batch records in requests (see below) |
| `max_retries` | `usize` | `0` | Number of retries on transient failures |
| `concurrency` | `usize` | `10` | Maximum number of concurrent requests in Individual mode |
| `batch_size` | `usize` | `1000` (`faucet_core::DEFAULT_BATCH_SIZE`) | Maximum records per outbound HTTP request. Re-chunks the upstream `StreamPage` in `Array` mode; no-op in `Individual` mode. `0` = "no batching" sentinel (one POST per `StreamPage`). |

### Authentication (`HttpSinkAuth`)

| Variant | Fields | Description |
|---------|--------|-------------|
| `None` | -- | No authentication |
| `Bearer { token }` | `String` | Bearer token in the Authorization header |
| `Basic { username, password }` | `String`, `String` | HTTP Basic authentication |
| `Custom { headers }` | `HashMap<String, String>` | Header name → value map attached to every request |

The `Debug` implementation masks tokens and passwords with `***` to prevent credential leakage in logs.

### Batch Modes (`HttpBatchMode`)

| Mode | Description |
|------|-------------|
| `Individual` | Send one HTTP request per record. Requests are executed concurrently up to the `concurrency` limit using a semaphore. |
| `Array` | Send all records as a JSON array in a single HTTP request. |

### Streaming and batching

The HTTP sink honours the workspace-wide `batch_size` contract. The exact effect on the wire depends on the configured `batch_mode`:

- **`Individual` mode** — one HTTP request per record, executed concurrently up to `concurrency` via a semaphore. `batch_size` has **no effect on wire framing** in this mode (each record is already its own request); the field is accepted only for config-shape parity with other sinks and validated via `faucet_core::validate_batch_size` at load time. Use `concurrency` to tune throughput.
- **`Array` mode** — the sink re-chunks the upstream `StreamPage` into `batch_size`-row slices and issues one POST request per chunk, with each request body a JSON array of up to `batch_size` records. With the default `batch_size = 1000`, a 2 500-record `write_batch` produces 3 POSTs (1000 + 1000 + 500). When `batch_size = 0` (the **"no batching" sentinel**), the entire records slice is forwarded as a single JSON array — useful when the upstream source already chunks the stream to a size the destination endpoint accepts (e.g. a Postgres source with its own `batch_size`).

Recommended value for HTTP POST endpoints that accept arrays: match the destination's documented batch limit (commonly 100–1000 records per request). For per-record send semantics, prefer `batch_mode: Individual` over `batch_size: 1` — the former is the more direct expression of intent and the only one that drives concurrent in-flight requests.

### Retry Behavior

When `max_retries > 0`, the sink retries requests that fail with retriable errors (network errors, 5xx status codes, etc.). Each retry is immediate (no backoff). After exhausting all retries, the last error is returned.

### Builder Methods

```rust
use faucet_sink_http::{HttpSinkConfig, HttpSinkAuth, HttpBatchMode};

let config = HttpSinkConfig::new("https://api.example.com/ingest")
    .method(reqwest::Method::PUT)
    .auth(HttpSinkAuth::Bearer { token: "my-token".into() })
    .batch_mode(HttpBatchMode::Array)
    .max_retries(3)
    .concurrency(20)
    .with_batch_size(500);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_http::HttpSinkConfig;

// From a JSON file
let config: HttpSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: HttpSinkConfig = load_env_file(".env", "HTTP_SINK")?;
```

Note: The `headers` field on `HttpSinkConfig` is `HeaderMap` and remains `#[serde(skip)]` — set it programmatically. The `Custom` auth variant uses a `HashMap<String, String>` and round-trips through JSON/YAML.

### Example JSON config (Individual mode with Bearer auth)

```json
{
  "url": "https://api.example.com/ingest",
  "method": "POST",
  "auth": {
    "type": "bearer",
    "config": {
      "token": "my-api-token"
    }
  },
  "batch_mode": {
    "type": "Individual"
  },
  "max_retries": 3,
  "concurrency": 10
}
```

### Example JSON config (Array mode with Basic auth)

```json
{
  "url": "https://api.example.com/bulk",
  "method": "POST",
  "auth": {
    "type": "basic",
    "config": {
      "username": "ingest-user",
      "password": "s3cret"
    }
  },
  "batch_mode": {
    "type": "Array"
  },
  "max_retries": 2,
  "concurrency": 1,
  "batch_size": 500
}
```

### Example .env file

```env
HTTP_SINK_URL=https://api.example.com/ingest
HTTP_SINK_METHOD=POST
HTTP_SINK_AUTH='{"type":"bearer","config":{"token":"my-api-token"}}'
HTTP_SINK_BATCH_MODE='{"type":"Individual"}'
HTTP_SINK_MAX_RETRIES=3
HTTP_SINK_CONCURRENCY=10
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = HttpSink::new(config);
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_http::{HttpSink, HttpSinkConfig, HttpBatchMode};

let source = RestStream::new(
    RestStreamConfig::new("https://source-api.example.com", "/v1/events")
);

let sink = HttpSink::new(
    HttpSinkConfig::new("https://dest-api.example.com/ingest")
        .batch_mode(HttpBatchMode::Array)
        .max_retries(3)
);

let result = Pipeline::new(source, sink).run().await?;
println!("Forwarded {} records", result.records_written);
```

## Examples

### Individual mode -- one request per record with concurrency

```rust
let config = HttpSinkConfig::new("https://webhooks.example.com/event")
    .auth(HttpSinkAuth::Bearer { token: "webhook-token".into() })
    .batch_mode(HttpBatchMode::Individual)
    .concurrency(20)
    .max_retries(2);

let sink = HttpSink::new(config);

let records = vec![
    json!({"event": "user.created", "user_id": "u1"}),
    json!({"event": "user.updated", "user_id": "u2"}),
    json!({"event": "order.placed", "order_id": "o1"}),
];

// All 3 records are sent concurrently (up to 20 at a time)
sink.write_batch(&records).await?;
```

### Array mode -- bulk send as JSON array

```rust
let config = HttpSinkConfig::new("https://api.example.com/bulk-ingest")
    .batch_mode(HttpBatchMode::Array)
    .max_retries(3);

let sink = HttpSink::new(config);

let records = vec![
    json!({"metric": "cpu_usage", "value": 0.85}),
    json!({"metric": "memory_usage", "value": 0.72}),
];

// Sends: POST with body [{"metric":"cpu_usage","value":0.85},{"metric":"memory_usage","value":0.72}]
sink.write_batch(&records).await?;
```

### Custom HTTP method and headers

```rust
use reqwest::header::{HeaderMap, HeaderValue};

let mut headers = HeaderMap::new();
headers.insert("X-Custom-Header", HeaderValue::from_static("my-value"));

let config = HttpSinkConfig::new("https://api.example.com/data")
    .method(reqwest::Method::PUT)
    .headers(headers)
    .auth(HttpSinkAuth::Basic {
        username: "api-user".into(),
        password: "api-pass".into(),
    });

let sink = HttpSink::new(config);
```

## How It Works

- The HTTP client is created in `HttpSink::new()` and reused across all requests.
- In **Individual** mode, each record is sent as a separate HTTP request. Requests are executed concurrently (bounded by `concurrency`). In `write_batch` the first error aborts the batch; when a [DLQ](https://pawansikawat.github.io/faucet-stream/cookbook/dlq.html) is configured the sink instead reports **per-row** outcomes (`write_batch_partial`), attempting every record and dead-lettering only the ones that actually failed — so already-delivered rows are never duplicated against a non-idempotent endpoint.
- In **Array** mode, all records are collected into a JSON array and sent as a single request body. A failure can't be attributed to specific rows, so the whole array surfaces as an error (the DLQ `on_batch_error` policy decides whether to abort or dead-letter the batch).
- Retry logic: on transient failures (network errors, retriable HTTP status codes), the request is retried up to `max_retries` times. Non-retriable errors (4xx client errors) are returned immediately.
- Authentication and custom headers are applied to every request.
- HTTP responses are validated using `check_http_response()` from `faucet-core`, which checks status codes and returns structured errors.

## Lineage dataset URI

`https://<host><path>` (credentials stripped) — e.g. `https://api.example.com/ingest`.

## License

Licensed under MIT or Apache-2.0.
