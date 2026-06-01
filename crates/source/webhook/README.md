# faucet-source-webhook

[![Crates.io](https://img.shields.io/crates/v/faucet-source-webhook.svg)](https://crates.io/crates/faucet-source-webhook)
[![Docs.rs](https://docs.rs/faucet-source-webhook/badge.svg)](https://docs.rs/faucet-source-webhook)

A webhook receiver source that starts a temporary HTTP server and collects incoming POST payloads as JSON records.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-webhook = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "1.0", features = ["source-webhook"] }
```

## Quick Start

```rust
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WebhookSourceConfig::new()
        .listen_addr("0.0.0.0:8080")
        .path("/webhook")
        .max_payloads(10)
        .timeout_secs(60);

    let source = WebhookSource::new(config);
    // Blocks until timeout or max_payloads is reached
    let records = source.fetch_all().await?;

    println!("Received {} webhook payloads", records.len());
    Ok(())
}
```

## How It Works

When `fetch_all()` is called, the source:

1. Starts an HTTP server on the configured address using axum
2. Listens for incoming POST requests on the configured path
3. Parses each request body as JSON (falls back to a plain string if not valid JSON)
4. Collects payloads until either the timeout expires or `max_payloads` is reached
5. Shuts down the server and returns all collected payloads as records

The server responds with `200 OK` to valid requests and `400 Bad Request` for non-UTF-8 bodies.

## Configuration

### WebhookSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen_addr` | `String` | `"127.0.0.1:8080"` | Address to bind the HTTP server to. Defaults to loopback; bind `0.0.0.0` only behind a trusted gateway. |
| `path` | `String` | `"/webhook"` | Endpoint path for receiving webhooks |
| `max_payloads` | `Option<usize>` | `None` | Stop after receiving this many payloads. `None` means collect until timeout |
| `timeout_secs` | `u64` | `30` | How long to listen before returning, in seconds |
| `max_body_bytes` | `usize` | `1048576` | Max accepted request body size (1 MiB). Larger POSTs are rejected with `413` so one huge request can't exhaust memory. |
| `auth_token` | `Option<String>` | `None` | Optional shared secret. When set, requests must send it in the `Authorization` header (raw or `Bearer <token>`); others get `401`. The comparison is constant-time (no timing side-channel). Strongly recommended whenever `listen_addr` isn't loopback. |
| `batch_size` | `usize` | `1000` | Records per emitted `StreamPage`. `0` is the "no batching" sentinel — emit the full flush window in one page. See [Streaming and batching](#streaming-and-batching) |

## Streaming and batching

The webhook source collects POST requests into a per-flush in-memory buffer during its receive window (bounded by `timeout_secs` and, optionally, `max_payloads`). It has no native streaming primitive — `Source::stream_pages` falls back to the default trait implementation, which buffers the full flush window and then chunks it into pages of `batch_size` records.

- `batch_size` is honoured for **chunking the flush** that is handed downstream, but does not change how POSTs are buffered server-side: the HTTP handler always pushes into the in-process `Vec` until the receive window closes.
- `batch_size = 0` is functionally equivalent to any positive value larger than the received payload count for this source — both emit one page containing every collected record.
- Flushes are bounded by the receive-window timer (`timeout_secs`) and `max_payloads`, configured separately on the source. Tune those if you need to bound memory at request-receive time; `batch_size` only shapes the downstream page size.

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_webhook::WebhookSourceConfig;

let config: WebhookSourceConfig = load_json("config.json")?;
let config: WebhookSourceConfig = load_env_file(".env", "WEBHOOK")?;
```

### Example JSON config

```json
{
  "listen_addr": "0.0.0.0:9090",
  "path": "/hooks/incoming",
  "max_payloads": 100,
  "timeout_secs": 120,
  "batch_size": 1000
}
```

### Example .env file

```env
WEBHOOK_LISTEN_ADDR=0.0.0.0:8080
WEBHOOK_PATH=/webhook
WEBHOOK_MAX_PAYLOADS=50
WEBHOOK_TIMEOUT_SECS=60
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = WebhookSource::new(config);
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Collect a fixed number of webhooks

```rust
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};
use faucet_core::Source;

let config = WebhookSourceConfig::new()
    .listen_addr("127.0.0.1:8080")
    .path("/events")
    .max_payloads(5)
    .timeout_secs(300);

let source = WebhookSource::new(config);
// Server runs until 5 payloads are received or 5 minutes elapse
let events = source.fetch_all().await?;
```

### Time-limited webhook collection

```rust
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};
use faucet_core::Source;

// Collect all webhooks received within 30 seconds
let config = WebhookSourceConfig::new()
    .timeout_secs(30);

let source = WebhookSource::new(config);
let payloads = source.fetch_all().await?;
println!("Collected {} payloads in 30 seconds", payloads.len());
```

### Using with a Pipeline

```rust
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};
use faucet_core::{Pipeline, Source, Sink};

let config = WebhookSourceConfig::new()
    .listen_addr("0.0.0.0:9090")
    .path("/github-webhooks")
    .max_payloads(100)
    .timeout_secs(3600);

let source = WebhookSource::new(config);
let pipeline = Pipeline::new(Box::new(source), Box::new(my_sink));
let result = pipeline.run().await?;
println!("Processed {} webhook events", result.records_written);
```

## Payload Handling

- **JSON bodies** are parsed and stored as native JSON values
- **Plain text bodies** (non-JSON) are stored as JSON strings
- **Non-UTF-8 bodies** receive a `400 Bad Request` response and are not stored

## License

Licensed under MIT or Apache-2.0.
