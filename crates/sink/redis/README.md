# faucet-sink-redis

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-redis.svg)](https://crates.io/crates/faucet-sink-redis)
[![Docs.rs](https://docs.rs/faucet-sink-redis/badge.svg)](https://docs.rs/faucet-sink-redis)

Redis sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to Redis data structures: lists (`RPUSH`), streams (`XADD`), or key-value pairs (`SET`). Uses Redis pipelines for efficient batched writes and a multiplexed async connection that is reused across all calls.

## Installation

```toml
[dependencies]
faucet-sink-redis = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "1.0", features = ["sink-redis"] }
```

## Quick Start

```rust
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RedisSinkConfig::new(
        "redis://127.0.0.1:6379",
        RedisSinkType::List { key: "events".into() },
    );

    let sink = RedisSink::new(config).await?;

    let records = vec![
        json!({"user_id": "u123", "event": "signup"}),
        json!({"user_id": "u456", "event": "login"}),
    ];

    let written = sink.write_batch(&records).await?;
    println!("Wrote {written} records to Redis");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | `String` | *(required)* | Redis connection URL (e.g. `redis://127.0.0.1:6379`) |
| `sink_type` | `RedisSinkType` | *(required)* | The type of Redis data structure to write to (see below) |
| `batch_size` | `usize` | `DEFAULT_BATCH_SIZE` (1000) | Maximum number of commands packed into a single Redis pipeline. Pass `0` to opt out of re-chunking — see [Streaming and batching](#streaming-and-batching) below. |

The `Debug` implementation masks the `url` with `***` to prevent credential leakage in logs.

### Sink Types (`RedisSinkType`)

| Variant | Fields | Description |
|---------|--------|-------------|
| `List` | `key: String` | Append each record as a serialized JSON string to a Redis list using `RPUSH`. |
| `Stream` | `key: String` | Add each record as an entry to a Redis stream using `XADD` with auto-generated IDs (`*`). Top-level JSON fields become stream entry fields. Non-object records are stored as a single `_data` field. |
| `KeyValue` | `key_field: String` | Store each record as a separate key using `SET`. The Redis key is extracted from the specified field in each record. The entire record is stored as a serialized JSON string value. |

### Stream Entry Field Mapping

When using the `Stream` sink type, top-level JSON object fields are flattened into Redis stream entry fields:

- String values are stored as-is
- Numbers, booleans, and null are converted to their string representation
- Nested objects and arrays are serialized as JSON strings

If a record is not a JSON object (e.g. a plain string), it is stored as a single `_data` field containing the serialized record.

### Builder Methods

```rust
use faucet_sink_redis::{RedisSinkConfig, RedisSinkType};

let config = RedisSinkConfig::new(
    "redis://localhost:6379",
    RedisSinkType::Stream { key: "events".into() },
)
.with_batch_size(1000);
```

## Streaming and batching

The sink fits the workspace's streaming pipeline contract: `Pipeline::run` drives `Source::stream_pages` and writes each `StreamPage` via `Sink::write_batch` as it arrives. `batch_size` controls how those records get packed into Redis pipelines on the way out:

| `batch_size` | Behaviour |
|--------------|-----------|
| `1`..`MAX_BATCH_SIZE` (default `1000`) | When `write_batch` receives a slice larger than `batch_size`, the sink re-chunks it into `batch_size` slices and issues one Redis pipeline per chunk. Recommended for high-throughput writes — Redis pipelined commands are cheap, and a 1000-command window comfortably amortises the round-trip without starving other clients. |
| `0` | "No batching" sentinel — the entire records slice is packed into a single Redis pipeline regardless of size, preserving upstream `StreamPage` framing. Use this when the source has already chosen a sensible page size (e.g. `RedisSourceConfig::batch_size`, or any other source's per-page knob) and you want one pipeline per page. |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_redis::RedisSinkConfig;

// From a JSON file
let config: RedisSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: RedisSinkConfig = load_env_file(".env", "REDIS_SINK")?;
```

### Example JSON config (List)

```json
{
  "url": "redis://127.0.0.1:6379",
  "sink_type": {
    "type": "List",
    "key": "event_queue"
  },
  "batch_size": 1000
}
```

### Example JSON config (Stream)

```json
{
  "url": "redis://127.0.0.1:6379",
  "sink_type": {
    "type": "Stream",
    "key": "event_stream"
  },
  "batch_size": 1000
}
```

### Example JSON config (KeyValue)

```json
{
  "url": "redis://127.0.0.1:6379",
  "sink_type": {
    "type": "KeyValue",
    "key_field": "id"
  },
  "batch_size": 1000
}
```

### Example .env file

```env
REDIS_SINK_URL=redis://127.0.0.1:6379
REDIS_SINK_SINK_TYPE='{"type":"List","key":"events"}'
REDIS_SINK_BATCH_SIZE=1000
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = RedisSink::new(config).await?;
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events")
);

let sink = RedisSink::new(RedisSinkConfig::new(
    "redis://localhost:6379",
    RedisSinkType::Stream { key: "api_events".into() },
)).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
```

## Examples

### Writing to a Redis list (queue pattern)

```rust
let config = RedisSinkConfig::new(
    "redis://localhost:6379",
    RedisSinkType::List { key: "job_queue".into() },
)
.with_batch_size(500);

let sink = RedisSink::new(config).await?;
sink.write_batch(&records).await?;
// Records are appended to the "job_queue" list via RPUSH
```

### Writing to a Redis stream (event sourcing)

```rust
let config = RedisSinkConfig::new(
    "redis://localhost:6379",
    RedisSinkType::Stream { key: "user_events".into() },
);

let sink = RedisSink::new(config).await?;

let records = vec![
    json!({"user_id": "u1", "action": "login", "ip": "10.0.0.1"}),
    json!({"user_id": "u2", "action": "purchase", "amount": 99.99}),
];

sink.write_batch(&records).await?;
// Each record becomes a stream entry with fields: user_id, action, ip/amount
```

### Writing to individual keys (cache/lookup pattern)

```rust
let config = RedisSinkConfig::new(
    "redis://localhost:6379",
    RedisSinkType::KeyValue { key_field: "user_id".into() },
);

let sink = RedisSink::new(config).await?;

let records = vec![
    json!({"user_id": "u123", "name": "Alice", "plan": "pro"}),
    json!({"user_id": "u456", "name": "Bob", "plan": "free"}),
];

sink.write_batch(&records).await?;
// Creates keys "u123" and "u456" with the full JSON as values
```

## How It Works

- A multiplexed async connection is opened in `RedisSink::new()` and reused across all `write_batch()` calls. The multiplexed connection is cheaply cloneable.
- `write_batch()` processes records in chunks of `batch_size`. Each chunk is sent as a Redis pipeline (multiple commands batched in a single round-trip) for maximum throughput. When `batch_size = 0`, the entire records slice is packed into a single pipeline — see [Streaming and batching](#streaming-and-batching).
- For `List` mode: each record is serialized to JSON and appended with `RPUSH`.
- For `Stream` mode: each record's top-level fields are flattened into stream entry fields for `XADD`. Auto-generated stream IDs (`*`) are used.
- For `KeyValue` mode: the specified `key_field` is extracted from each record to use as the Redis key. The entire record (serialized as JSON) is the value for `SET`. Records missing the key field produce an error.

## Lineage dataset URI

`redis://<host>:<port>?key=<key>` or `?key_field=<field>` (credentials stripped) — e.g. `redis://localhost:6379?key=events`.

## License

Licensed under MIT or Apache-2.0.
