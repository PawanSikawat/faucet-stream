# faucet-source-redis

[![Crates.io](https://img.shields.io/crates/v/faucet-source-redis.svg)](https://crates.io/crates/faucet-source-redis)
[![Docs.rs](https://docs.rs/faucet-source-redis/badge.svg)](https://docs.rs/faucet-source-redis)

A Redis source that reads records from lists, streams, or key patterns, returning them as JSON.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-redis = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "1.0", features = ["source-redis"] }
```

## Quick Start

```rust
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RedisSourceConfig::new(
        "redis://127.0.0.1:6379",
        RedisSourceType::List { key: "my_queue".into() },
    );

    let source = RedisSource::new(config);
    let records = source.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### RedisSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | `String` | *(required)* | Redis connection URL (e.g. `"redis://127.0.0.1:6379"`). Masked in debug output for security |
| `source_type` | `RedisSourceType` | *(required)* | The type of Redis data structure to read from |
| `max_records` | `Option<usize>` | `None` | Optional maximum number of records to return (truncates after fetching) |
| `batch_size` | `usize` | `DEFAULT_BATCH_SIZE` (1000) | Records per emitted `StreamPage` when driven via `Source::stream_pages`. `0` is the "no batching" sentinel — drains the underlying primitive and emits a single page. See [Streaming and batching](#streaming-and-batching). |

### Source Types (RedisSourceType)

#### List

Read all elements from a Redis list via `LRANGE 0 -1`.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | The list key |

Each list element is parsed as JSON. If an element is not valid JSON, it is returned as a JSON string.

#### Stream

Read entries from a Redis stream via `XREAD` or `XREADGROUP`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `key` | `String` | *(required)* | The stream key |
| `group` | `Option<String>` | `None` | Consumer group name. When set, uses `XREADGROUP` |
| `consumer` | `Option<String>` | `None` | Consumer name within the group (required when `group` is set) |
| `count` | `Option<usize>` | `None` (100 for `XREADGROUP`) | Maximum number of entries to read per call |

Each stream entry is returned as:
```json
{
  "id": "1234567890-0",
  "fields": { "field1": "value1", "field2": "value2" }
}
```

Stream field values are parsed as JSON where possible; otherwise they are returned as strings.

#### Keys

Scan for keys matching a glob pattern, then `MGET` all matched keys in a single round-trip.

| Field | Type | Description |
|-------|------|-------------|
| `pattern` | `String` | Glob pattern for `SCAN` (e.g. `"user:*"`) |

Each key-value pair is returned as:
```json
{
  "key": "user:123",
  "value": { "name": "Alice", "email": "alice@example.com" }
}
```

Values are parsed as JSON where possible; otherwise they are returned as strings.

## Streaming and batching

`RedisSource` overrides
[`Source::stream_pages`](https://docs.rs/faucet-core/latest/faucet_core/trait.Source.html#method.stream_pages)
so the pipeline can write pages to the sink as they arrive instead of
buffering the full result set. Each mode maps `batch_size` onto its native
paging primitive:

| Mode | Native primitive | Per-page command |
|------|------------------|------------------|
| `Keys` | `SCAN COUNT batch_size` cursor → `MGET` per batch | `SCAN MATCH <pattern> COUNT <batch_size>` then `MGET key1 ... keyN` |
| `Stream` | `XRANGE - + COUNT batch_size`, advancing start ID | `XRANGE <key> <start> + COUNT <batch_size>` |
| `List` | `LRANGE start stop`, sliding the window | `LRANGE <key> <start> <start + batch_size - 1>` |

`batch_size = 0` is the "no batching" sentinel:

- `Keys`: the `SCAN` cursor is drained to completion and a single `MGET`
  retrieves all matched keys; one `StreamPage` is yielded.
- `Stream`: a single `XRANGE - +` drains the stream; one `StreamPage` is
  yielded.
- `List`: a single `LRANGE 0 -1` drains the list; one `StreamPage` is
  yielded.

Bookmarks are `None` on every page — the Redis source has no
incremental-replication mode today.

When the consumer-group fields (`group` / `consumer`) are set, they apply only
to the `XREADGROUP` path used by `fetch_all`. Streaming via `stream_pages`
always uses `XRANGE` because the page-then-write contract is incompatible
with `XREADGROUP`'s deferred-acknowledgement semantics.

The trait-level `batch_size` argument that `stream_pages` receives is ignored
in favour of `RedisSourceConfig::batch_size` — the config is the user-facing
knob, and routing the pipeline hint through it would silently override an
explicit config value.

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_redis::RedisSourceConfig;

let config: RedisSourceConfig = load_json("config.json")?;
let config: RedisSourceConfig = load_env_file(".env", "REDIS_SOURCE")?;
```

### Example JSON config (List)

```json
{
  "url": "redis://127.0.0.1:6379",
  "source_type": {
    "type": "List",
    "key": "task_queue"
  },
  "max_records": 1000
}
```

### Example JSON config (Stream with consumer group)

```json
{
  "url": "redis://127.0.0.1:6379",
  "source_type": {
    "type": "Stream",
    "key": "events",
    "group": "my-consumer-group",
    "consumer": "worker-1",
    "count": 100
  }
}
```

### Example JSON config (Keys)

```json
{
  "url": "redis://127.0.0.1:6379",
  "source_type": {
    "type": "Keys",
    "pattern": "session:*"
  },
  "max_records": 500
}
```

### Example .env file

```env
REDIS_SOURCE_URL=redis://127.0.0.1:6379
REDIS_SOURCE_MAX_RECORDS=1000
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = RedisSource::new(config);
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Reading from a list

```rust
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use faucet_core::Source;

let config = RedisSourceConfig::new(
    "redis://127.0.0.1:6379",
    RedisSourceType::List { key: "notifications".into() },
)
.max_records(100);

let source = RedisSource::new(config);
let notifications = source.fetch_all().await?;
```

### Reading from a stream with consumer group

```rust
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use faucet_core::Source;

let config = RedisSourceConfig::new(
    "redis://127.0.0.1:6379",
    RedisSourceType::Stream {
        key: "order-events".into(),
        group: Some("analytics-group".into()),
        consumer: Some("worker-1".into()),
        count: Some(200),
    },
);

let source = RedisSource::new(config);
let events = source.fetch_all().await?;

for event in &events {
    println!("Event {}: {:?}", event["id"], event["fields"]);
}
```

### Scanning keys by pattern

```rust
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use faucet_core::Source;

let config = RedisSourceConfig::new(
    "redis://127.0.0.1:6379",
    RedisSourceType::Keys { pattern: "user:*".into() },
);

let source = RedisSource::new(config);
let users = source.fetch_all().await?;

for user in &users {
    println!("Key: {}, Value: {}", user["key"], user["value"]);
}
```

## Lineage dataset URI

`redis://<host>:<port>/<db>?key=<key>` or `?stream=<stream>` (credentials stripped) — e.g. `redis://localhost:6379/0?key=my-list`.

## License

Licensed under MIT or Apache-2.0.
