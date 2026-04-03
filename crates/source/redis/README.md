# faucet-source-redis

[![Crates.io](https://img.shields.io/crates/v/faucet-source-redis.svg)](https://crates.io/crates/faucet-source-redis)
[![Docs.rs](https://docs.rs/faucet-source-redis/badge.svg)](https://docs.rs/faucet-source-redis)

A Redis source that reads records from lists, streams, or key patterns, returning them as JSON.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-redis = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-redis"] }
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

## License

Licensed under MIT or Apache-2.0.
