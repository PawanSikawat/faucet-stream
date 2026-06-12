# faucet-source-mongodb

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mongodb.svg)](https://crates.io/crates/faucet-source-mongodb)
[![Docs.rs](https://docs.rs/faucet-source-mongodb/badge.svg)](https://docs.rs/faucet-source-mongodb)

A MongoDB source that executes `find()` queries with filter, projection, sort, and limit, returning documents as JSON records.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```bash
cargo add faucet-source-mongodb
cargo add tokio --features full
```

Or via the umbrella crate:
```bash
cargo add faucet-stream --features source-mongodb
```

## Quick Start

```rust
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = MongoSourceConfig::new(
        "mongodb://localhost:27017",
        "my_database",
        "users",
    );

    let source = MongoSource::new(config).await?;
    let records = source.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### MongoSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_uri` | `String` | *(required)* | MongoDB connection URI (e.g. `mongodb://localhost:27017`). Masked in debug output for security |
| `database` | `String` | *(required)* | Database name |
| `collection` | `String` | *(required)* | Collection name |
| `filter` | `Option<Value>` | `None` | Query filter as a JSON object, converted to a BSON `Document` at query time |
| `projection` | `Option<Value>` | `None` | Field projection as a JSON object (e.g. `{"_id": 0, "name": 1}`) |
| `sort` | `Option<Value>` | `None` | Sort specification as a JSON object (e.g. `{"created_at": -1}`) |
| `limit` | `Option<i64>` | `None` | Maximum number of documents to return |
| `cursor_batch_size` | `Option<u32>` | `None` | MongoDB driver cursor batch size (documents per server round-trip). Wire-level tuning knob — see [Streaming and batching](#streaming-and-batching) for how this differs from `batch_size` |
| `batch_size` | `usize` | `1000` | Records per `StreamPage` emitted by `Source::stream_pages`. See [Streaming and batching](#streaming-and-batching) below |

> ⚠️ **Breaking rename**: the previous `batch_size: Option<u32>` field has been renamed to `cursor_batch_size` so that the new standardised `batch_size: usize` (records per `StreamPage`) can live alongside it. Existing JSON/YAML configs using `batch_size: <n>` now configure the *pipeline* page size — if you relied on it as the driver cursor knob, rename it to `cursor_batch_size`.

### Streaming and batching

`MongoSource::stream_pages` drives the MongoDB driver cursor (`Collection::find`)
without buffering the full result. Documents are accumulated into a
`batch_size` buffer and yielded as a `StreamPage` once the buffer fills;
the trailing partial page (if any) is yielded after the cursor drains.

`batch_size = 0` is the **"no batching" sentinel** — the cursor is drained
completely and the entire result set is emitted in a single `StreamPage`.
Use it for small lookup tables, or for downstream sinks (SQL `COPY`,
BigQuery load jobs, Snowflake stage uploads) that prefer one large request
to many small ones. Values larger than `MAX_BATCH_SIZE` (1,000,000) are
rejected by `faucet_core::validate_batch_size`.

`cursor_batch_size` is the **MongoDB driver's per-round-trip batch size**
(`find().batch_size(N)`). It is independent of the pipeline-level
`batch_size`: you can have the driver fetch 100 docs per round-trip and
still yield `StreamPage`s of 1000 docs (or vice versa), trading network
round-trips for buffering. Leave it unset to use the MongoDB driver's
default (101 documents).

The MongoDB source has no incremental-replication mode, so every emitted
page carries `bookmark: None`.

### BSON Conversion

All JSON filter, projection, and sort values are automatically converted to BSON documents at query time. The JSON values must be objects -- arrays and scalars will produce an error. Documents returned from MongoDB are converted back to relaxed extended JSON format.

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_mongodb::MongoSourceConfig;

let config: MongoSourceConfig = load_json("config.json")?;
let config: MongoSourceConfig = load_env_file(".env", "MONGO_SOURCE")?;
```

### Example JSON config

```json
{
  "connection_uri": "mongodb://admin:password@mongo.example.com:27017",
  "database": "analytics",
  "collection": "events",
  "filter": {
    "event_type": "purchase",
    "timestamp": { "$gte": "2025-01-01T00:00:00Z" }
  },
  "projection": {
    "_id": 0,
    "user_id": 1,
    "event_type": 1,
    "amount": 1,
    "timestamp": 1
  },
  "sort": { "timestamp": -1 },
  "limit": 10000,
  "cursor_batch_size": 500,
  "batch_size": 5000
}
```

### Example .env file

```env
MONGO_SOURCE_CONNECTION_URI=mongodb://localhost:27017
MONGO_SOURCE_DATABASE=mydb
MONGO_SOURCE_COLLECTION=users
MONGO_SOURCE_LIMIT=5000
MONGO_SOURCE_CURSOR_BATCH_SIZE=200
MONGO_SOURCE_BATCH_SIZE=1000
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = MongoSource::new(config).await?;
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Simple collection read

```rust
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use faucet_core::Source;

let config = MongoSourceConfig::new(
    "mongodb://localhost:27017",
    "mydb",
    "customers",
);
let source = MongoSource::new(config).await?;
let customers = source.fetch_all().await?;
```

### Filtered query with projection and sort

```rust
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use faucet_core::Source;
use serde_json::json;

let config = MongoSourceConfig::new(
    "mongodb://localhost:27017",
    "ecommerce",
    "orders",
)
.filter(json!({
    "status": "completed",
    "total": { "$gt": 100 }
}))
.projection(json!({
    "_id": 0,
    "order_id": 1,
    "customer": 1,
    "total": 1,
    "items": 1
}))
.sort(json!({"total": -1}))
.limit(500)
.cursor_batch_size(100);

let source = MongoSource::new(config).await?;
let orders = source.fetch_all().await?;
println!("Found {} high-value orders", orders.len());
```

### Using with a Pipeline

```rust
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use faucet_core::{Pipeline, Source, Sink};
use serde_json::json;

let config = MongoSourceConfig::new(
    "mongodb://mongo.production.com:27017",
    "logs",
    "app_events",
)
.filter(json!({"level": "error"}))
.sort(json!({"timestamp": -1}))
.limit(10000);

let source = MongoSource::new(config).await?;
let pipeline = Pipeline::new(Box::new(source), Box::new(my_sink));
let result = pipeline.run().await?;
```

## Lineage dataset URI

`mongodb://<host>:<port>/<database>/<collection>` (credentials stripped) — e.g. `mongodb://host:27017/mydb/events`.

## License

Licensed under MIT or Apache-2.0.
