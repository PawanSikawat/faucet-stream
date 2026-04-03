# faucet-sink-mongodb

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-mongodb.svg)](https://crates.io/crates/faucet-sink-mongodb)
[![Docs.rs](https://docs.rs/faucet-sink-mongodb/badge.svg)](https://docs.rs/faucet-sink-mongodb)

MongoDB sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Inserts JSON records into a MongoDB collection using `insert_many` for efficient batch writes. Each JSON record is converted to a BSON document before insertion. The MongoDB client connection is established once and reused across all writes.

## Installation

```toml
[dependencies]
faucet-sink-mongodb = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-mongodb"] }
```

## Quick Start

```rust
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = MongoSinkConfig::new(
        "mongodb://localhost:27017",
        "analytics",
        "events",
    )
    .batch_size(1000);

    let sink = MongoSink::new(config).await?;

    let records = vec![
        json!({"user_id": "u123", "event": "signup", "source": "web"}),
        json!({"user_id": "u456", "event": "login", "source": "mobile"}),
    ];

    let written = sink.write_batch(&records).await?;
    println!("Inserted {written} documents");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_uri` | `String` | *(required)* | MongoDB connection URI (e.g. `mongodb://user:pass@host:27017`) |
| `database` | `String` | *(required)* | Database name |
| `collection` | `String` | *(required)* | Collection name |
| `batch_size` | `usize` | `500` | Number of documents per `insert_many` call |

The `Debug` implementation masks the `connection_uri` with `***` to prevent credential leakage in logs.

### Builder Methods

```rust
use faucet_sink_mongodb::MongoSinkConfig;

let config = MongoSinkConfig::new(
    "mongodb://writer:s3cret@mongo.example.com:27017",
    "my_database",
    "my_collection",
)
.batch_size(2000);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_mongodb::MongoSinkConfig;

// From a JSON file
let config: MongoSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: MongoSinkConfig = load_env_file(".env", "MONGO_SINK")?;
```

### Example JSON config

```json
{
  "connection_uri": "mongodb://writer:s3cret@mongo.example.com:27017",
  "database": "analytics",
  "collection": "events",
  "batch_size": 1000
}
```

### Example JSON config (replica set)

```json
{
  "connection_uri": "mongodb://writer:s3cret@mongo1:27017,mongo2:27017,mongo3:27017/analytics?replicaSet=rs0",
  "database": "analytics",
  "collection": "events",
  "batch_size": 500
}
```

### Example .env file

```env
MONGO_SINK_CONNECTION_URI=mongodb://writer:s3cret@mongo.example.com:27017
MONGO_SINK_DATABASE=analytics
MONGO_SINK_COLLECTION=events
MONGO_SINK_BATCH_SIZE=1000
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = MongoSink::new(config).await?;
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events")
);

let sink = MongoSink::new(
    MongoSinkConfig::new("mongodb://localhost:27017", "analytics", "events")
).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
```

## Examples

### Basic insert with default batch size

```rust
let config = MongoSinkConfig::new(
    "mongodb://localhost:27017",
    "mydb",
    "users",
);

let sink = MongoSink::new(config).await?;
sink.write_batch(&records).await?;
```

### High-throughput loading with large batches

```rust
let config = MongoSinkConfig::new(
    "mongodb://writer:pass@mongo-primary:27017",
    "warehouse",
    "raw_events",
)
.batch_size(5000);

let sink = MongoSink::new(config).await?;
sink.write_batch(&large_dataset).await?;
```

### Inserting nested documents

MongoDB natively supports nested documents, so complex JSON structures are preserved:

```rust
let records = vec![
    json!({
        "user": {"name": "Alice", "email": "alice@example.com"},
        "tags": ["premium", "active"],
        "metadata": {"source": "api", "version": 2}
    }),
];

sink.write_batch(&records).await?;
```

## How It Works

- The MongoDB client is created in `MongoSink::new()` using `Client::with_uri_str()`. The connection is established and validated at this point.
- `write_batch()` splits records into chunks of `batch_size`. Each chunk is converted from `serde_json::Value` to `bson::Document` and inserted using `collection.insert_many()`.
- Every record must be a JSON object. Non-object values (arrays, strings, numbers, null) produce an error during BSON conversion.
- Nested JSON objects, arrays, and all JSON types are correctly converted to their BSON equivalents.
- The MongoDB driver handles connection pooling and automatic reconnection internally.

## License

Licensed under MIT or Apache-2.0.
