# faucet-sink-mongodb

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-mongodb.svg)](https://crates.io/crates/faucet-sink-mongodb)
[![Docs.rs](https://docs.rs/faucet-sink-mongodb/badge.svg)](https://docs.rs/faucet-sink-mongodb)

MongoDB sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Inserts JSON records into a MongoDB collection using `insert_many` for efficient batch writes. Each JSON record is converted to a BSON document before insertion. The MongoDB client connection is established once and reused across all writes.

## Installation

```toml
[dependencies]
faucet-sink-mongodb = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "1.0", features = ["sink-mongodb"] }
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
    .with_batch_size(1000);

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
| `batch_size` | `usize` | `1000` | Maximum number of documents per `insert_many` call. See [Streaming and batching](#streaming-and-batching) below |
| `ordered` | `bool` | `false` | Whether `insert_many` is ordered. Default `false` (unordered) so one bad document — duplicate `_id`, validation error — doesn't drop the rest of the batch. Set `true` only if you require strict insertion order and want the batch to abort at the first failure. |
| `write_mode` | `string` | `append` | `append`, `upsert`, or `delete`. See [Write modes (upsert / delete)](#write-modes-upsert--delete) below |
| `key` | `[string]` | `[]` | Match-filter fields for `upsert` / `delete`. **Required and non-empty** for those modes; ignored for `append`. Typically `["_id"]` |
| `delete_marker` | `object` | *(none)* | Upsert only. `{ field, values }` — rows whose `field` matches one of `values` are routed to deletes instead of upserts. The marker field is stripped from upsert rows before writing |

The `Debug` implementation masks the `connection_uri` with `***` to prevent credential leakage in logs.

### Streaming and batching

`MongoSink::write_batch` re-chunks the incoming records slice into
`batch_size` slices and issues one `insert_many` call per chunk. The
default of `1000` matches MongoDB's documented sweet spot for
`insert_many` — roughly 1000 documents per call balances round-trip
overhead against the per-request BSON size budget (the server caps a
single request at 48 MB). Tune up for narrow documents where round-trip
latency dominates, and down for very wide documents that bump up against
the BSON size cap.

`batch_size = 0` is the **"no batching" sentinel** — `write_batch`
forwards the entire records slice in a single `insert_many` call, no
matter how large, so upstream `StreamPage` framing flows through
untouched. Use it when the upstream source already emits pages sized for
MongoDB's per-request limits. Values larger than `MAX_BATCH_SIZE`
(1,000,000) are rejected by `faucet_core::validate_batch_size`.

Note that this `batch_size` is a **write-side chunking knob** specific
to the sink. It is unrelated to the MongoDB driver's internal
`cursor_batch_size` (the wire-level read-side cursor tuning knob used by
`faucet-source-mongodb`) — the two concerns don't share a value because
`insert_many` and a query cursor are different operations.

### Write modes (upsert / delete)

By default the sink runs in `append` mode and inserts every record via
`insert_many`. Set `write_mode: upsert` or `write_mode: delete` to keep a
collection in sync with a changing source instead of only appending.

MongoDB is schemaless, so unlike the SQL sinks there are no key *columns* —
the `key` fields become the **match filter** for each document. `key` is
typically `["_id"]`, but any combination of top-level fields works
(composite keys are matched on all of them). `key` must be non-empty for
`upsert` / `delete`; an empty `key` is rejected at config-load time.

- **`upsert`** — each row is committed with a per-document
  `replace_one(filter, replacement).upsert(true)`. The filter is built from
  the row's `key` fields and the whole row is the replacement document, so an
  existing document is **replaced in place** (not field-merged) and a missing
  one is inserted.
- **`delete`** — each row is removed with `delete_one(filter)`, where the
  filter is built from the row's `key` fields.
- **`delete_marker`** (upsert only) — mix upserts and deletes in one stream:
  rows whose `delete_marker.field` matches one of `delete_marker.values` are
  routed to `delete_one`; all others are upserted. The marker field is
  stripped from the upsert replacement document so it never lands in the
  collection — handy for CDC streams that carry an operation flag like
  `__op: "u" | "d"`.

Within a single batch, repeated keys are deduped **last-write-wins** before
any write is issued, so a page that touches the same `_id` twice results in a
single `replace_one` / `delete_one` carrying the final value.

A document missing or null in a key field fails. When a `dlq:` block is
configured the good documents are still written and only the missing/null-key
documents are routed to the DLQ per-row; without a DLQ the whole batch fails.

Each `replace_one` / `delete_one` is a per-document primitive (not the
namespaced `Client::bulk_write`) for compatibility with all supported MongoDB
server versions; the sink recovers throughput by issuing the deduped ops
concurrently.

```yaml
sink:
  type: mongodb
  config:
    connection_uri: mongodb://localhost:27017
    database: analytics
    collection: users
    write_mode: upsert
    key: ["_id"]
    # Optional: route delete-flagged rows (e.g. from a CDC source) to deletes.
    delete_marker:
      field: __op
      values: ["d", "delete"]
```

### Builder Methods

```rust
use faucet_sink_mongodb::MongoSinkConfig;

let config = MongoSinkConfig::new(
    "mongodb://writer:s3cret@mongo.example.com:27017",
    "my_database",
    "my_collection",
)
.with_batch_size(2000);
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
.with_batch_size(5000);

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
- `write_batch()` splits records into chunks of `batch_size`. Each chunk is converted from `serde_json::Value` to `bson::Document` and inserted using `collection.insert_many()`. When `batch_size = 0`, the entire slice is sent in a single `insert_many` call — see [Streaming and batching](#streaming-and-batching).
- Every record must be a JSON object. Non-object values (arrays, strings, numbers, null) produce an error during BSON conversion.
- Nested JSON objects, arrays, and all JSON types are correctly converted to their BSON equivalents.
- The MongoDB driver handles connection pooling and automatic reconnection internally.

## Lineage dataset URI

`mongodb://<host>:<port>/<database>/<collection>` (credentials stripped) — e.g. `mongodb://host:27017/mydb/events`.

## License

Licensed under MIT or Apache-2.0.
