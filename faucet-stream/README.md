# faucet-stream

[![Crates.io](https://img.shields.io/crates/v/faucet-stream.svg)](https://crates.io/crates/faucet-stream)
[![Docs.rs](https://docs.rs/faucet-stream/badge.svg)](https://docs.rs/faucet-stream)

Umbrella crate for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem. Provides feature-gated access to all source and sink connectors through a single dependency.

## Installation

```toml
[dependencies]
# Default: REST source only
faucet-stream = "0.2"

# All sources
faucet-stream = { version = "0.2", features = ["source"] }

# All sinks
faucet-stream = { version = "0.2", features = ["sink"] }

# Everything
faucet-stream = { version = "0.2", features = ["full"] }

# Pick what you need
faucet-stream = { version = "0.2", features = ["source-rest", "source-s3", "sink-postgres", "sink-jsonl"] }
```

## Feature Flags

### Source Connectors

| Feature | Default | Crate |
|---------|---------|-------|
| `source-rest` | yes | REST API — auth, pagination, extraction, transforms |
| `source-graphql` | no | GraphQL API — cursor pagination, variable injection |
| `source-xml` | no | XML/SOAP API — XML-to-JSON conversion |
| `source-grpc` | no | gRPC — dynamic protobuf via prost-reflect |
| `source-postgres` | no | PostgreSQL — SQL queries as JSON |
| `source-mysql` | no | MySQL — SQL queries as JSON |
| `source-sqlite` | no | SQLite — SQL queries as JSON |
| `source-s3` | no | AWS S3 — read JSONL, JSON array, or raw text |
| `source-mongodb` | no | MongoDB — find() with filter/projection/sort |
| `source-redis` | no | Redis — streams, lists, or key patterns |
| `source-webhook` | no | Webhook — HTTP server collecting POST payloads |
| `source-csv` | no | CSV — read CSV files as JSON objects |
| `source-elasticsearch` | no | Elasticsearch — search/scroll API |

### Sink Connectors

| Feature | Default | Crate |
|---------|---------|-------|
| `sink-bigquery` | no | Google BigQuery — streaming inserts |
| `sink-postgres` | no | PostgreSQL — JSONB or auto-mapped columns |
| `sink-jsonl` | no | JSON Lines — file output |
| `sink-snowflake` | no | Snowflake — SQL REST API with JWT/OAuth |
| `sink-mysql` | no | MySQL — JSON or auto-mapped columns |
| `sink-sqlite` | no | SQLite — JSON or auto-mapped columns |
| `sink-s3` | no | AWS S3 — write JSONL files |
| `sink-mongodb` | no | MongoDB — insert_many |
| `sink-redis` | no | Redis — streams, lists, key-value |
| `sink-csv` | no | CSV — write JSON as CSV rows |
| `sink-elasticsearch` | no | Elasticsearch — bulk index API |
| `sink-http` | no | HTTP — POST records to any endpoint |

### Aggregate Features

| Feature | Description |
|---------|-------------|
| `source` | All source connectors |
| `sink` | All sink connectors |
| `full` | Every connector |

### Transform Features

| Feature | Default | Description |
|---------|---------|-------------|
| `transform-flatten` | yes (via source-rest) | Flatten nested objects |
| `transform-rename-keys` | yes (via source-rest) | Regex key renaming |
| `transform-snake-case` | yes (via source-rest) | snake_case normalisation |
| `transforms` | no | All transforms |

## Quick Start

```rust
use faucet_stream::{
    RestStream, RestStreamConfig, Auth, PaginationStyle,
    Pipeline,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure a source
    let source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/users")
            .auth(Auth::Bearer("my-token".into()))
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.meta.next_cursor".into(),
                param_name: "cursor".into(),
            }),
    )?;

    let records = source.fetch_all().await?;
    println!("Fetched {} records", records.len());
    Ok(())
}
```

### Pipeline: Source to Sink

```rust
use faucet_stream::{Pipeline, run_stream};

// Batch mode: fetch all, then write
let result = Pipeline::new(&source, &sink).run().await?;
println!("Wrote {} records", result.records_written);

// Streaming mode: write page-by-page (bounded memory)
let result = run_stream(source.stream_pages(), &sink).await?;
```

## Examples

Runnable examples live in [`examples/`](examples/):

| Example | What it shows |
|---------|---------------|
| `rest_to_jsonl` | Minimum-viable pipeline: REST source → JSONL sink |
| `rest_streaming` | `run_stream` mode — write each page as it arrives, bounded memory |
| `dag_users_posts` | `SourceDAG` — fetch users, then per-user posts with parent context injected |

Run any of them with:

```bash
cargo run -p faucet-stream --example rest_to_jsonl --features "source-rest sink-jsonl"
```

All three examples hit `https://jsonplaceholder.typicode.com` and write to `/tmp/`.

## What's Re-exported

This crate re-exports everything from `faucet-core` unconditionally:

- `Source`, `Sink` traits
- `Pipeline`, `PipelineResult`, `run_stream`
- `SourceDAG`, `DagNode`, `DagResult`, `DagNodeResult`, `DagNodeError`
- `FaucetError`
- `RecordTransform`, `ReplicationMethod`
- `config::load_json`, `config::load_env`, `config::load_env_file`
- `async_trait`, `serde_json`, `Value`, `json!`, `JsonSchema`, `schema_for!`

Plus all types from enabled connector features (e.g. `RestStream`, `RestStreamConfig`, `Auth`, `PaginationStyle` when `source-rest` is enabled).

## Using Individual Crates

You can also depend on connector crates directly instead of using the umbrella:

```toml
[dependencies]
faucet-core = "0.1"
faucet-source-rest = "0.1"
faucet-sink-postgres = "0.1"
```

This gives finer control over dependencies and compile times.

## License

Licensed under either of [MIT](../LICENSE-MIT) or [Apache-2.0](../LICENSE-APACHE) at your option.
