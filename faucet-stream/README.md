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
| `source-grpc` | no | gRPC — dynamic protobuf via prost-reflect (unary + server-streaming) |
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
| `transform-keys-case` | yes (via source-rest) | Re-case every key (snake / camel / pascal / kebab / screaming_snake) |
| `transform-select` | no | Keep only listed top-level fields |
| `transform-drop` | no | Remove listed top-level fields |
| `transform-set` | no | Add/overwrite top-level fields with constants |
| `transform-rename-field` | no | Exact-name field rename (single or batch) |
| `transform-cast` | no | Per-field type coercion with configurable `on_error` |
| `transform-redact` | no | Replace listed field values with a mask |
| `transform-value-case` | no | Lowercase / uppercase / trim string field values |
| `transform-spell-symbols` | no | Spell out symbols in keys (`%` → `percent`, `#` → `number`, …) |
| `transforms` | no | All built-in transforms |

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
            .auth(Auth::Bearer {
                token: "my-token".into(),
            })
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

Runnable examples live in [`examples/`](examples/). Each one declares its required Cargo features so `cargo check --all-targets` skips examples whose connectors aren't enabled.

### Pipeline shape (these run end-to-end against `jsonplaceholder.typicode.com`)

| Example | What it shows |
|---------|---------------|
| `rest_to_jsonl` | Minimum-viable pipeline: REST source → JSONL sink |
| `rest_streaming` | `run_stream` mode — write each page as it arrives, bounded memory |

### Connector matrix (compile-only without external infra)

The remaining examples illustrate popular source→sink pairings. They compile under their feature gates, but actually running them needs the listed infrastructure (DB, S3 bucket, GCP credentials, etc.).

| Example | Source → Sink |
|---------|---------------|
| `rest_to_postgres` | REST → PostgreSQL — canonical API → operational-DB ELT |
| `rest_to_bigquery` | REST → BigQuery |
| `rest_to_s3` | REST → S3 — data-lake landing zone |
| `graphql_to_postgres` | GraphQL → PostgreSQL |
| `graphql_to_bigquery` | GraphQL → BigQuery |
| `xml_to_s3` | XML/SOAP → S3 |
| `xml_to_mongodb` | XML/SOAP → MongoDB |
| `grpc_to_elasticsearch` | gRPC → Elasticsearch |
| `grpc_to_http` | gRPC → HTTP — also demonstrates the full HTTP sink builder (auth, headers, method, batch mode) |
| `postgres_to_bigquery` | PostgreSQL → BigQuery |
| `postgres_to_snowflake` | PostgreSQL → Snowflake (key-pair auth) |
| `postgres_to_s3` | PostgreSQL → S3 archive |
| `postgres_to_elasticsearch` | PostgreSQL → Elasticsearch search index |
| `mysql_to_postgres` | MySQL → PostgreSQL |
| `mysql_to_snowflake` | MySQL → Snowflake (OAuth) |
| `mysql_to_bigquery` | MySQL → BigQuery |
| `sqlite_to_csv` | SQLite → CSV |
| `sqlite_to_jsonl` | SQLite → JSONL |
| `s3_to_postgres` | S3 → PostgreSQL |
| `s3_to_mongodb` | S3 → MongoDB |
| `s3_to_bigquery` | S3 → BigQuery — classic data-lake → DW |
| `s3_to_snowflake` | S3 → Snowflake — data-lake → DW (alt) |
| `mongodb_to_elasticsearch` | MongoDB → Elasticsearch |
| `mongodb_to_redis` | MongoDB → Redis stream |
| `mongodb_to_postgres` | MongoDB → PostgreSQL — document → relational mirror |
| `redis_to_mysql` | Redis stream → MySQL |
| `redis_to_sqlite` | Redis list → SQLite |
| `webhook_to_http` | Webhook → HTTP forwarder |
| `webhook_to_csv` | Webhook → CSV |
| `webhook_to_postgres` | Webhook → PostgreSQL — durable webhook capture |
| `csv_to_mysql` | CSV → MySQL |
| `csv_to_sqlite` | CSV → SQLite |
| `csv_to_bigquery` | CSV → BigQuery |
| `elasticsearch_to_s3` | Elasticsearch → S3 backup |
| `elasticsearch_to_redis` | Elasticsearch → Redis cache |

Run any example with the feature flags it documents at the top of the file, e.g.:

```bash
cargo run -p faucet-stream --example postgres_to_bigquery \
    --features "source-postgres sink-bigquery"
```

## What's Re-exported

This crate re-exports everything from `faucet-core` unconditionally:

- `Source`, `Sink` traits
- `Pipeline`, `PipelineResult`, `run_stream`
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
