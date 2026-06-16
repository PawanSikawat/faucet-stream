# faucet-source-sqlite

[![Crates.io](https://img.shields.io/crates/v/faucet-source-sqlite.svg)](https://crates.io/crates/faucet-source-sqlite)
[![Docs.rs](https://docs.rs/faucet-source-sqlite/badge.svg)](https://docs.rs/faucet-source-sqlite)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-sqlite.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-sqlite.svg)](https://github.com/PawanSikawat/faucet-stream#license)

A SQLite query source that executes SQL queries and returns rows as JSON records, with connection pooling and dynamic type probing via sqlx.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```bash
cargo add faucet-source-sqlite
cargo add tokio --features full
```

Or via the umbrella crate:
```bash
cargo add faucet-stream --features source-sqlite
```

## Quick Start

```rust
use faucet_source_sqlite::{SqliteSource, SqliteSourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SqliteSourceConfig::new(
        "sqlite:data.db",
        "SELECT id, name, score FROM students ORDER BY score DESC",
    );

    let source = SqliteSource::new(config).await?;
    let records = source.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### SqliteSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url` | `String` | *(required)* | SQLite database URL. Can be a file path (e.g. `"sqlite:data.db"`, `"sqlite:/path/to/db"`) or in-memory (`"sqlite::memory:"`) |
| `query` | `String` | *(required)* | SQL query to execute |
| `max_connections` | `u32` | `10` | Maximum number of connections in the pool |
| `batch_size` | `usize` | `1000` | Rows per `StreamPage` emitted by `Source::stream_pages`. See [Streaming and batching](#streaming-and-batching) below |

### Streaming and batching

`SqliteSource::stream_pages` drives a sqlx row cursor (`Query::fetch`)
without buffering the full result. Rows are accumulated into a `batch_size`
buffer and yielded as a `StreamPage` once the buffer fills; the trailing
partial page (if any) is yielded after the cursor drains.

`batch_size = 0` is the **"no batching" sentinel** — the cursor is drained
completely and the entire result set is emitted in a single `StreamPage`.
Use it for small lookup tables, or for downstream sinks (SQL `COPY`,
BigQuery load jobs, Snowflake stage uploads) that prefer one large request
to many small ones. Values larger than `MAX_BATCH_SIZE` (1,000,000) are
rejected by `faucet_core::validate_batch_size`.

The sqlite query source has no incremental-replication mode, so every
emitted page carries `bookmark: None`.

> **Note** — SQLite is an in-process, file-based engine: there is no
> server-side cursor concept and no network wire to worry about. The
> streaming implementation bounds *client-side* memory at `O(batch_size)`
> and lets the sink begin writing as soon as the first batch is parsed
> off disk, rather than waiting for the whole result set to materialise
> in a `Vec`.

### Supported Column Types

SQLite has dynamic typing -- values are stored as INTEGER, REAL, TEXT, BLOB, or NULL. The source probes each column value in order of specificity:

| SQLite Storage Class | JSON Type |
|---------------------|-----------|
| TEXT (valid JSON) | Native JSON value |
| TEXT | `string` |
| INTEGER (i64) | `number` |
| INTEGER (i32) | `number` |
| REAL (f64) | `number` |
| BOOLEAN | `boolean` |
| BLOB | `string` (base64) |
| NULL / unsupported | `null` |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_sqlite::SqliteSourceConfig;

let config: SqliteSourceConfig = load_json("config.json")?;
let config: SqliteSourceConfig = load_env_file(".env", "SQLITE_SOURCE")?;
```

### Example JSON config

```json
{
  "database_url": "sqlite:/var/data/app.db",
  "query": "SELECT id, name, created_at, json_extract(metadata, '$.tags') AS tags FROM items WHERE active = 1",
  "max_connections": 5,
  "batch_size": 5000
}
```

### Example .env file

```env
SQLITE_SOURCE_DATABASE_URL=sqlite:data.db
SQLITE_SOURCE_QUERY=SELECT * FROM events
SQLITE_SOURCE_MAX_CONNECTIONS=10
SQLITE_SOURCE_BATCH_SIZE=1000
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = SqliteSource::new(config).await?;
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### File-based database

```rust
use faucet_source_sqlite::{SqliteSource, SqliteSourceConfig};
use faucet_core::Source;

let config = SqliteSourceConfig::new(
    "sqlite:analytics.db",
    "SELECT date, event_type, COUNT(*) as count FROM events GROUP BY date, event_type",
);
let source = SqliteSource::new(config).await?;
let records = source.fetch_all().await?;
```

### In-memory database

```rust
use faucet_source_sqlite::{SqliteSource, SqliteSourceConfig};
use faucet_core::Source;

let config = SqliteSourceConfig::new(
    "sqlite::memory:",
    "SELECT 1 AS id, 'hello' AS message",
);
let source = SqliteSource::new(config).await?;
let records = source.fetch_all().await?;
assert_eq!(records[0]["id"], 1);
assert_eq!(records[0]["message"], "hello");
```

### Custom pool size for concurrent access

```rust
use faucet_source_sqlite::{SqliteSource, SqliteSourceConfig};

let config = SqliteSourceConfig::new(
    "sqlite:shared.db",
    "SELECT * FROM large_table",
)
.with_max_connections(5);

let source = SqliteSource::new(config).await?;
let records = source.fetch_all().await?;
```

## Lineage dataset URI

`sqlite://<path>?query=<sql>` — e.g. `sqlite:///var/db/app.db?query=SELECT id FROM events`.

## License

Licensed under MIT or Apache-2.0.
