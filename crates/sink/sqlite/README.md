# faucet-sink-sqlite

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-sqlite.svg)](https://crates.io/crates/faucet-sink-sqlite)
[![Docs.rs](https://docs.rs/faucet-sink-sqlite/badge.svg)](https://docs.rs/faucet-sink-sqlite)

SQLite sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a SQLite table using either JSON column mode (storing each record as a serialized JSON text value) or AutoMap mode (mapping top-level JSON keys directly to table columns). Uses connection pooling via `sqlx`, multi-row `INSERT` statements, and wraps each batch in a transaction (`BEGIN`/`COMMIT`) for maximum write throughput. Column discovery uses `PRAGMA table_info`.

`write_batch` accepts whatever slice the pipeline hands it. When `batch_size > 0` and the slice is larger than `batch_size`, the sink re-chunks internally and issues one multi-row INSERT (in its own transaction) per chunk; when `batch_size = 0`, the entire slice is written in a single transaction — see [Streaming and batching](#streaming-and-batching) for the tradeoffs.

## Installation

```toml
[dependencies]
faucet-sink-sqlite = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-sqlite"] }
```

## Quick Start

```rust
use faucet_sink_sqlite::{SqliteSink, SqliteSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SqliteSinkConfig::new("sqlite:///tmp/app.db", "events");
    let sink = SqliteSink::new(config).await?;

    let records = vec![
        json!({"user_id": "u123", "event": "signup"}),
        json!({"user_id": "u456", "event": "login"}),
    ];

    let rows_written = sink.write_batch(&records).await?;
    println!("Wrote {rows_written} rows");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url` | `String` | *(required)* | SQLite database URL. Can be a file path (e.g. `/tmp/app.db`), a `sqlite:` URL, or `sqlite::memory:` for an in-memory database. |
| `table_name` | `String` | *(required)* | Target table name |
| `column_mapping` | `SqliteColumnMapping` | `Json { column: "data" }` | How to map JSON records to table columns (see below) |
| `batch_size` | `usize` | `1000` | Maximum number of rows per multi-row INSERT. See [Streaming and batching](#streaming-and-batching) below |
| `max_connections` | `u32` | `1` | Maximum number of connections in the connection pool. SQLite serializes writers at the file level, so one writer is the safe default — a multi-connection pool against a single file races for the write lock and risks `SQLITE_BUSY`. The pool opens connections in WAL mode with a 5s `busy_timeout`, so raising this lets extra connections read concurrently with the single writer. |

### Streaming and batching

The SQLite sink re-chunks each incoming `StreamPage` to keep individual
multi-row INSERT statements within SQLite's per-statement parameter limits
and to amortise per-transaction overhead.

- **`batch_size > 0`** (default `1000`) — the sink slices the incoming
  slice into `batch_size`-row chunks and issues one multi-row INSERT per
  chunk, each wrapped in its own `BEGIN`/`COMMIT` transaction.
  **Recommended value is `1000`**: large enough to amortise transaction
  overhead, small enough to stay well under SQLite's default
  `SQLITE_MAX_VARIABLE_NUMBER` (32766 since 3.32.0). In AutoMap mode the
  sink also splits each chunk further so `rows × columns` never exceeds that
  limit, so a wide table no longer fails with "too many SQL variables"
  regardless of `batch_size`.
- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream
  `StreamPage` is written in a single multi-row INSERT inside one
  transaction. Use this when the source already emits page sizes tuned for
  SQLite — for example a Postgres source with `batch_size: 1000`. Pages
  large enough to push the parameter count past SQLite's per-statement
  limit will fail at the prepare step.

`batch_size` is purely a chunk-size knob — transaction wrapping (one
`BEGIN`/`COMMIT` per chunk), identifier quoting, and per-record error
reporting are unchanged.

### Column Mapping (`SqliteColumnMapping`)

| Variant | Description |
|---------|-------------|
| `Json { column }` | Insert each record as a serialized JSON text string in a single column. The column name defaults to `"data"` but can be overridden. Uses a multi-row INSERT wrapped in a transaction. |
| `AutoMap` | Map top-level JSON keys directly to table columns. Column names are discovered using `PRAGMA table_info(table_name)`. Only keys that match existing columns are inserted; extra keys are silently ignored. Records with no matching keys are skipped with a warning. |

### Builder Methods

```rust
use faucet_sink_sqlite::{SqliteSinkConfig, SqliteColumnMapping};

// JSON mode with custom column
let config = SqliteSinkConfig::new("sqlite:///data/app.db", "events")
    .column_mapping(SqliteColumnMapping::Json { column: "payload".into() })
    .with_batch_size(1000)
    .max_connections(3);

// AutoMap mode
let config = SqliteSinkConfig::new("sqlite:///data/app.db", "events")
    .column_mapping(SqliteColumnMapping::AutoMap)
    .with_batch_size(250);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_sqlite::SqliteSinkConfig;

// From a JSON file
let config: SqliteSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: SqliteSinkConfig = load_env_file(".env", "SQLITE_SINK")?;
```

### Example JSON config (JSON mode)

```json
{
  "database_url": "/data/analytics.db",
  "table_name": "raw_events",
  "column_mapping": {
    "json": {
      "column": "data"
    }
  },
  "batch_size": 1000,
  "max_connections": 5
}
```

### Example JSON config (AutoMap mode)

```json
{
  "database_url": "/data/analytics.db",
  "table_name": "events",
  "column_mapping": "auto_map",
  "batch_size": 1000,
  "max_connections": 3
}
```

### Example .env file

```env
SQLITE_SINK_DATABASE_URL=/data/analytics.db
SQLITE_SINK_TABLE_NAME=raw_events
SQLITE_SINK_COLUMN_MAPPING='{"json":{"column":"data"}}'
SQLITE_SINK_BATCH_SIZE=1000
SQLITE_SINK_MAX_CONNECTIONS=5
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = SqliteSink::new(config).await?;
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_sqlite::{SqliteSink, SqliteSinkConfig, SqliteColumnMapping};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events")
);

let sink_config = SqliteSinkConfig::new("/data/events.db", "events")
    .column_mapping(SqliteColumnMapping::AutoMap);

let sink = SqliteSink::new(sink_config).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
```

## Examples

### JSON mode -- store records as serialized JSON text

```sql
-- Table schema
CREATE TABLE raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
```

```rust
let config = SqliteSinkConfig::new("/tmp/app.db", "raw_events")
    .column_mapping(SqliteColumnMapping::Json { column: "data".into() })
    .with_batch_size(1000);

let sink = SqliteSink::new(config).await?;
sink.write_batch(&records).await?;
```

### AutoMap mode -- map JSON keys to table columns

```sql
-- Table schema
CREATE TABLE events (
    user_id TEXT,
    event TEXT,
    amount REAL,
    created_at TEXT DEFAULT (datetime('now'))
);
```

```rust
let config = SqliteSinkConfig::new("/tmp/app.db", "events")
    .column_mapping(SqliteColumnMapping::AutoMap)
    .with_batch_size(1000);

let sink = SqliteSink::new(config).await?;

let records = vec![
    json!({"user_id": "u1", "event": "purchase", "amount": 29.99}),
    json!({"user_id": "u2", "event": "signup"}),  // missing "amount" will be null
];
sink.write_batch(&records).await?;
```

### In-memory database for testing

```rust
let config = SqliteSinkConfig::new("sqlite::memory:", "test_table")
    .column_mapping(SqliteColumnMapping::AutoMap);

let sink = SqliteSink::new(config).await?;
```

## How It Works

- A connection pool is created in `SqliteSink::new()` using `sqlx::SqlitePool` with the configured `max_connections` (default `1`). Each connection is opened in WAL journal mode (`journal_mode = WAL`) with a 5-second `busy_timeout` and `create_if_missing`, so a writer and readers can proceed concurrently and lock contention waits-and-retries instead of failing immediately with `SQLITE_BUSY`. WAL on a `sqlite::memory:` database is a harmless no-op.
- `write_batch()` slices the input into `batch_size`-row chunks (or forwards the whole slice when `batch_size = 0`). Each chunk is inserted using a single multi-row INSERT statement wrapped in a `BEGIN`/`COMMIT` transaction for write performance.
- In JSON mode, each record is serialized to a JSON string and inserted as `INSERT INTO t (col) VALUES (?), (?), ...`.
- In AutoMap mode, column names are discovered using `PRAGMA table_info(table_name)`. A multi-row INSERT is built dynamically. Column values are bound as **native SQLite types** — strings as `TEXT`, JSON numbers as `INTEGER`/`REAL`, booleans as `INTEGER` 0/1 — so column affinity and typed reads round-trip correctly. Arrays and objects (which have no scalar SQL representation) are bound as their JSON text. The INSERT column set is the **union** of record keys across the batch (in table order), so a field present only in a later record is still written; a row missing a column binds SQL `NULL`.
- All identifiers (table names, column names) are quoted using `quote_ident()` (double-quote escaping) to prevent SQL injection.
- Transaction wrapping ensures that either all rows in a batch are committed or none are, providing atomicity per batch.

## License

Licensed under MIT or Apache-2.0.
