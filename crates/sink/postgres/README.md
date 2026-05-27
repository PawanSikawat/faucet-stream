# faucet-sink-postgres

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-postgres.svg)](https://crates.io/crates/faucet-sink-postgres)
[![Docs.rs](https://docs.rs/faucet-sink-postgres/badge.svg)](https://docs.rs/faucet-sink-postgres)

PostgreSQL sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a PostgreSQL table using either JSONB column mode (storing each record as a single `jsonb` value) or AutoMap mode (mapping top-level JSON keys directly to table columns). Uses connection pooling via `sqlx` and efficient multi-row `INSERT` statements.

`write_batch` accepts whatever slice the pipeline hands it. When `batch_size > 0` and the slice is larger than `batch_size`, the sink re-chunks internally and issues one multi-row `INSERT` per chunk; when `batch_size = 0`, the entire slice is sent in a single `INSERT` — see [Streaming and batching](#streaming-and-batching) for the tradeoffs.

## Installation

```toml
[dependencies]
faucet-sink-postgres = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-postgres"] }
```

## Quick Start

```rust
use faucet_sink_postgres::{PostgresSink, PostgresSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PostgresSinkConfig::new(
        "postgres://user:password@localhost:5432/mydb",
        "events",
    );

    let sink = PostgresSink::new(config).await?;

    let records = vec![
        json!({"user_id": "u123", "event": "signup", "metadata": {"source": "web"}}),
        json!({"user_id": "u456", "event": "login", "metadata": {"source": "mobile"}}),
    ];

    let rows_written = sink.write_batch(&records).await?;
    println!("Wrote {rows_written} rows");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | `String` | *(required)* | PostgreSQL connection URL (e.g. `postgres://user:pass@host:5432/db`) |
| `table_name` | `String` | *(required)* | Target table name |
| `column_mapping` | `PostgresColumnMapping` | `Jsonb { column: "data" }` | How to map JSON records to table columns (see below) |
| `batch_size` | `usize` | `1000` | Maximum rows per multi-row `INSERT`. See [Streaming and batching](#streaming-and-batching) below |
| `max_connections` | `u32` | `5` | Maximum number of connections in the connection pool |

The `Debug` implementation masks the `connection_url` with `***` to prevent credential leakage in logs.

### Streaming and batching

The PostgreSQL sink re-chunks each incoming `StreamPage` so individual
multi-row `INSERT` statements stay well under Postgres' per-statement
bind-parameter limit.

- **`batch_size > 0`** (default `1000`) — the sink slices the incoming
  slice into `batch_size`-row chunks and issues one multi-row `INSERT`
  per chunk. **Recommended value is `1000`**: Postgres' multi-row
  `INSERT` sweet spot. Larger chunks rarely add throughput. AutoMap mode
  binds one parameter per column per row; the sink now splits each chunk
  further so `rows × columns` never exceeds Postgres' 65 535 bind-parameter
  ceiling, so a wide table no longer causes the server to reject the
  statement regardless of `batch_size`. JSONB mode binds a single `jsonb[]`
  array regardless of row count.
- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream
  `StreamPage` is forwarded in a single logical write. Use this when the
  source already emits page sizes tuned for Postgres — for example a
  REST source with `batch_size: 1000` feeding a JSONB table. AutoMap still
  sub-splits internally to respect the 65 535-parameter ceiling.

`batch_size` is purely a chunk-size knob — connection pooling, identifier
quoting, and JSONB vs AutoMap behaviour are unchanged.

### Column Mapping (`PostgresColumnMapping`)

| Variant | Description |
|---------|-------------|
| `Jsonb { column }` | Insert each record as a single `jsonb` column. The column name defaults to `"data"` but can be overridden. Uses PostgreSQL's `unnest($1::jsonb[])` for efficient batch inserts. |
| `AutoMap` | Map top-level JSON keys directly to table columns. Column names are discovered from `information_schema.columns`. Only keys that match existing columns are inserted; extra keys are silently ignored. Records with no matching keys are skipped with a warning. |

### Builder Methods

```rust
use faucet_sink_postgres::{PostgresSinkConfig, PostgresColumnMapping};

// JSONB mode with custom column name
let config = PostgresSinkConfig::new("postgres://localhost/mydb", "events")
    .column_mapping(PostgresColumnMapping::Jsonb { column: "payload".into() })
    .with_batch_size(1000)
    .max_connections(10);

// AutoMap mode
let config = PostgresSinkConfig::new("postgres://localhost/mydb", "events")
    .column_mapping(PostgresColumnMapping::AutoMap)
    .with_batch_size(250);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_postgres::PostgresSinkConfig;

// From a JSON file
let config: PostgresSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: PostgresSinkConfig = load_env_file(".env", "PG_SINK")?;
```

### Example JSON config (JSONB mode)

```json
{
  "connection_url": "postgres://writer:s3cret@db.example.com:5432/analytics",
  "table_name": "raw_events",
  "column_mapping": {
    "jsonb": {
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
  "connection_url": "postgres://writer:s3cret@db.example.com:5432/analytics",
  "table_name": "events",
  "column_mapping": "auto_map",
  "batch_size": 1000,
  "max_connections": 10
}
```

### Example .env file

```env
PG_SINK_CONNECTION_URL=postgres://writer:s3cret@db.example.com:5432/analytics
PG_SINK_TABLE_NAME=raw_events
PG_SINK_COLUMN_MAPPING='{"jsonb":{"column":"data"}}'
PG_SINK_BATCH_SIZE=1000
PG_SINK_MAX_CONNECTIONS=5
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = PostgresSink::new(config).await?;
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_postgres::{PostgresSink, PostgresSinkConfig, PostgresColumnMapping};

let source_config = RestStreamConfig::new("https://api.example.com", "/v1/users");
let source = RestStream::new(source_config);

let sink_config = PostgresSinkConfig::new(
    "postgres://writer:pass@localhost:5432/app",
    "users",
)
.column_mapping(PostgresColumnMapping::AutoMap);

let sink = PostgresSink::new(sink_config).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
```

## Examples

### JSONB mode -- store entire records in a single column

This mode is ideal for schemaless ingestion where you want to store raw JSON and query it later with PostgreSQL's JSONB operators.

```sql
-- Table schema
CREATE TABLE raw_events (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

```rust
let config = PostgresSinkConfig::new(
    "postgres://localhost/analytics",
    "raw_events",
)
.column_mapping(PostgresColumnMapping::Jsonb { column: "data".into() })
.with_batch_size(1000)
.max_connections(5);

let sink = PostgresSink::new(config).await?;
sink.write_batch(&records).await?;
```

### AutoMap mode -- map JSON keys to table columns

This mode automatically discovers column names from the table schema and maps matching JSON keys. Columns use SQL-injection-safe quoted identifiers.

```sql
-- Table schema
CREATE TABLE events (
    user_id TEXT,
    event TEXT,
    timestamp TIMESTAMPTZ,
    amount NUMERIC
);
```

```rust
let config = PostgresSinkConfig::new(
    "postgres://localhost/analytics",
    "events",
)
.column_mapping(PostgresColumnMapping::AutoMap)
.with_batch_size(1000)
.max_connections(10);

let sink = PostgresSink::new(config).await?;

let records = vec![
    json!({"user_id": "u1", "event": "purchase", "amount": 29.99}),
    json!({"user_id": "u2", "event": "signup"}),  // missing "amount" will be null
];
sink.write_batch(&records).await?;
```

### Connection pooling for high-throughput workloads

```rust
let config = PostgresSinkConfig::new(
    "postgres://writer:pass@db-primary.internal:5432/warehouse",
    "metrics",
)
.max_connections(20)
.with_batch_size(1000);

let sink = PostgresSink::new(config).await?;
```

## How It Works

- A connection pool is created in `PostgresSink::new()` using `sqlx::PgPool` with the configured `max_connections`.
- `write_batch()` slices records into chunks of `batch_size` (or forwards the whole slice when `batch_size = 0`) and inserts each chunk using a single multi-row INSERT statement.
- In JSONB mode, inserts use `INSERT INTO table (col) SELECT * FROM unnest($1::jsonb[])` for maximum efficiency.
- In AutoMap mode, column names are queried from `information_schema.columns`. A multi-row `INSERT INTO ... VALUES ($1, $2), ($3, $4), ...` is built dynamically. Column values are bound as JSONB. Missing keys are bound as null.
- All identifiers (table names, column names) are quoted using `quote_ident()` to prevent SQL injection.

## License

Licensed under MIT or Apache-2.0.
