# faucet-sink-mysql

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-mysql.svg)](https://crates.io/crates/faucet-sink-mysql)
[![Docs.rs](https://docs.rs/faucet-sink-mysql/badge.svg)](https://docs.rs/faucet-sink-mysql)

MySQL sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a MySQL table using either JSON column mode (storing each record as a serialized JSON string) or AutoMap mode (mapping top-level JSON keys directly to table columns). Uses connection pooling via `sqlx` and efficient multi-row `INSERT` statements with backtick-quoted identifiers.

`write_batch` accepts whatever slice the pipeline hands it. When `batch_size > 0` and the slice is larger than `batch_size`, the sink re-chunks internally and issues one multi-row `INSERT` per chunk; when `batch_size = 0`, the entire slice is sent in a single `INSERT` — see [Streaming and batching](#streaming-and-batching) for the tradeoffs.

## Installation

```toml
[dependencies]
faucet-sink-mysql = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "1.0", features = ["sink-mysql"] }
```

## Quick Start

```rust
use faucet_sink_mysql::{MysqlSink, MysqlSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = MysqlSinkConfig::new(
        "mysql://user:password@localhost:3306/mydb",
        "events",
    );

    let sink = MysqlSink::new(config).await?;

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
| `connection_url` | `String` | *(required)* | MySQL connection URL (e.g. `mysql://user:pass@host:3306/db`) |
| `table_name` | `String` | *(required)* | Target table name |
| `column_mapping` | `MysqlColumnMapping` | `Json { column: "data" }` | How to map JSON records to table columns (see below) |
| `batch_size` | `usize` | `1000` | Maximum rows per multi-row `INSERT`. See [Streaming and batching](#streaming-and-batching) below |
| `max_connections` | `u32` | `5` | Maximum number of connections in the connection pool |
| `write_mode` | `"append"` \| `"upsert"` \| `"delete"` | `"append"` | Write semantics. `upsert`/`delete` require `column_mapping: auto_map` and a non-empty `key`. See [Write modes](#write-modes-upsert--delete). |
| `key` | `[String]` | `[]` | Key columns for `upsert`/`delete`. The table must have a PRIMARY or UNIQUE index on these columns. |
| `delete_marker` | `{ field, values }` | *(none)* | Upsert-only: rows whose `field` matches one of `values` are treated as deletes; all others are upserts. The marker field is stripped before writing. |

The `Debug` implementation masks the `connection_url` with `***` to prevent credential leakage in logs.

### Streaming and batching

The MySQL sink re-chunks each incoming `StreamPage` to keep individual
multi-row `INSERT` statements under MySQL's `max_allowed_packet` limit.

- **`batch_size > 0`** (default `1000`) — the sink slices the incoming slice
  into `batch_size`-row chunks and issues one multi-row `INSERT INTO ...
  VALUES (...), (...), ...` per chunk. **Recommended value is `1000`**:
  that's the multi-row INSERT sweet spot for MySQL (small enough to stay
  well under the default 64MB `max_allowed_packet` even for wide rows,
  large enough to amortise per-statement overhead). Bump it higher when
  rows are narrow; drop it when rows are wide enough to push individual
  chunks past `max_allowed_packet`.
- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream
  `StreamPage` is forwarded in a single multi-row `INSERT`. Use this when
  the source already emits page sizes tuned for MySQL — for example a
  Postgres source configured with `batch_size: 1000`. Larger pages risk a
  `Packet too large` error from MySQL's `max_allowed_packet` limit.

`batch_size` is purely a chunk-size knob — the SQL semantics, identifier
quoting, and column-mapping behaviour are unchanged.

### Column Mapping (`MysqlColumnMapping`)

| Variant | Description |
|---------|-------------|
| `Json { column }` | Insert each record as a serialized JSON string in a single column. The column name defaults to `"data"` but can be overridden. Uses a multi-row `INSERT INTO t (col) VALUES (?), (?), ...` for efficiency. |
| `AutoMap` | Map top-level JSON keys directly to table columns. Column names are discovered from `INFORMATION_SCHEMA.COLUMNS`. Values are bound as **native MySQL types** (strings as text, JSON numbers as integer/double, booleans as `TINYINT` 0/1, arrays/objects as JSON text). The INSERT column set is the **union** of record keys across the batch, so a field present only in a later record is still written; a row missing a column binds SQL `NULL`. Only keys that match existing columns are inserted; extra keys are silently ignored. Records with no matching keys are skipped with a warning. |

### Builder Methods

```rust
use faucet_sink_mysql::{MysqlSinkConfig, MysqlColumnMapping};

// JSON mode with custom column name
let config = MysqlSinkConfig::new("mysql://localhost/mydb", "events")
    .column_mapping(MysqlColumnMapping::Json { column: "payload".into() })
    .with_batch_size(1000)
    .max_connections(10);

// AutoMap mode
let config = MysqlSinkConfig::new("mysql://localhost/mydb", "events")
    .column_mapping(MysqlColumnMapping::AutoMap)
    .with_batch_size(250);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_mysql::MysqlSinkConfig;

// From a JSON file
let config: MysqlSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: MysqlSinkConfig = load_env_file(".env", "MYSQL_SINK")?;
```

### Example JSON config (JSON mode)

```json
{
  "connection_url": "mysql://writer:s3cret@db.example.com:3306/analytics",
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
  "connection_url": "mysql://writer:s3cret@db.example.com:3306/analytics",
  "table_name": "events",
  "column_mapping": "auto_map",
  "batch_size": 1000,
  "max_connections": 10
}
```

### Example .env file

```env
MYSQL_SINK_CONNECTION_URL=mysql://writer:s3cret@db.example.com:3306/analytics
MYSQL_SINK_TABLE_NAME=raw_events
MYSQL_SINK_COLUMN_MAPPING='{"json":{"column":"data"}}'
MYSQL_SINK_BATCH_SIZE=1000
MYSQL_SINK_MAX_CONNECTIONS=5
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = MysqlSink::new(config).await?;
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_mysql::{MysqlSink, MysqlSinkConfig, MysqlColumnMapping};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/orders")
);

let sink_config = MysqlSinkConfig::new(
    "mysql://writer:pass@localhost:3306/app",
    "orders",
)
.column_mapping(MysqlColumnMapping::AutoMap);

let sink = MysqlSink::new(sink_config).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
```

## Examples

### JSON mode -- store records as serialized JSON

```sql
-- Table schema
CREATE TABLE raw_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```rust
let config = MysqlSinkConfig::new(
    "mysql://localhost/analytics",
    "raw_events",
)
.column_mapping(MysqlColumnMapping::Json { column: "data".into() })
.with_batch_size(1000);

let sink = MysqlSink::new(config).await?;
sink.write_batch(&records).await?;
```

### AutoMap mode -- map JSON keys to table columns

```sql
-- Table schema
CREATE TABLE events (
    user_id VARCHAR(255),
    event VARCHAR(255),
    amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```rust
let config = MysqlSinkConfig::new(
    "mysql://localhost/analytics",
    "events",
)
.column_mapping(MysqlColumnMapping::AutoMap)
.with_batch_size(1000)
.max_connections(10);

let sink = MysqlSink::new(config).await?;

let records = vec![
    json!({"user_id": "u1", "event": "purchase", "amount": 29.99}),
    json!({"user_id": "u2", "event": "signup"}),
];
sink.write_batch(&records).await?;
```

### High-throughput connection pooling

```rust
let config = MysqlSinkConfig::new(
    "mysql://writer:pass@db-primary.internal:3306/warehouse",
    "metrics",
)
.max_connections(20)
.with_batch_size(1000);

let sink = MysqlSink::new(config).await?;
```

## How It Works

- A connection pool is created in `MysqlSink::new()` using `sqlx::MySqlPool` with the configured `max_connections`.
- `write_batch()` slices the input into `batch_size`-row chunks (or forwards the whole slice when `batch_size = 0`) and inserts each chunk using a single multi-row INSERT statement.
- In JSON mode, each record is serialized to a JSON string and inserted as `INSERT INTO t (col) VALUES (?), (?), ...`.
- In AutoMap mode, column names are queried from `INFORMATION_SCHEMA.COLUMNS` for the current database. A multi-row INSERT is built dynamically with `?` placeholders. Column values are bound as **native MySQL types** (#78/#4). The column set is the **union** of record keys across the batch, so a field present only in a later record is still written; a row missing a column binds SQL `NULL`. The INSERT is sub-chunked so `rows × columns` never exceeds MySQL's 65,535-placeholder limit.
- All identifiers (table names, column names) are quoted with backticks using MySQL-safe escaping (embedded backticks are doubled).

## Write modes (upsert / delete)

`MysqlSink` supports `write_mode: upsert` and `write_mode: delete` in addition to the default `write_mode: append`.

**Requirements:**
- `column_mapping` must be `auto_map` — key columns must be real table columns, not packed inside a JSON blob.
- `key` must be a non-empty list of column names.
- The target table must have a **PRIMARY or UNIQUE key** on those columns. MySQL's `ON DUPLICATE KEY UPDATE` detects conflicts via that index — the `key` config field drives which columns are updated, not which index is used. All non-key columns in the INSERT are set from `VALUES(col)`.

**`write_mode: upsert`** — each record is INSERT … ON DUPLICATE KEY UPDATE (last-write-wins). Optionally, a `delete_marker` field routes certain records to deletes instead:

```yaml
pipeline:
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@localhost:3306/warehouse
      table_name: products
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker:
        field: __op
        values: [d, delete]
```

**`write_mode: delete`** — every record in the page is deleted by its key:

```yaml
pipeline:
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@localhost:3306/warehouse
      table_name: products
      column_mapping: auto_map
      write_mode: delete
      key: [id]
```

**Behaviour:**
- The planner (`plan_writes`) deduplicates within each page (last-write-wins), so a page that contains the same key twice produces exactly one MySQL statement touching that row.
- Upserts and deletes within a page are applied inside **one transaction**, so they commit atomically or not at all.
- The `delete_marker` field is stripped from upsert records before writing.

## Exactly-once delivery

`MysqlSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes `records` and UPSERTs the `token` into a `_faucet_commit_token(scope VARCHAR, token VARCHAR)` watermark table inside the **same transaction**, so both either commit together or neither does.
- `last_committed_token(scope)` — reads the current watermark to let the pipeline skip already-committed pages on resume.

To use exactly-once delivery, set `delivery: exactly_once` in your pipeline config and pair this sink with one of the CDC sources (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in exactly-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
pipeline:
  source:
    type: mysql-cdc
    config:
      connection_url: mysql://faucet:faucet@localhost:3306/appdb
      server_id: 1
  sink:
    type: mysql
    config:
      connection_url: mysql://writer:pass@localhost:3306/warehouse
      table_name: change_events
      column_mapping: auto_map
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

See the [Exactly-once delivery cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/state.html#exactly-once-delivery) for full rationale and the supported source/sink set.

## Lineage dataset URI

`mysql://<host>:<port>/<db>?table=<table>` (credentials stripped) — e.g. `mysql://host:3306/app?table=orders`.

## License

Licensed under MIT or Apache-2.0.
