# faucet-source-sqlite

[![Crates.io](https://img.shields.io/crates/v/faucet-source-sqlite.svg)](https://crates.io/crates/faucet-source-sqlite)
[![Docs.rs](https://docs.rs/faucet-source-sqlite/badge.svg)](https://docs.rs/faucet-source-sqlite)

A SQLite query source that executes SQL queries and returns rows as JSON records, with connection pooling and dynamic type probing via sqlx.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-sqlite = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-sqlite"] }
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
  "max_connections": 5
}
```

### Example .env file

```env
SQLITE_SOURCE_DATABASE_URL=sqlite:data.db
SQLITE_SOURCE_QUERY=SELECT * FROM events
SQLITE_SOURCE_MAX_CONNECTIONS=10
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

## License

Licensed under MIT or Apache-2.0.
