# faucet-source-postgres

[![Crates.io](https://img.shields.io/crates/v/faucet-source-postgres.svg)](https://crates.io/crates/faucet-source-postgres)
[![Docs.rs](https://docs.rs/faucet-source-postgres/badge.svg)](https://docs.rs/faucet-source-postgres)

A PostgreSQL query source that executes SQL queries and returns rows as JSON records, with connection pooling via sqlx.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-postgres = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-postgres"] }
```

## Quick Start

```rust
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PostgresSourceConfig::new(
        "postgres://user:password@localhost:5432/mydb",
        "SELECT id, name, email FROM users WHERE active = true",
    );

    let source = PostgresSource::new(config).await?;
    let records = source.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### PostgresSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | `String` | *(required)* | PostgreSQL connection URL (e.g. `postgres://user:pass@host:5432/db`). Masked in debug output for security |
| `query` | `String` | *(required)* | SQL query to execute |
| `params` | `Vec<Value>` | `[]` | Bind parameters for the query (positional: `$1`, `$2`, etc.) |
| `max_connections` | `u32` | `10` | Maximum number of connections in the pool |

### Supported Column Types

Columns are automatically converted to JSON values:

| PostgreSQL Type | JSON Type |
|-----------------|-----------|
| `json`, `jsonb` | Native JSON value |
| `text`, `varchar`, `char` | `string` |
| `int8`, `bigint` | `number` (i64) |
| `int4`, `integer` | `number` (i32) |
| `int2`, `smallint` | `number` (i16) |
| `float8`, `double precision` | `number` (f64) |
| `float4`, `real` | `number` (f32) |
| `bool`, `boolean` | `boolean` |
| Other / `NULL` | `null` |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_postgres::PostgresSourceConfig;

let config: PostgresSourceConfig = load_json("config.json")?;
let config: PostgresSourceConfig = load_env_file(".env", "PG_SOURCE")?;
```

### Example JSON config

```json
{
  "connection_url": "postgres://analytics:password@db.example.com:5432/warehouse",
  "query": "SELECT id, name, created_at, metadata FROM events WHERE created_at > $1 ORDER BY created_at",
  "params": ["2025-01-01T00:00:00Z"],
  "max_connections": 5
}
```

### Example .env file

```env
PG_SOURCE_CONNECTION_URL=postgres://user:password@localhost:5432/mydb
PG_SOURCE_QUERY=SELECT * FROM users
PG_SOURCE_MAX_CONNECTIONS=10
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = PostgresSource::new(config).await?;
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Simple query

```rust
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use faucet_core::Source;

let config = PostgresSourceConfig::new(
    "postgres://localhost/mydb",
    "SELECT id, name, email FROM customers ORDER BY id",
);
let source = PostgresSource::new(config).await?;
let customers = source.fetch_all().await?;
```

### Parameterized query

```rust
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use faucet_core::Source;
use serde_json::json;

let config = PostgresSourceConfig::new(
    "postgres://localhost/mydb",
    "SELECT * FROM orders WHERE status = $1 AND total > $2",
)
.params(vec![json!("shipped"), json!(100.0)]);

let source = PostgresSource::new(config).await?;
let orders = source.fetch_all().await?;
```

### Custom connection pool size

```rust
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};

let config = PostgresSourceConfig::new(
    "postgres://localhost/mydb",
    "SELECT * FROM large_table",
)
.with_max_connections(20);

let source = PostgresSource::new(config).await?;
let records = source.fetch_all().await?;
```

## License

Licensed under MIT or Apache-2.0.
