# faucet-source-mysql

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mysql.svg)](https://crates.io/crates/faucet-source-mysql)
[![Docs.rs](https://docs.rs/faucet-source-mysql/badge.svg)](https://docs.rs/faucet-source-mysql)

A MySQL query source that executes SQL queries and returns rows as JSON records, with connection pooling via sqlx.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-mysql = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-mysql"] }
```

## Quick Start

```rust
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = MysqlSourceConfig::new(
        "mysql://user:password@localhost:3306/mydb",
        "SELECT id, name, email FROM users WHERE active = 1",
    );

    let source = MysqlSource::new(config).await?;
    let records = source.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### MysqlSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | `String` | *(required)* | MySQL connection URL (e.g. `mysql://user:pass@host:3306/db`). Masked in debug output for security |
| `query` | `String` | *(required)* | SQL query to execute |
| `max_connections` | `u32` | `10` | Maximum number of connections in the pool |

### Supported Column Types

Columns are automatically converted to JSON values:

| MySQL Type | JSON Type |
|------------|-----------|
| `json` | Native JSON value |
| `varchar`, `text`, `char` | `string` |
| `bigint` | `number` (i64) |
| `int`, `mediumint` | `number` (i32) |
| `smallint`, `tinyint` | `number` (i16) |
| `double` | `number` (f64) |
| `float` | `number` (f32) |
| `tinyint(1)`, `boolean` | `boolean` |
| Other / `NULL` | `null` |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_mysql::MysqlSourceConfig;

let config: MysqlSourceConfig = load_json("config.json")?;
let config: MysqlSourceConfig = load_env_file(".env", "MYSQL_SOURCE")?;
```

### Example JSON config

```json
{
  "connection_url": "mysql://analytics:password@db.example.com:3306/warehouse",
  "query": "SELECT id, name, created_at, status FROM orders WHERE created_at > '2025-01-01' ORDER BY created_at",
  "max_connections": 5
}
```

### Example .env file

```env
MYSQL_SOURCE_CONNECTION_URL=mysql://user:password@localhost:3306/mydb
MYSQL_SOURCE_QUERY=SELECT * FROM users
MYSQL_SOURCE_MAX_CONNECTIONS=10
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = MysqlSource::new(config).await?;
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Simple query

```rust
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use faucet_core::Source;

let config = MysqlSourceConfig::new(
    "mysql://localhost/mydb",
    "SELECT id, name, email FROM customers ORDER BY id",
);
let source = MysqlSource::new(config).await?;
let customers = source.fetch_all().await?;
```

### Custom connection pool size

```rust
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};

let config = MysqlSourceConfig::new(
    "mysql://localhost/mydb",
    "SELECT * FROM large_table LIMIT 50000",
)
.with_max_connections(20);

let source = MysqlSource::new(config).await?;
let records = source.fetch_all().await?;
```

### Using with a Pipeline

```rust
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use faucet_core::{Pipeline, Source, Sink};

let config = MysqlSourceConfig::new(
    "mysql://localhost/production",
    "SELECT * FROM events WHERE date >= CURDATE() - INTERVAL 7 DAY",
);
let source = MysqlSource::new(config).await?;
let pipeline = Pipeline::new(Box::new(source), Box::new(my_sink));
let result = pipeline.run().await?;
```

## License

Licensed under MIT or Apache-2.0.
