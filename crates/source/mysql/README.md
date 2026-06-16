# faucet-source-mysql

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mysql.svg)](https://crates.io/crates/faucet-source-mysql)
[![Docs.rs](https://docs.rs/faucet-source-mysql/badge.svg)](https://docs.rs/faucet-source-mysql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-mysql.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-mysql.svg)](https://github.com/PawanSikawat/faucet-stream#license)

A MySQL query source that executes SQL queries and returns rows as JSON records, with connection pooling via sqlx.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```bash
cargo add faucet-source-mysql
cargo add tokio --features full
```

Or via the umbrella crate:
```bash
cargo add faucet-stream --features source-mysql
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
| `batch_size` | `usize` | `1000` | Rows per `StreamPage` emitted by `Source::stream_pages`. See [Streaming and batching](#streaming-and-batching) below |

### Streaming and batching

`MysqlSource::stream_pages` drives a sqlx row cursor (`Query::fetch`)
without buffering the full result. Rows are accumulated into a `batch_size`
buffer and yielded as a `StreamPage` once the buffer fills; the trailing
partial page (if any) is yielded after the cursor drains.

`batch_size = 0` is the **"no batching" sentinel** — the cursor is drained
completely and the entire result set is emitted in a single `StreamPage`.
Use it for small lookup tables, or for downstream sinks (SQL `COPY`,
BigQuery load jobs, Snowflake stage uploads) that prefer one large request
to many small ones. Values larger than `MAX_BATCH_SIZE` (1,000,000) are
rejected by `faucet_core::validate_batch_size`.

The mysql query source has no incremental-replication mode, so every
emitted page carries `bookmark: None`.

> **Note** — MySQL's wire protocol sends rows from a simple `SELECT` in a
> single response (no server-side cursor). The streaming implementation
> bounds *client-side* memory at `O(batch_size)` and lets the sink begin
> writing as soon as the first batch is parsed off the wire. True
> server-side cursor streaming (via stored procedures or `STREAM`-style
> options) is tracked separately as a follow-up.

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
| `datetime`, `timestamp`, `date`, `time` | `string` (RFC 3339 / ISO-8601) |
| `decimal`, `numeric` | `string` (exact precision preserved) |
| `blob`, `binary`, `varbinary` | `string` (base64) |
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
  "max_connections": 5,
  "batch_size": 5000
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

## Lineage dataset URI

`mysql://<host>:<port>/<db>?query=<sql>` (credentials stripped) — e.g. `mysql://host:3306/app?query=SELECT id FROM orders`.

## License

Licensed under MIT or Apache-2.0.
