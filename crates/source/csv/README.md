# faucet-source-csv

[![Crates.io](https://img.shields.io/crates/v/faucet-source-csv.svg)](https://crates.io/crates/faucet-source-csv)
[![Docs.rs](https://docs.rs/faucet-source-csv/badge.svg)](https://docs.rs/faucet-source-csv)

A CSV file source that reads rows from CSV files and returns them as JSON objects, with configurable delimiters, headers, and quote characters.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-csv = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-csv"] }
```

## Quick Start

```rust
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CsvSourceConfig::new("/path/to/data.csv");

    let source = CsvSource::new(config);
    let records = source.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## How It Works

- If the file has headers, each row becomes a JSON object with header names as keys
- If the file has no headers, keys are generated as `column_0`, `column_1`, etc.
- All field values are returned as JSON strings (no type inference)
- CSV reading is performed on a blocking thread via `spawn_blocking` to avoid starving the async runtime

## Configuration

### CsvSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | `String` | *(required)* | Path to the CSV file |
| `has_headers` | `bool` | `true` | Whether the file has a header row |
| `delimiter` | `u8` | `b','` (comma) | Field delimiter byte |
| `quote` | `u8` | `b'"'` (double quote) | Quote character byte |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_csv::CsvSourceConfig;

let config: CsvSourceConfig = load_json("config.json")?;
let config: CsvSourceConfig = load_env_file(".env", "CSV_SOURCE")?;
```

### Example JSON config

```json
{
  "path": "/data/exports/customers.csv",
  "has_headers": true,
  "delimiter": 44,
  "quote": 34
}
```

Note: `delimiter` and `quote` are specified as byte values (44 = comma, 34 = double quote, 9 = tab).

### Example .env file

```env
CSV_SOURCE_PATH=/data/exports/customers.csv
CSV_SOURCE_HAS_HEADERS=true
CSV_SOURCE_DELIMITER=44
CSV_SOURCE_QUOTE=34
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = CsvSource::new(config);
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Reading a standard CSV file

```rust
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use faucet_core::Source;

let config = CsvSourceConfig::new("/data/users.csv");
let source = CsvSource::new(config);
let records = source.fetch_all().await?;

// Example record: {"id": "1", "name": "Alice", "email": "alice@example.com"}
for record in &records {
    println!("User: {}", record["name"]);
}
```

### Reading a TSV (tab-separated) file

```rust
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use faucet_core::Source;

let config = CsvSourceConfig::new("/data/export.tsv")
    .delimiter(b'\t');

let source = CsvSource::new(config);
let records = source.fetch_all().await?;
```

### Reading a file without headers

```rust
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use faucet_core::Source;

let config = CsvSourceConfig::new("/data/raw_data.csv")
    .has_headers(false);

let source = CsvSource::new(config);
let records = source.fetch_all().await?;

// Keys are generated: column_0, column_1, column_2, ...
println!("First field: {}", records[0]["column_0"]);
```

### Pipe-delimited file with single-quote quoting

```rust
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use faucet_core::Source;

let config = CsvSourceConfig::new("/data/legacy_export.csv")
    .delimiter(b'|')
    .quote(b'\'');

let source = CsvSource::new(config);
let records = source.fetch_all().await?;
```

## License

Licensed under MIT or Apache-2.0.
