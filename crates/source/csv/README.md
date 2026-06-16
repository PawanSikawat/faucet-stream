# faucet-source-csv

[![Crates.io](https://img.shields.io/crates/v/faucet-source-csv.svg)](https://crates.io/crates/faucet-source-csv)
[![Docs.rs](https://docs.rs/faucet-source-csv/badge.svg)](https://docs.rs/faucet-source-csv)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-csv.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-csv.svg)](https://github.com/PawanSikawat/faucet-stream#license)

A CSV file source that reads rows from CSV files and returns them as JSON objects, with configurable delimiters, headers, and quote characters.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```bash
cargo add faucet-source-csv
cargo add tokio --features full
```

Or via the umbrella crate:
```bash
cargo add faucet-stream --features source-csv
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
- `fetch_all` / `fetch_with_context` read the file via blocking I/O on a `spawn_blocking` task to avoid starving the async runtime
- `Source::stream_pages` reads the file via async line-streaming on a tokio `BufReader` and parses each line through a single-record `csv::ReaderBuilder` parse

## Configuration

### CsvSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | `String` | *(required)* | Path to the CSV file |
| `has_headers` | `bool` | `true` | Whether the file has a header row |
| `delimiter` | `u8` | `b','` (comma) | Field delimiter byte |
| `quote` | `u8` | `b'"'` (double quote) | Quote character byte |
| `batch_size` | `usize` | `DEFAULT_BATCH_SIZE` (1000) | Rows per emitted `StreamPage` in `Source::stream_pages`. `0` is the "no batching" sentinel — emits all rows in a single page |

### Streaming and batching

`CsvSource::stream_pages` is a true client-side stream: it opens the file via
`tokio::fs::File` + `tokio::io::BufReader`, reads the header line first (if
`has_headers`), then iterates the remaining lines via
`AsyncBufReadExt::lines`. Each line is parsed through a single-record
`csv::ReaderBuilder` so quoted fields containing the delimiter
(e.g. `"hello, world"`) parse correctly. There is no server-side concern —
the file is consumed lazily from the local filesystem, so client-side memory
is bounded at O(`batch_size`) regardless of file size.

`batch_size = 0` is the "no batching" sentinel: the file is fully drained
and emitted as one page. Useful for small lookup tables or for sinks (SQL
`COPY`, BigQuery load jobs) that prefer one large request to many small
ones.

#### Multi-line quoted records

Parsing uses `csv-async`, a streaming RFC-4180 reader that tracks quote
state across physical lines. Quoted fields containing embedded newlines
(and embedded delimiters) are parsed correctly as a single record, so a
file produced by `faucet-sink-csv` round-trips back losslessly through
both `fetch_all` and the `stream_pages` streaming path.

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

## Compression

Behind the crate-local `compression` Cargo feature. Adds a `compression` config
field with values `none`, `gzip`, `zstd`, or `auto` (the default — detects
`.gz` / `.zst` from the file path / object key).

YAML example:

```yaml
kind: csv
config:
  # ... existing fields ...
  compression: auto  # or 'gzip' | 'zstd' | 'none'
```

Compression is detected from the file path. Multi-line quoted fields (records with embedded newlines inside quotes) are parsed correctly on both the streaming and `fetch_all` paths, regardless of compression.

## Lineage dataset URI

`file://<path>` — e.g. `file:///data/input.csv`.

## License

Licensed under MIT or Apache-2.0.
