# faucet-sink-csv

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-csv.svg)](https://crates.io/crates/faucet-sink-csv)
[![Docs.rs](https://docs.rs/faucet-sink-csv/badge.svg)](https://docs.rs/faucet-sink-csv)

CSV file sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a CSV file. Column order is the union of keys across the records of the first `write_batch()` call. Supports configurable delimiters, optional header rows, and append mode. Uses `spawn_blocking` to avoid blocking the async runtime during file I/O.

## Installation

```toml
[dependencies]
faucet-sink-csv = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-csv"] }
```

## Quick Start

```rust
use faucet_sink_csv::{CsvSink, CsvSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CsvSinkConfig::new("/tmp/output.csv");
    let sink = CsvSink::new(config);

    let records = vec![
        json!({"id": 1, "name": "Alice", "email": "alice@example.com"}),
        json!({"id": 2, "name": "Bob", "email": "bob@example.com"}),
    ];

    let written = sink.write_batch(&records).await?;
    sink.flush().await?;

    println!("Wrote {written} rows to CSV");
    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | `String` | *(required)* | Path to the output CSV file |
| `delimiter` | `u8` | `b','` (comma) | Field delimiter byte |
| `write_headers` | `bool` | `true` | Whether to write a header row with column names |
| `append` | `bool` | `false` | Whether to append to an existing file. When `false`, the file is truncated on open. |
| `batch_size` | `usize` | `1000` | Records per upstream `StreamPage`. **No behavioural impact** at this sink — present for symmetry. See [Streaming and batching](#streaming-and-batching). |

## Streaming and batching

This sink writes rows to the output CSV file one at a time via a buffered `csv::Writer` running inside `tokio::task::spawn_blocking`. The per-page memory bound for the pipeline is set by the **source's** `batch_size` (the size of each `StreamPage` that `Pipeline::run` hands to `Sink::write_batch`); how that page is then iterated record-by-record on the sink side is what determines on-disk output, and that path does not depend on `batch_size` at all.

`batch_size` is exposed on this config purely for symmetry across every sink in the workspace — sinks like `faucet-sink-postgres` or `faucet-sink-bigquery` use the field to size their multi-row inserts / streaming-insert requests, but a per-record file sink has nothing to tune. `batch_size = 0` (the "no batching" sentinel) and any positive value are observably identical for this sink: both produce byte-for-byte the same `.csv` file.

### Builder Methods

```rust
use faucet_sink_csv::CsvSinkConfig;

let config = CsvSinkConfig::new("/data/output.tsv")
    .delimiter(b'\t')
    .write_headers(true)
    .append(false);
```

### Column Order and Missing Fields

- Column order is the **union of keys across all records in the first** `write_batch()` call, in first-seen order — so a field present only in a later record of that batch is still captured (audit #146 H2).
- All subsequent records use the same column order, regardless of their key order.
- If a record is missing a field that exists in the column list, an empty string is written for that cell.
- Keys that appear only in a **later** `write_batch()` call (after the header is written) are still ignored — the header is fixed once on the first call.

### Value Conversion

| JSON Type | CSV Output |
|-----------|-----------|
| `null` | empty string |
| `string` | the string value |
| `number` | string representation |
| `boolean` | `"true"` or `"false"` |
| `object` / `array` | JSON serialization (e.g. `{"nested":true}`) |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_csv::CsvSinkConfig;

// From a JSON file
let config: CsvSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: CsvSinkConfig = load_env_file(".env", "CSV_SINK")?;
```

### Example JSON config

```json
{
  "path": "/data/exports/users.csv",
  "delimiter": 44,
  "write_headers": true,
  "append": false,
  "batch_size": 1000
}
```

Note: The `delimiter` is a byte value (44 = comma, 9 = tab, 124 = pipe `|`).

### Example JSON config (TSV)

```json
{
  "path": "/data/exports/users.tsv",
  "delimiter": 9,
  "write_headers": true,
  "append": false,
  "batch_size": 1000
}
```

### Example .env file

```env
CSV_SINK_PATH=/data/exports/users.csv
CSV_SINK_DELIMITER=44
CSV_SINK_WRITE_HEADERS=true
CSV_SINK_APPEND=false
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = CsvSink::new(config);
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_csv::{CsvSink, CsvSinkConfig};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/users")
);

let sink = CsvSink::new(CsvSinkConfig::new("/data/users.csv"));

let result = Pipeline::new(source, sink).run().await?;
println!("Exported {} records to CSV", result.records_written);
```

## Examples

### Basic CSV export

```rust
let config = CsvSinkConfig::new("/tmp/users.csv");
let sink = CsvSink::new(config);

let records = vec![
    json!({"id": 1, "name": "Alice", "email": "alice@example.com"}),
    json!({"id": 2, "name": "Bob", "email": "bob@example.com"}),
];

sink.write_batch(&records).await?;
sink.flush().await?;
```

Output (`/tmp/users.csv`):
```csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

### Tab-separated output (TSV)

```rust
let config = CsvSinkConfig::new("/tmp/data.tsv")
    .delimiter(b'\t');

let sink = CsvSink::new(config);
sink.write_batch(&records).await?;
sink.flush().await?;
```

### Append mode for incremental exports

```rust
// First run: creates the file with headers
let sink = CsvSink::new(CsvSinkConfig::new("/data/events.csv"));
sink.write_batch(&batch_1).await?;
sink.flush().await?;
drop(sink);

// Second run: appends without headers
let sink = CsvSink::new(
    CsvSinkConfig::new("/data/events.csv")
        .append(true)
        .write_headers(false)
);
sink.write_batch(&batch_2).await?;
sink.flush().await?;
```

### Without header row

```rust
let config = CsvSinkConfig::new("/tmp/data.csv")
    .write_headers(false);

let sink = CsvSink::new(config);
sink.write_batch(&records).await?;
sink.flush().await?;
```

## How It Works

- The file is opened lazily on the first `write_batch()` call. Column order is the union of keys across the records of the first `write_batch()` call.
- Missing parent directories of `path` are created automatically (equivalent to `mkdir -p`) before the file is opened, matching the behaviour of the parquet sink. Dated-subdirectory paths such as `./data/dt=2026-03-08/part.csv` work without any pre-creation step.
- All CSV I/O runs inside `tokio::task::spawn_blocking` to avoid blocking the async runtime.
- A `Mutex` protects the writer state (column order + csv::Writer) for thread-safe access.
- Headers are written only on file creation (not in append mode) when `write_headers` is `true`.
- Multiple `write_batch()` calls accumulate rows in the same file using the same column order.
- Call `flush()` to ensure all buffered data is written to disk before dropping the sink or reading the file.

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

`flush()` explicitly finalises the compression encoder (writing the gzip/zstd trailer) and propagates any trailer-write I/O error, rather than swallowing it on drop — so a bookmark never advances over a truncated/corrupt file. Subsequent writes append a fresh member. Headers are emitted only on the first open.

## License

Licensed under MIT or Apache-2.0.
