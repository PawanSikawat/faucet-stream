# faucet-sink-jsonl

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-jsonl.svg)](https://crates.io/crates/faucet-sink-jsonl)
[![Docs.rs](https://docs.rs/faucet-sink-jsonl/badge.svg)](https://docs.rs/faucet-sink-jsonl)

JSON Lines file sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a file in [JSON Lines](https://jsonlines.org/) format (one JSON object per line). The file is opened lazily on the first write. Uses buffered async I/O via `tokio::io::BufWriter` for high throughput. Supports append mode, pretty-printing, and explicit flush control.

## Installation

```toml
[dependencies]
faucet-sink-jsonl = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-jsonl"] }
```

## Quick Start

```rust
use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = JsonlSinkConfig::new("/tmp/output.jsonl");
    let sink = JsonlSink::new(config);

    let records = vec![
        json!({"id": 1, "name": "Alice", "email": "alice@example.com"}),
        json!({"id": 2, "name": "Bob", "email": "bob@example.com"}),
    ];

    let written = sink.write_batch(&records).await?;
    sink.flush().await?;

    println!("Wrote {written} records");
    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | `PathBuf` | *(required)* | Path to the output file |
| `append` | `bool` | `false` | Whether to append to an existing file. When `false`, the file is truncated on open. |
| `pretty` | `bool` | `false` | Whether to pretty-print each JSON record with indentation. Note: this breaks strict JSONL format since records span multiple lines. |
| `batch_size` | `usize` | `1000` | Records per upstream `StreamPage`. **No behavioural impact** at this sink — present for symmetry. See [Streaming and batching](#streaming-and-batching). |

## Streaming and batching

This sink writes records to the output file one at a time via `tokio::io::BufWriter`. The per-page memory bound for the pipeline is set by the **source's** `batch_size` (the size of each `StreamPage` that `Pipeline::run` hands to `Sink::write_batch`); how that page is then iterated record-by-record on the sink side is what determines on-disk output, and that path does not depend on `batch_size` at all.

`batch_size` is exposed on this config purely for symmetry across every sink in the workspace — sinks like `faucet-sink-postgres` or `faucet-sink-bigquery` use the field to size their multi-row inserts / streaming-insert requests, but a per-record file sink has nothing to tune. `batch_size = 0` (the "no batching" sentinel) and any positive value are observably identical for this sink: both produce byte-for-byte the same `.jsonl` file.

### Builder Methods

```rust
use faucet_sink_jsonl::JsonlSinkConfig;

let config = JsonlSinkConfig::new("/data/output.jsonl")
    .append(true)
    .pretty(false);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_jsonl::JsonlSinkConfig;

// From a JSON file
let config: JsonlSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: JsonlSinkConfig = load_env_file(".env", "JSONL_SINK")?;
```

### Example JSON config

```json
{
  "path": "/data/exports/events.jsonl",
  "append": false,
  "pretty": false,
  "batch_size": 1000
}
```

### Example .env file

```env
JSONL_SINK_PATH=/data/exports/events.jsonl
JSONL_SINK_APPEND=false
JSONL_SINK_PRETTY=false
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = JsonlSink::new(config);
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};

let source_config = RestStreamConfig::new("https://api.example.com", "/v1/events");
let source = RestStream::new(source_config);

let sink_config = JsonlSinkConfig::new("/data/events.jsonl");
let sink = JsonlSink::new(sink_config);

let result = Pipeline::new(source, sink).run().await?;
println!("Exported {} records to JSONL", result.records_written);
```

## Examples

### Basic file export (truncate mode)

```rust
let config = JsonlSinkConfig::new("/tmp/users.jsonl");
let sink = JsonlSink::new(config);

sink.write_batch(&records).await?;
sink.flush().await?;
```

Output (`/tmp/users.jsonl`):
```
{"id":1,"name":"Alice","email":"alice@example.com"}
{"id":2,"name":"Bob","email":"bob@example.com"}
```

### Append mode for incremental exports

```rust
// First run: creates the file
let sink = JsonlSink::new(JsonlSinkConfig::new("/data/events.jsonl"));
sink.write_batch(&batch_1).await?;
sink.flush().await?;
drop(sink);

// Second run: appends to existing file
let sink = JsonlSink::new(JsonlSinkConfig::new("/data/events.jsonl").append(true));
sink.write_batch(&batch_2).await?;
sink.flush().await?;
```

### Pretty-printed output for debugging

```rust
let config = JsonlSinkConfig::new("/tmp/debug.json").pretty(true);
let sink = JsonlSink::new(config);
sink.write_batch(&records).await?;
sink.flush().await?;
```

Output:
```json
{
  "id": 1,
  "name": "Alice"
}
{
  "id": 2,
  "name": "Bob"
}
```

## How It Works

- The file is opened lazily on the first `write_batch()` call and wrapped in a `tokio::io::BufWriter` for efficient buffered writes.
- A `Mutex` protects the writer for thread-safe concurrent access.
- Each record is serialized to a single JSON line (or pretty-printed if configured) followed by a newline character.
- Multiple `write_batch()` calls accumulate data in the same file without re-opening it.
- Call `flush()` to ensure all buffered data is written to disk. This is important before dropping the sink or reading the file.
- In truncate mode (default), the file is emptied on first write. In append mode, new records are added after existing content.

## License

Licensed under MIT or Apache-2.0.
