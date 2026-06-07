# faucet-source-s3

[![Crates.io](https://img.shields.io/crates/v/faucet-source-s3.svg)](https://crates.io/crates/faucet-source-s3)
[![Docs.rs](https://docs.rs/faucet-source-s3/badge.svg)](https://docs.rs/faucet-source-s3)

An AWS S3 source that reads objects from a bucket and parses them as JSON Lines, JSON arrays, or raw text, with concurrent object reads via `buffer_unordered`.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-s3 = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "1.0", features = ["source-s3"] }
```

## Quick Start

```rust
use faucet_source_s3::{S3Source, S3SourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = S3SourceConfig::new("my-data-bucket")
        .prefix("exports/2025/")
        .region("us-west-2");

    let source = S3Source::new(config).await?;
    let records = source.fetch_all().await?;

    println!("Read {} records from S3", records.len());
    Ok(())
}
```

## Configuration

### S3SourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | `String` | *(required)* | S3 bucket name |
| `prefix` | `Option<String>` | `None` | Object key prefix filter. Only objects whose key starts with this prefix are read |
| `region` | `Option<String>` | `None` | AWS region. `None` uses the SDK default (from env vars or instance metadata) |
| `endpoint_url` | `Option<String>` | `None` | Custom endpoint URL for S3-compatible services (e.g. MinIO, LocalStack) |
| `file_format` | `S3FileFormat` | `JsonLines` | Format of the files to read |
| `max_objects` | `Option<usize>` | `None` | Maximum number of objects to read. `None` means read all matching objects |
| `concurrency` | `usize` | `10` | Maximum number of concurrent object reads |
| `batch_size` | `usize` | `1000` | Records per `StreamPage` emitted by `Source::stream_pages`. See [Streaming and batching](#streaming-and-batching) below |

### Streaming and batching

`S3Source::stream_pages` lists objects matching the configured prefix and
streams their records into `StreamPage`s without buffering the full scan.
The exact behaviour depends on `file_format`:

- **`JsonLines`** — the object body is decoded line-by-line via
  `tokio::io::AsyncBufReadExt::lines`, so client-side memory is bounded at
  `O(batch_size)` lines regardless of file or scan size. Records flow
  across object boundaries — a single page may carry lines drawn from
  multiple objects.
- **`RawText`** — each object contributes exactly one record
  (`{"key": ..., "content": ...}`). The contract requires the whole file
  as a single string, so the object is necessarily held in memory once,
  but it is streamed straight into that one `String` via the same
  decoding reader `JsonLines` uses (no separate raw + decompressed copies
  for compressed objects). Records are then accumulated into the same
  `batch_size` buffer JsonLines uses.
- **`JsonArray`** — the array can only be validated once the closing `]`
  is observed, so each object is buffered fully and then its records are
  chunked into pages of `batch_size`. This bounds *page* size at
  `batch_size` but not *peak per-object* memory — sources that must
  stream arbitrarily large array files should be re-emitted as `JsonLines`
  upstream. The default `batch_size = 1000` keeps allocation reasonable
  for typical sizes.

> **Memory ceiling — `RawText` / `JsonArray`.** Both formats hold one
> whole decoded object in memory at a time (this is inherent: `RawText`'s
> record *is* the whole file, and a JSON array isn't valid until its
> closing `]`). Because objects are fetched concurrently, peak memory is
> bounded by roughly **`concurrency` × (largest object's decoded size)**,
> not by `batch_size`. If you read large `RawText` / `JsonArray` objects,
> lower `concurrency` to cap peak memory, or re-emit the data as
> `JsonLines` upstream so it streams line-by-line at `O(batch_size)`.

`batch_size = 0` is the **"no batching" sentinel**: every emitted
`StreamPage` corresponds to exactly one S3 object — no within-object
chunking and no cross-object accumulation. Use it for small lookup files,
or for downstream sinks (SQL `COPY`, BigQuery load jobs, Snowflake stage
uploads) that prefer one large request per file to many small ones.
Values larger than `MAX_BATCH_SIZE` (1,000,000) are rejected by
`faucet_core::validate_batch_size`.

The S3 source has no incremental-replication mode today, so every emitted
page carries `bookmark: None`.

> **Note** — `JsonArray` streaming is bounded at one full object in memory
> at a time. True incremental JSON-array parsing (e.g. via
> `serde_json::StreamDeserializer` over an `AsyncRead`) is a separate
> follow-up; today the parse happens after the body is fully buffered.

### File Formats (S3FileFormat)

| Variant | Description | Record Output |
|---------|-------------|---------------|
| `JsonLines` (default) | Each line in the file is a separate JSON record | One record per non-empty line |
| `JsonArray` | The entire file is a JSON array of records | One record per array element |
| `RawText` | Each file becomes a single record | `{"key": "<object-key>", "content": "<file-text>"}` |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_s3::S3SourceConfig;

let config: S3SourceConfig = load_json("config.json")?;
let config: S3SourceConfig = load_env_file(".env", "S3_SOURCE")?;
```

### Example JSON config

```json
{
  "bucket": "my-data-lake",
  "prefix": "raw/events/2025-03/",
  "region": "us-east-1",
  "file_format": "json_lines",
  "max_objects": 100,
  "concurrency": 20,
  "batch_size": 5000
}
```

### Example .env file

```env
S3_SOURCE_BUCKET=my-data-lake
S3_SOURCE_PREFIX=raw/events/
S3_SOURCE_REGION=us-east-1
S3_SOURCE_CONCURRENCY=10
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = S3Source::new(config).await?;
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Reading JSON Lines files from S3

```rust
use faucet_source_s3::{S3Source, S3SourceConfig};
use faucet_core::Source;

let config = S3SourceConfig::new("analytics-bucket")
    .prefix("logs/2025/03/")
    .region("us-west-2")
    .concurrency(20);

let source = S3Source::new(config).await?;
let records = source.fetch_all().await?;
println!("Read {} log records", records.len());
```

### Reading JSON array files from MinIO

```rust
use faucet_source_s3::{S3Source, S3SourceConfig, S3FileFormat};
use faucet_core::Source;

let config = S3SourceConfig::new("local-bucket")
    .endpoint_url("http://localhost:9000")
    .region("us-east-1")
    .file_format(S3FileFormat::JsonArray)
    .prefix("exports/");

let source = S3Source::new(config).await?;
let records = source.fetch_all().await?;
```

### Reading raw text files

```rust
use faucet_source_s3::{S3Source, S3SourceConfig, S3FileFormat};
use faucet_core::Source;

let config = S3SourceConfig::new("documents-bucket")
    .prefix("reports/")
    .file_format(S3FileFormat::RawText)
    .max_objects(50);

let source = S3Source::new(config).await?;
let records = source.fetch_all().await?;

// Each record has "key" and "content" fields
for record in &records {
    println!("File: {}, Size: {} bytes",
        record["key"].as_str().unwrap(),
        record["content"].as_str().unwrap().len()
    );
}
```

## AWS Authentication

This source uses the standard AWS SDK credential chain. Credentials are resolved automatically from (in order):

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
2. AWS config files (`~/.aws/credentials`, `~/.aws/config`)
3. IAM instance roles (on EC2/ECS/Lambda)

No credential fields are included in the config -- use the standard AWS environment instead.

## Compression

Behind the crate-local `compression` Cargo feature. Adds a `compression` config
field with values `none`, `gzip`, `zstd`, or `auto` (the default — detects
`.gz` / `.zst` from the file path / object key).

YAML example:

```yaml
kind: s3
config:
  # ... existing fields ...
  compression: auto  # or 'gzip' | 'zstd' | 'none'
```

The codec resolves per object key, so a single source can read a mix of compressed and uncompressed objects in one run.

## Lineage dataset URI

`s3://<bucket>` or `s3://<bucket>/<prefix>` — e.g. `s3://my-bucket/data/2026/`.

## License

Licensed under MIT or Apache-2.0.
