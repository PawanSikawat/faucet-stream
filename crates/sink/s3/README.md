# faucet-sink-s3

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-s3.svg)](https://crates.io/crates/faucet-sink-s3)
[![Docs.rs](https://docs.rs/faucet-sink-s3/badge.svg)](https://docs.rs/faucet-sink-s3)

AWS S3 sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to S3 as JSON Lines (NDJSON) files. Each file is keyed with a UUID for uniqueness. Supports file splitting by record count, configurable key prefixes and file extensions, concurrent uploads via `buffer_unordered`, and custom S3-compatible endpoints (MinIO, LocalStack, etc.).

## Installation

```bash
cargo add faucet-sink-s3
cargo add tokio --features full
```

Or via the umbrella crate:

```bash
cargo add faucet-stream --features sink-s3
```

## Quick Start

```rust
use faucet_sink_s3::{S3Sink, S3SinkConfig};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = S3SinkConfig::new("my-data-bucket")
        .prefix("events/2026/04/02/")
        .region("us-east-1");

    let sink = S3Sink::new(config).await?;

    let records = vec![
        json!({"id": 1, "event": "page_view", "user": "alice"}),
        json!({"id": 2, "event": "click", "user": "bob"}),
    ];

    let written = sink.write_batch(&records).await?;
    println!("Wrote {written} records to S3");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | `String` | *(required)* | S3 bucket name |
| `prefix` | `String` | `""` (empty) | Key prefix for written objects (e.g. `"data/events/"`) |
| `region` | `Option<String>` | `None` (SDK default) | AWS region |
| `endpoint_url` | `Option<String>` | `None` | Custom endpoint URL for S3-compatible services (MinIO, LocalStack, etc.) |
| `file_extension` | `String` | `".jsonl"` | File extension appended to each object key |
| `max_records_per_file` | `Option<usize>` | `None` (all in one file) | Maximum records per file. When set, records are split across multiple files. |
| `concurrency` | `usize` | `10` | Maximum number of concurrent file uploads |
| `batch_size` | `usize` | `1000` ([`DEFAULT_BATCH_SIZE`]) | Records per S3 object written by a single `write_batch` call. `0` opts out of write-side re-chunking — see *Streaming and batching* below. |

[`DEFAULT_BATCH_SIZE`]: https://docs.rs/faucet-core/latest/faucet_core/constant.DEFAULT_BATCH_SIZE.html

### Streaming and batching

`batch_size` controls write-side re-chunking inside a single `write_batch`
call. When the pipeline hands the sink `N` records and `batch_size = M > 0`,
the sink writes `ceil(N / M)` separate S3 objects (each containing at most
`M` records, with the final object holding the remainder). When
`batch_size = 0`, the sink writes whatever upstream hands it as a single
object — no re-chunking.

**Recommended value: `0`.** S3 is the canonical case where one large object
beats many small ones — per-request overhead, slower downstream scans, and
LIST/PUT cost all compound when a pipeline produces a flood of tiny objects.
The source's `batch_size` already sizes each `write_batch` call, and most
sources expose a `batch_size` field tuned to their native paging primitive
(REST page, sqlx cursor chunk, Kafka poll, etc.). Leave this at `0` unless
you explicitly want the sink to subdivide each upstream page further.

When both `batch_size > 0` and `max_records_per_file` are set, the effective
per-object cap is `min(batch_size, max_records_per_file)`. When both are `0`
/ unset, the sink writes one object per `write_batch` call.

> **Memory ceiling.** Each object's body is buffered fully in memory (and,
> with compression enabled, briefly held as both the raw and compressed
> body) before a single-shot `PutObject`. Up to `concurrency` objects
> upload at once, so peak memory is roughly **`concurrency` × (object
> size) × ~2**. A `batch_size = 0` / `max_records_per_file = None` fed by
> a `fetch_all`-style source produces one potentially huge object per
> `write_batch` — pair `batch_size = 0` with a streaming source that
> already sizes its pages, or set `max_records_per_file` / lower
> `concurrency` to cap peak memory. Streaming multipart upload for very
> large objects is a future enhancement.

### Builder Methods

```rust
use faucet_sink_s3::S3SinkConfig;

let config = S3SinkConfig::new("my-bucket")
    .prefix("output/events/")
    .region("eu-west-1")
    .endpoint_url("http://localhost:9000")
    .file_extension(".ndjson")
    .max_records_per_file(10000)
    .concurrency(20);
```

### Object Key Format

Each file is written with the key: `{prefix}{uuid}{file_extension}`

For example, with `prefix = "events/"` and `file_extension = ".jsonl"`:
```
events/a1b2c3d4-e5f6-7890-abcd-ef1234567890.jsonl
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_s3::S3SinkConfig;

// From a JSON file
let config: S3SinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: S3SinkConfig = load_env_file(".env", "S3_SINK")?;
```

### Example JSON config

```json
{
  "bucket": "my-data-lake",
  "prefix": "raw/events/2026/04/",
  "region": "us-east-1",
  "file_extension": ".jsonl",
  "max_records_per_file": 50000,
  "concurrency": 10
}
```

### Example JSON config (S3-compatible endpoint)

```json
{
  "bucket": "local-bucket",
  "prefix": "test/",
  "endpoint_url": "http://localhost:9000",
  "region": "us-east-1",
  "file_extension": ".jsonl",
  "concurrency": 5
}
```

### Example .env file

```env
S3_SINK_BUCKET=my-data-lake
S3_SINK_PREFIX=raw/events/
S3_SINK_REGION=us-east-1
S3_SINK_FILE_EXTENSION=.jsonl
S3_SINK_MAX_RECORDS_PER_FILE=50000
S3_SINK_CONCURRENCY=10
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = S3Sink::new(config).await?;
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_s3::{S3Sink, S3SinkConfig};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events")
);

let sink = S3Sink::new(
    S3SinkConfig::new("my-data-lake")
        .prefix("ingest/events/")
        .region("us-east-1")
        .max_records_per_file(100000)
).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records to S3", result.records_written);
```

## Examples

### Basic upload to S3

```rust
let config = S3SinkConfig::new("my-bucket")
    .prefix("data/")
    .region("us-west-2");

let sink = S3Sink::new(config).await?;
sink.write_batch(&records).await?;
// Writes: s3://my-bucket/data/<uuid>.jsonl
```

### Splitting large datasets across multiple files

```rust
let config = S3SinkConfig::new("data-lake")
    .prefix("events/daily/")
    .max_records_per_file(100000)
    .concurrency(20);

let sink = S3Sink::new(config).await?;

// 500,000 records will be split into 5 files of 100,000 each,
// uploaded concurrently (up to 20 at a time).
sink.write_batch(&large_record_set).await?;
```

### Using MinIO or LocalStack for local development

```rust
let config = S3SinkConfig::new("test-bucket")
    .endpoint_url("http://localhost:9000")
    .region("us-east-1")
    .prefix("dev/");

let sink = S3Sink::new(config).await?;
sink.write_batch(&records).await?;
```

## How It Works

- The S3 client is created eagerly in `S3Sink::new()` using the AWS SDK default credential chain. Custom regions and endpoints are applied if configured.
- `write_batch()` splits records into chunks based on `max_records_per_file` (or writes all records to a single file if unset).
- Each chunk is serialized to a JSON Lines string, assigned a UUID-based key, and uploaded to S3.
- Uploads are performed concurrently using `futures::stream::buffer_unordered` with the configured `concurrency` limit.
- Objects are uploaded with `Content-Type: application/x-ndjson`.
- AWS credentials are resolved via the standard AWS SDK credential chain (environment variables, shared credentials file, instance profiles, etc.).

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

The codec resolves from `file_extension`. Append `.gz` / `.zst` to `file_extension` so consumers can detect the codec from the object key. The S3 `Content-Encoding` header is deliberately unset — consumers must decompress explicitly.

## Lineage dataset URI

`s3://<bucket>/<prefix>` — e.g. `s3://my-bucket/data/events/`.

## License

Licensed under MIT or Apache-2.0.
