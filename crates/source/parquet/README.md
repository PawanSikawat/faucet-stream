# faucet-source-parquet

Apache Parquet file source connector for
[faucet-stream](https://github.com/PawanSikawat/faucet-stream).

Reads one or more Parquet files from a **local path**, a **local glob
pattern**, or **Amazon S3** (single object or prefix) and yields each row as a
`serde_json::Value` object. Built on the `parquet` + `arrow` crates for
vectorised, streaming reads — `RecordBatch`es are decoded and converted to JSON
incrementally, so the source never buffers a whole file in memory.

## At a glance

| Feature | Notes |
|---|---|
| Local file | `ParquetLocation::LocalPath { path }` |
| Local glob | `ParquetLocation::Glob { pattern }`, sorted for determinism |
| S3 single object | `ParquetS3Config::object(bucket, key)` |
| S3 prefix listing | `ParquetS3Config::prefix(bucket, prefix)` |
| Column projection | `columns: ["a", "b"]` — column pruned at the Parquet level |
| Concurrency | `concurrency: N` — `buffer_unordered` across files |
| Batch size | `batch_size: N` — Arrow `RecordBatch` rows per chunk |
| Schema validation | Cross-file Arrow schema mismatches fail fast |
| Object safety | `Box<dyn Source>` compatible — no generics |

## Configuration

```rust,no_run
use faucet_source_parquet::{ParquetSource, ParquetSourceConfig, ParquetS3Config};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// Local file
let source = ParquetSource::new(
    ParquetSourceConfig::local("/data/events.parquet"),
).await?;

// Local glob, only two columns, eight files in parallel
let source = ParquetSource::new(
    ParquetSourceConfig::glob("/data/year=2024/*.parquet")
        .columns(["id", "amount"])
        .concurrency(8)
        .batch_size(4096),
).await?;

// S3 prefix
let source = ParquetSource::new(
    ParquetSourceConfig::s3(
        ParquetS3Config::prefix("my-bucket", "events/2024/")
            .region("us-east-1"),
    ),
).await?;
# Ok(()) }
```

### YAML example for the `faucet` CLI

```yaml
version: 1
source:
  kind: parquet
  config:
    source:
      type: glob
      pattern: "/data/year=2024/*.parquet"
    batch_size: 4096
    columns: [id, amount]
    concurrency: 8
sink:
  kind: jsonl
  config:
    path: out.jsonl
```

For S3:

```yaml
source:
  kind: parquet
  config:
    source:
      type: s3
      bucket: my-bucket
      prefix: "events/2024/"
      region: us-east-1
```

## JSON representation

Row → `serde_json::Value::Object`. Field encoding is delegated to
`arrow_json::ArrayWriter`, so the conversion follows Arrow's standard rules:

| Arrow type | JSON shape |
|---|---|
| `Int32`, `Int64`, `Float32`, `Float64` | JSON number |
| `Utf8` / `LargeUtf8` | JSON string |
| `Boolean` | JSON `true` / `false` |
| `Date32` / `Date64` | ISO-8601 date string (`"2024-01-15"`) |
| `Time32` / `Time64` | ISO-8601 time string |
| `Timestamp(unit, tz)` | ISO-8601 timestamp string (`"2024-01-15T12:34:56Z"`) |
| `Decimal128` / `Decimal256` | JSON string preserving precision/scale |
| `Binary` / `LargeBinary` / `FixedSizeBinary` | base64-encoded string |
| `Struct(...)` | nested JSON object |
| `List(...)` / `LargeList(...)` / `FixedSizeList(...)` | JSON array |
| `Map(K, V)` | JSON object keyed by `K` |
| Null values | field is omitted (Arrow `ArrayWriter` default) |

## Configuration fields

| Field | Type | Default | Description |
|---|---|---|---|
| `source` | enum | required | One of `LocalPath`, `Glob`, `S3`. See below. |
| `batch_size` | `usize` | `1000` (`faucet_core::DEFAULT_BATCH_SIZE`) | Per-page row-count hint passed to `ParquetRecordBatchStreamBuilder::with_batch_size`. Arrow may emit smaller batches at row-group boundaries. `0` disables the override — the file's native row-group size drives page cadence. |
| `columns` | `Vec<String>?` | `None` | Top-level columns to read. Unknown names error out. |
| `concurrency` | `usize` | `4` | Files read in parallel for Glob / S3-prefix modes (used by the eager `fetch_with_context` path; the streaming `stream_pages` path iterates files sequentially). |

### `ParquetS3Config`

| Field | Type | Description |
|---|---|---|
| `bucket` | `String` | S3 bucket. |
| `key` | `Option<String>` | Single object key. Mutually exclusive with `prefix`. |
| `prefix` | `Option<String>` | Object key prefix to list. Mutually exclusive with `key`. |
| `region` | `Option<String>` | AWS region. Defaults to standard resolution chain. |
| `endpoint_url` | `Option<String>` | Custom endpoint for MinIO / LocalStack / etc. HTTP endpoints automatically allow plain HTTP. |

S3 credentials are loaded from the standard `object_store` AWS chain
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, IMDS, profile, etc.).

## Streaming and batching

`ParquetSource` implements
[`Source::stream_pages`](https://docs.rs/faucet-core) so the pipeline can
write each Arrow `RecordBatch` to the sink as it is decoded — client-side
memory is bounded by `batch_size * row_width` rather than the file size.

- `batch_size` is forwarded to
  `ParquetRecordBatchStreamBuilder::with_batch_size`. The Arrow reader
  treats it as a **hint**: a batch is *at most* `batch_size` rows, but a
  smaller batch may be emitted at a row-group boundary. Consequently an
  emitted `StreamPage` can hold fewer than `batch_size` rows.
- `batch_size = 0` is the "no batching" sentinel. The call to
  `with_batch_size` is skipped, so the Arrow reader emits one batch per
  Parquet row-group. Useful for sinks (SQL `COPY`, BigQuery load jobs)
  that prefer one large request per natural file boundary.
- Multi-file scans (Glob / S3 prefix) flatten through the streaming
  pipeline in sorted-path order. The first file's Arrow schema is the
  reference; any subsequent file with a different schema surfaces as
  `FaucetError::Source` naming both paths and the first diverging field
  — matching the eager `fetch_with_context` behaviour.
- Every page carries `bookmark: None` — the Parquet source has no
  incremental-replication mode.
- The trait-level `batch_size` argument that `Pipeline::run` passes is
  intentionally ignored; the config field is the user-facing knob.

## Performance notes

- Decoding is **streaming**: each row group is read as an Arrow `RecordBatch`
  and converted to JSON on the fly. Memory is bounded by `batch_size` rather
  than the file size.
- The S3 client is built once in `new()` and reused across every per-file read.
- Glob / S3-prefix mode reads up to `concurrency` files in parallel via
  `futures::buffer_unordered`.
- Column projection is applied via `ProjectionMask::columns` so unread columns
  are skipped at the Parquet I/O layer — not just at the JSON layer.

A 100k-row, all-primitive file reads in well under 500 ms on a recent laptop in
release mode. Throughput depends heavily on row width, projection, and storage
medium; benchmark with your own data.

## Failure modes

| Condition | Error |
|---|---|
| `concurrency == 0` | `FaucetError::Config` |
| S3 config with both `key` **and** `prefix`, or neither | `FaucetError::Config` |
| Empty bucket name | `FaucetError::Config` |
| Local file missing / unreadable | `FaucetError::Source` |
| Projected column not found in file | `FaucetError::Source` (names file + column) |
| Different Arrow schema across globbed / prefixed files | `FaucetError::Source` (names both paths + first diverging field) |
| Parquet decode error | `FaucetError::Source` |

## Status

Issue [#28](https://github.com/PawanSikawat/faucet-stream/issues/28).
