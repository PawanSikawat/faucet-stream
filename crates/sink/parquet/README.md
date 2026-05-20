# faucet-sink-parquet

Apache Parquet file sink connector for [faucet-stream](https://github.com/PawanSikawat/faucet-stream).

Writes JSON records as columnar Parquet files to a local filesystem path or
an S3 bucket. Schema is inferred from the first batch (or supplied
explicitly), every field is nullable so missing keys round-trip as `NULL`,
and large outputs can be rolled over to multiple files by row count or byte
budget.

## Quick start

```rust
use faucet_core::Sink;
use faucet_sink_parquet::{
    ParquetCompression, ParquetSink, ParquetSinkConfig,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = ParquetSinkConfig::local("/tmp/events")
        .compression(ParquetCompression::Snappy)
        .max_rows_per_file(100_000);

    let sink = ParquetSink::new(cfg).await?;
    sink.write_batch(&[
        json!({"id": 1, "name": "alice"}),
        json!({"id": 2, "name": "bob"}),
    ])
    .await?;
    // `flush()` writes the Parquet footer. Skipping it leaves no visible file.
    sink.flush().await?;
    Ok(())
}
```

## Destinations

```yaml
# Local filesystem — directory mode (UUID-suffixed files):
destination:
  type: local_path
  path: /var/lib/exports/events
```

```yaml
# Local filesystem — single-file mode (no rollover thresholds set):
destination:
  type: local_path
  path: /var/lib/exports/events.parquet
```

```yaml
# S3 (or any S3-compatible service via endpoint_url, e.g. MinIO/LocalStack):
destination:
  type: s3
  bucket: my-bucket
  prefix: events/
  region: us-east-1
  endpoint_url: http://localhost:4566   # optional
  allow_http: true                      # required for http:// endpoints
```

AWS credentials follow the standard `aws_config` chain (env vars, profile,
instance metadata).

## Schema handling

- **Inferred (default)** — the first batch is used to learn an Arrow schema
  via `arrow_json::reader::infer_json_schema_from_iterator`. Up to 100
  records by default (`SchemaSource::Inferred { sample_size }` overrides
  this). All inferred fields are forced nullable.
- **Explicit** — reserved (`SchemaSource::Explicit { }`); currently returns a
  `Config` error.
- **Missing fields** — recorded as Arrow `NULL` columns.
- **Unknown fields** — silently dropped, with a one-shot `tracing::warn!` per
  field name per batch.
- **Type drift** — if a later batch sends a value whose JSON type disagrees
  with the locked-in schema (e.g. int → string), `write_batch` returns
  `FaucetError::Sink` naming the field, the schema's declared type, and the
  drifting record's type.

## Rollover

| Threshold              | Trigger                                              |
|------------------------|------------------------------------------------------|
| `max_rows_per_file`    | row count of the current writer >= limit             |
| `max_bytes_per_file`   | `bytes_written + in_progress_size` >= limit          |

When either limit fires, the current Parquet writer is `close()`-d (writing
the footer) and the next `write_batch` opens a fresh `<uuid>.parquet`. Both
thresholds are checked after each batch, so the actual file size may
slightly exceed the limit by one batch worth of data.

Setting neither produces a single file per sink instance (until `flush()`).

## Streaming and batching

The sink accepts whatever the upstream pipeline hands it — the streaming
runtime in `faucet-core` already caps per-call memory at the upstream
source's `batch_size` (see the *Streaming and batching* section of the root
CLAUDE.md). On top of that, the sink exposes its own `batch_size` knob that
re-chunks every incoming page before it reaches the Arrow writer:

| Config              | Default                          | Meaning                                              |
|---------------------|----------------------------------|------------------------------------------------------|
| `batch_size`        | `faucet_core::DEFAULT_BATCH_SIZE` | Re-chunk pages into this many records per write.    |
| `batch_size = 0`    | n/a                              | "No batching" sentinel — pass pages through as-is.   |

For Parquet (local or S3) the source-defined page size is usually optimal
because the writer streams into the destination as one multipart upload and
benefits from larger row groups. **Recommended: leave `batch_size` at `0` (or
at its default) and let the upstream `batch_size` drive sizing.** Set a
smaller value only if you have a specific reason to slice incoming pages
finer than the upstream provides.

The row/byte rollover thresholds (`max_rows_per_file`, `max_bytes_per_file`)
are **independent of `batch_size`** and continue to work unchanged: when a
chunk pushes the writer past either threshold, the current file is closed
and the next chunk opens a fresh `<uuid>.parquet`.

## Compression

| Codec          | Notes                                          |
|----------------|------------------------------------------------|
| `uncompressed` | Largest output, fastest writes.                |
| `snappy`       | Default. Best balance of speed and size.       |
| `gzip`         | Smaller than snappy, ~3-5x slower.             |
| `zstd`         | Strong compression; default level.             |
| `lz4`          | Maps to `LZ4_RAW` (the Parquet-spec variant).  |

## Flush semantics

Parquet files are only valid once the trailing footer is written. The sink
streams data into the destination as an `object_store` multipart upload, and
the footer is emitted only when `flush()` is called (or when a row/byte
threshold triggers automatic rollover). **If you drop the sink without
calling `flush()`, no visible file is produced** — the upload is aborted by
`object_store` so you never end up with a corrupt half-written object on
disk or in S3.

Pipelines must therefore always call `flush()` at the end of their run.

## Round-tripping with `faucet-source-parquet`

The companion source crate (issue #28) reads files produced by this sink
back into JSON records using the same Arrow schema. Until both crates are
released, the round-trip is validated against the raw `parquet` and `arrow`
crates in this crate's own test suite.

## Performance

On a recent MacBook Air (M-series, local SSD) writing 1,000,000 records of
shape `{"id": i64, "name": utf8}` to a single Parquet file with Snappy
compression takes well under 5 seconds. CI does not gate on this number,
since runner hardware varies, but the goal informs every design decision:
the writer reuses a single `object_store` client, batches records into
`RecordBatch` via `arrow_json::Decoder`, and pages data through
`AsyncArrowWriter` with bounded buffering.

## LocalStack / MinIO testing (future work)

The sink works against any S3-compatible service by setting `endpoint_url`
and `allow_http: true`. We exercise the S3 code path in the test suite
against `object_store::memory::InMemory` to avoid a network dependency on
CI; running against a real LocalStack instance is straightforward and
documented here for callers who want to verify the wire-level behavior.

## License

MIT
