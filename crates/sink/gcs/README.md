# faucet-sink-gcs

Google Cloud Storage sink connector for the
[faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Serializes batches of `serde_json::Value` records as JSON Lines files and
uploads them concurrently to a GCS bucket, with time-sortable UUIDv7 object
keys. Mirrors `faucet-sink-s3` structurally with two improvements:
UUIDv7 keys (vs UUIDv4) for natural sort order, and explicit
`application/x-ndjson` content type.

Built on the official [`google-cloud-storage`](https://crates.io/crates/google-cloud-storage)
SDK (1.12).

## Config

```rust
pub struct GcsSinkConfig {
    pub bucket: String,
    pub prefix: String,
    pub credentials: GcsCredentials,
    pub file_extension: String,          // default ".jsonl"
    pub max_records_per_file: Option<usize>,
    pub concurrency: usize,              // default 10
    pub batch_size: usize,               // default DEFAULT_BATCH_SIZE
    pub storage_host: Option<String>,
}
```

| Field | Description |
|---|---|
| `bucket` | GCS bucket name. |
| `prefix` | Object-name prefix; concatenated with the UUIDv7 key and file extension. |
| `credentials` | See [`GcsCredentials`](../../gcs-common/README.md). Defaults to `application_default`. |
| `file_extension` | Suffix appended to every object name (default `.jsonl`). |
| `max_records_per_file` | Hard cap on records per uploaded object. `None` means a single object per `write_batch` call (still subject to `batch_size`). |
| `concurrency` | Maximum concurrent uploads. |
| `batch_size` | Pipeline-level chunk size. See [Streaming](#streaming-and-batching). |
| `storage_host` | Endpoint override (integration tests only). |

YAML example:

```yaml
sink:
  type: gcs
  bucket: my-bucket
  prefix: events/2026/
  credentials:
    method: application_default
  file_extension: .jsonl
  max_records_per_file: 50000
  concurrency: 16
  batch_size: 0   # let the source's batch_size drive object sizing
```

## Streaming and batching

`write_batch` chunks the incoming records by `min(batch_size, max_records_per_file)`
(treating `None` as "no limit" and `batch_size = 0` as "no limit"). Each
chunk becomes a single GCS object with a UUIDv7 key:
`{prefix}{uuidv7}{file_extension}`.

**Recommended: `batch_size = 0`** for GCS. Writing many small objects is a
well-known anti-pattern — per-request overhead, slower downstream reads,
inflated LIST/GET costs. Let the source's `batch_size` drive object sizing,
or set an explicit `max_records_per_file` if you need a hard cap.

Uploads dispatch via `buffer_unordered(concurrency)` and `try_collect()`,
so the first upload error aborts the batch. **Partial-failure caveat:** a
batch that errors mid-flight may leave already-uploaded chunks in the
bucket. Same semantics as `faucet-sink-s3`. v1 does not use resumable
uploads.

## Errors

| Failure | `FaucetError` variant | Message shape |
|---|---|---|
| Bad / missing credentials | `Auth` | `"GCS auth: ..."` |
| Upload API error | `Sink` | `"GCS put object error for key '{key}': {e}"` |
| JSON serialization error | `Sink` | `"JSON serialization failed: {e}"` |

## Running the tests

```bash
cargo test -p faucet-sink-gcs                # unit tests (no network)
cargo test -p faucet-sink-gcs --test integration -- --ignored
```

Integration tests are marked `#[ignore]` because they require a real
GCS-compatible **gRPC** backend. The `google-cloud-storage` SDK's
control-plane calls (used during round-trip readback) need gRPC, and
`fake-gcs-server` only speaks REST — so the test fails with
`h2 protocol error / GoAway`. Run with `--ignored` against a real GCS
bucket or a gRPC-capable emulator when validating changes.

## Compression

Behind the crate-local `compression` Cargo feature. Adds a `compression` config
field with values `none`, `gzip`, `zstd`, or `auto` (the default — detects
`.gz` / `.zst` from the file path / object key).

YAML example:

```yaml
kind: gcs
config:
  # ... existing fields ...
  compression: auto  # or 'gzip' | 'zstd' | 'none'
```

Same as S3: codec resolves from `file_extension`, no `Content-Encoding` metadata set.

## Out of scope (v1)

- Resumable uploads.
- Signed URLs.
- Custom object metadata or user headers.
- KMS CMEK encryption configuration.

## License

Dual-licensed under MIT and Apache-2.0, per the workspace `license` field.
