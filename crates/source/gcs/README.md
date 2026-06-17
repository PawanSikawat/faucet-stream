# faucet-source-gcs

[![Crates.io](https://img.shields.io/crates/v/faucet-source-gcs.svg)](https://crates.io/crates/faucet-source-gcs)
[![Docs.rs](https://docs.rs/faucet-source-gcs/badge.svg)](https://docs.rs/faucet-source-gcs)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-gcs.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-gcs.svg)](https://github.com/PawanSikawat/faucet-stream#license)

Google **Cloud Storage** source connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem. Lists objects in a bucket (with an optional prefix) or reads an explicit list of object keys, fetches each one over the official [`google-cloud-storage`](https://crates.io/crates/google-cloud-storage) SDK, and parses it as **JSON Lines**, **JSON Array**, or **raw text** — yielding records as `serde_json::Value`.

Reach for it when your data already lives in GCS — event exports, log dumps, analytics extracts, daily snapshots — and you want to move it into any faucet-stream sink (a database, a warehouse, a queue, a file) with one declarative config and no glue code. Objects are read concurrently and JSONL/raw-text bodies stream line-by-line, so a multi-gigabyte export lands without buffering the whole bucket in memory.

## Feature highlights

- **Three file formats** — `json_lines` (one record per line), `json_array` (one array per object), and `raw_text` (each object becomes a `{key, content}` record).
- **List or explicit keys** — scan a bucket by `prefix`, or skip listing entirely by passing an exact `object_keys` list.
- **Concurrent reads** — objects are fetched in parallel via `buffer_unordered(concurrency)` (default 10), so wall-clock time is bounded by your slowest objects, not their sum.
- **Bounded-memory streaming** — `json_lines` and `raw_text` decode straight off the GCS body reader, so peak memory is `O(batch_size)` regardless of total file size.
- **Compression auto-detect** — behind the `compression` feature, `.gz` / `.zst` objects are transparently decompressed; the codec resolves *per object key*, so one run can mix compressed and uncompressed objects.
- **Four credential modes** — Application Default Credentials, a service-account key file, inline service-account JSON, or anonymous (for emulators). The shared `GcsCredentials` enum is re-exported from [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs) so it matches the GCS **sink** byte-for-byte.
- **Clients built once** — the data-plane and control-plane clients are constructed in `new()` and reused for every list and read.
- **Matrix-aware prefixes** — `${parent.path}` placeholders in `prefix` are resolved per-record at runtime, so a parent matrix row can fan out into many per-record bucket scans.

## Installation

```bash
# As a library:
cargo add faucet-source-gcs

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-gcs

# With transparent gzip/zstd decompression:
cargo install faucet-cli --features "source-gcs,compression"
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: gcs
    config:
      bucket: my-bucket
      prefix: events/2026/
      auth:
        type: service_account_json_file
        config:
          path: /run/secrets/gcp-sa.json
      file_format: json_lines
  sink:
    type: jsonl
    config:
      path: ./events.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | — *(required)* | GCS bucket name. No `gs://` prefix, no path. |
| `prefix` | string | *(unset)* | Object-name prefix filter for listing. Ignored when `object_keys` is set. Supports `${field.path}` placeholders resolved against the parent-record context at runtime. |
| `object_keys` | array of string | *(unset)* | Explicit object names to read. When set, listing is skipped and `prefix` is ignored. |
| `auth` | `GcsCredentials` | `application_default` | Authentication — see [Authentication](#authentication). |
| `file_format` | enum | `json_lines` | `json_lines`, `json_array`, or `raw_text` — see [File formats](#file-formats). |
| `max_objects` | int | *(unset)* | Hard cap on the number of objects read (applied after listing, and to an explicit `object_keys` list). |

### Performance

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `concurrency` | int | `10` | Maximum concurrent object reads. Higher = faster on many small objects; lower caps peak memory for large `raw_text` / `json_array` objects. |
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. **`0` = no batching** (one page per object). See [Streaming & batching](#streaming--batching). |

### Format & testing

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression` | enum | `auto` | *(requires the `compression` feature)* Decompression codec — `none`, `gzip`, `zstd`, or `auto`. `auto` detects `.gz` / `.zst` from the object key. |
| `storage_host` | string | *(unset)* | Endpoint override (integration tests / emulators only, e.g. `http://localhost:4443`). Production users leave this unset. |

## Authentication

`auth` uses the shared `GcsCredentials` enum from [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs) (the project-wide `{ type, config }` shape):

| `type` | `config` | Use when |
|--------|----------|----------|
| `application_default` | *(none)* | Running on GCE/GKE (metadata server / workload identity) or after `gcloud auth application-default login`. Also honours `GOOGLE_APPLICATION_CREDENTIALS`. **Default.** |
| `service_account_json_file` | `{ path: <file> }` | You have a service-account key file on disk. |
| `service_account_json_inline` | `{ json: <string> }` | You want to inject the key JSON inline, typically via `${env:VAR}` / `${secret:…}` indirection. |
| `anonymous` | *(none)* | Talking to an emulator (e.g. `fake-gcs-server`) that does not validate bearer tokens. |

```yaml
# Application Default Credentials (workload identity, gcloud, GOOGLE_APPLICATION_CREDENTIALS)
auth:
  type: application_default
```

```yaml
# Service-account key file
auth:
  type: service_account_json_file
  config:
    path: /run/secrets/gcp-sa.json
```

```yaml
# Inline service-account JSON via env indirection
auth:
  type: service_account_json_inline
  config:
    json: ${env:GCP_SA_JSON}
```

HMAC-key auth, signed-URL generation, and KMS/CMEK encryption configuration are out of scope.

## File formats

| `file_format` | Behaviour | Streaming |
|---------------|-----------|-----------|
| `json_lines` *(default)* | One JSON record per line; blank lines are skipped. | Streams line-by-line — `O(batch_size)` memory. |
| `json_array` | The entire object is a single JSON array of records. | Buffered fully per object (the closing `]` is required to parse), then chunked. |
| `raw_text` | The whole object becomes one record `{"key": <name>, "content": <utf-8>}`. | Streamed into one `String` per object. |

Parse errors are precise: a JSONL failure carries the object key **and** the 1-based line number; a JSON-array failure carries the key. A `json_array` object whose top-level value isn't an array fails with an `"expected JSON array"` message. Non-UTF-8 bodies surface as `FaucetError::Source` with a `"not valid UTF-8"` hint.

## Examples

### Scan a prefix as JSON Lines (the 80% path)

```yaml
source:
  type: gcs
  config:
    bucket: analytics-exports
    prefix: events/dt=2026-06-16/
    auth: { type: application_default }
    file_format: json_lines
    concurrency: 20
    batch_size: 5000
```

### Read an explicit set of objects, no listing

```yaml
source:
  type: gcs
  config:
    bucket: my-bucket
    object_keys:
      - lookups/countries.json
      - lookups/currencies.json
    auth:
      type: service_account_json_file
      config: { path: /run/secrets/gcp-sa.json }
    file_format: json_array
    batch_size: 0      # emit one page per object — ideal for small lookup tables
```

### Raw text files, compressed, with a cap

```yaml
source:
  type: gcs
  config:
    bucket: log-archive
    prefix: app/2026/06/
    auth: { type: application_default }
    file_format: raw_text
    compression: auto    # requires the `compression` feature; decompresses .gz / .zst objects
    max_objects: 100     # read at most the first 100 objects
    concurrency: 4       # raw_text holds a whole object in memory — keep concurrency low
```

### Date-templated prefix driven by the run clock

```yaml
source:
  type: gcs
  config:
    bucket: analytics-exports
    prefix: events/dt=${now.date}/   # resolves to e.g. events/dt=2026-06-16/ at run time
    auth: { type: application_default }
    file_format: json_lines
```

## Streaming & batching

The source overrides `Source::stream_pages`. It lists object keys once, then walks them in order:

- **`json_lines`** decodes the (optionally decompressed) body line-by-line off an async buffered reader, emitting a `StreamPage` every `batch_size` records. Memory stays at `O(batch_size)` no matter how large the file is.
- **`raw_text`** emits one `{key, content}` record per object, streamed straight into a single `String` (no separate raw + decompressed copies for compressed objects).
- **`json_array`** buffers each object fully (a JSON array isn't parseable until its closing `]`), then chunks its records into pages.

For a non-zero `batch_size`, records from multiple objects can share a page (cross-object flattening) — the page boundary follows the record count, not the object boundary. This matches the `faucet-source-s3` source and is intentional.

**`batch_size: 0`** is the no-batching sentinel: every page contains exactly one complete object's records, with no within-object chunking and no cross-object accumulation. Useful for small lookup tables or when a downstream sink prefers one large request per object.

> **Memory ceiling — `raw_text` / `json_array`.** Both hold one whole decoded object in memory at a time (inherent: a raw-text record *is* the whole file, and a JSON array isn't valid until its closing `]`). Because objects are fetched concurrently, peak memory is bounded by roughly **`concurrency` × (largest object's decoded size)**, not by `batch_size`. For large `raw_text` / `json_array` objects, lower `concurrency` to cap peak memory, or re-emit the data as `json_lines` upstream so it streams at `O(batch_size)`.

This is a one-shot scan source — it has no incremental bookmark / resume support, so each run re-lists and re-reads the matching objects. For incremental loads, advance the `prefix` between runs (e.g. a dated `events/dt=${now.date}/` prefix) so each run reads only fresh objects.

## Compression

Behind the crate-local `compression` Cargo feature. Adds the `compression` config field with values `none`, `gzip`, `zstd`, or `auto` (the default). `auto` detects `.gz` / `.zst` from the object key; explicit codecs apply to every object.

```yaml
source:
  type: gcs
  config:
    bucket: log-archive
    prefix: app/
    auth: { type: application_default }
    compression: auto     # or 'gzip' | 'zstd' | 'none'
```

The codec resolves per object key, so a single source can read a mix of compressed and uncompressed objects in one run. A one-shot warning fires when an explicit codec disagrees with the object's filename suffix.

## Config loading & schema

Config loads from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema source gcs
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_gcs::{GcsCredentials, GcsFileFormat, GcsSource, GcsSourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = GcsSourceConfig::new("analytics-exports")
    .prefix("events/dt=2026-06-16/")
    .auth(GcsCredentials::ApplicationDefault)
    .file_format(GcsFileFormat::JsonLines)
    .concurrency(20)
    .with_batch_size(5000);

let records = GcsSource::new(cfg).await?.fetch_all().await?;
println!("records: {}", records.len());
# Ok(())
# }
```

## How it works

1. `new()` resolves `GcsCredentials` and builds both a data-plane `Storage` client and a control-plane `StorageControl` client **once**, reusing them across calls.
2. Listing uses the control-plane `list_objects` paginator (page size 1000), filtered by `prefix` and capped at `max_objects`. An explicit `object_keys` list skips listing entirely.
3. Each object's body is opened as an async buffered reader; with the `compression` feature it is wrapped in a per-key decompressor.
4. Objects are read concurrently via `buffer_unordered(concurrency)` and parsed per `file_format`.
5. `stream_pages` re-frames the decoded records into `batch_size` pages and yields them to the pipeline, keeping peak memory bounded for the streaming formats.

## Lineage dataset URI

`gs://<bucket>` or `gs://<bucket>/<prefix>` — e.g. `gs://my-bucket/events/2026/`.

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| `compression` | off | Adds the `compression` config field and transparent gzip/zstd decompression (pulls in `faucet-core/compression`). |

Enable the connector itself in the CLI/umbrella via the `source-gcs` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error / 401 / 403 | Credentials invalid or missing scope. Confirm the service account has **Storage Object Viewer** (`roles/storage.objectViewer`) on the bucket, or that ADC is initialized (`gcloud auth application-default login`). |
| `GCS list error for bucket '…'` | The bucket name is wrong, doesn't exist, or the principal lacks `storage.objects.list`. Pass the bare bucket name (no `gs://`, no path) and grant the Viewer role. |
| No objects read / empty output | The `prefix` matched nothing. Prefixes are literal (not globs) and case-sensitive; verify the exact object-name prefix, and remember `prefix` is ignored when `object_keys` is set. |
| `GCS JSON parse error in '…' at line N` | A line in a `json_lines` object isn't valid JSON. The message pins the object key and 1-based line; fix the source data or switch `file_format`. |
| `GCS expected JSON array in '…'` | `file_format: json_array` but the object's top-level value isn't an array. Use `json_lines` for newline-delimited data or `raw_text` for opaque blobs. |
| `not valid UTF-8` | A `raw_text` / `json_array` object has a non-UTF-8 body. These formats require UTF-8; binary objects aren't supported. |
| Compressed objects come through as garbled text | The `compression` feature isn't enabled, or `compression: none` is set. Build with `--features compression` and leave `compression: auto` (the default) so `.gz` / `.zst` keys are decompressed. |
| Out-of-memory on large `raw_text` / `json_array` objects | These formats hold a whole object in memory and peak at ~`concurrency × largest-object size`. Lower `concurrency`, cap with `max_objects`, or re-emit the data as `json_lines`. |
| Each run re-reads everything | This source has no resume bookmark. Advance the `prefix` between runs (e.g. a dated `events/dt=${now.date}/`) so each run reads only new objects. |
| `h2 protocol error / GoAway` against `fake-gcs-server` | The SDK uses gRPC for control-plane operations; `fake-gcs-server` only speaks REST. Use a real GCS bucket or a gRPC-capable emulator, with `auth: { type: anonymous }` and a `storage_host` override. |

## See also

- [Connector reference](https://pawansikawat.github.io/faucet-stream/reference/connectors.html) — the full source/sink capability matrix.
- [Authentication cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/auth.html) — shared auth providers and the `{type, config}` shape.
- [Compression cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/compression.html) — gzip/zstd across file connectors.
- [`faucet-sink-gcs`](https://crates.io/crates/faucet-sink-gcs) — the matching GCS sink.
- [`faucet-source-s3`](https://crates.io/crates/faucet-source-s3) — the AWS S3 equivalent with the same format semantics.
- [`faucet-common-gcs`](https://crates.io/crates/faucet-common-gcs) — the shared credentials enum and client builders.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
