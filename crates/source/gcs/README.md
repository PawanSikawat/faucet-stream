# faucet-source-gcs

Google Cloud Storage source connector for the
[faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Lists objects in a bucket (with optional prefix) or accepts an explicit list
of object keys, then fetches and parses each object as one of three formats
— **JSON Lines**, **JSON Array**, or **raw text** — yielding records as
`serde_json::Value`. Mirrors the existing `faucet-source-s3` crate
structurally and shares its semantics.

Built on the official [`google-cloud-storage`](https://crates.io/crates/google-cloud-storage)
SDK (1.12).

## Config

```rust
pub struct GcsSourceConfig {
    pub bucket: String,
    pub prefix: Option<String>,
    pub object_keys: Option<Vec<String>>,
    pub credentials: GcsCredentials,
    pub file_format: GcsFileFormat,
    pub max_objects: Option<usize>,
    pub concurrency: usize,        // default 10
    pub batch_size: usize,         // default DEFAULT_BATCH_SIZE; 0 = one page per object
    pub storage_host: Option<String>,
}
```

| Field | Description |
|---|---|
| `bucket` | GCS bucket name (no `gs://` prefix, no path). |
| `prefix` | Object-name prefix filter for listing. Ignored when `object_keys` is set. |
| `object_keys` | Explicit list of object names. When set, listing is skipped. |
| `credentials` | See [`GcsCredentials`](#authentication) below. Defaults to `application_default`. |
| `file_format` | `json_lines` *(default)*, `json_array`, or `raw_text`. |
| `max_objects` | Hard cap on the number of objects scanned. |
| `concurrency` | Maximum concurrent object reads. |
| `batch_size` | Records per emitted `StreamPage`. See [Streaming](#streaming-and-batching). |
| `storage_host` | Endpoint override (integration tests only — production users leave unset). |

YAML example:

```yaml
source:
  type: gcs
  bucket: my-bucket
  prefix: events/2026/
  auth:
    type: service_account_json_file
    config:
      path: /run/secrets/gcp.json
  file_format: json_lines
  concurrency: 20
  batch_size: 5000
```

## Authentication

See [`faucet-common-gcs`](../../common/gcs/README.md) for the full
`GcsCredentials` reference. v1 supports:

- `application_default` (ADC — recommended on GCE/GKE).
- `service_account_json_file` (path to a key file).
- `service_account_json_inline` (key as an inline string, env-injectable
  via `${env:GCP_SA_JSON}` in CLI configs).

HMAC-key auth and signed-URL generation are out of scope for v1.

## File formats

| Format | Behaviour |
|---|---|
| `json_lines` | One JSON record per line. Blank lines skipped. Streams line-by-line. |
| `json_array` | The entire object is a JSON array of records. Buffered fully per object. |
| `raw_text` | The whole object becomes one record `{"key": <name>, "content": <utf-8>}`. |

JSONL parse errors carry the object key + 1-based line number. JSON-array
parse errors include the object key. Non-UTF-8 bodies surface as
`FaucetError::Source` with a `"not valid UTF-8"` hint.

## Streaming and batching

`Source::stream_pages` decodes `JsonLines` line-by-line so client-side
memory stays bounded at O(`batch_size`) regardless of file size.
`RawText` emits one record per object (`{"key": ..., "content": ...}`):
the whole file is inherently its record, but it is streamed straight into
one `String` via the same decoding reader `JsonLines` uses — no separate
raw + decompressed copies for compressed objects. `JsonArray` buffers
each object fully (the closing `]` is required to parse the structure)
and then chunks; very large arrays hold the full object in memory once.

`batch_size = 0` is the **no-batching sentinel**: every page contains one
complete object, with no within-object chunking and no cross-object
accumulation. Useful for small lookup tables.

For non-zero `batch_size`, lines from multiple objects can share a page
(cross-object flattening). The S3 source documents the same caveat — this
is intentional.

> **Memory ceiling — `RawText` / `JsonArray`.** Both hold one whole
> decoded object in memory at a time (inherent: `RawText`'s record *is*
> the whole file, and a JSON array isn't valid until its closing `]`).
> Objects are fetched concurrently, so peak memory is bounded by roughly
> **`concurrency` × (largest object's decoded size)**, not by
> `batch_size`. For large `RawText` / `JsonArray` objects, lower
> `concurrency` to cap peak memory, or re-emit the data as `JsonLines`
> upstream so it streams at `O(batch_size)`.

## Errors

| Failure | `FaucetError` variant | Message shape |
|---|---|---|
| Bad / missing credentials | `Auth` | `"GCS auth: ..."` |
| List API error | `Source` | `"GCS list error for bucket '{bucket}': {e}"` |
| Get object API error | `Source` | `"GCS get error for bucket '{bucket}' key '{key}': {e}"` |
| Body read error | `Source` | `"GCS read body error for key '{key}': {e}"` |
| Read / decode / non-UTF-8 body (`RawText` / `JsonArray`) | `Source` | `"GCS read/decode error for key '{key}' (not valid UTF-8?): {e}"` |
| JSON parse error | `Source` | `"GCS JSON parse error in '{key}' at line N: ..."` |

## Running the tests

```bash
cargo test -p faucet-source-gcs              # unit tests (no network)
cargo test -p faucet-source-gcs --test integration -- --ignored
```

Integration tests are marked `#[ignore]` because they require a real
GCS-compatible **gRPC** backend. The `google-cloud-storage` SDK uses
gRPC for control-plane operations (listing, metadata), and
`fake-gcs-server` only speaks the REST API — so `cargo test` against
the emulator fails with `h2 protocol error / GoAway`. Run the
`--ignored` suite against a real GCS bucket or a gRPC-capable
emulator when validating changes.

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

The codec resolves per object key, so a single source can read a mix of compressed and uncompressed objects in one run.

## Out of scope (v1)

- HMAC-key auth.
- Signed URL generation.
- Mid-scan resumable bookmarks (matches `faucet-source-s3` behaviour).
- KMS CMEK encryption configuration.
- Server-streaming gRPC reads.

## Lineage dataset URI

`gs://<bucket>` or `gs://<bucket>/<prefix>` — e.g. `gs://my-bucket/data/2026/`.

## License

Dual-licensed under MIT and Apache-2.0, per the workspace `license` field.
