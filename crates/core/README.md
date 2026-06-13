# faucet-core

[![Crates.io](https://img.shields.io/crates/v/faucet-core.svg)](https://crates.io/crates/faucet-core)
[![Docs.rs](https://docs.rs/faucet-core/badge.svg)](https://docs.rs/faucet-core)

Shared types, traits, and utilities for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

This is the foundation crate that all faucet source and sink connectors depend on. If you're building a custom connector, this is the only dependency you need.

## Installation

```bash
cargo add faucet-core
cargo add tokio --features rt
```

## What's Inside

### Traits

- **`Source`** — async trait for fetching records from external systems
- **`Sink`** — async trait for writing records to external systems

Both traits include a `config_schema()` method that returns a JSON Schema describing the connector's configuration.

### Decorators

- **`TransformingSource`** — wraps any `Source` with a fixed `Vec<TransformStage>` (covering 1→1 `Map(RecordTransform)`, 1→0|1 `Filter`, 1→0..N `Explode`, and arbitrary `Custom` closures) applied per page via `instrumented_apply_stages`. The canonical way library callers attach transforms — including filter and explode — to any source. See [`docs.rs`](https://docs.rs/faucet-core/latest/faucet_core/struct.TransformingSource.html).

### `Source::stream_pages` (recommended for large sources)

`stream_pages(ctx, batch_size)` returns a `Stream<Item = Result<StreamPage, FaucetError>>` where each `StreamPage` contains a chunk of records plus an optional bookmark. The default implementation wraps `fetch_with_context_incremental` and chunks the result in memory; sources that can stream natively (REST, CDC, query DBs with cursor pagination) override this method directly to bound source-side memory at O(batch_size). `Pipeline::run` drives this stream internally; library users do not normally call it themselves.

`DEFAULT_BATCH_SIZE` is `1000`, `MAX_BATCH_SIZE` is `1_000_000`, and `validate_batch_size(n)` enforces the range with `FaucetError::Config` errors for connector authors to use at config-load time. **`batch_size = 0` is the "no batching" sentinel** — sources emit the entire result set in a single `StreamPage` (and sinks that expose their own `batch_size` accept whatever upstream hands them without re-chunking). Use it for small lookup tables or for bulk-load-style sinks (SQL `COPY`, BigQuery load jobs) that prefer one large request to many small ones.

```rust
use faucet_core::{async_trait, FaucetError, Source, Sink, Value};

#[async_trait]
impl Source for MySource {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        // Fetch records
        todo!()
    }

    // Optional: incremental replication with bookmark
    // async fn fetch_all_incremental(&self) -> Result<(Vec<Value>, Option<Value>), FaucetError>

    // Optional: return JSON Schema of config
    // fn config_schema(&self) -> Value
}

#[async_trait]
impl Sink for MySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        // Write records, return count written
        todo!()
    }

    // Optional: flush buffered data
    // async fn flush(&self) -> Result<(), FaucetError>

    // Optional: return JSON Schema of config
    // fn config_schema(&self) -> Value
}
```

### Pipeline

Connect any source to any sink:

```rust
use faucet_core::{Pipeline, run_stream};

// Batch mode: fetch all, then write
let result = Pipeline::new(&source, &sink).run().await?;
println!("Wrote {} records", result.records_written);

// Streaming mode: write page-by-page (bounded memory)
let result = run_stream(source.stream_pages(), &sink).await?;
```

### Error Types

`FaucetError` covers all failure modes:

| Variant | Use Case |
|---------|----------|
| `Http(reqwest::Error)` | HTTP transport errors |
| `HttpStatus { status, url, body }` | Non-success HTTP responses |
| `Json(serde_json::Error)` | JSON parse/serialize errors |
| `JsonPath(String)` | JSONPath extraction failures |
| `Auth(String)` | Authentication errors |
| `RateLimited { retry_after }` | 429 rate limit responses |
| `Url(String)` | URL construction errors |
| `Transform(String)` | Record transform errors |
| `Config(String)` | Configuration/validation errors |
| `Source(String)` | Source-specific errors |
| `Sink(String)` | Sink-specific errors |
| `Custom(Box<dyn Error>)` | Wrap any third-party error |

### Config Loading

Load any `Deserialize`-able config struct from JSON files or environment variables:

```rust
use faucet_core::config::{load_json, load_env, load_env_file};

// From a JSON file
let config: MyConfig = load_json("config.json")?;

// From environment variables (reads MYAPP_URL, MYAPP_BATCH_SIZE, etc.)
let config: MyConfig = load_env("MYAPP")?;

// From a .env file + environment variables
let config: MyConfig = load_env_file(".env", "MYAPP")?;
```

#### Duration Serde Helpers

For `Duration` fields in configs, use the provided serde modules:

```rust
use std::time::Duration;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct MyConfig {
    #[serde(with = "faucet_core::config::duration_secs")]
    timeout: Duration,                    // serializes as u64 seconds

    #[serde(with = "faucet_core::config::duration_secs_option", default)]
    retry_delay: Option<Duration>,        // serializes as Option<u64>
}
```

### Record Transforms

Transform records as they flow through the pipeline:

```rust
use faucet_core::RecordTransform;

// Flatten nested objects: {"user": {"id": 1}} -> {"user__id": 1}
RecordTransform::Flatten { separator: "__".into() }

// Convert keys to snake_case (or camel / pascal / kebab / screaming_snake)
RecordTransform::KeysCase { mode: KeyCaseMode::Snake }

// Regex key renaming
RecordTransform::RenameKeys {
    pattern: r"^_sdc_".into(),
    replacement: "".into(),
}

// Custom closure
RecordTransform::custom(|mut record| {
    // modify record
    record
})
```

### Replication

Incremental replication support:

```rust
use faucet_core::ReplicationMethod;
use faucet_core::replication::{filter_incremental, max_replication_value};

// Filter records newer than a bookmark
let filtered = filter_incremental(&records, "updated_at", &bookmark_value);

// Compute new bookmark from records
let new_bookmark = max_replication_value(&records, "updated_at");
```

### Schema Inference

Infer JSON Schema from record samples:

```rust
use faucet_core::schema::infer_schema;

let schema = infer_schema(&records);
// Returns a JSON Schema with inferred types, nullable fields, nested objects
```

### JSON Schema Generation

All config structs derive `schemars::JsonSchema`. Use `schema_for!` to generate schemas:

```rust
use faucet_core::{schema_for, JsonSchema};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, JsonSchema)]
struct MyConfig {
    url: String,
    batch_size: usize,
}

let schema = schema_for!(MyConfig);
let json = serde_json::to_value(schema)?;
```

## Re-exports

`faucet-core` re-exports common dependencies so connector authors only need one dependency:

| Re-export | From |
|-----------|------|
| `async_trait` | `async-trait` |
| `serde_json`, `Value`, `json!` | `serde_json` |
| `schemars`, `JsonSchema`, `schema_for!` | `schemars` |

### Quality checks

Add a `quality:` block (sibling of `transforms:` and `dlq:` under `pipeline:`) to
assert invariants on every page of records before they reach the sink. The quality
pass runs **after** transforms and **before** `write_batch`; per-record checks
partition the page into survivors and quarantined rows, then per-batch checks run
over the survivors. Quarantined rows are routed to the DLQ sink.

Enable with the `quality` Cargo feature (base) or `quality-jsonschema` to add the
`json_schema` record check.

#### Check catalog

**Per-record checks** — each record is evaluated in declared order; first failure
wins. `on_failure` may be `quarantine` (route the row to the DLQ) or `abort` (fail
the run immediately with `FaucetError::QualityFailure`).

| Check | Key config fields | Passes when | Missing field |
|---|---|---|---|
| `not_null` | `field`, `treat_missing_as_null` (default `true`) | value present and non-null | fail (pass iff `treat_missing_as_null: false`) |
| `not_empty` | `field` | value is a non-empty string after `trim()` | fail |
| `regex_match` | `field`, `pattern` | value is a string matching `pattern` | fail |
| `value_in_set` | `field`, `values: [...]` | value is in `values` (exact JSON equality) | fail |
| `not_in_set` | `field`, `values: [...]` | value is NOT in `values` | pass (trivially not in set) |
| `compare` | `field`, `op` (`gt`/`gte`/`lt`/`lte`/`eq`/`ne`), `value` | ordering or equality holds | fail |
| `type_is` | `field`, `expected` (`boolean`/`number`/`string`/`array`/`object`/`null`) | JSON type matches | fail |
| `string_length` | `field`, `min?`, `max?` (at least one required) | char count in `[min, max]` | fail |
| `json_schema` *(quality-jsonschema feature)* | `schema` (JSON Schema doc) | record validates against `schema` | (whole-record check; always evaluated) |

`json_schema` is the most expressive check; its cost scales with schema
complexity — for very large or deeply nested schemas on hot paths, prefer the
granular checks above and benchmark your case.

**Per-batch checks** — evaluated per page over the survivors (records that passed
the per-record pass). `on_failure` for aggregate checks (`row_count`, `null_rate`,
`distinct_count`) may be `abort` or `quarantine_batch` (route all current survivors
to the DLQ, write nothing this page). `unique` is row-attributable and accepts
`quarantine` (route the duplicate rows) or `abort`.

| Check | Key config fields | Passes when |
|---|---|---|
| `row_count` | `min?`, `max?` (at least one required) | survivor count in `[min, max]` |
| `null_rate` | `field`, `max: f64` (0.0–1.0) | null-or-missing rate ≤ `max`; zero survivors → 0.0 → pass |
| `unique` | `fields: [...]` (composite key, ≥1) | every survivor's key is unique within the page |
| `distinct_count` | `field`, `min?`, `max?` | distinct values of `field` in `[min, max]` |

#### `on_failure` policies

| Policy | Meaning | Allowed on |
|---|---|---|
| `quarantine` | Route the specific offending row(s) to the DLQ; keep the rest | per-record checks; `unique` |
| `quarantine_batch` | Route all survivors of the page to the DLQ; write nothing this page | aggregate batch checks |
| `abort` | Surface `FaucetError::QualityFailure` and fail the run | every check |

**`quarantine` and `quarantine_batch` require a DLQ sink.** Configuring either
without a `dlq:` block is rejected at config-load time with `FaucetError::Config`.

#### Example

```yaml
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com/v1
      path: /users
      method: GET
      auth: { type: bearer, config: { token: "${env:API_TOKEN}" } }
      pagination: { type: Cursor, next_token_path: $.meta.next_cursor, param_name: cursor }
      max_retries: 3
      retry_backoff: 2
      tolerated_http_errors: []
      replication_method: { type: Incremental }
      replication_key: updated_at
      primary_keys: ["id"]
      partitions: []
      schema_sample_size: 100

  transforms:
    - type: keys_case
      config: { mode: snake }

  quality:
    record:
      - type: not_null
        field: id
        on_failure: abort
      - type: not_null
        field: email
        on_failure: quarantine
      - type: regex_match
        field: email
        pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
        on_failure: quarantine
      - type: value_in_set
        field: status
        values: ["active", "inactive", "pending", "suspended"]
        on_failure: quarantine
    batch:
      - type: row_count
        min: 1
        on_failure: abort
      - type: unique
        fields: [id]
        on_failure: quarantine

  dlq:
    sink:
      type: jsonl
      config: { path: ./dlq/quality_failures.jsonl }
    max_failures_per_page: 50
    max_failures_total: 500

  sink:
    type: postgres
    config:
      connection_url: "${env:PG_URL}"
      table_name: users
      column_mapping: { type: jsonb, column: data }
      batch_size: 500
```

#### Rust API

```rust
use faucet_core::{Pipeline, CompiledQuality, QualitySpec};

let quality_spec: QualitySpec = serde_json::from_value(/* ... */)?;
let compiled = CompiledQuality::compile(&quality_spec)?;
let result = Pipeline::new(&source, &sink)
    .with_dlq(dlq_config)   // required when any check uses quarantine
    .with_quality(compiled)
    .run()
    .await?;
```

## Modules

| Module | Contents |
|--------|----------|
| `traits` | `Source` and `Sink` async traits |
| `error` | `FaucetError` enum |
| `pipeline` | `Pipeline`, `PipelineResult`, `run_stream` |
| `config` | `load_json`, `load_env`, `load_env_file`, duration serde helpers |
| `transform` | `RecordTransform`, `CompiledTransform`, plus support enums (`CastType`, `CastOnError`, `ValueCaseMode`) |
| `replication` | `ReplicationMethod`, `filter_incremental`, `max_replication_value` |
| `schema` | `infer_schema` |
| `stage` | `TransformStage`, `FilterSpec`, `ExplodeSpec`, `OnMissing`. The pipeline-level stage type that wraps `RecordTransform` (1→1) and adds filter (1→0\|1) and explode (1→0..N). See `docs/book/src/cookbook/transforms.md` for the merge rule and JSONPath subset. |
| `quality` | Per-record and per-batch data-quality checks: `QualitySpec` config, `CompiledQuality`, `apply_quality`, `QualityOutcome`. Gated on the `quality` / `quality-jsonschema` features. |
| `util` | `quote_ident`, `extract_records`, `check_http_response` |

## License

Licensed under either of [MIT](../../LICENSE-MIT) or [Apache-2.0](../../LICENSE-APACHE) at your option.
