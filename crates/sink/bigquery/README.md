# faucet-sink-bigquery

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-bigquery.svg)](https://crates.io/crates/faucet-sink-bigquery)
[![Docs.rs](https://docs.rs/faucet-sink-bigquery/badge.svg)](https://docs.rs/faucet-sink-bigquery)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-bigquery.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-bigquery.svg)](https://github.com/faucet-hq/faucet-stream#license)

Google **BigQuery** streaming-insert sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records to a BigQuery table via the [`tabledata.insertAll`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll) streaming API, automatically re-chunking each batch to stay within BigQuery's per-request limits.

Reach for it when you want to land events, CDC streams, or query results into a BigQuery table from any faucet-stream source with one declarative config — and, when you need it, full **upsert/delete** mirroring and **effectively-once** delivery on top of the same connector.

## Feature highlights

- **Streaming inserts** — high-throughput `tabledata.insertAll`; each batch is re-chunked to BigQuery's ~10 MB / ~500-row sweet spot.
- **Three credential modes** — Application Default Credentials, a service-account key file, or inline service-account JSON. The shared `BigQueryCredentials` enum is re-exported from [`faucet-common-bigquery`](https://crates.io/crates/faucet-common-bigquery) so it matches the BigQuery **source** byte-for-byte.
- **Write modes** — `append` (default), `upsert`, and `delete` via an in-place `MERGE` over the target table; no staging table required.
- **Effectively-once delivery** — pair with a CDC source for a multi-statement `MERGE`/`INSERT` transaction that commits records and a `_faucet_commit_token` watermark atomically.
- **Retry de-duplication** — set `insert_id_field` to a stable per-row key so BigQuery best-effort de-dupes the at-least-once streaming path.
- **Dead-letter queue** — surfaces per-row `insertErrors` so only the rows BigQuery actually rejected are routed to the DLQ; committed rows are never duplicated.
- **Client built once** — the authenticated BigQuery client is constructed (and credentials validated) in `new()` and reused for every write.

## Installation

```bash
# As a library:
cargo add faucet-sink-bigquery
cargo add tokio --features full

# Or via the umbrella crate:
cargo add faucet-stream --features sink-bigquery

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-bigquery
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: analytics
      table_id: events
      auth:
        type: service_account_key_path
        config:
          path: /etc/secrets/bigquery-sa.json
      batch_size: 500
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_id` | string | — *(required)* | GCP project ID that owns the dataset and is billed for the inserts. |
| `dataset_id` | string | — *(required)* | BigQuery dataset ID. |
| `table_id` | string | — *(required)* | BigQuery table ID. The table must already exist with a defined schema. |
| `auth` | `BigQueryCredentials` | — *(required)* | Authentication — see [Authentication](#authentication). |

### Reliability & batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Maximum rows per `insertAll` request. **`0` = no batching**: the whole upstream page is sent in one call (use when the source already chunks to BigQuery's preferred size). See [Streaming & batching](#streaming--batching). |
| `insert_id_field` | string | *(unset)* | Record field whose value is sent as the per-row streaming `insertId` for best-effort de-duplication on retry. See [Retry de-duplication](#retry-de-duplication). |

### Write mode

These fields are flattened, so they appear at the sink `config` top level.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_mode` | enum | `append` | `append` (streaming `insertAll`), `upsert`, or `delete` (in-place `MERGE` / keyed `DELETE`). |
| `key` | array | `[]` | Key column(s). **Required and non-empty** for `upsert`/`delete`; must be real columns of the target table. |
| `delete_marker` | object | *(none)* | `upsert` only — `{ field: <name>, values: [<str>, …] }`; rows whose `field` matches one of `values` become deletes instead of upserts. |

## Authentication

`auth` uses the shared `BigQueryCredentials` enum (the project-wide `{ type, config }` shape). Its `Debug` impl masks inline key JSON as `***` to prevent accidental credential leakage in logs.

| `type` | `config` | Use when |
|--------|----------|----------|
| `application_default` | *(none)* | Running on GCP (workload identity / metadata server) or after `gcloud auth application-default login`. |
| `service_account_key_path` | `{ path: <file> }` | You have a service-account key file on disk. |
| `service_account_key` | `{ json: <string> }` | You want to inject the key JSON inline, typically via `${env:VAR}` / `${secret:…}` indirection (e.g. Kubernetes secrets, CI/CD). |

```yaml
# Application Default Credentials (workload identity, gcloud)
auth:
  type: application_default
```

```yaml
# Service-account key file
auth:
  type: service_account_key_path
  config:
    path: /etc/secrets/bigquery-sa.json
```

```yaml
# Inline service-account JSON via env indirection
auth:
  type: service_account_key
  config:
    json: ${env:GCP_SERVICE_ACCOUNT_JSON}
```

## Examples

### Streaming inserts with retry de-duplication

```yaml
sink:
  type: bigquery
  config:
    project_id: production-project
    dataset_id: warehouse
    table_id: user_events
    auth:
      type: service_account_key_path
      config: { path: /etc/secrets/bigquery-writer.json }
    batch_size: 500
    insert_id_field: event_id   # stable per-row key → best-effort dedup
```

### Local development with Application Default Credentials

```yaml
sink:
  type: bigquery
  config:
    project_id: dev-project
    dataset_id: scratch
    table_id: test_table
    auth:
      type: application_default
    batch_size: 100
```

### Postgres CDC → BigQuery upsert mirror (effectively-once)

```yaml
version: 1
name: pg_cdc_to_bq_mirror
delivery: exactly_once

pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: faucet_bq_mirror
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30

  transforms:
    - type: cdc_unwrap

  sink:
    type: bigquery
    config:
      project_id: myproject
      dataset_id: warehouse
      table_id: users_mirror
      auth:
        type: application_default
      write_mode: upsert
      key: [id]
      delete_marker: { field: __op, values: [d] }

  state:
    type: file
    config:
      path: ./state
```

See the full runnable config at [`cli/examples/postgres_cdc_to_bigquery_upsert.yaml`](../../../cli/examples/postgres_cdc_to_bigquery_upsert.yaml).

## Streaming & batching

The BigQuery sink re-chunks each incoming `StreamPage` to keep individual `tabledata.insertAll` calls under BigQuery's limits. `write_batch` accepts whatever slice the pipeline hands it:

- **`batch_size > 0`** (default `1000`) — the sink slices the incoming slice into `batch_size`-row chunks and issues one `insertAll` HTTP call per chunk. **Recommended value is `500`**: the documented sweet spot for BigQuery streaming inserts — small enough to stay well under the ~10 MB request limit even for wide rows, large enough to amortise per-call overhead. Bump it higher when rows are narrow; drop it when rows are wide enough to push individual chunks past 10 MB.
- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream `StreamPage` is forwarded in a single `insertAll` call. Use this when the source already emits page sizes tuned for BigQuery (e.g. a Postgres source configured with `batch_size: 500`). Larger pages risk HTTP-413 from BigQuery's ~10 MB body limit.

`batch_size` is purely a chunk-size knob — BigQuery's per-row error reporting and the sink's "first row that failed" error message are unchanged. Per-call retry is delegated to `gcp_bigquery_client`'s built-in retry middleware.

### Retry de-duplication

BigQuery streaming inserts are **at-least-once** — a transport retry can insert the same row twice. Setting `insert_id_field` to a stable per-row key (e.g. a primary key or event id) makes the sink send that value as the row's `insertId`, which BigQuery uses for best-effort de-duplication over a short window. When `insert_id_field` is unset, or a given row lacks the field, that row is inserted without an `insertId` (no dedup for it).

## Write modes (`upsert` / `delete`)

In addition to the default `append` mode (streaming `insertAll`), the BigQuery sink supports `write_mode: upsert` and `write_mode: delete`. These use an in-place `MERGE … USING (SELECT … FROM UNNEST(JSON_QUERY_ARRAY(@payload)))` over the target table — **no staging table required**.

- **`upsert`** — insert-or-update by `key` via an in-place `MERGE`. If `delete_marker` is set (the standard CDC pattern), rows whose marker field matches are routed to a keyed `DELETE` instead, and the marker field is stripped from upserted rows.
- **`delete`** — delete by `key` for every record in the batch (direct keyed `DELETE`, no MERGE).

The `key` columns must be real columns of the target BigQuery table (validated against the table schema via `tables.get` at write time). The target table must already exist with a defined schema.

**Page-size limit.** The whole page is sent as one `jobs.query` request — BigQuery's ~10 MB body limit applies. The default `batch_size: 1000` suits most schemas; lower it for very wide rows.

**Effectively-once composition.** `delivery: exactly_once` composes with `write_mode: upsert`: the data `MERGE` and the watermark `MERGE` (into `_faucet_commit_token`) commit inside a single BigQuery transaction. See [Effectively-once delivery](#effectively-once-delivery) below.

## Effectively-once delivery

`BigQuerySink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes the page **and** records the `token` for `scope` in **one BigQuery multi-statement transaction**: a typed `INSERT INTO <target> SELECT … FROM UNNEST(JSON_QUERY_ARRAY(@payload))` (the page ships as a single bound JSON parameter; each column is cast per the target table's schema, fetched once via `tables.get`) followed by a `MERGE` into the `_faucet_commit_token` watermark table in the target dataset. Either both commit or neither does. Success is verified authoritatively via the job's `errorResult`, so a runtime failure (a bad `CAST`, a `NULL` into a `REQUIRED` column) aborts the page rather than advancing the bookmark over data that never landed.
- `last_committed_token(scope)` — reads the watermark row for `scope` so the pipeline can skip already-committed pages on resume.

On crash/resume the pipeline reads `last_committed_token` and skips any page whose token is already committed, so the target table never sees duplicates. This is **append-with-idempotency**, distinct from the default streaming `insertAll` path and from key-based upsert (`write_mode`).

**Requirements & limits.** The target table must already exist with a defined schema (the sink reads it to build the typed INSERT — a schemaless table is rejected with a clear error). Because each page commits as one atomic transaction (one token per page, no `batch_size` re-chunking on this path), the page must serialize within BigQuery's ~10 MB `jobs.query` request limit — keep the CDC source's per-page size modest. A scalar column whose JSON value is an object/array is coerced to `NULL` (or, for a `REQUIRED` column, fails the INSERT). This path uses BigQuery DML, which has concurrency limits; it is intended for the opt-in effectively-once mode, not high-rate streaming (use the default streaming `insertAll` path for that).

To use effectively-once delivery, set `delivery: exactly_once` in your pipeline config and pair this sink with one of the CDC sources (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
pipeline:
  source:
    type: mongodb-cdc
    config:
      connection_uri: "mongodb://localhost:27017/?replicaSet=rs0"
      scope:
        type: collection
        database: app
        collection: orders
      start_from:
        type: now
  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: analytics
      table_id: orders
      auth:
        type: application_default
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

See the [Effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery) for full rationale and the supported source/sink set.

## Schema evolution

`BigQuerySink` reports its live destination schema via `current_schema()` (a schema-only `tables.get`; a missing table → `None`, so every page column reads as new), so the pipeline-level `schema:` policy can detect drift between an incoming page's top-level shape and the real table. All five `on_drift` modes (`warn` / `ignore` / `quarantine` / `fail` / `evolve`) work against this sink.

Under `on_drift: evolve`, `BigQuerySink::evolve_schema()` applies `ALTER TABLE` DDL, each statement run as its own `jobs.query` job and verified to completion:

- **New columns** → `ADD COLUMN IF NOT EXISTS <col> <type>` (idempotent).
- **Lossless widenings** (e.g. integer → number) → `ALTER COLUMN <col> SET DATA TYPE <type>` — gated on `allow_type_widening`.
- **Nullability relaxations** → `ALTER COLUMN <col> DROP NOT NULL`.

The cached schema is invalidated afterwards so the next page (and the next effectively-once / upsert page) reads the evolved table. Incompatible changes (narrowing / type swaps) are never auto-applied — they are routed by `on_incompatible` (`fail` or `quarantine`). See the [schema-drift cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html).

## Dead-letter queue

This sink overrides `Sink::write_batch_partial` to surface per-row failures from BigQuery `tabledata.insertAll`'s `insertErrors` response. Configure a DLQ at the pipeline level (see [cli/README.md — `dlq:`](../../../cli/README.md)) and only the rows BigQuery actually rejected are routed there — already-committed rows stay in the main sink with no duplicates. (DLQ is not permitted in effectively-once mode.)

## Config loading & schema introspection

Load from YAML/JSON files or environment variables via the `faucet_core::config` helpers, and inspect the full JSON Schema with `faucet schema sink bigquery`.

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_bigquery::BigQuerySinkConfig;

// From a JSON file
let config: BigQuerySinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: BigQuerySinkConfig = load_env_file(".env", "BIGQUERY")?;
```

### Example JSON config

```json
{
  "project_id": "my-gcp-project",
  "dataset_id": "analytics",
  "table_id": "events",
  "auth": {
    "type": "service_account_key_path",
    "config": { "path": "/etc/secrets/bigquery-sa.json" }
  },
  "batch_size": 1000
}
```

### Example `.env` file

```env
BIGQUERY_PROJECT_ID=my-gcp-project
BIGQUERY_DATASET_ID=analytics
BIGQUERY_TABLE_ID=events
BIGQUERY_AUTH='{"type":"service_account_key_path","config":{"path":"/etc/secrets/bigquery-sa.json"}}'
BIGQUERY_BATCH_SIZE=1000
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_bigquery::{BigQuerySink, BigQuerySinkConfig, BigQueryCredentials};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = BigQuerySinkConfig::new(
    "my-gcp-project",
    "analytics",
    "events",
    BigQueryCredentials::ServiceAccountKeyPath { path: "/path/to/service-account.json".into() },
)
.with_batch_size(500)
.with_insert_id_field("event_id");

let sink = BigQuerySink::new(config).await?;

let records = vec![
    json!({"user_id": "u123", "event": "page_view", "timestamp": "2026-04-02T10:00:00Z"}),
    json!({"user_id": "u456", "event": "click",     "timestamp": "2026-04-02T10:01:00Z"}),
];

let rows_written = sink.write_batch(&records).await?;
println!("Wrote {rows_written} rows to BigQuery");
# Ok(())
# }
```

Drive it from a full pipeline with any source:

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_bigquery::{BigQuerySink, BigQuerySinkConfig, BigQueryCredentials};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let source = RestStream::new(RestStreamConfig::new("https://api.example.com", "/v1/events"));
let sink = BigQuerySink::new(BigQuerySinkConfig::new(
    "my-project", "analytics", "events", BigQueryCredentials::ApplicationDefault,
)).await?;

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
# Ok(())
# }
```

## How it works

1. `new()` resolves `BigQueryCredentials` and builds an authenticated client **once**, validating credentials eagerly so failures surface immediately.
2. `write_batch()` slices the input into `batch_size`-row chunks (or forwards the whole slice when `batch_size = 0`) and sends each chunk as a separate `insertAll` request.
3. Per-row errors in the BigQuery response are detected and reported. If any rows fail, the batch returns an error with details about the first failure; `write_batch_partial` exposes them per-row for DLQ routing.
4. `write_mode: upsert`/`delete` and the effectively-once path build pure-SQL `MERGE` / `INSERT` statements and execute them via `jobs.query`, verifying success against the job's `errorResult`.
5. The client is reused across all calls — no re-authentication per request.

## Lineage dataset URI

`bigquery://<project_id>.<dataset_id>.<table_id>` — e.g. `bigquery://my-project.warehouse.orders`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-bigquery` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error / 401 / 403 | Credentials invalid or missing scope. Confirm the service account has **BigQuery Data Editor** + **BigQuery Job User** on the project, or that ADC is initialized (`gcloud auth application-default login`). |
| `403 ... billing` | The `project_id` has no billing enabled, or the SA lacks `bigquery.jobs.create`. Use a project with billing and the Job User role. |
| HTTP 413 / request too large | A chunk exceeded BigQuery's ~10 MB body limit. Lower `batch_size` (or set a non-zero `batch_size` if you were using `0` with wide rows). |
| Duplicate rows after a retry | Streaming inserts are at-least-once. Set `insert_id_field` to a stable per-row key for best-effort de-dup, or use `delivery: exactly_once`. |
| `write_mode: upsert` / `key` rejected | `key` must be non-empty and name real columns of the target table; the table must already exist with a defined schema. |
| Effectively-once run rejected at validate | Effectively-once requires a CDC source (`postgres-cdc`/`mysql-cdc`/`mongodb-cdc`), this sink, a `state:` block, and **no** `dlq:`. `faucet validate` reports the missing requirement. |
| Idempotent INSERT fails on a `REQUIRED` column | A scalar column received a JSON object/array (coerced to `NULL`) or a missing value. Shape the records (e.g. `cdc_unwrap` + a transform) so every `REQUIRED` column is populated with a scalar. |
| Inline key JSON appears as `***` in logs | Intentional — `BigQueryCredentials`'s `Debug` masks inline JSON to prevent credential leakage. |

## See also

- [Connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [Effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery).
- [Upsert / write-modes cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html).
- [`faucet-source-bigquery`](https://crates.io/crates/faucet-source-bigquery) — the matching BigQuery query source.
- [`faucet-common-bigquery`](https://crates.io/crates/faucet-common-bigquery) — shared `BigQueryCredentials` + client builder.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
</content>
</invoke>

## Arrow columnar load job (opt-in, `arrow` feature)

With a binary built `--features arrow`, add a `bulk_load` block to switch the
sink onto the Arrow columnar fast path: incoming Arrow `RecordBatch`es are
written to Parquet on a GCS staging bucket, then loaded with a BigQuery
`PARQUET` load job (`jobs.insert`, polled to `DONE`) instead of per-row
`tabledata.insertAll`. This runs end-to-end without per-row JSON when the source
is also columnar (e.g. `parquet → bigquery`). Load jobs are append/truncate
only, so the columnar path is used **only** when `write_mode` is `append`
(upsert/delete stay on the streaming/MERGE path).

```yaml
sink:
  type: bigquery
  config:
    project_id: my-project
    dataset_id: analytics
    table_id: events
    auth: { type: application_default }
    bulk_load:
      staging_bucket: my-bq-staging          # GCS bucket for Parquet staging
      staging_prefix: faucet-bq-load/         # default
      gcs_auth: { type: application_default }  # creds for the staging upload
      write_disposition: WRITE_APPEND          # default (or WRITE_TRUNCATE / WRITE_EMPTY)
```

`bulk_load` is only present in `arrow` builds. The Storage **Write** API
(gRPC `AppendRows`) is a separate future enhancement.

## Overwrite (`write_mode: overwrite`)

Full-refresh: each run atomically **replaces** the whole table — and it is
**bucket-free** (no GCS staging bucket required, unlike the `bulk_load`
`WRITE_TRUNCATE` path). The page is loaded into a `LIKE` temp table via the
query API (not streaming `insertAll`), then swapped with a
`BEGIN TRANSACTION; TRUNCATE; INSERT … SELECT; COMMIT` that preserves the
target's partitioning and clustering — only after the run succeeds, so a mid-run
failure leaves the previous rows intact. No `key` is needed; the target table
must already exist.
